"""
Rate Control Module
Manages generation rate to maintain specified records per second
"""

import time
import threading
from typing import Optional
from collections import deque
from dataclasses import dataclass
import logging

@dataclass
class RateStats:
    """Statistics for rate control"""
    target_rate: float
    actual_rate: float
    total_records: int
    total_time: float
    sleep_time_total: float
    overshoots: int
    undershoots: int

class RateLimiter:
    """
    Controls the rate of record generation to maintain target records per second
    """
    
    def __init__(
        self,
        records_per_second: float,
        batch_size: int = 1,
        adaptive: bool = True,
        smoothing_window: int = 10,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize RateLimiter
        
        Args:
            records_per_second: Target rate in records/sec
            batch_size: Number of records per batch
            adaptive: Whether to use adaptive rate control
            smoothing_window: Number of samples for rate smoothing
            logger: Optional logger
        """
        self.target_rate = records_per_second
        self.batch_size = batch_size
        self.adaptive = adaptive
        self.smoothing_window = smoothing_window
        self.logger = logger or logging.getLogger(__name__)
        
        # Calculate intervals
        self.target_batch_interval = batch_size / records_per_second
        self.min_sleep_time = 0.0001  # Minimum sleep time (100 microseconds)
        
        # State
        self.start_time = None
        self.last_batch_time = None
        self.total_records = 0
        self.total_sleep_time = 0
        self.overshoots = 0
        self.undershoots = 0
        
        # Adaptive control
        self.rate_history = deque(maxlen=smoothing_window)
        self.error_integral = 0  # For PI controller
        self.last_error = 0
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Control parameters (tuned for stability)
        self.kp = 0.5  # Proportional gain
        self.ki = 0.1  # Integral gain
        self.max_adjustment = 2.0  # Maximum rate adjustment factor
        
        self.logger.debug(
            f"RateLimiter initialized: target={records_per_second:.1f} rec/s, "
            f"batch={batch_size}, interval={self.target_batch_interval:.6f}s"
        )

    def start(self):
        """Start rate limiting (call before first record)"""
        with self.lock:
            self.start_time = time.perf_counter()
            self.last_batch_time = self.start_time
            self.total_records = 0
            self.rate_history.clear()

    def wait_if_needed(self, records_in_batch: Optional[int] = None):
        """
        Wait if necessary to maintain target rate
        
        Args:
            records_in_batch: Number of records in this batch (default: batch_size)
        """
        if records_in_batch is None:
            records_in_batch = self.batch_size
        
        with self.lock:
            current_time = time.perf_counter()
            
            # Initialize on first call
            if self.start_time is None:
                self.start()
                return
            
            # Calculate how long this batch should take
            batch_duration = records_in_batch / self.target_rate
            
            # Calculate actual time since last batch
            time_since_last = current_time - self.last_batch_time
            
            # Calculate required sleep time
            sleep_time = batch_duration - time_since_last
            
            if self.adaptive:
                # Adaptive adjustment based on actual rate
                sleep_time = self._adaptive_adjustment(
                    sleep_time, 
                    records_in_batch, 
                    current_time
                )
            
            # Sleep if needed
            if sleep_time > self.min_sleep_time:
                self.total_sleep_time += sleep_time
                time.sleep(sleep_time)
                actual_sleep = time.perf_counter() - current_time
                
                # Track overshoots (slept longer than intended)
                if actual_sleep > sleep_time * 1.1:
                    self.overshoots += 1
            elif sleep_time < -batch_duration:
                # We're way behind schedule
                self.undershoots += 1
            
            # Update state
            self.last_batch_time = time.perf_counter()
            self.total_records += records_in_batch
            
            # Update rate history
            elapsed = self.last_batch_time - self.start_time
            if elapsed > 0:
                actual_rate = self.total_records / elapsed
                self.rate_history.append(actual_rate)

    def _adaptive_adjustment(
        self, 
        base_sleep_time: float, 
        records_in_batch: int, 
        current_time: float
    ) -> float:
        """
        Adaptively adjust sleep time based on actual vs target rate
        
        Uses a PI (Proportional-Integral) controller for smooth rate control
        """
        elapsed = current_time - self.start_time
        
        if elapsed <= 0 or self.total_records == 0:
            return base_sleep_time
        
        # Calculate actual rate
        actual_rate = self.total_records / elapsed
        
        # Calculate error (positive means we're too fast)
        error = actual_rate - self.target_rate
        relative_error = error / self.target_rate
        
        # PI controller
        self.error_integral += relative_error * self.target_batch_interval
        self.error_integral = max(-1.0, min(1.0, self.error_integral))  # Clamp integral
        
        # Calculate adjustment
        adjustment = (
            self.kp * relative_error +  # Proportional term
            self.ki * self.error_integral  # Integral term
        )
        
        # Limit adjustment magnitude
        adjustment = max(-self.max_adjustment, min(self.max_adjustment, adjustment))
        
        # Apply adjustment
        adjusted_sleep = base_sleep_time * (1 + adjustment)
        
        # Ensure non-negative sleep time
        adjusted_sleep = max(0, adjusted_sleep)
        
        # Log significant adjustments
        if abs(adjustment) > 0.1:
            self.logger.debug(
                f"Rate adjustment: error={relative_error:.3f}, "
                f"adjustment={adjustment:.3f}, "
                f"sleep: {base_sleep_time:.6f} -> {adjusted_sleep:.6f}"
            )
        
        self.last_error = relative_error
        return adjusted_sleep

    def get_actual_rate(self) -> float:
        """Get the actual achieved rate"""
        with self.lock:
            if self.start_time is None or self.total_records == 0:
                return 0.0
            
            elapsed = time.perf_counter() - self.start_time
            if elapsed <= 0:
                return 0.0
            
            return self.total_records / elapsed

    def get_smoothed_rate(self) -> float:
        """Get smoothed rate over recent history"""
        with self.lock:
            if not self.rate_history:
                return self.get_actual_rate()
            
            return sum(self.rate_history) / len(self.rate_history)

    def get_stats(self) -> RateStats:
        """Get detailed statistics"""
        with self.lock:
            elapsed = time.perf_counter() - self.start_time if self.start_time else 0
            actual_rate = self.total_records / elapsed if elapsed > 0 else 0
            
            return RateStats(
                target_rate=self.target_rate,
                actual_rate=actual_rate,
                total_records=self.total_records,
                total_time=elapsed,
                sleep_time_total=self.total_sleep_time,
                overshoots=self.overshoots,
                undershoots=self.undershoots
            )

    def adjust_rate(self, new_rate: float):
        """Dynamically adjust target rate"""
        with self.lock:
            self.target_rate = new_rate
            self.target_batch_interval = self.batch_size / new_rate
            self.error_integral = 0  # Reset integral term
            self.logger.info(f"Rate adjusted to {new_rate:.1f} records/sec")


class BurstController:
    """
    Controls burst generation patterns for more realistic load patterns
    """
    
    def __init__(
        self,
        base_rate: float,
        burst_rate: float,
        burst_duration: float,
        burst_interval: float,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize BurstController
        
        Args:
            base_rate: Base records per second
            burst_rate: Peak rate during burst
            burst_duration: Duration of each burst in seconds
            burst_interval: Time between burst starts in seconds
            logger: Optional logger
        """
        self.base_rate = base_rate
        self.burst_rate = burst_rate
        self.burst_duration = burst_duration
        self.burst_interval = burst_interval
        self.logger = logger or logging.getLogger(__name__)
        
        self.start_time = None
        self.in_burst = False
        self.last_burst_start = None
        
        # Create two rate limiters
        self.base_limiter = RateLimiter(base_rate)
        self.burst_limiter = RateLimiter(burst_rate)
        
        self.logger.info(
            f"BurstController: base={base_rate:.1f}, burst={burst_rate:.1f}, "
            f"duration={burst_duration:.1f}s, interval={burst_interval:.1f}s"
        )

    def start(self):
        """Start burst controller"""
        self.start_time = time.perf_counter()
        self.base_limiter.start()
        self.burst_limiter.start()

    def wait_if_needed(self, batch_size: int = 1):
        """Wait according to current burst state"""
        if self.start_time is None:
            self.start()
        
        current_time = time.perf_counter()
        elapsed = current_time - self.start_time
        
        # Determine if we should be in burst mode
        cycle_position = elapsed % self.burst_interval
        should_burst = cycle_position < self.burst_duration
        
        # Transition logging
        if should_burst != self.in_burst:
            if should_burst:
                self.logger.debug(f"Entering burst mode at t={elapsed:.1f}s")
            else:
                self.logger.debug(f"Exiting burst mode at t={elapsed:.1f}s")
            self.in_burst = should_burst
        
        # Use appropriate rate limiter
        if should_burst:
            self.burst_limiter.wait_if_needed(batch_size)
        else:
            self.base_limiter.wait_if_needed(batch_size)

    def get_current_rate(self) -> float:
        """Get current target rate based on burst state"""
        return self.burst_rate if self.in_burst else self.base_rate


class VariableRateController:
    """
    Implements variable rate patterns (sine wave, random walk, etc.)
    """
    
    def __init__(
        self,
        pattern: str = 'sine',
        min_rate: float = 100,
        max_rate: float = 1000,
        period: float = 60,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize VariableRateController
        
        Args:
            pattern: Rate pattern ('sine', 'square', 'sawtooth', 'random')
            min_rate: Minimum rate
            max_rate: Maximum rate
            period: Period of variation in seconds
            logger: Optional logger
        """
        self.pattern = pattern
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.period = period
        self.logger = logger or logging.getLogger(__name__)
        
        self.start_time = None
        self.rate_limiter = RateLimiter(min_rate, adaptive=True)
        
        # Random walk state
        self.current_random_rate = (min_rate + max_rate) / 2
        self.random_velocity = 0
        
        import math
        self.math = math

    def start(self):
        """Start controller"""
        self.start_time = time.perf_counter()
        self.rate_limiter.start()

    def wait_if_needed(self, batch_size: int = 1):
        """Wait according to current variable rate"""
        if self.start_time is None:
            self.start()
        
        # Calculate current rate based on pattern
        current_rate = self._calculate_current_rate()
        
        # Adjust rate limiter
        self.rate_limiter.adjust_rate(current_rate)
        self.rate_limiter.wait_if_needed(batch_size)

    def _calculate_current_rate(self) -> float:
        """Calculate current rate based on pattern and time"""
        elapsed = time.perf_counter() - self.start_time
        t = elapsed / self.period  # Normalized time
        
        if self.pattern == 'sine':
            # Sine wave pattern
            normalized = (self.math.sin(2 * self.math.pi * t) + 1) / 2
            
        elif self.pattern == 'square':
            # Square wave pattern
            normalized = 1.0 if (t % 1.0) < 0.5 else 0.0
            
        elif self.pattern == 'sawtooth':
            # Sawtooth pattern
            normalized = t % 1.0
            
        elif self.pattern == 'random':
            # Random walk pattern
            import random
            self.random_velocity += random.uniform(-0.1, 0.1)
            self.random_velocity *= 0.9  # Damping
            self.current_random_rate += self.random_velocity
            self.current_random_rate = max(
                self.min_rate, 
                min(self.max_rate, self.current_random_rate)
            )
            return self.current_random_rate
            
        else:
            # Default to constant midpoint
            normalized = 0.5
        
        # Map normalized value to rate range
        rate = self.min_rate + normalized * (self.max_rate - self.min_rate)
        
        return rate

    def get_current_rate(self) -> float:
        """Get current target rate"""
        return self._calculate_current_rate()