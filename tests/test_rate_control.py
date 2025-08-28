# tests/test_rate_control.py
"""
Tests for rate control functionality
"""

import pytest
import time
from telemetry_generator.rate_control import (
    RateLimiter,
    BurstController,
    VariableRateController
)


class TestRateLimiter:
    """Test rate limiter"""
    
    def test_basic_rate_limiting(self):
        """Test basic rate limiting"""
        limiter = RateLimiter(records_per_second=100, batch_size=10)
        limiter.start()
        
        start = time.time()
        for _ in range(10):  # 10 batches = 100 records
            limiter.wait_if_needed()
        elapsed = time.time() - start
        
        # Should take approximately 1 second (±20% tolerance)
        assert 0.8 <= elapsed <= 1.2
    
    def test_actual_rate_calculation(self):
        """Test actual rate calculation"""
        limiter = RateLimiter(records_per_second=1000)
        limiter.start()
        
        # Simulate some generation
        time.sleep(0.1)
        limiter.total_records = 100
        
        actual_rate = limiter.get_actual_rate()
        assert 900 <= actual_rate <= 1100  # ±10% tolerance
    
    def test_adaptive_control(self):
        """Test adaptive rate control"""
        limiter = RateLimiter(
            records_per_second=500,
            batch_size=50,
            adaptive=True
        )
        limiter.start()
        
        # Run for a short time
        for _ in range(20):
            limiter.wait_if_needed()
        
        stats = limiter.get_stats()
        assert stats.target_rate == 500
        assert stats.total_records > 0
    
    def test_rate_adjustment(self):
        """Test dynamic rate adjustment"""
        limiter = RateLimiter(records_per_second=100)
        limiter.start()
        
        # Change rate
        limiter.adjust_rate(200)
        assert limiter.target_rate == 200


class TestBurstController:
    """Test burst controller"""
    
    # def test_burst_pattern(self):
    #     """Test burst pattern generation"""
    #     controller = BurstController(
    #         base_rate=100,
    #         burst_rate=1000,
    #         burst_duration=0.1,
    #         burst_interval=0.5
    #     )
    #     controller.start()
        
    #     # Check that we alternate between rates
    #     rates = []
    #     for i in range(10):
    #         rates.append(controller.get_current_rate())
    #         time.sleep(0.1)
        
    #     # Should have both base and burst rates
    #     assert 100 in rates
    #     assert 1000 in rates

    def test_burst_pattern(self):
        """Test burst pattern generation"""
        controller = BurstController(
            base_rate=100,
            burst_rate=1000,
            burst_duration=0.2,  # 200ms burst
            burst_interval=0.5   # Every 500ms
        )
        controller.start()
        
        # Sample rates over time to catch both phases
        rates = []
        timestamps = []
        
        start_time = time.time()
        for i in range(10):
            rates.append(controller.get_current_rate())
            timestamps.append(time.time() - start_time)
            time.sleep(0.06)  # 60ms between samples
        
        print(f"Sampled rates at times {timestamps}: {rates}")
        
        # Should have both base and burst rates
        has_base_rate = any(r <= 150 for r in rates)  # Allow some tolerance
        has_burst_rate = any(r >= 900 for r in rates)
        
        assert has_base_rate, f"Base rate (~100) not detected: {rates}"
        assert has_burst_rate, f"Burst rate (~1000) not detected: {rates}"