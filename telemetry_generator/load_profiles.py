"""
Load Profiles
Predefined load profiles for common testing scenarios
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass
class LoadProfile:
    """Configuration for a load profile"""
    name: str
    rate: int  # Records per second
    batch_size: int  # Records per batch
    duration: Optional[int] = None  # Duration in seconds
    workers: Optional[int] = None  # Number of worker threads
    use_gpu: bool = False  # Enable GPU acceleration
    description: str = ""  # Profile description
    
    # Advanced settings
    burst_config: Optional[Dict[str, Any]] = None
    rate_pattern: Optional[str] = None  # 'constant', 'sine', 'burst', etc.
    memory_limit_mb: Optional[int] = None
    cpu_cores: Optional[int] = None


# Predefined load profiles
LOAD_PROFILES = {
    'low': LoadProfile(
        name='low',
        rate=100,
        batch_size=10,
        duration=60,
        workers=1,
        use_gpu=False,
        description='Low load for development and testing'
    ),
    
    'medium': LoadProfile(
        name='medium',
        rate=1000,
        batch_size=100,
        duration=300,
        workers=2,
        use_gpu=False,
        description='Medium load for integration testing'
    ),
    
    'high': LoadProfile(
        name='high',
        rate=10000,
        batch_size=1000,
        duration=600,
        workers=4,
        use_gpu=True,
        description='High load for performance testing',
        memory_limit_mb=4096
    ),
    
    'stress': LoadProfile(
        name='stress',
        rate=50000,
        batch_size=5000,
        duration=1800,
        workers=8,
        use_gpu=True,
        description='Stress test - maximum load',
        memory_limit_mb=8192,
        cpu_cores=8
    ),
    
    'burst': LoadProfile(
        name='burst',
        rate=1000,
        batch_size=100,
        duration=600,
        workers=4,
        use_gpu=False,
        description='Burst pattern - alternating high/low load',
        burst_config={
            'base_rate': 100,
            'burst_rate': 5000,
            'burst_duration': 10,
            'burst_interval': 60
        },
        rate_pattern='burst'
    ),
    
    'realistic': LoadProfile(
        name='realistic',
        rate=2000,
        batch_size=200,
        duration=3600,
        workers=3,
        use_gpu=False,
        description='Realistic load with variable patterns',
        rate_pattern='sine',
        burst_config={
            'min_rate': 500,
            'max_rate': 3500,
            'period': 300
        }
    ),
    
    'endurance': LoadProfile(
        name='endurance',
        rate=500,
        batch_size=50,
        duration=86400,  # 24 hours
        workers=2,
        use_gpu=False,
        description='Long-running endurance test',
        memory_limit_mb=2048
    ),
    
    'spike': LoadProfile(
        name='spike',
        rate=100,
        batch_size=10,
        duration=900,
        workers=4,
        use_gpu=True,
        description='Spike test - sudden load increases',
        burst_config={
            'base_rate': 100,
            'burst_rate': 20000,
            'burst_duration': 30,
            'burst_interval': 300
        },
        rate_pattern='burst'
    ),
    
    'ramp': LoadProfile(
        name='ramp',
        rate=5000,
        batch_size=500,
        duration=1200,
        workers=4,
        use_gpu=False,
        description='Ramp-up test - gradually increasing load',
        rate_pattern='sawtooth',
        burst_config={
            'min_rate': 100,
            'max_rate': 10000,
            'period': 600
        }
    ),
    
    'chaos': LoadProfile(
        name='chaos',
        rate=3000,
        batch_size=300,
        duration=1800,
        workers=6,
        use_gpu=True,
        description='Chaos test - random load variations',
        rate_pattern='random',
        burst_config={
            'min_rate': 100,
            'max_rate': 15000,
            'period': 60
        }
    )
}


class ProfileManager:
    """Manages and applies load profiles"""
    
    @staticmethod
    def get_profile(name: str) -> LoadProfile:
        """Get a profile by name"""
        if name not in LOAD_PROFILES:
            raise ValueError(f"Unknown profile: {name}. Available: {list(LOAD_PROFILES.keys())}")
        return LOAD_PROFILES[name]
    
    @staticmethod
    def list_profiles() -> Dict[str, str]:
        """List all available profiles with descriptions"""
        return {name: profile.description for name, profile in LOAD_PROFILES.items()}
    
    @staticmethod
    def create_custom_profile(**kwargs) -> LoadProfile:
        """Create a custom profile with specified parameters"""
        defaults = {
            'name': 'custom',
            'rate': 1000,
            'batch_size': 100,
            'duration': None,
            'workers': None,
            'use_gpu': False,
            'description': 'Custom profile',
            'burst_config': None,
            'rate_pattern': None,
            'memory_limit_mb': None,
            'cpu_cores': None
        }
        
        # Merge with provided kwargs
        config = {**defaults, **kwargs}
        
        return LoadProfile(**config)
    
    @staticmethod
    def estimate_resources(profile: LoadProfile) -> Dict[str, Any]:
        """Estimate resource requirements for a profile"""
        
        # Estimate memory usage
        avg_record_size = 100  # bytes (rough estimate)
        buffer_size = profile.batch_size * avg_record_size
        overhead_factor = 2.5  # Account for overhead
        
        estimated_memory_mb = (buffer_size * profile.workers * overhead_factor) / (1024 * 1024)
        
        if profile.use_gpu:
            estimated_memory_mb *= 1.5  # GPU memory overhead
        
        # Estimate disk usage
        bytes_per_second = profile.rate * avg_record_size
        total_bytes = bytes_per_second * (profile.duration or 60)
        estimated_disk_mb = total_bytes / (1024 * 1024)
        
        # Estimate CPU usage
        cpu_usage_percent = min(100, (profile.rate / 10000) * 100)  # Rough estimate
        if profile.workers:
            cpu_usage_percent = min(100, cpu_usage_percent * (profile.workers / 4))
        
        return {
            'estimated_memory_mb': int(estimated_memory_mb),
            'estimated_disk_mb': int(estimated_disk_mb),
            'estimated_cpu_percent': int(cpu_usage_percent),
            'recommended_cores': profile.cpu_cores or max(1, profile.workers or 1),
            'requires_gpu': profile.use_gpu,
            'network_bandwidth_mbps': (bytes_per_second * 8) / (1024 * 1024) if bytes_per_second > 1024 * 1024 else None
        }
    
    @staticmethod
    def validate_profile(profile: LoadProfile) -> bool:
        """Validate profile parameters"""
        if profile.rate <= 0:
            raise ValueError("Rate must be positive")
        
        if profile.batch_size <= 0:
            raise ValueError("Batch size must be positive")
        
        if profile.batch_size > profile.rate:
            raise ValueError("Batch size cannot exceed rate")
        
        if profile.duration and profile.duration <= 0:
            raise ValueError("Duration must be positive")
        
        if profile.workers and profile.workers <= 0:
            raise ValueError("Workers must be positive")
        
        if profile.burst_config:
            burst = profile.burst_config
            if 'base_rate' in burst and 'burst_rate' in burst:
                if burst['base_rate'] >= burst['burst_rate']:
                    raise ValueError("Burst rate must exceed base rate")
        
        return True


# Scenario-based profile recommendations
SCENARIO_RECOMMENDATIONS = {
    'development': ['low'],
    'ci_cd': ['low', 'medium'],
    'integration_testing': ['medium', 'realistic'],
    'performance_testing': ['high', 'burst', 'ramp'],
    'stress_testing': ['stress', 'spike', 'chaos'],
    'capacity_planning': ['high', 'stress', 'endurance'],
    'sla_validation': ['realistic', 'burst'],
    'disaster_recovery': ['spike', 'chaos'],
    'long_term_stability': ['endurance', 'realistic']
}


def recommend_profiles(scenario: str) -> list:
    """Recommend profiles for a given scenario"""
    if scenario not in SCENARIO_RECOMMENDATIONS:
        return ['medium']  # Default recommendation
    
    return SCENARIO_RECOMMENDATIONS[scenario]


def combine_profiles(*profiles: LoadProfile) -> LoadProfile:
    """Combine multiple profiles into a composite profile"""
    if not profiles:
        raise ValueError("At least one profile required")
    
    # Use the first profile as base
    base = profiles[0]
    
    # Average the rates and batch sizes
    avg_rate = sum(p.rate for p in profiles) // len(profiles)
    avg_batch = sum(p.batch_size for p in profiles) // len(profiles)
    
    # Take maximum for workers and duration
    max_workers = max(p.workers or 1 for p in profiles)
    max_duration = max(p.duration or 60 for p in profiles)
    
    # OR the GPU flag
    use_gpu = any(p.use_gpu for p in profiles)
    
    return LoadProfile(
        name='combined',
        rate=avg_rate,
        batch_size=avg_batch,
        duration=max_duration,
        workers=max_workers,
        use_gpu=use_gpu,
        description=f"Combined profile from {len(profiles)} profiles"
    )