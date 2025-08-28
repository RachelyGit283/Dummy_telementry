# tests/test_load_profiles.py
"""
Tests for load profiles
"""

import pytest
from telemetry_generator.load_profiles import (
    LOAD_PROFILES,
    ProfileManager,
    LoadProfile,
    recommend_profiles,
    combine_profiles
)


class TestLoadProfiles:
    """Test load profiles"""
    
    def test_predefined_profiles_exist(self):
        """Test that predefined profiles exist"""
        expected_profiles = [
            'low', 'medium', 'high', 'stress', 
            'burst', 'realistic', 'endurance'
        ]
        
        for profile_name in expected_profiles:
            assert profile_name in LOAD_PROFILES
            profile = LOAD_PROFILES[profile_name]
            assert isinstance(profile, LoadProfile)
            assert profile.rate > 0
            assert profile.batch_size > 0
    
    def test_profile_manager_get(self):
        """Test ProfileManager.get_profile"""
        profile = ProfileManager.get_profile('medium')
        assert profile.name == 'medium'
        assert profile.rate == 1000
    
    def test_profile_manager_invalid(self):
        """Test getting invalid profile"""
        with pytest.raises(ValueError, match="Unknown profile"):
            ProfileManager.get_profile('nonexistent')
    
    def test_custom_profile_creation(self):
        """Test creating custom profile"""
        custom = ProfileManager.create_custom_profile(
            rate=5000,
            batch_size=500,
            duration=120,
            workers=4
        )
        
        assert custom.name == 'custom'
        assert custom.rate == 5000
        assert custom.batch_size == 500
        assert custom.duration == 120
    
    def test_resource_estimation(self):
        """Test resource estimation"""
        profile = LOAD_PROFILES['high']
        estimate = ProfileManager.estimate_resources(profile)
        
        assert 'estimated_memory_mb' in estimate
        assert 'estimated_disk_mb' in estimate
        assert 'estimated_cpu_percent' in estimate
        assert estimate['estimated_memory_mb'] > 0
    
    def test_profile_validation(self):
        """Test profile validation"""
        # Valid profile
        valid = LoadProfile(
            name='test',
            rate=1000,
            batch_size=100
        )
        assert ProfileManager.validate_profile(valid)
        
        # Invalid profile
        invalid = LoadProfile(
            name='test',
            rate=-100,  # Invalid
            batch_size=100
        )
        with pytest.raises(ValueError, match="Rate must be positive"):
            ProfileManager.validate_profile(invalid)
    
    def test_scenario_recommendations(self):
        """Test scenario-based recommendations"""
        dev_profiles = recommend_profiles('development')
        assert 'low' in dev_profiles
        
        perf_profiles = recommend_profiles('performance_testing')
        assert 'high' in perf_profiles
    
    def test_combine_profiles(self):
        """Test combining profiles"""
        low = LOAD_PROFILES['low']
        high = LOAD_PROFILES['high']
        
        combined = combine_profiles(low, high)
        
        assert combined.name == 'combined'
        # Should average rates
        assert low.rate < combined.rate < high.rate


