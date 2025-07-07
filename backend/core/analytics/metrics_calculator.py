"""
Core Metrics Calculator - Main orchestrator for metrics calculation
"""
import os
import sys
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from .models import JobData
from .metrics.performance_metrics import PerformanceMetricsCalculator
from .metrics.model_metrics import ModelMetricsCalculator
from .metrics.quality_metrics import QualityMetricsCalculator
from .metrics.system_metrics import SystemMetricsCalculator

class MetricsCalculator:
    """Main orchestrator for calculating various metrics from job data"""
    
    def __init__(self, data_collector):
        self.data_collector = data_collector
        
        # Initialize specialized metrics calculators
        self.performance_calc = PerformanceMetricsCalculator()
        self.model_calc = ModelMetricsCalculator()
        self.quality_calc = QualityMetricsCalculator()
        self.system_calc = SystemMetricsCalculator()
    
    def calculate_performance_metrics(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate detailed performance metrics"""
        return self.performance_calc.calculate(jobs)
    
    def calculate_model_analytics(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate model-specific analytics and comparisons"""
        return self.model_calc.calculate(jobs)
    
    def calculate_quality_metrics(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate quality and confidence metrics"""
        return self.quality_calc.calculate(jobs)
    
    def calculate_advanced_analytics(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate advanced analytics metrics"""
        return self.system_calc.calculate_advanced_analytics(jobs)
    
    def assess_system_health(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Assess overall system health"""
        return self.system_calc.assess_system_health(jobs)
    
    def calculate_productivity_metrics(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate productivity metrics"""
        return self.system_calc.calculate_productivity_metrics(jobs)
