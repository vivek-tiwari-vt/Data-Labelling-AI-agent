"""
Core Chart Data Processor - Main orchestrator for chart data preparation
"""
import os
import sys
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from .models import JobData
from .chart_processors.daily_charts import DailyChartsProcessor
from .chart_processors.model_charts import ModelChartsProcessor
from .chart_processors.quality_charts import QualityChartsProcessor
from .chart_processors.performance_charts import PerformanceChartsProcessor

class ChartDataProcessor:
    """Main orchestrator for preparing chart data for frontend visualization"""
    
    def __init__(self, data_collector):
        self.data_collector = data_collector
        
        # Initialize specialized chart processors
        self.daily_processor = DailyChartsProcessor()
        self.model_processor = ModelChartsProcessor()
        self.quality_processor = QualityChartsProcessor()
        self.performance_processor = PerformanceChartsProcessor()
    
    def prepare_charts_data(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare comprehensive chart data for frontend"""
        return {
            # Daily and temporal charts
            **self.daily_processor.prepare_daily_charts(jobs),
            
            # Model-related charts
            **self.model_processor.prepare_model_charts(jobs),
            
            # Quality and confidence charts
            **self.quality_processor.prepare_quality_charts(jobs),
            
            # Performance and efficiency charts
            **self.performance_processor.prepare_performance_charts(jobs)
        }
