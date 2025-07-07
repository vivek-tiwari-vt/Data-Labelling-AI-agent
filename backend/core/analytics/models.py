"""
Data Types and Models for Visual Creator
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

@dataclass
class AnalyticsMetric:
    name: str
    value: Union[float, int, str]
    timestamp: str
    job_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class PerformanceInsight:
    type: str  # "trend", "anomaly", "recommendation", "benchmark"
    title: str
    description: str
    severity: str  # "info", "warning", "critical"
    action_items: List[str]
    confidence: float

@dataclass
class JobData:
    """Standardized job data structure for analytics"""
    job_id: str
    created: str
    status: str
    total_texts: int = 0
    success_rate: float = 0.0
    processing_time_ms: int = 0
    ai_model_used: Optional[str] = None
    mother_ai_model: Optional[str] = None
    child_ai_model: Optional[str] = None
    models_used: List[str] = None
    results_data: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.models_used is None:
            self.models_used = []

@dataclass
class AnalyticsResponse:
    """Standardized analytics response structure"""
    time_period: str
    total_jobs: int
    performance_metrics: Dict[str, Any]
    model_analytics: Dict[str, Any]
    quality_metrics: Dict[str, Any]
    trend_analysis: Dict[str, Any]
    efficiency_insights: List[PerformanceInsight]
    cost_analysis: Dict[str, Any]
    predictions: Dict[str, Any]
    recommendations: List[Dict[str, Any]]
    charts_data: Dict[str, Any]
    advanced_analytics: Optional[Dict[str, Any]] = None
    system_health: Optional[Dict[str, Any]] = None
    productivity_metrics: Optional[Dict[str, Any]] = None
