"""
Model Charts Processor - Handles model-related chart data preparation
"""
import statistics
from collections import defaultdict, Counter
from typing import Dict, List, Any

from ..models import JobData

class ModelChartsProcessor:
    """Processes model-related chart data"""
    
    def prepare_model_charts(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare all model-related chart data"""
        return {
            "model_usage": self._prepare_model_usage_chart(jobs),
            "model_performance_matrix": self._prepare_model_performance_matrix(jobs)
        }
    
    def _prepare_model_usage_chart(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare model usage distribution chart"""
        model_usage = Counter()
        model_performance = defaultdict(list)
        
        for job in jobs:
            # Get primary model
            primary_model = job.child_ai_model or job.mother_ai_model or job.ai_model_used or "Unknown"
            model_usage[primary_model] += 1
            
            # Track performance metrics per model
            if job.status == "completed" and job.processing_time_ms > 0 and job.total_texts > 0:
                model_performance[primary_model].append({
                    "processing_time": job.processing_time_ms,
                    "texts_processed": job.total_texts,
                    "efficiency": job.total_texts / (job.processing_time_ms / 1000)
                })
        
        # Calculate model efficiency scores
        model_efficiency = {}
        for model, performances in model_performance.items():
            if performances:
                avg_efficiency = statistics.mean([p["efficiency"] for p in performances])
                model_efficiency[model] = round(avg_efficiency, 2)
        
        return {
            "models": list(model_usage.keys()),
            "counts": list(model_usage.values()),
            "efficiency_scores": model_efficiency
        }
    
    def _prepare_model_performance_matrix(self, jobs: List[JobData]) -> List[Dict[str, Any]]:
        """Prepare model performance comparison matrix"""
        model_metrics = defaultdict(lambda: {
            "usage_count": 0,
            "total_processing_time": 0,
            "total_texts": 0,
            "success_count": 0,
            "total_jobs": 0
        })
        
        for job in jobs:
            primary_model = job.child_ai_model or job.mother_ai_model or job.ai_model_used or "Unknown"
            metrics = model_metrics[primary_model]
            
            metrics["total_jobs"] += 1
            metrics["usage_count"] += 1
            
            if job.status == "completed":
                metrics["success_count"] += 1
                metrics["total_processing_time"] += job.processing_time_ms
                metrics["total_texts"] += job.total_texts
        
        # Convert to list format for frontend
        matrix_data = []
        for model, metrics in model_metrics.items():
            if metrics["total_jobs"] > 0:
                success_rate = (metrics["success_count"] / metrics["total_jobs"]) * 100
                avg_processing_time = (metrics["total_processing_time"] / max(1, metrics["success_count"]))
                efficiency = (metrics["total_texts"] / (metrics["total_processing_time"] / 1000)) if metrics["total_processing_time"] > 0 else 0
                
                matrix_data.append({
                    "model": model,
                    "usage_count": metrics["usage_count"],
                    "success_rate": round(success_rate, 1),
                    "avg_processing_time": round(avg_processing_time, 2),
                    "efficiency": round(efficiency, 2),
                    "total_texts_processed": metrics["total_texts"]
                })
        
        # Sort by efficiency score
        matrix_data.sort(key=lambda x: x["efficiency"], reverse=True)
        
        return matrix_data
