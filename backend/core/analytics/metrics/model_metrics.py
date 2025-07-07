"""
Model Metrics Calculator - Handles model-specific metrics
"""
import statistics
from collections import defaultdict, Counter
from typing import Dict, List, Any

from ..models import JobData

class ModelMetricsCalculator:
    """Calculates model-specific metrics and comparisons"""
    
    def calculate(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate model-specific analytics and comparisons"""
        model_performance = defaultdict(list)
        model_usage = Counter()
        
        for job in jobs:
            # Use the primary model (prefer child, fallback to mother, then ai_model_used)
            primary_model = job.child_ai_model or job.mother_ai_model or job.ai_model_used or "Unknown"
            
            if primary_model and primary_model != "Unknown":
                model_usage[primary_model] += 1
                
                if job.status == "completed" and job.processing_time_ms > 0 and job.total_texts > 0:
                    efficiency = job.total_texts / (job.processing_time_ms / 1000)  # texts per second
                    model_performance[primary_model].append({
                        "processing_time": job.processing_time_ms,
                        "success_rate": job.success_rate,
                        "efficiency": efficiency,
                        "total_texts": job.total_texts
                    })
        
        # Calculate model efficiency scores
        model_scores = {}
        for model, performances in model_performance.items():
            if performances:
                avg_efficiency = statistics.mean([p["efficiency"] for p in performances])
                avg_success_rate = statistics.mean([p["success_rate"] for p in performances])
                avg_processing_time = statistics.mean([p["processing_time"] for p in performances])
                
                # Combined efficiency score (0-100)
                efficiency_score = (avg_efficiency * 40) + (avg_success_rate * 30) + \
                                 (max(0, 30 - (avg_processing_time / 1000)))
                
                model_scores[model] = {
                    "efficiency_score": efficiency_score,
                    "avg_efficiency": avg_efficiency,
                    "avg_success_rate": avg_success_rate,
                    "avg_processing_time": avg_processing_time,
                    "usage_count": model_usage[model]
                }
        
        return {
            "model_usage_distribution": dict(model_usage),
            "model_performance_scores": model_scores,
            "top_performing_models": sorted(
                model_scores.items(), 
                key=lambda x: x[1]["efficiency_score"], 
                reverse=True
            )[:5],
            "total_models_used": len(model_usage),
            "most_used_model": model_usage.most_common(1) if model_usage else [("Unknown", 0)]
        }
