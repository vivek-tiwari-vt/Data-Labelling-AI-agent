"""
Quality Metrics Calculator - Handles quality and confidence metrics
"""
import statistics
from collections import defaultdict, Counter
from typing import Dict, List, Any

from ..models import JobData

class QualityMetricsCalculator:
    """Calculates quality and confidence metrics"""
    
    def calculate(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate quality and confidence metrics"""
        confidence_scores = []
        label_consistency = defaultdict(list)
        quality_flags = Counter()
        
        for job in jobs:
            # Use success rate as a proxy for confidence since we don't have detailed logs access here
            if job.success_rate > 0:
                estimated_confidence = min(0.95, max(0.5, job.success_rate * 0.9))
                confidence_scores.append(estimated_confidence)
                
                # Quality flags based on estimated confidence
                if estimated_confidence < 0.6:
                    quality_flags["low_confidence"] += 1
                elif estimated_confidence > 0.9:
                    quality_flags["high_confidence"] += 1
                else:
                    quality_flags["medium_confidence"] += 1
            elif job.status == "completed":
                # Default confidence for completed jobs without success rate
                confidence_scores.append(0.85)
                quality_flags["medium_confidence"] += 1
            else:
                # Low confidence for failed jobs
                confidence_scores.append(0.3)
                quality_flags["low_confidence"] += 1
        
        # Calculate label quality scores (simplified since we don't have access to detailed results)
        label_quality_scores = {}
        if jobs:
            for job in jobs:
                if job.results_data and 'test_texts' in job.results_data:
                    for text_result in job.results_data['test_texts']:
                        label = text_result.get('ai_assigned_label', 'Unknown')
                        if label not in label_quality_scores:
                            label_quality_scores[label] = {
                                "avg_confidence": job.success_rate * 0.9 if job.success_rate > 0 else 0.7,
                                "consistency_score": 0.8,  # Estimated
                                "usage_count": 1
                            }
                        else:
                            label_quality_scores[label]["usage_count"] += 1
        
        return {
            "overall_confidence": {
                "average": statistics.mean(confidence_scores) if confidence_scores else 0,
                "median": statistics.median(confidence_scores) if confidence_scores else 0,
                "std_deviation": statistics.stdev(confidence_scores) if len(confidence_scores) > 1 else 0,
                "min_confidence": min(confidence_scores) if confidence_scores else 0,
                "max_confidence": max(confidence_scores) if confidence_scores else 0,
                "total_predictions": len(confidence_scores),
                "distribution": {
                    "high_confidence": quality_flags.get("high_confidence", 0),
                    "medium_confidence": quality_flags.get("medium_confidence", 0),
                    "low_confidence": quality_flags.get("low_confidence", 0)
                }
            },
            "label_quality_scores": label_quality_scores,
            "quality_score": self._calculate_overall_quality_score(confidence_scores, quality_flags),
            "confidence_trend": "stable"  # Simplified since we don't have temporal analysis here
        }
    
    def _calculate_overall_quality_score(self, confidence_scores: List[float], quality_flags: Counter) -> float:
        """Calculate an overall quality score (0-100)"""
        if not confidence_scores:
            return 0
        
        avg_confidence = statistics.mean(confidence_scores)
        high_confidence_ratio = quality_flags.get("high_confidence", 0) / len(confidence_scores)
        low_confidence_ratio = quality_flags.get("low_confidence", 0) / len(confidence_scores)
        
        # Quality score based on average confidence and distribution
        quality_score = (avg_confidence * 60) + (high_confidence_ratio * 30) - (low_confidence_ratio * 20)
        
        return max(0, min(100, quality_score))
