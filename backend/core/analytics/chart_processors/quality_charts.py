"""
Quality Charts Processor - Handles quality and confidence chart data preparation
"""
import statistics
from collections import defaultdict
from typing import Dict, List, Any

from ..models import JobData

class QualityChartsProcessor:
    """Processes quality and confidence chart data"""
    
    def prepare_quality_charts(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare all quality-related chart data"""
        return {
            "confidence_distribution": self._prepare_confidence_chart(jobs),
            "label_distribution": self._prepare_label_distribution_chart(jobs),
            "quality_evolution": self._prepare_quality_evolution(jobs)
        }
    
    def _prepare_confidence_chart(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare confidence score distribution chart"""
        confidence_scores = []
        
        for job in jobs:
            # Use success rate as confidence proxy since we don't have access to job logs here
            if job.success_rate > 0:
                estimated_confidence = min(0.95, max(0.5, job.success_rate * 0.9))
                for _ in range(max(1, job.total_texts)):
                    confidence_scores.append(estimated_confidence)
            elif job.status == "completed":
                # Default confidence for completed jobs
                confidence_scores.append(0.85)
            else:
                # Lower confidence for failed jobs
                confidence_scores.append(0.3)
        
        # Create distribution bins
        bins = ["0.0-0.1", "0.1-0.3", "0.3-0.5", "0.5-0.7", "0.7-0.85", "0.85-0.95", "0.95-1.0"]
        counts = [
            len([c for c in confidence_scores if 0.0 <= c < 0.1]),
            len([c for c in confidence_scores if 0.1 <= c < 0.3]),
            len([c for c in confidence_scores if 0.3 <= c < 0.5]),
            len([c for c in confidence_scores if 0.5 <= c < 0.7]),
            len([c for c in confidence_scores if 0.7 <= c < 0.85]),
            len([c for c in confidence_scores if 0.85 <= c < 0.95]),
            len([c for c in confidence_scores if 0.95 <= c <= 1.0])
        ]
        
        return {
            "bins": bins,
            "counts": counts,
            "average_confidence": statistics.mean(confidence_scores) if confidence_scores else 0,
            "total_predictions": len(confidence_scores)
        }
    
    def _prepare_label_distribution_chart(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare label distribution data"""
        label_counts = defaultdict(int)
        
        for job in jobs:
            if job.results_data and 'test_texts' in job.results_data:
                for text_result in job.results_data['test_texts']:
                    label = text_result.get('ai_assigned_label', 'Unknown')
                    label_counts[label] += 1
            else:
                # If no detailed results, estimate based on job metadata
                label_counts['Processed'] += job.total_texts
        
        # Convert to lists for frontend
        labels = list(label_counts.keys())
        counts = list(label_counts.values())
        
        return {
            "labels": labels,
            "counts": counts,
            "total_labels": sum(counts)
        }
    
    def _prepare_quality_evolution(self, jobs: List[JobData]) -> List[Dict[str, Any]]:
        """Prepare quality evolution over time"""
        from datetime import datetime
        
        daily_quality = defaultdict(lambda: {"total_confidence": 0, "count": 0, "success_count": 0, "total_jobs": 0})
        
        for job in jobs:
            try:
                job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                day_key = job_date.strftime("%Y-%m-%d")
                
                quality_metrics = daily_quality[day_key]
                quality_metrics["total_jobs"] += 1
                
                if job.status == "completed":
                    quality_metrics["success_count"] += 1
                    # Estimate confidence based on success rate
                    estimated_confidence = min(0.95, max(0.5, job.success_rate * 0.9)) if job.success_rate > 0 else 0.85
                    quality_metrics["total_confidence"] += estimated_confidence
                    quality_metrics["count"] += 1
                else:
                    quality_metrics["total_confidence"] += 0.3  # Low confidence for failed jobs
                    quality_metrics["count"] += 1
                    
            except:
                continue
        
        evolution_data = []
        for day in sorted(daily_quality.keys()):
            metrics = daily_quality[day]
            avg_confidence = metrics["total_confidence"] / metrics["count"] if metrics["count"] > 0 else 0
            success_rate = (metrics["success_count"] / metrics["total_jobs"]) * 100 if metrics["total_jobs"] > 0 else 0
            
            # Quality score combines confidence and success rate
            quality_score = (avg_confidence * 50) + (success_rate * 0.5)
            
            evolution_data.append({
                "date": day,
                "confidence": round(avg_confidence, 3),
                "success_rate": round(success_rate, 1),
                "quality_score": round(quality_score, 1),
                "job_count": metrics["total_jobs"]
            })
        
        return evolution_data
