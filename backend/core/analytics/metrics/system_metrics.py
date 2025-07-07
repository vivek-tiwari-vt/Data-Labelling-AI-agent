"""
System Metrics Calculator - Handles system health and advanced metrics
"""
import statistics
from collections import defaultdict
from typing import Dict, List, Any
from datetime import datetime

from ..models import JobData

class SystemMetricsCalculator:
    """Calculates system health and advanced metrics"""
    
    def calculate_advanced_analytics(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate advanced analytics metrics"""
        if not jobs:
            return {"error": "No jobs available for analysis"}
        
        # Temporal analysis
        hourly_distribution = defaultdict(int)
        daily_patterns = defaultdict(int)
        
        for job in jobs:
            try:
                job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                hourly_distribution[job_date.hour] += 1
                daily_patterns[job_date.weekday()] += 1
            except:
                continue
        
        # Peak usage analysis
        peak_hour = max(hourly_distribution.items(), key=lambda x: x[1]) if hourly_distribution else (0, 0)
        peak_day = max(daily_patterns.items(), key=lambda x: x[1]) if daily_patterns else (0, 0)
        
        return {
            "peak_usage_hour": peak_hour[0],
            "peak_usage_count": peak_hour[1],
            "peak_usage_day": peak_day[0],  # 0=Monday, 6=Sunday
            "usage_consistency": self._calculate_usage_consistency(hourly_distribution),
            "load_distribution": dict(hourly_distribution)
        }
    
    def assess_system_health(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Assess overall system health"""
        if not jobs:
            return {"score": 100, "status": "No data", "metrics": {}}
        
        completed_jobs = [job for job in jobs if job.status == "completed"]
        failed_jobs = [job for job in jobs if job.status == "failed"]
        
        # Calculate health metrics
        success_rate = (len(completed_jobs) / len(jobs)) * 100 if jobs else 0
        
        # Processing time health
        processing_times = [job.processing_time_ms for job in completed_jobs if job.processing_time_ms > 0]
        avg_processing_time = statistics.mean(processing_times) if processing_times else 0
        
        # Throughput health
        throughput_data = []
        for job in completed_jobs:
            if job.processing_time_ms > 0 and job.total_texts > 0:
                throughput = job.total_texts / (job.processing_time_ms / 1000)
                throughput_data.append(throughput)
        
        avg_throughput = statistics.mean(throughput_data) if throughput_data else 0
        
        # Calculate overall health score (0-100)
        health_score = 0
        
        # Success rate component (40% weight)
        health_score += min(40, success_rate * 0.4)
        
        # Processing time component (30% weight) - lower is better
        if avg_processing_time > 0:
            time_score = max(0, 30 - (avg_processing_time / 1000))  # Penalty for long processing times
            health_score += min(30, time_score)
        else:
            health_score += 15  # Partial score if no data
        
        # Throughput component (30% weight)
        if avg_throughput > 0:
            throughput_score = min(30, avg_throughput * 3)  # Scale throughput to 0-30
            health_score += throughput_score
        else:
            health_score += 10  # Partial score if no data
        
        # Determine status
        if health_score >= 85:
            status = "excellent"
        elif health_score >= 70:
            status = "good"
        elif health_score >= 50:
            status = "fair"
        else:
            status = "poor"
        
        return {
            "score": round(health_score, 1),
            "status": status,
            "metrics": {
                "success_rate": success_rate,
                "avg_processing_time_ms": avg_processing_time,
                "avg_throughput": avg_throughput,
                "total_jobs": len(jobs),
                "failed_jobs": len(failed_jobs)
            }
        }
    
    def calculate_productivity_metrics(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate productivity metrics"""
        if not jobs:
            return {"error": "No jobs available for productivity analysis"}
        
        completed_jobs = [job for job in jobs if job.status == "completed"]
        
        # Throughput analysis
        total_texts = sum(job.total_texts for job in completed_jobs)
        total_time_hours = sum(job.processing_time_ms for job in completed_jobs if job.processing_time_ms > 0) / (1000 * 60 * 60)
        
        texts_per_hour = total_texts / total_time_hours if total_time_hours > 0 else 0
        
        # Efficiency metrics
        efficiency_scores = []
        for job in completed_jobs:
            if job.processing_time_ms > 0 and job.total_texts > 0:
                efficiency = job.total_texts / (job.processing_time_ms / 1000)
                efficiency_scores.append(efficiency)
        
        return {
            "throughput": {
                "texts_per_hour": round(texts_per_hour, 2),
                "total_texts_processed": total_texts,
                "total_processing_hours": round(total_time_hours, 2)
            },
            "efficiency_metrics": {
                "average_efficiency": round(statistics.mean(efficiency_scores), 2) if efficiency_scores else 0,
                "peak_efficiency": round(max(efficiency_scores), 2) if efficiency_scores else 0,
                "efficiency_variance": round(statistics.variance(efficiency_scores), 2) if len(efficiency_scores) > 1 else 0
            },
            "capacity_analysis": {
                "jobs_per_day": round(len(jobs) / 7, 1),  # Assuming 7-day period
                "peak_efficiency": round(max(efficiency_scores), 2) if efficiency_scores else 0,
                "utilization_score": min(100, round(texts_per_hour / 10, 1))  # Normalized score
            }
        }
    
    def _calculate_usage_consistency(self, hourly_distribution: Dict[int, int]) -> float:
        """Calculate how consistent usage is throughout the day"""
        if not hourly_distribution:
            return 0
        
        values = list(hourly_distribution.values())
        if len(values) <= 1:
            return 100
        
        # Lower standard deviation means more consistent usage
        std_dev = statistics.stdev(values)
        mean_val = statistics.mean(values)
        
        # Convert to consistency score (0-100, higher is more consistent)
        if mean_val > 0:
            coefficient_of_variation = std_dev / mean_val
            consistency_score = max(0, 100 - (coefficient_of_variation * 100))
        else:
            consistency_score = 0
        
        return round(consistency_score, 1)
