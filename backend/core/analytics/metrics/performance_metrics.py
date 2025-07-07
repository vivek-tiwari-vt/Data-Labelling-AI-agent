"""
Performance Metrics Calculator - Handles performance-related metrics
"""
import statistics
from typing import Dict, List, Any

from ..models import JobData

class PerformanceMetricsCalculator:
    """Calculates performance-related metrics"""
    
    def calculate(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate detailed performance metrics"""
        completed_jobs = [job for job in jobs if job.status == "completed"]
        failed_jobs = [job for job in jobs if job.status == "failed"]
        
        if not completed_jobs:
            return {"error": "No completed jobs found"}
        
        # Processing times
        processing_times = [job.processing_time_ms for job in completed_jobs if job.processing_time_ms > 0]
        
        # Throughput data
        throughput_data = []
        for job in completed_jobs:
            if job.processing_time_ms > 0 and job.total_texts > 0:
                throughput = job.total_texts / (job.processing_time_ms / 1000)  # texts per second
                throughput_data.append(throughput)
        
        return {
            "success_rate": len(completed_jobs) / len(jobs) * 100 if jobs else 0,
            "failure_rate": len(failed_jobs) / len(jobs) * 100 if jobs else 0,
            "avg_processing_time_ms": statistics.mean(processing_times) if processing_times else 0,
            "median_processing_time_ms": statistics.median(processing_times) if processing_times else 0,
            "processing_time_std": statistics.stdev(processing_times) if len(processing_times) > 1 else 0,
            "avg_throughput_texts_per_sec": statistics.mean(throughput_data) if throughput_data else 0,
            "total_texts_processed": sum(job.total_texts for job in completed_jobs),
            "total_processing_time_hours": sum(processing_times) / (1000 * 60 * 60) if processing_times else 0,
            "peak_throughput": max(throughput_data) if throughput_data else 0,
            "min_throughput": min(throughput_data) if throughput_data else 0
        }
