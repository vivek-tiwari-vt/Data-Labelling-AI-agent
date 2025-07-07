"""
Performance Charts Processor - Handles performance and efficiency chart data preparation
"""
import statistics
from collections import defaultdict
from typing import Dict, List, Any
from datetime import datetime

from ..models import JobData

class PerformanceChartsProcessor:
    """Processes performance and efficiency chart data"""
    
    def prepare_performance_charts(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare all performance-related chart data"""
        return {
            "efficiency_trends": self._prepare_efficiency_trends(jobs),
            "throughput_analysis": self._prepare_throughput_analysis(jobs)
        }
    
    def _prepare_efficiency_trends(self, jobs: List[JobData]) -> List[Dict[str, Any]]:
        """Prepare efficiency trends over time"""
        daily_efficiency = defaultdict(list)
        
        for job in jobs:
            if job.processing_time_ms > 0 and job.total_texts > 0:
                try:
                    job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                    day_key = job_date.strftime("%Y-%m-%d")
                    
                    # Calculate efficiency as texts per second
                    efficiency = job.total_texts / (job.processing_time_ms / 1000)
                    daily_efficiency[day_key].append(efficiency)
                except:
                    continue
        
        trend_data = []
        for day in sorted(daily_efficiency.keys()):
            efficiencies = daily_efficiency[day]
            trend_data.append({
                "date": day,
                "avg_efficiency": round(statistics.mean(efficiencies), 2),
                "max_efficiency": round(max(efficiencies), 2),
                "min_efficiency": round(min(efficiencies), 2),
                "job_count": len(efficiencies)
            })
        
        return trend_data
    
    def _prepare_throughput_analysis(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare throughput analysis data"""
        hourly_throughput = defaultdict(lambda: {"texts": 0, "time": 0, "jobs": 0})
        
        for job in jobs:
            if job.processing_time_ms > 0 and job.status == "completed":
                try:
                    job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                    hour_key = job_date.hour
                    
                    throughput_data = hourly_throughput[hour_key]
                    throughput_data["texts"] += job.total_texts
                    throughput_data["time"] += job.processing_time_ms / 1000  # Convert to seconds
                    throughput_data["jobs"] += 1
                except:
                    continue
        
        # Calculate throughput metrics
        throughput_by_hour = []
        peak_hour = 0
        peak_throughput = 0
        
        for hour in range(24):
            data = hourly_throughput.get(hour, {"texts": 0, "time": 0, "jobs": 0})
            
            if data["time"] > 0:
                texts_per_second = data["texts"] / data["time"]
                if texts_per_second > peak_throughput:
                    peak_throughput = texts_per_second
                    peak_hour = hour
            else:
                texts_per_second = 0
            
            throughput_by_hour.append({
                "hour": f"{hour:02d}:00",
                "texts_per_second": round(texts_per_second, 2),
                "total_texts": data["texts"],
                "job_count": data["jobs"]
            })
        
        # Calculate overall statistics
        all_throughputs = [h["texts_per_second"] for h in throughput_by_hour if h["texts_per_second"] > 0]
        
        return {
            "hourly_throughput": throughput_by_hour,
            "peak_hour": peak_hour,
            "peak_throughput": round(peak_throughput, 2),
            "average_throughput": round(statistics.mean(all_throughputs), 2) if all_throughputs else 0,
            "total_texts_processed": sum(h["total_texts"] for h in throughput_by_hour)
        }
