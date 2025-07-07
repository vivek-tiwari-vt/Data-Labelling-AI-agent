"""
Daily Charts Processor - Handles temporal chart data preparation
"""
import statistics
from collections import defaultdict
from typing import Dict, List, Any
from datetime import datetime

from ..models import JobData

class DailyChartsProcessor:
    """Processes daily and temporal chart data"""
    
    def prepare_daily_charts(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare all daily/temporal chart data"""
        return {
            "daily_jobs": self._prepare_daily_jobs_chart(jobs),
            "hourly_distribution": self._prepare_hourly_distribution(jobs),
            "processing_time_trend": self._prepare_processing_time_trend(jobs),
            "success_rate_trends": self._prepare_success_rate_trends(jobs)
        }
    
    def _prepare_daily_jobs_chart(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare daily job distribution chart data"""
        daily_counts = defaultdict(int)
        daily_success = defaultdict(int)
        daily_texts = defaultdict(int)
        daily_confidence = defaultdict(list)
        
        for job in jobs:
            try:
                job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                day_key = job_date.strftime("%Y-%m-%d")
                daily_counts[day_key] += 1
                
                if job.status == "completed":
                    daily_success[day_key] += 1
                
                daily_texts[day_key] += job.total_texts
                
                # Estimate confidence based on success rate
                if job.success_rate > 0:
                    estimated_confidence = min(0.95, max(0.5, job.success_rate * 0.9))
                    for _ in range(max(1, job.total_texts)):
                        daily_confidence[day_key].append(estimated_confidence)
                        
            except Exception as e:
                print(f"Error processing job {job.job_id} for daily chart: {e}")
                continue
        
        # Calculate average confidence by day
        daily_avg_confidence = {}
        for day, confidences in daily_confidence.items():
            daily_avg_confidence[day] = statistics.mean(confidences) if confidences else 0
        
        # Sort dates for consistent ordering
        sorted_dates = sorted(daily_counts.keys())
        
        return {
            "dates": sorted_dates,
            "job_counts": [daily_counts[date] for date in sorted_dates],
            "success_counts": [daily_success[date] for date in sorted_dates],
            "total_texts": [daily_texts[date] for date in sorted_dates],
            "avg_confidence": [daily_avg_confidence.get(date, 0) for date in sorted_dates]
        }
    
    def _prepare_hourly_distribution(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Prepare hourly job distribution"""
        hourly_counts = defaultdict(int)
        
        for job in jobs:
            try:
                job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                hour_key = job_date.hour
                hourly_counts[hour_key] += 1
            except:
                continue
        
        # Create 24-hour data
        hours = list(range(24))
        counts = [hourly_counts.get(hour, 0) for hour in hours]
        
        return {
            "hours": [f"{hour:02d}:00" for hour in hours],
            "counts": counts
        }
    
    def _prepare_processing_time_trend(self, jobs: List[JobData]) -> List[Dict[str, Any]]:
        """Prepare processing time trend data"""
        daily_processing = defaultdict(list)
        
        for job in jobs:
            if job.processing_time_ms > 0:
                try:
                    job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                    day_key = job_date.strftime("%Y-%m-%d")
                    daily_processing[day_key].append(job.processing_time_ms / 1000)  # Convert to seconds
                except:
                    continue
        
        trend_data = []
        for day in sorted(daily_processing.keys()):
            times = daily_processing[day]
            trend_data.append({
                "date": day,
                "processing_time": statistics.mean(times),
                "min_time": min(times),
                "max_time": max(times),
                "job_count": len(times)
            })
        
        return trend_data
    
    def _prepare_success_rate_trends(self, jobs: List[JobData]) -> List[Dict[str, Any]]:
        """Prepare success rate trend data"""
        daily_stats = defaultdict(lambda: {"total": 0, "success": 0})
        
        for job in jobs:
            try:
                job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                day_key = job_date.strftime("%Y-%m-%d")
                daily_stats[day_key]["total"] += 1
                if job.status == "completed":
                    daily_stats[day_key]["success"] += 1
            except:
                continue
        
        trend_data = []
        for day in sorted(daily_stats.keys()):
            stats = daily_stats[day]
            success_rate = (stats["success"] / stats["total"] * 100) if stats["total"] > 0 else 0
            trend_data.append({
                "date": day,
                "success_rate": success_rate,
                "total_jobs": stats["total"],
                "failed_jobs": stats["total"] - stats["success"]
            })
        
        return trend_data
