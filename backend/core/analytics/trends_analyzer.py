"""
Trends Analyzer - Calculates trends and patterns in analytics data
"""
import statistics
from collections import defaultdict
from typing import Dict, List, Any
from datetime import datetime, timedelta

# Add parent directory to path for imports
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from .models import JobData

class TrendsAnalyzer:
    """Analyzes trends and patterns in job data over time"""
    
    def calculate_trends(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate comprehensive trend analysis over time"""
        if len(jobs) < 2:
            return {"error": "Insufficient data for trend analysis"}
        
        # Group jobs by day
        daily_stats = defaultdict(lambda: {
            "jobs": 0, 
            "success": 0, 
            "total_texts": 0, 
            "processing_times": [],
            "confidence_scores": []
        })
        
        for job in jobs:
            try:
                job_date = datetime.fromisoformat(job.created.replace('Z', '+00:00')).date()
                day_key = job_date.isoformat()
                
                daily_stats[day_key]["jobs"] += 1
                daily_stats[day_key]["total_texts"] += job.total_texts
                
                if job.status == "completed":
                    daily_stats[day_key]["success"] += 1
                    
                if job.processing_time_ms > 0:
                    daily_stats[day_key]["processing_times"].append(job.processing_time_ms)
                
                # Extract confidence scores if available
                if job.results_data and isinstance(job.results_data, dict):
                    results = job.results_data.get('labeled_data', [])
                    for result in results:
                        if isinstance(result, dict) and 'confidence' in result:
                            daily_stats[day_key]["confidence_scores"].append(result['confidence'])
                            
            except Exception as e:
                print(f"Error processing job {job.job_id} for trends: {e}")
                continue
        
        # Calculate daily averages and trends
        trend_data = []
        for day, stats in sorted(daily_stats.items()):
            success_rate = (stats["success"] / stats["jobs"] * 100) if stats["jobs"] > 0 else 0
            avg_processing_time = statistics.mean(stats["processing_times"]) if stats["processing_times"] else 0
            avg_confidence = statistics.mean(stats["confidence_scores"]) if stats["confidence_scores"] else 0
            
            trend_data.append({
                "date": day,
                "jobs": stats["jobs"],
                "success_rate": success_rate,
                "total_texts": stats["total_texts"],
                "avg_processing_time": avg_processing_time,
                "avg_confidence": avg_confidence
            })
        
        # Analyze trends
        trend_analysis = self._analyze_trends(trend_data)
        
        return {
            "daily_data": trend_data,
            "trend_direction": trend_analysis.get("direction", "stable"),
            "trend_strength": trend_analysis.get("strength", 0),
            "growth_rate": trend_analysis.get("growth_rate", 0),
            "efficiency_trend": self._calculate_efficiency_trend(trend_data),
            "quality_trend": self._calculate_quality_trend(trend_data),
            "volume_trend": self._calculate_volume_trend(trend_data)
        }
    
    def _analyze_trends(self, trend_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze overall trends in the data"""
        if len(trend_data) < 3:
            return {"direction": "insufficient_data", "strength": 0, "growth_rate": 0}
        
        try:
            # Calculate linear regression on job counts
            x_values = list(range(len(trend_data)))
            y_values = [day["jobs"] for day in trend_data]
            
            # Simple linear regression
            n = len(x_values)
            sum_x = sum(x_values)
            sum_y = sum(y_values)
            sum_xy = sum(x * y for x, y in zip(x_values, y_values))
            sum_x_squared = sum(x * x for x in x_values)
            
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x * sum_x)
            
            # Determine trend direction and strength
            if abs(slope) < 0.1:
                direction = "stable"
                strength = 0
            elif slope > 0:
                direction = "increasing"
                strength = min(abs(slope) * 10, 10)  # Scale to 0-10
            else:
                direction = "decreasing"
                strength = min(abs(slope) * 10, 10)
            
            # Calculate growth rate as percentage
            if len(y_values) >= 2:
                growth_rate = ((y_values[-1] - y_values[0]) / max(y_values[0], 1)) * 100
            else:
                growth_rate = 0
            
            return {
                "direction": direction,
                "strength": strength,
                "growth_rate": growth_rate,
                "slope": slope
            }
            
        except Exception as e:
            print(f"Error analyzing trends: {e}")
            return {"direction": "error", "strength": 0, "growth_rate": 0}
    
    def _calculate_efficiency_trend(self, trend_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate efficiency trends over time"""
        efficiency_scores = []
        
        for day in trend_data:
            # Calculate efficiency as texts per second
            if day["avg_processing_time"] > 0:
                texts_per_ms = day["total_texts"] / day["avg_processing_time"]
                efficiency_score = texts_per_ms * 1000  # Convert to per second
            else:
                efficiency_score = 0
            
            efficiency_scores.append(efficiency_score)
        
        if len(efficiency_scores) < 2:
            return {"trend": "insufficient_data", "change": 0}
        
        # Calculate trend
        first_half = efficiency_scores[:len(efficiency_scores)//2]
        second_half = efficiency_scores[len(efficiency_scores)//2:]
        
        avg_first = statistics.mean(first_half) if first_half else 0
        avg_second = statistics.mean(second_half) if second_half else 0
        
        if avg_first == 0:
            change = 0
        else:
            change = ((avg_second - avg_first) / avg_first) * 100
        
        if abs(change) < 5:
            trend = "stable"
        elif change > 0:
            trend = "improving"
        else:
            trend = "declining"
        
        return {
            "trend": trend,
            "change": change,
            "current_avg": avg_second,
            "previous_avg": avg_first
        }
    
    def _calculate_quality_trend(self, trend_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate quality trends based on confidence scores"""
        confidence_scores = [day["avg_confidence"] for day in trend_data if day["avg_confidence"] > 0]
        
        if len(confidence_scores) < 2:
            return {"trend": "insufficient_data", "change": 0}
        
        # Calculate trend
        first_half = confidence_scores[:len(confidence_scores)//2]
        second_half = confidence_scores[len(confidence_scores)//2:]
        
        avg_first = statistics.mean(first_half) if first_half else 0
        avg_second = statistics.mean(second_half) if second_half else 0
        
        if avg_first == 0:
            change = 0
        else:
            change = ((avg_second - avg_first) / avg_first) * 100
        
        if abs(change) < 2:  # 2% threshold for quality
            trend = "stable"
        elif change > 0:
            trend = "improving"
        else:
            trend = "declining"
        
        return {
            "trend": trend,
            "change": change,
            "current_avg": avg_second,
            "previous_avg": avg_first
        }
    
    def _calculate_volume_trend(self, trend_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate volume trends in job processing"""
        job_counts = [day["jobs"] for day in trend_data]
        text_counts = [day["total_texts"] for day in trend_data]
        
        if len(job_counts) < 2:
            return {"trend": "insufficient_data", "job_change": 0, "text_change": 0}
        
        # Calculate trends for both jobs and texts
        job_first_half = job_counts[:len(job_counts)//2]
        job_second_half = job_counts[len(job_counts)//2:]
        
        text_first_half = text_counts[:len(text_counts)//2]
        text_second_half = text_counts[len(text_counts)//2:]
        
        avg_job_first = statistics.mean(job_first_half) if job_first_half else 0
        avg_job_second = statistics.mean(job_second_half) if job_second_half else 0
        
        avg_text_first = statistics.mean(text_first_half) if text_first_half else 0
        avg_text_second = statistics.mean(text_second_half) if text_second_half else 0
        
        job_change = ((avg_job_second - avg_job_first) / max(avg_job_first, 1)) * 100
        text_change = ((avg_text_second - avg_text_first) / max(avg_text_first, 1)) * 100
        
        # Determine overall trend
        if abs(job_change) < 10 and abs(text_change) < 10:
            trend = "stable"
        elif job_change > 0 or text_change > 0:
            trend = "increasing"
        else:
            trend = "decreasing"
        
        return {
            "trend": trend,
            "job_change": job_change,
            "text_change": text_change,
            "current_job_avg": avg_job_second,
            "current_text_avg": avg_text_second
        }
    
    def detect_anomalies(self, jobs: List[JobData]) -> List[Dict[str, Any]]:
        """Detect anomalies in job processing patterns"""
        anomalies = []
        
        if len(jobs) < 10:  # Need sufficient data for anomaly detection
            return anomalies
        
        # Processing time anomalies
        processing_times = [job.processing_time_ms for job in jobs if job.processing_time_ms > 0]
        if processing_times:
            mean_time = statistics.mean(processing_times)
            std_time = statistics.stdev(processing_times) if len(processing_times) > 1 else 0
            
            for job in jobs:
                if job.processing_time_ms > mean_time + 3 * std_time:
                    anomalies.append({
                        "type": "processing_time",
                        "job_id": job.job_id,
                        "value": job.processing_time_ms,
                        "expected_range": f"{mean_time - std_time:.0f}-{mean_time + std_time:.0f}",
                        "severity": "high" if job.processing_time_ms > mean_time + 5 * std_time else "medium"
                    })
        
        # Success rate anomalies (daily)
        daily_success_rates = self._calculate_daily_success_rates(jobs)
        if len(daily_success_rates) > 3:
            mean_success = statistics.mean(daily_success_rates.values())
            for date, rate in daily_success_rates.items():
                if rate < mean_success - 20:  # 20% below average
                    anomalies.append({
                        "type": "success_rate",
                        "date": date,
                        "value": rate,
                        "expected": mean_success,
                        "severity": "high" if rate < mean_success - 30 else "medium"
                    })
        
        return anomalies
    
    def _calculate_daily_success_rates(self, jobs: List[JobData]) -> Dict[str, float]:
        """Calculate daily success rates"""
        daily_stats = defaultdict(lambda: {"total": 0, "success": 0})
        
        for job in jobs:
            try:
                job_date = datetime.fromisoformat(job.created.replace('Z', '+00:00')).date()
                day_key = job_date.isoformat()
                
                daily_stats[day_key]["total"] += 1
                if job.status == "completed":
                    daily_stats[day_key]["success"] += 1
                    
            except Exception:
                continue
        
        return {
            day: (stats["success"] / stats["total"] * 100) if stats["total"] > 0 else 0
            for day, stats in daily_stats.items()
        }
