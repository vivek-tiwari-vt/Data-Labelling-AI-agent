"""
Analytics Core - Main orchestrator for the visual creator system
"""
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from .models import AnalyticsResponse, JobData
from .data_collector import DataCollector
from .metrics_calculator import MetricsCalculator
from .chart_data_processor import ChartDataProcessor
from .insights_generator import InsightsGenerator
from .report_generator import ReportGenerator
from .analytics_database import AnalyticsDatabase
from .trends_analyzer import TrendsAnalyzer

class AnalyticsCore:
    """Main analytics engine that orchestrates all components"""
    
    def __init__(self):
        self.data_dir = Path("/Volumes/DATA/Projects/data_label_agent/data")
        
        # Initialize components
        self.data_collector = DataCollector()
        self.metrics_calculator = MetricsCalculator(self.data_collector)
        self.chart_processor = ChartDataProcessor(self.data_collector)
        self.insights_generator = InsightsGenerator()
        self.report_generator = ReportGenerator()
        self.analytics_database = AnalyticsDatabase(self.data_dir)
        self.trends_analyzer = TrendsAnalyzer()
    
    
    def generate_comprehensive_analytics(self, time_period: str = "7d") -> Dict[str, Any]:
        """Generate comprehensive analytics for the specified time period"""
        try:
            # Get job data
            jobs = self.data_collector.get_jobs_for_period(time_period)
            
            if not jobs:
                return self._empty_analytics_response(time_period)
            
            # Calculate all metrics
            performance_metrics = self.metrics_calculator.calculate_performance_metrics(jobs)
            model_analytics = self.metrics_calculator.calculate_model_analytics(jobs)
            quality_metrics = self.metrics_calculator.calculate_quality_metrics(jobs)
            advanced_analytics = self.metrics_calculator.calculate_advanced_analytics(jobs)
            system_health = self.metrics_calculator.assess_system_health(jobs)
            productivity_metrics = self.metrics_calculator.calculate_productivity_metrics(jobs)
            
            # Generate insights and recommendations
            efficiency_insights = self.insights_generator.generate_efficiency_insights(jobs)
            cost_analysis = self.insights_generator.calculate_cost_analysis(jobs)
            predictions = self.insights_generator.generate_predictions(jobs)
            
            # Prepare chart data
            charts_data = self.chart_processor.prepare_charts_data(jobs)
            
            # Calculate trends using the dedicated analyzer
            trend_analysis = self.trends_analyzer.calculate_trends(jobs)
            
            # Create analytics data for recommendations
            analytics_for_recommendations = {
                "performance_metrics": performance_metrics,
                "model_analytics": model_analytics,
                "quality_metrics": quality_metrics,
                "system_health": system_health,
                "cost_analysis": cost_analysis
            }
            
            recommendations = self.insights_generator.generate_recommendations(analytics_for_recommendations)
            
            # Compile comprehensive analytics
            analytics = {
                "time_period": time_period,
                "total_jobs": len(jobs),
                "performance_metrics": performance_metrics,
                "model_analytics": model_analytics,
                "quality_metrics": quality_metrics,
                "trend_analysis": trend_analysis,
                "efficiency_insights": efficiency_insights,
                "cost_analysis": cost_analysis,
                "predictions": predictions,
                "recommendations": recommendations,
                "charts_data": charts_data,
                "advanced_analytics": advanced_analytics,
                "system_health": system_health,
                "productivity_metrics": productivity_metrics,
                "generated_at": datetime.now().isoformat()
            }
            
            # Store analytics metrics in database
            self.analytics_database.store_metrics(analytics)
            self.analytics_database.store_analytics_snapshot(analytics, time_period)
            
            # Store insights
            if efficiency_insights:
                insights_for_db = [
                    {
                        "type": insight.type,
                        "title": insight.title,
                        "description": insight.description,
                        "severity": insight.severity,
                        "action_items": insight.action_items,
                        "confidence": insight.confidence
                    }
                    for insight in efficiency_insights
                ]
                self.analytics_database.store_insights(insights_for_db)
            
            return analytics
            
        except Exception as e:
            print(f"Error generating analytics: {e}")
            return self._empty_analytics_response(time_period)
    
    def _empty_analytics_response(self, time_period: str = "7d") -> Dict[str, Any]:
        """Return empty analytics response when no data is available"""
        return {
            "time_period": time_period,
            "total_jobs": 0,
            "performance_metrics": {"error": "No data available"},
            "model_analytics": {"error": "No data available"},
            "quality_metrics": {"error": "No data available"},
            "trend_analysis": {"error": "No data available"},
            "efficiency_insights": [],
            "cost_analysis": {"total_estimated_cost": 0},
            "predictions": {"error": "Insufficient data"},
            "recommendations": [],
            "charts_data": {},
            "advanced_analytics": {"error": "No data available"},
            "system_health": {"score": 100, "status": "No data"},
            "productivity_metrics": {"error": "No data available"},
            "generated_at": datetime.now().isoformat()
        }
    
    def export_analytics_report(self, analytics_data: Dict[str, Any], format_type: str = "pdf") -> bytes:
        """Export analytics report using the report generator"""
        return self.report_generator.generate_pdf_report(analytics_data, format_type)
    
    def get_historical_metrics(self, metric_name: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get historical metrics from the database"""
        return self.analytics_database.get_historical_metrics(metric_name, days)
    
    def get_recent_insights(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get recent insights from the database"""
        return self.analytics_database.get_recent_insights(days)
    
    def _calculate_trends(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate trend analysis over time"""
        from collections import defaultdict
        import statistics
        
        # Group jobs by day
        daily_stats = defaultdict(lambda: {"jobs": 0, "success": 0, "total_texts": 0, "avg_time": 0})
        
        for job in jobs:
            try:
                job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                day_key = job_date.strftime("%Y-%m-%d")
                
                daily_stats[day_key]["jobs"] += 1
                if job.status == "completed":
                    daily_stats[day_key]["success"] += 1
                
                daily_stats[day_key]["total_texts"] += job.total_texts
                
                if job.processing_time_ms > 0:
                    daily_stats[day_key]["avg_time"] += job.processing_time_ms
            except:
                continue
        
        # Calculate success rate trends
        trend_data = []
        for day, stats in sorted(daily_stats.items()):
            success_rate = (stats["success"] / stats["jobs"] * 100) if stats["jobs"] > 0 else 0
            avg_processing_time = (stats["avg_time"] / stats["jobs"]) if stats["jobs"] > 0 else 0
            
            trend_data.append({
                "date": day,
                "jobs_count": stats["jobs"],
                "success_rate": success_rate,
                "total_texts": stats["total_texts"],
                "avg_processing_time": avg_processing_time
            })
        
        # Analyze trends
        if len(trend_data) >= 3:
            success_rates = [d["success_rate"] for d in trend_data[-7:]]  # Last 7 days
            recent_trend = "improving" if len(success_rates) > 1 and success_rates[-1] > success_rates[0] else "stable"
        else:
            recent_trend = "insufficient_data"
        
        return {
            "daily_trends": trend_data,
            "trend_analysis": {"recent_trend": recent_trend, "confidence": 0.7},
            "predictions": {"next_week_jobs": "stable", "confidence": 0.6}
        }
    
    def _empty_analytics_response(self, time_period: str = "7d") -> Dict[str, Any]:
        """Return empty analytics response when no data is available"""
        return {
            "time_period": time_period,
            "total_jobs": 0,
            "performance_metrics": {"error": "No data available"},
            "model_analytics": {"error": "No data available"},
            "quality_metrics": {"error": "No data available"},
            "trend_analysis": {"error": "No data available"},
            "efficiency_insights": [],
            "cost_analysis": {"total_estimated_cost": 0},
            "predictions": {"error": "Insufficient data"},
            "recommendations": [],
            "charts_data": {
                "daily_jobs": {"dates": [], "job_counts": [], "success_counts": []},
                "confidence_distribution": {"bins": [], "counts": []},
                "model_usage": {"models": [], "counts": []}
            },
            "advanced_analytics": {"error": "No data available"},
            "system_health": {"status": "unknown", "score": 0},
            "productivity_metrics": {"error": "No data available"}
        }
    
    def _store_analytics_metrics(self, analytics: Dict[str, Any]):
        """Store analytics metrics in database for historical tracking"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            timestamp = datetime.now().isoformat()
            
            # Store key metrics
            metrics_to_store = [
                ("total_jobs", analytics.get("total_jobs", 0)),
                ("success_rate", analytics.get("performance_metrics", {}).get("success_rate", 0)),
                ("avg_processing_time", analytics.get("performance_metrics", {}).get("avg_processing_time_ms", 0)),
                ("total_cost", analytics.get("cost_analysis", {}).get("total_estimated_cost", 0))
            ]
            
            for name, value in metrics_to_store:
                cursor.execute(
                    "INSERT INTO metrics (name, value, timestamp) VALUES (?, ?, ?)",
                    (name, value, timestamp)
                )
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Error storing analytics metrics: {e}")
    
    def get_historical_metrics(self, metric_name: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get historical metrics from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT value, timestamp FROM metrics WHERE name = ? ORDER BY timestamp DESC LIMIT ?",
                (metric_name, days)
            )
            
            results = cursor.fetchall()
            conn.close()
            
            return [{"value": row[0], "timestamp": row[1]} for row in results]
            
        except Exception as e:
            print(f"Error retrieving historical metrics: {e}")
            return []
