"""
Insights Generator - Generates actionable insights and recommendations
"""
import statistics
import os
import sys
from typing import Dict, List, Any
from collections import Counter

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from .models import JobData, PerformanceInsight

class InsightsGenerator:
    """Generates insights, recommendations, and predictions from analytics data"""
    
    def generate_efficiency_insights(self, jobs: List[JobData]) -> List[PerformanceInsight]:
        """Generate actionable efficiency insights"""
        insights = []
        
        if not jobs:
            return insights
        
        completed_jobs = [job for job in jobs if job.status == "completed"]
        
        # Analyze processing times
        processing_times = [job.processing_time_ms for job in completed_jobs if job.processing_time_ms > 0]
        
        if processing_times:
            avg_time = statistics.mean(processing_times)
            
            # High processing time insight
            if avg_time > 10000:  # > 10 seconds
                insights.append(PerformanceInsight(
                    type="performance",
                    title="High Processing Times Detected",
                    description=f"Average processing time is {avg_time/1000:.1f} seconds, which is above optimal range.",
                    severity="warning",
                    action_items=[
                        "Consider using faster AI models for Child AI",
                        "Implement batch processing optimization",
                        "Review API rate limiting settings"
                    ],
                    confidence=0.8
                ))
        
        # Analyze failure patterns
        failed_jobs = [job for job in jobs if job.status == "failed"]
        if len(failed_jobs) / len(jobs) > 0.1:  # > 10% failure rate
            insights.append(PerformanceInsight(
                type="reliability",
                title="High Failure Rate Detected",
                description=f"Failure rate is {len(failed_jobs)/len(jobs)*100:.1f}%, indicating system reliability issues.",
                severity="critical",
                action_items=[
                    "Review error logs for common failure patterns",
                    "Implement better error handling and retries",
                    "Consider API key rotation or limits increase"
                ],
                confidence=0.9
            ))
        
        # Model performance insights
        model_usage = Counter()
        model_efficiency = {}
        
        for job in completed_jobs:
            if job.processing_time_ms > 0 and job.total_texts > 0:
                model = job.child_ai_model or job.mother_ai_model or job.ai_model_used or "Unknown"
                model_usage[model] += 1
                
                efficiency = job.total_texts / (job.processing_time_ms / 1000)
                if model not in model_efficiency:
                    model_efficiency[model] = []
                model_efficiency[model].append(efficiency)
        
        # Find best performing model
        if model_efficiency:
            model_avg_efficiency = {
                model: statistics.mean(efficiencies) 
                for model, efficiencies in model_efficiency.items()
                if len(efficiencies) > 0
            }
            
            if model_avg_efficiency:
                best_model = max(model_avg_efficiency.items(), key=lambda x: x[1])
                insights.append(PerformanceInsight(
                    type="optimization",
                    title="Model Performance Opportunity",
                    description=f"Model {best_model[0]} shows best efficiency at {best_model[1]:.2f} texts/second.",
                    severity="info",
                    action_items=[
                        f"Consider using {best_model[0]} more frequently",
                        "Analyze what makes this model more efficient",
                        "Configure model selection preferences"
                    ],
                    confidence=0.7
                ))
        
        return insights
    
    def generate_recommendations(self, analytics_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate actionable recommendations based on analytics"""
        recommendations = []
        
        # Model performance recommendations
        model_analytics = analytics_data.get("model_analytics", {})
        top_models = model_analytics.get("top_performing_models", [])
        
        if top_models:
            best_model = top_models[0]
            recommendations.append({
                "type": "model_optimization",
                "title": f"Optimize with {best_model[0]}",
                "description": f"Model {best_model[0]} shows the highest efficiency score of {best_model[1]['efficiency_score']:.1f}",
                "priority": "high",
                "impact": "performance",
                "expected_improvement": "15-25% faster processing"
            })
        
        # Quality recommendations
        quality_metrics = analytics_data.get("quality_metrics", {})
        avg_confidence = quality_metrics.get("overall_confidence", {}).get("average", 0)
        
        if avg_confidence < 0.7:
            recommendations.append({
                "type": "quality_improvement",
                "title": "Improve Classification Confidence",
                "description": f"Average confidence score is {avg_confidence:.2f}, consider refining instructions or using better models",
                "priority": "medium",
                "impact": "quality",
                "expected_improvement": "10-20% higher confidence scores"
            })
        
        # Performance recommendations
        perf_metrics = analytics_data.get("performance_metrics", {})
        avg_processing_time = perf_metrics.get("avg_processing_time_ms", 0)
        
        if avg_processing_time > 5000:  # > 5 seconds
            recommendations.append({
                "type": "performance_optimization",
                "title": "Reduce Processing Time",
                "description": f"Average processing time of {avg_processing_time/1000:.1f}s can be optimized",
                "priority": "medium",
                "impact": "user_experience",
                "expected_improvement": "30-50% faster response times"
            })
        
        # System health recommendations
        system_health = analytics_data.get("system_health", {})
        health_score = system_health.get("score", 100)
        
        if health_score < 80:
            recommendations.append({
                "type": "system_health",
                "title": "Improve System Health",
                "description": f"System health score is {health_score}/100, indicating areas for improvement",
                "priority": "high",
                "impact": "reliability",
                "expected_improvement": "Better system stability and uptime"
            })
        
        # Cost optimization recommendations
        cost_analysis = analytics_data.get("cost_analysis", {})
        total_cost = cost_analysis.get("total_estimated_cost", 0)
        
        if total_cost > 0.1:  # If spending more than $0.10
            recommendations.append({
                "type": "cost_optimization",
                "title": "Optimize API Costs",
                "description": f"Current estimated cost is ${total_cost:.3f}, consider cost-effective models",
                "priority": "low",
                "impact": "cost_savings",
                "expected_improvement": "20-40% cost reduction"
            })
        
        return recommendations
    
    def generate_predictions(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Generate predictive analytics"""
        if len(jobs) < 7:  # Need at least a week of data
            return {"error": "Insufficient data for predictions"}
        
        from collections import defaultdict
        from scipy import stats
        
        # Daily job count trend
        daily_counts = defaultdict(int)
        for job in jobs:
            try:
                from datetime import datetime
                job_date = datetime.fromisoformat(job.created).replace(tzinfo=None)
                day_key = job_date.strftime("%Y-%m-%d")
                daily_counts[day_key] += 1
            except:
                continue
        
        # Simple linear regression for job count prediction
        days = list(range(len(daily_counts)))
        counts = list(daily_counts.values())
        
        if len(days) > 1:
            slope, intercept, r_value, p_value, std_err = stats.linregress(days, counts)
            
            # Predict next 7 days
            predictions = []
            for i in range(7):
                predicted_count = slope * (len(days) + i) + intercept
                predictions.append(max(0, int(predicted_count)))
            
            return {
                "next_7_days_job_count": predictions,
                "trend_strength": abs(r_value),
                "confidence": 1 - p_value if p_value < 1 else 0,
                "predicted_weekly_total": sum(predictions),
                "trend_direction": "increasing" if slope > 0 else "decreasing" if slope < 0 else "stable"
            }
        
        return {"error": "Insufficient data for trend analysis"}
    
    def calculate_cost_analysis(self, jobs: List[JobData]) -> Dict[str, Any]:
        """Calculate cost analysis based on API usage"""
        # Approximate costs per 1000 tokens for different models
        model_costs = {
            "deepseek": 0.0002,
            "mistral": 0.0005,
            "llama": 0.0001,
            "gemini": 0.0001,
            "gpt-4": 0.03,
            "gpt-3.5": 0.002
        }
        
        total_cost = 0
        model_usage_costs = Counter()
        
        for job in jobs:
            if job.total_texts == 0:
                continue
            
            # Estimate tokens (rough approximation: 1 text = 100 tokens)
            estimated_tokens = job.total_texts * 100
            
            # Get model used
            model_name = job.child_ai_model or job.mother_ai_model or job.ai_model_used
            
            if model_name and estimated_tokens > 0:
                # Find matching cost
                cost_per_1k = 0.001  # default
                for model_prefix, cost in model_costs.items():
                    if model_prefix.lower() in model_name.lower():
                        cost_per_1k = cost
                        break
                
                job_cost = (estimated_tokens / 1000) * cost_per_1k
                total_cost += job_cost
                model_usage_costs[model_name] += job_cost
        
        cost_optimization_suggestions = []
        if model_usage_costs:
            most_expensive = model_usage_costs.most_common(1)[0]
            cost_optimization_suggestions.append(
                f"Consider alternatives to {most_expensive[0]} to reduce costs (${most_expensive[1]:.4f} spent)"
            )
        
        return {
            "total_estimated_cost": round(total_cost, 4),
            "cost_per_job": round(total_cost / len(jobs), 4) if jobs else 0,
            "model_cost_breakdown": dict(model_usage_costs),
            "cost_optimization_suggestions": cost_optimization_suggestions
        }
    
    def generate_health_recommendations(self, health_score: float, error_rates: Dict) -> List[Dict[str, Any]]:
        """Generate health improvement recommendations"""
        recommendations = []
        
        if health_score < 70:
            recommendations.append({
                "title": "System Health Alert",
                "description": f"Overall system health score is {health_score:.1f}/100. Immediate attention required.",
                "priority": "high",
                "action_items": [
                    "Review recent error logs",
                    "Check system resource utilization",
                    "Verify API key validity and limits"
                ]
            })
        
        if error_rates:
            most_common_error = max(error_rates.items(), key=lambda x: x[1])
            recommendations.append({
                "title": "Error Pattern Detected",
                "description": f"Most frequent error: {most_common_error[0]} ({most_common_error[1]} occurrences)",
                "priority": "medium",
                "action_items": [
                    f"Investigate {most_common_error[0]} error pattern",
                    "Implement specific error handling",
                    "Consider preventive measures"
                ]
            })
        
        recommendations.extend([
            {
                "title": "Performance Monitoring",
                "description": "Implement continuous health monitoring",
                "priority": "low",
                "action_items": [
                    "Set up automated health checks",
                    "Configure performance alerts",
                    "Establish baseline metrics"
                ]
            }
        ])
        
        return recommendations
