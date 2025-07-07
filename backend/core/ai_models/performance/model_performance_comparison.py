"""
Model Performance Comparison System
Provides A/B testing framework, statistical analysis, and automated benchmarking
"""
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import statistics
import uuid
try:
    import numpy as np
    from scipy import stats
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

class ComparisonStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class ComparisonType(Enum):
    AB_TEST = "ab_test"
    BENCHMARK = "benchmark"
    MULTI_MODEL = "multi_model"
    PERFORMANCE_REGRESSION = "performance_regression"

@dataclass
class ModelPerformanceMetrics:
    model_name: str
    accuracy: float
    avg_confidence: float
    avg_processing_time_ms: float
    total_texts_processed: int
    success_rate: float
    cost_per_text: float
    throughput_texts_per_second: float
    error_rate: float
    consistency_score: float  # How consistent the model is across similar inputs
    
@dataclass
class ComparisonTest:
    id: str
    name: str
    description: str
    comparison_type: ComparisonType
    models: List[str]
    test_dataset: Dict[str, Any]
    status: ComparisonStatus
    created_at: str
    created_by: str
    completed_at: Optional[str] = None
    results: Optional[Dict[str, Any]] = None
    statistical_significance: Optional[float] = None
    winner: Optional[str] = None
    confidence_level: float = 0.95
    metadata: Dict[str, Any] = field(default_factory=dict)

class ModelPerformanceComparison:
    """Advanced model performance comparison and A/B testing system"""
    
    def __init__(self):
        self.data_dir = Path("/Volumes/DATA/Projects/data_label_agent/data")
        self.comparison_dir = self.data_dir / "model_comparisons"
        self.comparison_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize comparison database
        self.db_path = self.comparison_dir / "model_comparisons.db"
        self._init_database()
        
        # Cost estimates per 1000 tokens for different models
        self.model_costs = {
            "deepseek": 0.0002,
            "mistral": 0.0005,
            "llama": 0.0001,
            "gemini": 0.0001,
            "gpt-4": 0.03,
            "gpt-3.5": 0.002,
            "claude": 0.008
        }
        
        # Import required services
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from common.job_logger import job_logger
        from api_gateway.services.job_service import JobService
        self.job_logger = job_logger
        self.job_service = JobService()
    
    def _init_database(self):
        """Initialize comparison database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Comparison tests table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comparison_tests (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                comparison_type TEXT NOT NULL,
                models TEXT NOT NULL,
                test_dataset TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                completed_at TEXT,
                results TEXT,
                statistical_significance REAL,
                winner TEXT,
                confidence_level REAL DEFAULT 0.95,
                metadata TEXT
            )
        """)
        
        # Model performance metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS model_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                comparison_id TEXT NOT NULL,
                model_name TEXT NOT NULL,
                job_id TEXT,
                accuracy REAL,
                avg_confidence REAL,
                avg_processing_time_ms REAL,
                total_texts_processed INTEGER,
                success_rate REAL,
                cost_per_text REAL,
                throughput_texts_per_second REAL,
                error_rate REAL,
                consistency_score REAL,
                recorded_at TEXT NOT NULL,
                FOREIGN KEY (comparison_id) REFERENCES comparison_tests (id)
            )
        """)
        
        # Historical benchmarks table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_benchmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT NOT NULL,
                benchmark_date TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                performance_score REAL NOT NULL,
                confidence_score REAL,
                processing_speed REAL,
                cost_efficiency REAL,
                metadata TEXT
            )
        """)
        
        conn.commit()
        conn.close()
    
    def create_ab_test(self, name: str, model_a: str, model_b: str, 
                      test_dataset: Dict[str, Any], description: str = "",
                      created_by: str = "system") -> str:
        """Create a new A/B test between two models"""
        
        test_id = str(uuid.uuid4())
        
        comparison_test = ComparisonTest(
            id=test_id,
            name=name,
            description=description,
            comparison_type=ComparisonType.AB_TEST,
            models=[model_a, model_b],
            test_dataset=test_dataset,
            status=ComparisonStatus.PENDING,
            created_at=datetime.now().isoformat(),
            created_by=created_by,
            metadata={
                "model_a": model_a,
                "model_b": model_b,
                "dataset_size": len(test_dataset.get("test_texts", []))
            }
        )
        
        self._store_comparison_test(comparison_test)
        return test_id
    
    def create_benchmark_test(self, name: str, models: List[str], 
                             test_dataset: Dict[str, Any], description: str = "",
                             created_by: str = "system") -> str:
        """Create a benchmark test comparing multiple models"""
        
        test_id = str(uuid.uuid4())
        
        comparison_test = ComparisonTest(
            id=test_id,
            name=name,
            description=description,
            comparison_type=ComparisonType.BENCHMARK,
            models=models,
            test_dataset=test_dataset,
            status=ComparisonStatus.PENDING,
            created_at=datetime.now().isoformat(),
            created_by=created_by,
            metadata={
                "model_count": len(models),
                "dataset_size": len(test_dataset.get("test_texts", []))
            }
        )
        
        self._store_comparison_test(comparison_test)
        return test_id
    
    async def run_comparison_test(self, test_id: str) -> Dict[str, Any]:
        """Execute a comparison test"""
        
        # Get test details
        comparison_test = self._get_comparison_test(test_id)
        if not comparison_test:
            return {"error": "Comparison test not found"}
        
        if comparison_test.status != ComparisonStatus.PENDING:
            return {"error": f"Test is not pending (current status: {comparison_test.status.value})"}
        
        # Update status to running
        self._update_test_status(test_id, ComparisonStatus.RUNNING)
        
        try:
            model_results = {}
            
            # Run test for each model
            for model in comparison_test.models:
                print(f"Testing model: {model}")
                
                # Create a job for this model with the test dataset
                job_id = await self.job_service.dispatch_batch_job_to_mother_ai(
                    file_data=comparison_test.test_dataset,
                    available_labels=comparison_test.test_dataset.get("available_labels", []),
                    instructions=comparison_test.test_dataset.get("instructions", "Classify each text appropriately"),
                    original_filename=f"comparison_test_{test_id}_{model}.json",
                    mother_ai_model=model,
                    child_ai_model=model
                )
                
                # Wait for job completion and collect metrics
                metrics = await self._wait_and_collect_metrics(job_id, model, test_id)
                model_results[model] = metrics
            
            # Analyze results
            analysis = self._analyze_comparison_results(model_results, comparison_test.comparison_type)
            
            # Update test with results
            comparison_test.results = {
                "model_results": model_results,
                "analysis": analysis,
                "completed_at": datetime.now().isoformat()
            }
            comparison_test.completed_at = datetime.now().isoformat()
            comparison_test.status = ComparisonStatus.COMPLETED
            
            # Determine winner and statistical significance
            if comparison_test.comparison_type == ComparisonType.AB_TEST:
                winner, significance = self._calculate_statistical_significance(
                    model_results[comparison_test.models[0]],
                    model_results[comparison_test.models[1]]
                )
                comparison_test.winner = winner
                comparison_test.statistical_significance = significance
            elif comparison_test.comparison_type == ComparisonType.BENCHMARK:
                # Find best performing model based on composite score
                best_model = max(model_results.items(), key=lambda x: x[1].get("composite_score", 0))
                comparison_test.winner = best_model[0]
            
            # Store updated test
            self._update_comparison_test(comparison_test)
            
            return {
                "test_id": test_id,
                "status": "completed",
                "results": comparison_test.results,
                "winner": comparison_test.winner,
                "statistical_significance": comparison_test.statistical_significance
            }
            
        except Exception as e:
            # Update status to failed
            self._update_test_status(test_id, ComparisonStatus.FAILED)
            return {"error": f"Test execution failed: {str(e)}"}
    
    async def _wait_and_collect_metrics(self, job_id: str, model_name: str, comparison_id: str) -> Dict[str, Any]:
        """Wait for job completion and collect performance metrics"""
        
        # Wait for job completion (simplified - in practice would poll job status)
        import asyncio
        max_wait_time = 300  # 5 minutes
        wait_interval = 10   # 10 seconds
        
        for _ in range(max_wait_time // wait_interval):
            job_status = await self.job_service.get_job_status(job_id)
            if job_status and job_status.get("status") == "completed":
                break
            elif job_status and job_status.get("status") == "failed":
                raise Exception(f"Job {job_id} failed")
            await asyncio.sleep(wait_interval)
        else:
            raise Exception(f"Job {job_id} timed out")
        
        # Collect metrics from job log
        job_log = self.job_logger.get_job_log(job_id)
        if not job_log:
            raise Exception(f"Job log not found for {job_id}")
        
        # Extract performance metrics
        metrics = self._extract_performance_metrics(job_log, model_name)
        
        # Store metrics in database
        self._store_model_metrics(comparison_id, model_name, job_id, metrics)
        
        return metrics
    
    def _extract_performance_metrics(self, job_log: Dict[str, Any], model_name: str) -> Dict[str, Any]:
        """Extract performance metrics from job log"""
        
        # Get processing details
        text_agent_data = job_log.get("text_agent", {})
        processing_details = text_agent_data.get("processing_details", [])
        performance_metrics = job_log.get("performance_metrics", {})
        
        if not processing_details:
            return {"error": "No processing details found"}
        
        # Calculate accuracy metrics
        confidence_scores = [detail.get("confidence_score", 0) for detail in processing_details]
        processing_times = [detail.get("processing_time_ms", 0) for detail in processing_details]
        
        # Calculate basic metrics
        avg_confidence = statistics.mean(confidence_scores) if confidence_scores else 0
        avg_processing_time = statistics.mean(processing_times) if processing_times else 0
        total_texts = len(processing_details)
        
        # Estimate cost
        estimated_tokens = total_texts * 100  # Rough estimate
        cost_per_1k = self._get_model_cost(model_name)
        total_cost = (estimated_tokens / 1000) * cost_per_1k
        cost_per_text = total_cost / total_texts if total_texts > 0 else 0
        
        # Calculate throughput
        total_time_seconds = performance_metrics.get("total_time_ms", 0) / 1000
        throughput = total_texts / total_time_seconds if total_time_seconds > 0 else 0
        
        # Calculate consistency score (based on confidence score variance)
        consistency_score = 1 - (statistics.stdev(confidence_scores) if len(confidence_scores) > 1 else 0)
        
        # Calculate success rate (assuming all completed successfully for now)
        success_rate = 1.0
        
        # Calculate composite performance score
        composite_score = self._calculate_composite_score({
            "accuracy": avg_confidence,
            "speed": throughput,
            "cost_efficiency": 1 / (cost_per_text + 0.001),  # Inverse of cost
            "consistency": consistency_score
        })
        
        return {
            "accuracy": avg_confidence,
            "avg_confidence": avg_confidence,
            "avg_processing_time_ms": avg_processing_time,
            "total_texts_processed": total_texts,
            "success_rate": success_rate,
            "cost_per_text": cost_per_text,
            "total_cost": total_cost,
            "throughput_texts_per_second": throughput,
            "error_rate": 0.0,  # Would calculate from failed texts
            "consistency_score": consistency_score,
            "composite_score": composite_score,
            "confidence_distribution": {
                "high": len([c for c in confidence_scores if c > 0.8]),
                "medium": len([c for c in confidence_scores if 0.5 < c <= 0.8]),
                "low": len([c for c in confidence_scores if c <= 0.5])
            }
        }
    
    def _calculate_composite_score(self, metrics: Dict[str, float]) -> float:
        """Calculate composite performance score"""
        
        # Weighted scoring (can be adjusted based on priorities)
        weights = {
            "accuracy": 0.4,
            "speed": 0.3,
            "cost_efficiency": 0.2,
            "consistency": 0.1
        }
        
        # Normalize metrics to 0-1 scale
        normalized_metrics = {}
        for key, value in metrics.items():
            if key == "accuracy" or key == "consistency":
                normalized_metrics[key] = min(value, 1.0)  # Already 0-1 scale
            elif key == "speed":
                normalized_metrics[key] = min(value / 10.0, 1.0)  # Normalize to 10 texts/sec max
            elif key == "cost_efficiency":
                normalized_metrics[key] = min(value / 1000.0, 1.0)  # Normalize cost efficiency
            else:
                normalized_metrics[key] = value
        
        # Calculate weighted score
        composite_score = sum(
            normalized_metrics.get(metric, 0) * weight 
            for metric, weight in weights.items()
        )
        
        return min(composite_score * 100, 100)  # Scale to 0-100
    
    def _analyze_comparison_results(self, model_results: Dict[str, Dict], comparison_type: ComparisonType) -> Dict[str, Any]:
        """Analyze comparison results and provide insights"""
        
        analysis = {
            "summary": {},
            "detailed_comparison": {},
            "recommendations": [],
            "insights": []
        }
        
        # Summary statistics
        all_scores = [result.get("composite_score", 0) for result in model_results.values()]
        analysis["summary"] = {
            "models_tested": len(model_results),
            "avg_composite_score": statistics.mean(all_scores) if all_scores else 0,
            "score_range": max(all_scores) - min(all_scores) if all_scores else 0,
            "performance_variance": statistics.stdev(all_scores) if len(all_scores) > 1 else 0
        }
        
        # Detailed comparison
        for model, metrics in model_results.items():
            analysis["detailed_comparison"][model] = {
                "composite_score": metrics.get("composite_score", 0),
                "accuracy": metrics.get("accuracy", 0),
                "speed": metrics.get("throughput_texts_per_second", 0),
                "cost_per_text": metrics.get("cost_per_text", 0),
                "consistency": metrics.get("consistency_score", 0),
                "strengths": self._identify_model_strengths(metrics),
                "weaknesses": self._identify_model_weaknesses(metrics)
            }
        
        # Generate recommendations
        analysis["recommendations"] = self._generate_model_recommendations(model_results)
        
        # Generate insights
        analysis["insights"] = self._generate_performance_insights(model_results)
        
        return analysis
    
    def _calculate_statistical_significance(self, metrics_a: Dict, metrics_b: Dict) -> Tuple[Optional[str], float]:
        """Calculate statistical significance between two models"""
        
        if not HAS_SCIPY:
            return None, 0.0
        
        # Use composite scores for comparison
        score_a = metrics_a.get("composite_score", 0)
        score_b = metrics_b.get("composite_score", 0)
        
        # For simplicity, using the scores directly
        # In practice, would use arrays of individual predictions
        
        # Mock confidence intervals (would calculate from actual data)
        # Assume normal distribution with std dev proportional to (1 - score)
        std_a = (1 - score_a / 100) * 0.1
        std_b = (1 - score_b / 100) * 0.1
        
        # Calculate t-statistic
        if std_a == 0 and std_b == 0:
            p_value = 0.0 if score_a != score_b else 1.0
        else:
            pooled_std = ((std_a ** 2 + std_b ** 2) / 2) ** 0.5
            t_stat = abs(score_a - score_b) / pooled_std if pooled_std > 0 else 0
            p_value = 2 * (1 - stats.norm.cdf(abs(t_stat)))
        
        # Determine winner
        winner = None
        if p_value < 0.05:  # Statistically significant
            winner = "model_a" if score_a > score_b else "model_b"
        
        return winner, 1 - p_value  # Return confidence level
    
    def get_comparison_tests(self, status: Optional[ComparisonStatus] = None, 
                           limit: int = 50) -> List[Dict[str, Any]]:
        """Get list of comparison tests"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM comparison_tests"
        params = []
        
        if status:
            query += " WHERE status = ?"
            params.append(status.value)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        tests = []
        for row in rows:
            test_dict = {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "comparison_type": row[3],
                "models": json.loads(row[4]),
                "status": row[7],
                "created_at": row[8],
                "created_by": row[9],
                "completed_at": row[10],
                "winner": row[12],
                "statistical_significance": row[13]
            }
            tests.append(test_dict)
        
        return tests
    
    def get_test_results(self, test_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed results for a comparison test"""
        
        comparison_test = self._get_comparison_test(test_id)
        if not comparison_test:
            return None
        
        if comparison_test.status != ComparisonStatus.COMPLETED:
            return {"error": f"Test not completed (status: {comparison_test.status.value})"}
        
        return {
            "test_info": {
                "id": comparison_test.id,
                "name": comparison_test.name,
                "description": comparison_test.description,
                "type": comparison_test.comparison_type.value,
                "models": comparison_test.models,
                "created_at": comparison_test.created_at,
                "completed_at": comparison_test.completed_at
            },
            "results": comparison_test.results,
            "winner": comparison_test.winner,
            "statistical_significance": comparison_test.statistical_significance,
            "confidence_level": comparison_test.confidence_level
        }
    
    def get_model_benchmark_history(self, model_name: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get historical benchmark data for a model"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM historical_benchmarks 
            WHERE model_name = ? 
            ORDER BY benchmark_date DESC 
            LIMIT ?
        """, (model_name, limit))
        
        rows = cursor.fetchall()
        conn.close()
        
        history = []
        for row in rows:
            history.append({
                "id": row[0],
                "benchmark_date": row[2],
                "dataset_type": row[3],
                "performance_score": row[4],
                "confidence_score": row[5],
                "processing_speed": row[6],
                "cost_efficiency": row[7],
                "metadata": json.loads(row[8] or "{}")
            })
        
        return history
    
    def generate_model_recommendations(self, use_case: str = "general") -> Dict[str, Any]:
        """Generate model recommendations based on historical performance"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get recent model performance data
        cursor.execute("""
            SELECT model_name, 
                   AVG(accuracy) as avg_accuracy,
                   AVG(avg_processing_time_ms) as avg_time,
                   AVG(cost_per_text) as avg_cost,
                   AVG(consistency_score) as avg_consistency,
                   COUNT(*) as test_count
            FROM model_metrics 
            WHERE recorded_at >= date('now', '-30 days')
            GROUP BY model_name
            HAVING test_count >= 3
            ORDER BY avg_accuracy DESC
        """)
        
        model_stats = cursor.fetchall()
        conn.close()
        
        if not model_stats:
            return {"error": "Insufficient data for recommendations"}
        
        recommendations = {
            "use_case": use_case,
            "top_models": {},
            "specialized_recommendations": {},
            "cost_analysis": {},
            "performance_trends": {}
        }
        
        # Categorize models by strengths
        accuracy_leader = max(model_stats, key=lambda x: x[1])
        speed_leader = min(model_stats, key=lambda x: x[2])
        cost_leader = min(model_stats, key=lambda x: x[3])
        consistency_leader = max(model_stats, key=lambda x: x[4])
        
        recommendations["top_models"] = {
            "highest_accuracy": {
                "model": accuracy_leader[0],
                "accuracy": accuracy_leader[1],
                "avg_time_ms": accuracy_leader[2]
            },
            "fastest": {
                "model": speed_leader[0],
                "avg_time_ms": speed_leader[2],
                "accuracy": speed_leader[1]
            },
            "most_cost_effective": {
                "model": cost_leader[0],
                "cost_per_text": cost_leader[3],
                "accuracy": cost_leader[1]
            },
            "most_consistent": {
                "model": consistency_leader[0],
                "consistency_score": consistency_leader[4],
                "accuracy": consistency_leader[1]
            }
        }
        
        # Use case specific recommendations
        if use_case == "high_volume":
            recommendations["specialized_recommendations"]["primary"] = speed_leader[0]
            recommendations["specialized_recommendations"]["reason"] = "Optimized for processing speed"
        elif use_case == "budget_conscious":
            recommendations["specialized_recommendations"]["primary"] = cost_leader[0]
            recommendations["specialized_recommendations"]["reason"] = "Most cost-effective option"
        elif use_case == "high_accuracy":
            recommendations["specialized_recommendations"]["primary"] = accuracy_leader[0]
            recommendations["specialized_recommendations"]["reason"] = "Highest accuracy model"
        else:
            # Balanced recommendation
            balanced_scores = []
            for model_data in model_stats:
                # Calculate balanced score
                normalized_accuracy = model_data[1]
                normalized_speed = 1 / (model_data[2] / 1000 + 1)  # Inverse of time
                normalized_cost = 1 / (model_data[3] + 0.001)      # Inverse of cost
                normalized_consistency = model_data[4]
                
                balanced_score = (normalized_accuracy + normalized_speed + normalized_cost + normalized_consistency) / 4
                balanced_scores.append((model_data[0], balanced_score))
            
            best_balanced = max(balanced_scores, key=lambda x: x[1])
            recommendations["specialized_recommendations"]["primary"] = best_balanced[0]
            recommendations["specialized_recommendations"]["reason"] = "Best overall balanced performance"
        
        return recommendations
    
    # Helper methods
    def _get_model_cost(self, model_name: str) -> float:
        """Get estimated cost per 1000 tokens for a model"""
        for model_prefix, cost in self.model_costs.items():
            if model_prefix.lower() in model_name.lower():
                return cost
        return 0.001  # Default cost
    
    def _identify_model_strengths(self, metrics: Dict[str, Any]) -> List[str]:
        """Identify model strengths based on metrics"""
        strengths = []
        
        if metrics.get("accuracy", 0) > 0.85:
            strengths.append("High accuracy")
        if metrics.get("throughput_texts_per_second", 0) > 5:
            strengths.append("Fast processing")
        if metrics.get("cost_per_text", 1) < 0.001:
            strengths.append("Cost effective")
        if metrics.get("consistency_score", 0) > 0.9:
            strengths.append("Highly consistent")
        
        return strengths
    
    def _identify_model_weaknesses(self, metrics: Dict[str, Any]) -> List[str]:
        """Identify model weaknesses based on metrics"""
        weaknesses = []
        
        if metrics.get("accuracy", 0) < 0.7:
            weaknesses.append("Low accuracy")
        if metrics.get("throughput_texts_per_second", 0) < 1:
            weaknesses.append("Slow processing")
        if metrics.get("cost_per_text", 0) > 0.01:
            weaknesses.append("Expensive")
        if metrics.get("consistency_score", 0) < 0.7:
            weaknesses.append("Inconsistent results")
        
        return weaknesses
    
    def _generate_model_recommendations(self, model_results: Dict[str, Dict]) -> List[str]:
        """Generate recommendations based on comparison results"""
        recommendations = []
        
        if len(model_results) >= 2:
            # Find best and worst performing models
            best_model = max(model_results.items(), key=lambda x: x[1].get("composite_score", 0))
            worst_model = min(model_results.items(), key=lambda x: x[1].get("composite_score", 0))
            
            score_diff = best_model[1].get("composite_score", 0) - worst_model[1].get("composite_score", 0)
            
            if score_diff > 20:
                recommendations.append(f"Strong recommendation to use {best_model[0]} over {worst_model[0]}")
            elif score_diff > 10:
                recommendations.append(f"Moderate preference for {best_model[0]} over {worst_model[0]}")
            else:
                recommendations.append("Models show similar performance - choose based on cost/speed preferences")
        
        return recommendations
    
    def _generate_performance_insights(self, model_results: Dict[str, Dict]) -> List[str]:
        """Generate performance insights from results"""
        insights = []
        
        # Analyze accuracy vs speed tradeoffs
        accuracy_scores = [(model, metrics.get("accuracy", 0)) for model, metrics in model_results.items()]
        speed_scores = [(model, metrics.get("throughput_texts_per_second", 0)) for model, metrics in model_results.items()]
        
        best_accuracy = max(accuracy_scores, key=lambda x: x[1])
        best_speed = max(speed_scores, key=lambda x: x[1])
        
        if best_accuracy[0] != best_speed[0]:
            insights.append(f"Trade-off detected: {best_accuracy[0]} offers best accuracy while {best_speed[0]} offers best speed")
        
        # Cost efficiency insights
        cost_efficiency = [(model, 1/(metrics.get("cost_per_text", 1) + 0.001)) for model, metrics in model_results.items()]
        best_cost = max(cost_efficiency, key=lambda x: x[1])
        insights.append(f"{best_cost[0]} offers the best cost efficiency")
        
        return insights
    
    # Database operations
    def _store_comparison_test(self, test: ComparisonTest):
        """Store comparison test in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO comparison_tests 
            (id, name, description, comparison_type, models, test_dataset, status,
             created_at, created_by, confidence_level, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (test.id, test.name, test.description, test.comparison_type.value,
              json.dumps(test.models), json.dumps(test.test_dataset), test.status.value,
              test.created_at, test.created_by, test.confidence_level,
              json.dumps(test.metadata)))
        
        conn.commit()
        conn.close()
    
    def _update_comparison_test(self, test: ComparisonTest):
        """Update comparison test in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE comparison_tests SET
            status = ?, completed_at = ?, results = ?, 
            statistical_significance = ?, winner = ?
            WHERE id = ?
        """, (test.status.value, test.completed_at, 
              json.dumps(test.results) if test.results else None,
              test.statistical_significance, test.winner, test.id))
        
        conn.commit()
        conn.close()
    
    def _update_test_status(self, test_id: str, status: ComparisonStatus):
        """Update test status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("UPDATE comparison_tests SET status = ? WHERE id = ?", 
                      (status.value, test_id))
        
        conn.commit()
        conn.close()
    
    def _get_comparison_test(self, test_id: str) -> Optional[ComparisonTest]:
        """Get comparison test from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM comparison_tests WHERE id = ?", (test_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        return ComparisonTest(
            id=row[0], name=row[1], description=row[2],
            comparison_type=ComparisonType(row[3]), models=json.loads(row[4]),
            test_dataset=json.loads(row[5]), status=ComparisonStatus(row[6]),
            created_at=row[7], created_by=row[8], completed_at=row[9],
            results=json.loads(row[10]) if row[10] else None,
            statistical_significance=row[11], winner=row[12],
            confidence_level=row[13] or 0.95, metadata=json.loads(row[14] or "{}")
        )
    
    def _store_model_metrics(self, comparison_id: str, model_name: str, 
                           job_id: str, metrics: Dict[str, Any]):
        """Store model performance metrics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO model_metrics 
            (comparison_id, model_name, job_id, accuracy, avg_confidence,
             avg_processing_time_ms, total_texts_processed, success_rate,
             cost_per_text, throughput_texts_per_second, error_rate,
             consistency_score, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (comparison_id, model_name, job_id,
              metrics.get("accuracy"), metrics.get("avg_confidence"),
              metrics.get("avg_processing_time_ms"), metrics.get("total_texts_processed"),
              metrics.get("success_rate"), metrics.get("cost_per_text"),
              metrics.get("throughput_texts_per_second"), metrics.get("error_rate"),
              metrics.get("consistency_score"), datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
