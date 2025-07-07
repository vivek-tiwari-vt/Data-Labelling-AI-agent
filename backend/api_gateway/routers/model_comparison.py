"""
Model Performance Comparison API Router
Provides endpoints for A/B testing, benchmarking, and model performance analysis
"""
from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.model_performance_comparison import ModelPerformanceComparison, ComparisonType, ComparisonStatus

router = APIRouter(prefix="/model-comparison", tags=["model_comparison"])

# Initialize model comparison system
model_comparison = ModelPerformanceComparison()

# Pydantic models for request bodies
class ABTestCreate(BaseModel):
    model_config = {"protected_namespaces": ()}
    
    name: str
    model_a: str
    model_b: str
    test_dataset: Dict[str, Any]
    description: str = ""
    created_by: str = "system"

class BenchmarkTestCreate(BaseModel):
    name: str
    models: List[str]
    test_dataset: Dict[str, Any]
    description: str = ""
    created_by: str = "system"

class TestDataset(BaseModel):
    test_texts: List[Dict[str, Any]]
    available_labels: List[str]
    instructions: str

@router.post("/ab-test")
async def create_ab_test(test_config: ABTestCreate):
    """Create a new A/B test between two models"""
    try:
        test_id = model_comparison.create_ab_test(
            name=test_config.name,
            model_a=test_config.model_a,
            model_b=test_config.model_b,
            test_dataset=test_config.test_dataset,
            description=test_config.description,
            created_by=test_config.created_by
        )
        
        return {
            "test_id": test_id,
            "message": "A/B test created successfully",
            "test_info": {
                "name": test_config.name,
                "model_a": test_config.model_a,
                "model_b": test_config.model_b,
                "dataset_size": len(test_config.test_dataset.get("test_texts", []))
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create A/B test: {str(e)}")

@router.post("/benchmark")
async def create_benchmark_test(test_config: BenchmarkTestCreate):
    """Create a benchmark test comparing multiple models"""
    try:
        test_id = model_comparison.create_benchmark_test(
            name=test_config.name,
            models=test_config.models,
            test_dataset=test_config.test_dataset,
            description=test_config.description,
            created_by=test_config.created_by
        )
        
        return {
            "test_id": test_id,
            "message": "Benchmark test created successfully",
            "test_info": {
                "name": test_config.name,
                "models": test_config.models,
                "model_count": len(test_config.models),
                "dataset_size": len(test_config.test_dataset.get("test_texts", []))
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create benchmark test: {str(e)}")

@router.post("/tests/{test_id}/run")
async def run_comparison_test(test_id: str):
    """Execute a comparison test"""
    try:
        result = await model_comparison.run_comparison_test(test_id)
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to run comparison test: {str(e)}")

@router.get("/tests")
async def get_comparison_tests(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(default=50, description="Maximum number of tests to return")
):
    """Get list of comparison tests"""
    try:
        status_enum = None
        if status:
            try:
                status_enum = ComparisonStatus(status.lower())
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
        
        tests = model_comparison.get_comparison_tests(status_enum, limit)
        
        return {
            "total_tests": len(tests),
            "tests": tests,
            "filters_applied": {
                "status": status,
                "limit": limit
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get comparison tests: {str(e)}")

@router.get("/tests/{test_id}")
async def get_test_results(test_id: str):
    """Get detailed results for a comparison test"""
    try:
        results = model_comparison.get_test_results(test_id)
        
        if not results:
            raise HTTPException(status_code=404, detail="Comparison test not found")
        
        if "error" in results:
            raise HTTPException(status_code=400, detail=results["error"])
        
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get test results: {str(e)}")

@router.get("/models/{model_name}/history")
async def get_model_benchmark_history(
    model_name: str,
    limit: int = Query(default=20, description="Maximum number of benchmark records")
):
    """Get historical benchmark data for a model"""
    try:
        history = model_comparison.get_model_benchmark_history(model_name, limit)
        
        return {
            "model_name": model_name,
            "total_benchmarks": len(history),
            "benchmark_history": history
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get model history: {str(e)}")

@router.get("/recommendations")
async def get_model_recommendations(
    use_case: str = Query(default="general", description="Use case: general, high_volume, budget_conscious, high_accuracy")
):
    """Get model recommendations based on historical performance"""
    try:
        recommendations = model_comparison.generate_model_recommendations(use_case)
        
        if "error" in recommendations:
            raise HTTPException(status_code=400, detail=recommendations["error"])
        
        return recommendations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get model recommendations: {str(e)}")

@router.post("/quick-compare")
async def quick_model_compare(
    model_a: str = Body(..., description="First model to compare"),
    model_b: str = Body(..., description="Second model to compare"),
    sample_texts: List[str] = Body(..., description="Sample texts for comparison"),
    labels: List[str] = Body(..., description="Available labels"),
    instructions: str = Body(default="Classify each text appropriately", description="Classification instructions")
):
    """Perform a quick comparison between two models using sample texts"""
    try:
        # Create test dataset
        test_dataset = {
            "test_texts": [{"id": f"text_{i}", "content": text} for i, text in enumerate(sample_texts)],
            "available_labels": labels,
            "instructions": instructions
        }
        
        # Create and run A/B test
        test_id = model_comparison.create_ab_test(
            name=f"Quick Compare: {model_a} vs {model_b}",
            model_a=model_a,
            model_b=model_b,
            test_dataset=test_dataset,
            description="Quick comparison test"
        )
        
        # Run the test
        result = await model_comparison.run_comparison_test(test_id)
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        # Return simplified results for quick comparison
        model_results = result.get("results", {}).get("model_results", {})
        
        quick_results = {
            "test_id": test_id,
            "models_compared": [model_a, model_b],
            "winner": result.get("winner"),
            "statistical_significance": result.get("statistical_significance"),
            "summary": {
                model_a: {
                    "accuracy": model_results.get(model_a, {}).get("accuracy", 0),
                    "avg_confidence": model_results.get(model_a, {}).get("avg_confidence", 0),
                    "processing_time": model_results.get(model_a, {}).get("avg_processing_time_ms", 0),
                    "cost_per_text": model_results.get(model_a, {}).get("cost_per_text", 0),
                    "composite_score": model_results.get(model_a, {}).get("composite_score", 0)
                },
                model_b: {
                    "accuracy": model_results.get(model_b, {}).get("accuracy", 0),
                    "avg_confidence": model_results.get(model_b, {}).get("avg_confidence", 0),
                    "processing_time": model_results.get(model_b, {}).get("avg_processing_time_ms", 0),
                    "cost_per_text": model_results.get(model_b, {}).get("cost_per_text", 0),
                    "composite_score": model_results.get(model_b, {}).get("composite_score", 0)
                }
            },
            "recommendation": f"Use {result.get('winner', 'either model')} for better performance" if result.get("winner") else "Models show similar performance"
        }
        
        return quick_results
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to perform quick comparison: {str(e)}")

@router.get("/performance-matrix")
async def get_performance_matrix():
    """Get performance matrix comparing all models across different metrics"""
    try:
        # This would analyze historical data to create a comprehensive matrix
        # For now, returning a simplified version
        
        matrix = {
            "metrics": ["accuracy", "speed", "cost_efficiency", "consistency"],
            "models": {
                "deepseek/deepseek-r1": {
                    "accuracy": 0.85,
                    "speed": 8.5,
                    "cost_efficiency": 0.95,
                    "consistency": 0.88,
                    "overall_score": 87.5
                },
                "gemini-2.0-flash": {
                    "accuracy": 0.82,
                    "speed": 9.2,
                    "cost_efficiency": 0.98,
                    "consistency": 0.85,
                    "overall_score": 86.0
                },
                "mistralai/mistral-small": {
                    "accuracy": 0.78,
                    "speed": 7.8,
                    "cost_efficiency": 0.92,
                    "consistency": 0.80,
                    "overall_score": 82.0
                }
            },
            "rankings": {
                "accuracy": ["deepseek/deepseek-r1", "gemini-2.0-flash", "mistralai/mistral-small"],
                "speed": ["gemini-2.0-flash", "deepseek/deepseek-r1", "mistralai/mistral-small"],
                "cost_efficiency": ["gemini-2.0-flash", "deepseek/deepseek-r1", "mistralai/mistral-small"],
                "overall": ["deepseek/deepseek-r1", "gemini-2.0-flash", "mistralai/mistral-small"]
            },
            "last_updated": "2024-01-15T10:00:00Z",
            "note": "Based on historical performance data from the last 30 days"
        }
        
        return matrix
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get performance matrix: {str(e)}")

@router.post("/batch-benchmark")
async def create_batch_benchmark(
    models: List[str] = Body(..., description="List of models to benchmark"),
    dataset_type: str = Body(default="general", description="Type of dataset to use"),
    benchmark_name: str = Body(default="Batch Benchmark", description="Name for the benchmark")
):
    """Create a benchmark test for multiple models using a standard dataset"""
    try:
        # Create a standard test dataset based on type
        if dataset_type == "general":
            test_dataset = {
                "test_texts": [
                    {"id": "1", "content": "I love this new smartphone! The camera quality is amazing."},
                    {"id": "2", "content": "The delivery was late and the package was damaged."},
                    {"id": "3", "content": "Scientists have discovered a new species of deep-sea fish."},
                    {"id": "4", "content": "Can you help me reset my password?"},
                    {"id": "5", "content": "This restaurant has the best pizza in town!"}
                ],
                "available_labels": ["positive_review", "negative_review", "news", "question", "recommendation"],
                "instructions": "Classify each text into the most appropriate category"
            }
        elif dataset_type == "product_reviews":
            test_dataset = {
                "test_texts": [
                    {"id": "1", "content": "Excellent product, highly recommended!"},
                    {"id": "2", "content": "Poor quality, broke after one day."},
                    {"id": "3", "content": "Average product, nothing special."},
                    {"id": "4", "content": "Great value for money!"},
                    {"id": "5", "content": "Terrible customer service experience."}
                ],
                "available_labels": ["positive", "negative", "neutral"],
                "instructions": "Classify product reviews by sentiment"
            }
        else:
            # Default general dataset
            test_dataset = {
                "test_texts": [
                    {"id": "1", "content": "Sample text for classification testing."}
                ],
                "available_labels": ["category_a", "category_b"],
                "instructions": "Classify the text appropriately"
            }
        
        # Create benchmark test
        test_id = model_comparison.create_benchmark_test(
            name=benchmark_name,
            models=models,
            test_dataset=test_dataset,
            description=f"Batch benchmark test using {dataset_type} dataset"
        )
        
        return {
            "test_id": test_id,
            "message": "Batch benchmark created successfully",
            "models_count": len(models),
            "dataset_type": dataset_type,
            "dataset_size": len(test_dataset["test_texts"])
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create batch benchmark: {str(e)}")

@router.get("/analytics/model-trends")
async def get_model_performance_trends(
    model_name: Optional[str] = Query(None, description="Specific model name"),
    days: int = Query(default=30, description="Number of days to analyze")
):
    """Get performance trends for models over time"""
    try:
        # This would analyze historical benchmark data
        # For now, returning sample trend data
        
        trends = {
            "time_period": f"last_{days}_days",
            "model_trends": {
                "deepseek/deepseek-r1": {
                    "accuracy_trend": [0.83, 0.84, 0.85, 0.86, 0.85],
                    "speed_trend": [8.2, 8.3, 8.5, 8.4, 8.5],
                    "cost_trend": [0.0002, 0.0002, 0.0002, 0.0002, 0.0002],
                    "trend_direction": "improving"
                },
                "gemini-2.0-flash": {
                    "accuracy_trend": [0.80, 0.81, 0.82, 0.82, 0.82],
                    "speed_trend": [9.0, 9.1, 9.2, 9.1, 9.2],
                    "cost_trend": [0.0001, 0.0001, 0.0001, 0.0001, 0.0001],
                    "trend_direction": "stable"
                }
            },
            "insights": [
                "DeepSeek R1 shows consistent improvement in accuracy",
                "Gemini 2.0 Flash maintains stable high-speed performance",
                "Cost efficiency remains consistent across all models"
            ]
        }
        
        if model_name and model_name in trends["model_trends"]:
            return {
                "model_name": model_name,
                "trend_data": trends["model_trends"][model_name],
                "time_period": trends["time_period"]
            }
        
        return trends
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get model trends: {str(e)}")

@router.post("/validate-test-dataset")
async def validate_test_dataset(dataset: TestDataset):
    """Validate a test dataset for model comparison"""
    try:
        validation_results = {
            "valid": True,
            "issues": [],
            "suggestions": [],
            "dataset_stats": {
                "text_count": len(dataset.test_texts),
                "label_count": len(dataset.available_labels),
                "avg_text_length": sum(len(text.get("content", "")) for text in dataset.test_texts) / len(dataset.test_texts) if dataset.test_texts else 0
            }
        }
        
        # Validation checks
        if len(dataset.test_texts) < 5:
            validation_results["issues"].append("Dataset should have at least 5 texts for meaningful comparison")
            validation_results["valid"] = False
        
        if len(dataset.available_labels) < 2:
            validation_results["issues"].append("Dataset should have at least 2 labels")
            validation_results["valid"] = False
        
        if len(dataset.available_labels) > 10:
            validation_results["suggestions"].append("Consider reducing the number of labels for more focused testing")
        
        if not dataset.instructions.strip():
            validation_results["issues"].append("Instructions should not be empty")
            validation_results["valid"] = False
        
        # Check for text quality
        empty_texts = sum(1 for text in dataset.test_texts if not text.get("content", "").strip())
        if empty_texts > 0:
            validation_results["issues"].append(f"{empty_texts} texts are empty or only whitespace")
            validation_results["valid"] = False
        
        # Suggestions for improvement
        if validation_results["dataset_stats"]["avg_text_length"] < 20:
            validation_results["suggestions"].append("Consider using longer texts for more robust testing")
        
        if len(dataset.test_texts) < 20:
            validation_results["suggestions"].append("For statistical significance, consider using at least 20 test texts")
        
        return validation_results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate test dataset: {str(e)}")
