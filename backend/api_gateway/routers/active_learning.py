"""
Active Learning API Router
Provides endpoints for active learning functionality and uncertain sample identification
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from core.ai_models.active_learning import ActiveLearningSystem

router = APIRouter(tags=["active-learning"])

# Initialize active learning system
active_learning = ActiveLearningSystem()

@router.post("/analyze")
async def analyze_predictions(analysis_request: Dict[str, Any]):
    """Analyze predictions and identify samples for active learning"""
    try:
        # Validate required fields
        required_fields = ['job_id', 'predictions']
        for field in required_fields:
            if field not in analysis_request:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        job_id = analysis_request['job_id']
        predictions = analysis_request['predictions']
        strategy = analysis_request.get('strategy', 'uncertainty')
        sample_count = analysis_request.get('sample_count', 10)
        
        # Convert string strategy to enum
        from core.ai_models.active_learning import UncertaintyStrategy
        
        strategy_map = {
            'uncertainty': UncertaintyStrategy.CONFIDENCE_BASED,
            'confidence': UncertaintyStrategy.CONFIDENCE_BASED,
            'diversity': UncertaintyStrategy.DIVERSITY_BASED,
            'committee': UncertaintyStrategy.QUERY_BY_COMMITTEE,
            'sampling': UncertaintyStrategy.UNCERTAINTY_SAMPLING
        }
        
        strategy_enum = strategy_map.get(strategy, UncertaintyStrategy.CONFIDENCE_BASED)
        
        # Analyze predictions
        result = active_learning.analyze_job_for_active_learning(
            job_id=job_id,
            strategy=strategy_enum,
            max_items=sample_count
        )
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to analyze predictions: {str(e)}")

@router.get("/suggestions/{job_id}")
async def get_learning_suggestions(
    job_id: str,
    strategy: str = Query(default="uncertainty", description="Learning strategy: uncertainty, diversity, committee"),
    count: int = Query(default=10, description="Number of suggestions to return")
):
    """Get active learning suggestions for a job"""
    try:
        suggestions = active_learning.get_learning_suggestions(
            job_id=job_id,
            strategy=strategy,
            count=count
        )
        
        return {"status": "success", "suggestions": suggestions}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get suggestions: {str(e)}")

@router.post("/feedback")
async def submit_feedback(feedback_data: Dict[str, Any]):
    """Submit human feedback for active learning samples"""
    try:
        # Validate required fields
        required_fields = ['job_id', 'sample_id', 'feedback']
        for field in required_fields:
            if field not in feedback_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        result = active_learning.submit_feedback(
            job_id=feedback_data['job_id'],
            sample_id=feedback_data['sample_id'],
            feedback=feedback_data['feedback']
        )
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to submit feedback: {str(e)}")

@router.get("/analytics")
async def get_active_learning_analytics(
    days: int = Query(default=7, description="Number of days for analytics")
):
    """Get active learning analytics and metrics"""
    try:
        analytics = active_learning.get_analytics(days)
        return {"status": "success", "analytics": analytics}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analytics: {str(e)}")

@router.get("/strategies")
async def get_learning_strategies():
    """Get available active learning strategies"""
    return {
        "status": "success",
        "strategies": [
            {
                "name": "uncertainty",
                "description": "Select samples with highest prediction uncertainty",
                "best_for": "General purpose uncertainty reduction"
            },
            {
                "name": "diversity",
                "description": "Select diverse samples using clustering",
                "best_for": "Exploring different data regions"
            },
            {
                "name": "committee",
                "description": "Select samples with highest disagreement among models",
                "best_for": "Ensemble model improvement"
            },
            {
                "name": "margin",
                "description": "Select samples with smallest margin between top predictions",
                "best_for": "Binary and multi-class classification"
            },
            {
                "name": "entropy",
                "description": "Select samples with highest prediction entropy",
                "best_for": "Multi-class classification with many labels"
            }
        ]
    }

@router.get("/jobs/{job_id}/progress")
async def get_learning_progress(job_id: str):
    """Get active learning progress for a specific job"""
    try:
        progress = active_learning.get_learning_progress(job_id)
        return {"status": "success", "progress": progress}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get learning progress: {str(e)}")

@router.post("/batch-analyze")
async def batch_analyze_jobs(batch_request: Dict[str, Any]):
    """Analyze multiple jobs for active learning opportunities"""
    try:
        job_ids = batch_request.get('job_ids', [])
        if not job_ids:
            raise HTTPException(status_code=400, detail="No job IDs provided")
        
        strategy = batch_request.get('strategy', 'uncertainty')
        sample_count = batch_request.get('sample_count', 5)
        
        results = {}
        for job_id in job_ids:
            try:
                suggestions = active_learning.get_learning_suggestions(
                    job_id=job_id,
                    strategy=strategy,
                    count=sample_count
                )
                results[job_id] = {
                    "status": "success",
                    "suggestions": suggestions
                }
            except Exception as e:
                results[job_id] = {
                    "status": "error",
                    "message": str(e)
                }
        
        return {"status": "success", "results": results}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to batch analyze: {str(e)}")

@router.get("/dashboard")
async def get_active_learning_dashboard():
    """Get dashboard data for active learning overview"""
    try:
        analytics = active_learning.get_analytics(30)  # 30 days of data
        
        # Extract dashboard-specific metrics
        dashboard_data = {
            "overview": {
                "total_analyses": analytics.get("total_analyses", 0),
                "pending_reviews": analytics.get("pending_reviews", 0),
                "feedback_received": analytics.get("feedback_received", 0),
                "improvement_rate": analytics.get("improvement_rate", 0)
            },
            "strategy_usage": analytics.get("strategy_usage", {}),
            "recent_suggestions": analytics.get("recent_suggestions", [])[:10],
            "job_progress": analytics.get("job_progress", {}),
            "trends": {
                "daily_analyses": analytics.get("daily_analyses", []),
                "feedback_trends": analytics.get("feedback_trends", [])
            }
        }
        
        return {"status": "success", "dashboard": dashboard_data}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard data: {str(e)}")

@router.post("/analyze-job/{job_id}")
async def analyze_job_predictions(
    job_id: str,
    strategy: str = Query(default="uncertainty", description="Learning strategy"),
    sample_count: int = Query(default=10, description="Number of samples to return"),
    confidence_threshold: float = Query(default=0.7, description="Confidence threshold")
):
    """Analyze a completed job's predictions for active learning opportunities"""
    try:
        # Convert string strategy to enum
        from core.ai_models.active_learning import UncertaintyStrategy
        
        strategy_map = {
            'uncertainty': UncertaintyStrategy.CONFIDENCE_BASED,
            'confidence': UncertaintyStrategy.CONFIDENCE_BASED,
            'diversity': UncertaintyStrategy.DIVERSITY_BASED,
            'committee': UncertaintyStrategy.QUERY_BY_COMMITTEE,
            'sampling': UncertaintyStrategy.UNCERTAINTY_SAMPLING
        }
        
        strategy_enum = strategy_map.get(strategy, UncertaintyStrategy.CONFIDENCE_BASED)
        
        # Analyze job using active learning system
        result = active_learning.analyze_job_for_active_learning(
            job_id=job_id,
            strategy=strategy_enum,
            max_items=sample_count
        )
        
        # Add job metadata to result if it's a successful analysis
        if "message" not in result:
            result.update({
                "job_id": job_id,
                "strategy_used": strategy,
                "confidence_threshold": confidence_threshold
            })
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to analyze job: {str(e)}")

@router.post("/samples/{job_id}")
async def get_samples_for_labeling(
    job_id: str,
    request_data: Dict[str, Any]
):
    """Get uncertain samples from a job for human labeling"""
    try:
        strategy = request_data.get('strategy', 'uncertainty')
        count = request_data.get('count', 10)
        
        # Import job logger to get job data
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
        from infrastructure.monitoring.job_logger import job_logger
        
        # Get job data
        job_log = job_logger.get_job_log(job_id)
        if not job_log:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Try to get results from output file first
        try:
            from pathlib import Path
            import json
            
            outputs_dir = Path(__file__).parent.parent.parent.parent / "data" / "outputs"
            output_file = outputs_dir / f"job_{job_id}_labeled.json"
            
            if output_file.exists():
                with open(output_file, 'r') as f:
                    output_data = json.load(f)
                
                test_texts = output_data.get("test_texts", [])
                if test_texts:
                    # Convert output data to samples format
                    samples = []
                    for i, text_item in enumerate(test_texts):
                        confidence = text_item.get("confidence", 0.8)  # Default confidence if not available
                        sample = {
                            "sample_id": text_item.get("id", f"text_{i+1:03d}"),
                            "content": text_item.get("content", ""),
                            "predicted_label": text_item.get("ai_assigned_label", ""),
                            "confidence_score": confidence,
                            "uncertainty_score": 1.0 - confidence,
                            "reasoning": text_item.get("reasoning", "AI classification")
                        }
                        samples.append(sample)
                else:
                    raise ValueError("No test_texts found in output file")
            else:
                raise ValueError("Output file not found")
                
        except Exception as e:
            # Fallback: try to get from job log processing details
            text_agent_data = job_log.get("text_agent", {})
            processing_details = text_agent_data.get("processing_details", [])
            
            if not processing_details:
                # Check if job status indicates completion
                job_status = job_log.get("job_metadata", {}).get("status", "")
                if job_status not in ["completed", "processing", "created"]:
                    raise HTTPException(status_code=400, detail=f"Job status '{job_status}' not suitable for sampling")
                else:
                    raise HTTPException(status_code=400, detail=f"No prediction data found in job. Job may still be processing or failed. Error: {str(e)}")
            
            # Convert processing details to samples format
            samples = []
            for detail in processing_details:
                sample = {
                    "sample_id": detail.get("text_id", f"sample_{len(samples)}"),
                    "content": detail.get("content_preview", ""),
                    "predicted_label": detail.get("assigned_label", ""),
                    "confidence_score": detail.get("confidence_score", 0.0),
                    "uncertainty_score": 1.0 - detail.get("confidence_score", 0.0),
                    "reasoning": detail.get("classification_reasoning", "")
                }
                samples.append(sample)
        
        # Sort by uncertainty (lowest confidence first) for uncertainty sampling
        if strategy == 'uncertainty':
            samples.sort(key=lambda x: x['confidence_score'])
        elif strategy == 'entropy':
            # For entropy, we'd need more detailed probability distributions
            # For now, use uncertainty as approximation
            samples.sort(key=lambda x: x['confidence_score'])
        else:
            # For other strategies, use uncertainty as default
            samples.sort(key=lambda x: x['confidence_score'])
        
        # Return top uncertain samples
        uncertain_samples = samples[:count]
        
        return {
            "status": "success",
            "job_id": job_id,
            "strategy": strategy,
            "samples": uncertain_samples,
            "total_available": len(samples),
            "returned_count": len(uncertain_samples)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get samples: {str(e)}")
