from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, List, Optional
import sys
import os
from datetime import datetime, timedelta

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.job_logger import job_logger

router = APIRouter()

@router.get("/logs/jobs/")
async def list_job_logs(
    limit: int = Query(default=20, ge=1, le=100),
    status: Optional[str] = Query(default=None, description="Filter by job status")
):
    """List recent job logs with filtering options."""
    try:
        jobs = job_logger.list_recent_jobs(limit)
        
        # Filter by status if provided
        if status:
            jobs = [job for job in jobs if job.get("status") == status]
        
        return {
            "jobs": jobs,
            "total": len(jobs),
            "filters": {
                "status": status,
                "limit": limit
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list job logs: {str(e)}")

@router.get("/logs/jobs/{job_id}")
async def get_detailed_job_log(job_id: str):
    """Get the complete detailed log for a specific job."""
    try:
        log_entry = job_logger.get_job_log(job_id)
        if not log_entry:
            raise HTTPException(status_code=404, detail="Job log not found")
        
        return log_entry
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job log: {str(e)}")

@router.get("/logs/jobs/{job_id}/summary")
async def get_job_log_summary(job_id: str):
    """Get a summary of a job's processing log."""
    try:
        summary = job_logger.get_job_summary(job_id)
        if not summary:
            raise HTTPException(status_code=404, detail="Job summary not found")
        
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job summary: {str(e)}")

@router.get("/logs/jobs/{job_id}/mother_ai")
async def get_mother_ai_processing_log(job_id: str):
    """Get Mother AI specific processing details for a job."""
    try:
        log_entry = job_logger.get_job_log(job_id)
        if not log_entry:
            raise HTTPException(status_code=404, detail="Job log not found")
        
        mother_ai_data = log_entry.get("mother_ai", {})
        return {
            "job_id": job_id,
            "mother_ai_processing": mother_ai_data,
            "timestamps": {
                "job_started": log_entry.get("timestamps", {}).get("job_started"),
                "processing_timestamp": mother_ai_data.get("processing_timestamp")
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get Mother AI log: {str(e)}")

@router.get("/logs/jobs/{job_id}/text_agent")
async def get_text_agent_processing_log(job_id: str):
    """Get Text Agent specific processing details for a job."""
    try:
        log_entry = job_logger.get_job_log(job_id)
        if not log_entry:
            raise HTTPException(status_code=404, detail="Job log not found")
        
        text_agent_data = log_entry.get("text_agent", {})
        return {
            "job_id": job_id,
            "text_agent_processing": text_agent_data,
            "classification_details": text_agent_data.get("processing_details", []),
            "texts_processed": text_agent_data.get("texts_processed", 0)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get Text Agent log: {str(e)}")

@router.get("/logs/jobs/{job_id}/instructions")
async def get_job_instructions_flow(job_id: str):
    """Get the complete instruction flow from user to Mother AI to Text Agent."""
    try:
        log_entry = job_logger.get_job_log(job_id)
        if not log_entry:
            raise HTTPException(status_code=404, detail="Job log not found")
        
        return {
            "job_id": job_id,
            "instruction_flow": {
                "user_input": {
                    "original_instructions": log_entry.get("user_input", {}).get("user_instructions", ""),
                    "available_labels": log_entry.get("user_input", {}).get("available_labels", []),
                    "labels_count": log_entry.get("user_input", {}).get("labels_count", 0)
                },
                "mother_ai_enhancement": {
                    "enhanced_instructions": log_entry.get("mother_ai", {}).get("instructions_created", ""),
                    "instructions_length": log_entry.get("mother_ai", {}).get("instructions_length", 0),
                    "content_analysis": log_entry.get("mother_ai", {}).get("content_analysis", ""),
                    "label_strategies": log_entry.get("mother_ai", {}).get("label_strategies", ""),
                    "classification_rules": log_entry.get("mother_ai", {}).get("classification_rules", "")
                },
                "text_agent_processing": {
                    "instructions_received": log_entry.get("text_agent", {}).get("instructions_received", ""),
                    "strategy_parsed": log_entry.get("text_agent", {}).get("classification_strategy_parsed", "")
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get instruction flow: {str(e)}")

@router.get("/logs/jobs/{job_id}/models")
async def get_job_ai_models_usage(job_id: str):
    """Get information about AI models used in a job."""
    try:
        log_entry = job_logger.get_job_log(job_id)
        if not log_entry:
            raise HTTPException(status_code=404, detail="Job log not found")
        
        ai_models_data = log_entry.get("ai_models", {})
        return {
            "job_id": job_id,
            "ai_models": ai_models_data,
            "model_usage_timeline": [
                {
                    "component": "mother_ai",
                    "timestamp": log_entry.get("mother_ai", {}).get("processing_timestamp"),
                    "models_available": ai_models_data.get("models_available", []),
                    "providers": ai_models_data.get("api_providers", [])
                },
                {
                    "component": "text_agent", 
                    "timestamp": log_entry.get("text_agent", {}).get("processing_started"),
                    "models_used": ai_models_data.get("models_used", [])
                }
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get AI models usage: {str(e)}")

@router.get("/logs/jobs/{job_id}/performance")
async def get_job_performance_metrics(job_id: str):
    """Get performance metrics for a job."""
    try:
        log_entry = job_logger.get_job_log(job_id)
        if not log_entry:
            raise HTTPException(status_code=404, detail="Job log not found")
        
        timestamps = log_entry.get("timestamps", {})
        performance = log_entry.get("performance_metrics", {})
        results = log_entry.get("results", {})
        job_metadata = log_entry.get("job_metadata", {})
        
        return {
            "job_id": job_id,
            "performance_metrics": {
                "total_time_ms": performance.get("total_time_ms", 0),
                "processing_time_seconds": results.get("processing_time_seconds", 0),
                "texts_processed": results.get("total_texts_processed", 0),
                "success_rate": results.get("success_rate", 0.0),
                "texts_per_second": (
                    results.get("total_texts_processed", 0) / results.get("processing_time_seconds", 1)
                    if results.get("processing_time_seconds", 0) > 0 else 0
                )
            },
            "timeline": {
                "job_created": timestamps.get("job_created"),
                "job_started": timestamps.get("job_started"),
                "job_completed": timestamps.get("job_completed"),
                "status": job_metadata.get("status")
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get performance metrics: {str(e)}")

@router.get("/logs/analytics/overview")
async def get_logging_analytics():
    """Get comprehensive analytics about job processing and system performance."""
    try:
        recent_jobs = job_logger.list_recent_jobs(100)  # Last 100 jobs
        
        if not recent_jobs:
            return {
                "total_jobs": 0,
                "system_performance": {},
                "label_analytics": {},
                "error_analytics": {}
            }
        
        # Calculate system performance metrics
        total_jobs = len(recent_jobs)
        completed_jobs = [job for job in recent_jobs if job.get("status") == "completed"]
        failed_jobs = [job for job in recent_jobs if job.get("status") == "failed"]
        
        # Get detailed logs for completed jobs to calculate performance
        processing_times = []
        text_counts = []
        
        for job in completed_jobs[:20]:  # Sample last 20 completed jobs
            job_log = job_logger.get_job_log(job["job_id"])
            if job_log:
                perf_time = job_log.get("performance_metrics", {}).get("total_time_ms", 0)
                text_count = job_log.get("job_metadata", {}).get("total_texts", 0)
                if perf_time > 0 and text_count > 0:
                    processing_times.append(perf_time)
                    text_counts.append(text_count)
        
        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        avg_text_count = sum(text_counts) / len(text_counts) if text_counts else 0
        
        # Label usage analytics
        all_labels = []
        for job in recent_jobs:
            all_labels.extend(job.get("labels", []))
        
        label_counts = {}
        for label in all_labels:
            label_counts[label] = label_counts.get(label, 0) + 1
        
        most_used_labels = sorted(label_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            "total_jobs": total_jobs,
            "system_performance": {
                "success_rate": len(completed_jobs) / total_jobs * 100 if total_jobs > 0 else 0,
                "failure_rate": len(failed_jobs) / total_jobs * 100 if total_jobs > 0 else 0,
                "average_processing_time_ms": round(avg_processing_time, 2),
                "average_texts_per_job": round(avg_text_count, 2),
                "completed_jobs": len(completed_jobs),
                "failed_jobs": len(failed_jobs),
                "pending_jobs": total_jobs - len(completed_jobs) - len(failed_jobs)
            },
            "label_analytics": {
                "total_unique_labels": len(label_counts),
                "most_used_labels": most_used_labels,
                "label_usage_distribution": label_counts
            },
            "recent_activity": recent_jobs[:10]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analytics: {str(e)}")

@router.get("/logs/search")
async def search_job_logs(
    query: str = Query(..., description="Search term for job content or labels"),
    limit: int = Query(default=10, ge=1, le=50)
):
    """Search through job logs based on content, labels, or other criteria."""
    try:
        all_jobs = job_logger.list_recent_jobs(200)  # Search through last 200 jobs
        
        matching_jobs = []
        query_lower = query.lower()
        
        for job in all_jobs:
            # Search in labels
            labels = job.get("labels", [])
            label_match = any(query_lower in label.lower() for label in labels)
            
            # Search in job ID
            job_id_match = query_lower in job.get("job_id", "").lower()
            
            # Get detailed log for content search
            if label_match or job_id_match:
                matching_jobs.append({
                    **job,
                    "match_reason": "label_match" if label_match else "job_id_match"
                })
            else:
                # Search in job content (sample texts)
                job_log = job_logger.get_job_log(job["job_id"])
                if job_log:
                    sample_texts = job_log.get("sample_texts", [])
                    content_match = any(
                        query_lower in sample.get("content", "").lower()
                        for sample in sample_texts
                    )
                    
                    if content_match:
                        matching_jobs.append({
                            **job,
                            "match_reason": "content_match"
                        })
            
            if len(matching_jobs) >= limit:
                break
        
        return {
            "query": query,
            "matches": matching_jobs[:limit],
            "total_matches": len(matching_jobs)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to search logs: {str(e)}") 