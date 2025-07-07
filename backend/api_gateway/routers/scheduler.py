"""
Batch Job Scheduler API Router
Provides endpoints for job scheduling, recurring jobs, and scheduler management
"""
from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.batch_scheduler import BatchJobScheduler, JobPriority, ScheduleType, JobStatus

router = APIRouter(prefix="/scheduler", tags=["scheduler"])

# Initialize scheduler
scheduler = BatchJobScheduler()

# Pydantic models for request bodies
class ScheduledJobCreate(BaseModel):
    name: str
    description: str = ""
    job_type: str
    priority: int = 2  # Normal priority
    schedule_type: str  # "one_time" or "recurring"
    schedule_expression: str  # cron expression or ISO datetime
    job_data: Dict[str, Any]
    max_runs: Optional[int] = None
    max_retries: int = 3
    timeout_minutes: int = 60
    dependencies: List[str] = []
    metadata: Dict[str, Any] = {}
    created_by: str = "system"

class RecurringBatchJobCreate(BaseModel):
    name: str
    file_path: str
    labels: List[str]
    instructions: str
    cron_expression: str
    mother_ai_model: str
    child_ai_model: str
    description: str = ""

class ExportJobCreate(BaseModel):
    job_id: str
    export_format: str
    schedule_time: str  # ISO datetime
    recipients: List[str] = []

class SchedulerConfiguration(BaseModel):
    max_concurrent_jobs: int = 3
    default_timeout_minutes: int = 60
    cleanup_days: int = 30

@router.post("/start")
async def start_scheduler():
    """Start the background scheduler"""
    try:
        result = scheduler.start_scheduler()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start scheduler: {str(e)}")

@router.post("/stop")
async def stop_scheduler():
    """Stop the background scheduler"""
    try:
        result = scheduler.stop_scheduler()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop scheduler: {str(e)}")

@router.post("/jobs")
async def create_scheduled_job(job_config: ScheduledJobCreate):
    """Create a new scheduled job"""
    try:
        job_id = scheduler.create_scheduled_job(job_config.dict())
        
        return {
            "job_id": job_id,
            "message": "Scheduled job created successfully",
            "job_info": {
                "name": job_config.name,
                "type": job_config.job_type,
                "schedule": job_config.schedule_expression,
                "priority": job_config.priority
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create scheduled job: {str(e)}")

@router.post("/jobs/recurring-batch")
async def create_recurring_batch_job(job_config: RecurringBatchJobCreate):
    """Create a recurring batch text classification job"""
    try:
        job_id = scheduler.create_recurring_batch_job(
            file_path=job_config.file_path,
            labels=job_config.labels,
            instructions=job_config.instructions,
            cron_expression=job_config.cron_expression,
            mother_ai_model=job_config.mother_ai_model,
            child_ai_model=job_config.child_ai_model,
            name=job_config.name
        )
        
        return {
            "job_id": job_id,
            "message": "Recurring batch job created successfully",
            "schedule": job_config.cron_expression,
            "file_path": job_config.file_path
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create recurring batch job: {str(e)}")

@router.post("/jobs/export")
async def schedule_export_job(export_config: ExportJobCreate):
    """Schedule an export job for specific time"""
    try:
        schedule_time = datetime.fromisoformat(export_config.schedule_time)
        
        job_id = scheduler.schedule_export_job(
            job_id=export_config.job_id,
            export_format=export_config.export_format,
            schedule_time=schedule_time,
            recipients=export_config.recipients
        )
        
        return {
            "scheduled_job_id": job_id,
            "message": "Export job scheduled successfully",
            "source_job_id": export_config.job_id,
            "export_format": export_config.export_format,
            "scheduled_time": export_config.schedule_time
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to schedule export job: {str(e)}")

@router.get("/jobs")
async def get_scheduled_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    limit: int = Query(default=100, description="Maximum number of jobs to return")
):
    """Get list of scheduled jobs with optional filtering"""
    try:
        status_enum = None
        if status:
            try:
                status_enum = JobStatus(status.lower())
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
        
        jobs = scheduler.get_scheduled_jobs(status_enum, job_type, limit)
        
        # Convert to dictionaries for JSON response
        jobs_data = []
        for job in jobs:
            jobs_data.append({
                "id": job.id,
                "name": job.name,
                "description": job.description,
                "job_type": job.job_type,
                "priority": job.priority.value,
                "schedule_type": job.schedule_type.value,
                "schedule_expression": job.schedule_expression,
                "status": job.status.value,
                "created_at": job.created_at,
                "created_by": job.created_by,
                "next_run_time": job.next_run_time,
                "last_run_time": job.last_run_time,
                "run_count": job.run_count,
                "max_runs": job.max_runs
            })
        
        return {
            "total_jobs": len(jobs_data),
            "jobs": jobs_data,
            "filters_applied": {
                "status": status,
                "job_type": job_type,
                "limit": limit
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduled jobs: {str(e)}")

@router.get("/jobs/{job_id}")
async def get_scheduled_job(job_id: str):
    """Get details of a specific scheduled job"""
    try:
        jobs = scheduler.get_scheduled_jobs(limit=1000)  # Get all jobs to find the specific one
        
        target_job = None
        for job in jobs:
            if job.id == job_id:
                target_job = job
                break
        
        if not target_job:
            raise HTTPException(status_code=404, detail="Scheduled job not found")
        
        # Get execution history
        executions = scheduler.get_job_executions(job_id, 20)
        
        return {
            "job_info": {
                "id": target_job.id,
                "name": target_job.name,
                "description": target_job.description,
                "job_type": target_job.job_type,
                "priority": target_job.priority.value,
                "schedule_type": target_job.schedule_type.value,
                "schedule_expression": target_job.schedule_expression,
                "status": target_job.status.value,
                "created_at": target_job.created_at,
                "created_by": target_job.created_by,
                "next_run_time": target_job.next_run_time,
                "last_run_time": target_job.last_run_time,
                "run_count": target_job.run_count,
                "max_runs": target_job.max_runs,
                "retry_count": target_job.retry_count,
                "max_retries": target_job.max_retries,
                "job_data": target_job.job_data,
                "metadata": target_job.metadata
            },
            "execution_history": [
                {
                    "id": execution.id,
                    "status": execution.status.value,
                    "start_time": execution.start_time,
                    "end_time": execution.end_time,
                    "error_message": execution.error_message
                }
                for execution in executions
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduled job: {str(e)}")

@router.put("/jobs/{job_id}")
async def update_scheduled_job(job_id: str, updates: Dict[str, Any]):
    """Update an existing scheduled job"""
    try:
        success = scheduler.update_scheduled_job(job_id, updates)
        
        if success:
            return {
                "message": "Scheduled job updated successfully",
                "job_id": job_id,
                "updates_applied": list(updates.keys())
            }
        else:
            raise HTTPException(status_code=404, detail="Scheduled job not found")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update scheduled job: {str(e)}")

@router.delete("/jobs/{job_id}")
async def cancel_scheduled_job(job_id: str):
    """Cancel a scheduled job"""
    try:
        success = scheduler.cancel_scheduled_job(job_id)
        
        if success:
            return {
                "message": "Scheduled job cancelled successfully",
                "job_id": job_id
            }
        else:
            raise HTTPException(status_code=404, detail="Scheduled job not found")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to cancel scheduled job: {str(e)}")

@router.get("/dashboard")
async def get_scheduler_dashboard():
    """Get scheduler dashboard data"""
    try:
        dashboard_data = scheduler.get_scheduler_dashboard()
        return dashboard_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler dashboard: {str(e)}")

@router.get("/jobs/{job_id}/executions")
async def get_job_executions(
    job_id: str,
    limit: int = Query(default=50, description="Maximum number of executions to return")
):
    """Get execution history for a scheduled job"""
    try:
        executions = scheduler.get_job_executions(job_id, limit)
        
        executions_data = []
        for execution in executions:
            executions_data.append({
                "id": execution.id,
                "scheduled_job_id": execution.scheduled_job_id,
                "status": execution.status.value,
                "start_time": execution.start_time,
                "end_time": execution.end_time,
                "result": execution.result,
                "error_message": execution.error_message,
                "logs": execution.logs
            })
        
        return {
            "job_id": job_id,
            "total_executions": len(executions_data),
            "executions": executions_data
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job executions: {str(e)}")

@router.post("/jobs/{job_id}/trigger")
async def trigger_job_manually(job_id: str):
    """Manually trigger a scheduled job to run immediately"""
    try:
        # Get the job
        jobs = scheduler.get_scheduled_jobs(limit=1000)
        target_job = None
        
        for job in jobs:
            if job.id == job_id:
                target_job = job
                break
        
        if not target_job:
            raise HTTPException(status_code=404, detail="Scheduled job not found")
        
        # Update next run time to now
        updates = {"next_run_time": datetime.now().isoformat()}
        scheduler.update_scheduled_job(job_id, updates)
        
        return {
            "message": "Job triggered manually",
            "job_id": job_id,
            "job_name": target_job.name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger job manually: {str(e)}")

@router.get("/cron-helper")
async def cron_expression_helper(
    schedule_description: str = Query(..., description="Human-readable schedule description")
):
    """Helper endpoint to generate cron expressions"""
    try:
        # Common schedule patterns
        patterns = {
            "every minute": "* * * * *",
            "every hour": "0 * * * *",
            "every day": "0 0 * * *",
            "every day at 9am": "0 9 * * *",
            "every week": "0 0 * * 0",
            "every month": "0 0 1 * *",
            "weekdays at 9am": "0 9 * * 1-5",
            "weekends at 10am": "0 10 * * 6,0"
        }
        
        schedule_lower = schedule_description.lower()
        
        if schedule_lower in patterns:
            cron_expression = patterns[schedule_lower]
        else:
            # Default fallback
            cron_expression = "0 0 * * *"  # Daily at midnight
        
        return {
            "input_description": schedule_description,
            "cron_expression": cron_expression,
            "explanation": f"Runs {schedule_description.lower()}",
            "common_patterns": patterns
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate cron expression: {str(e)}")

@router.post("/batch-operations")
async def batch_job_operations(
    operation: str = Body(..., description="Operation: pause, resume, cancel"),
    job_ids: List[str] = Body(..., description="List of job IDs")
):
    """Perform batch operations on multiple scheduled jobs"""
    try:
        results = {"successful": [], "failed": []}
        
        for job_id in job_ids:
            try:
                if operation == "cancel":
                    success = scheduler.cancel_scheduled_job(job_id)
                    if success:
                        results["successful"].append(job_id)
                    else:
                        results["failed"].append(job_id)
                elif operation == "pause":
                    # Update status to paused
                    success = scheduler.update_scheduled_job(job_id, {"status": "paused"})
                    if success:
                        results["successful"].append(job_id)
                    else:
                        results["failed"].append(job_id)
                elif operation == "resume":
                    # Update status to scheduled
                    success = scheduler.update_scheduled_job(job_id, {"status": "scheduled"})
                    if success:
                        results["successful"].append(job_id)
                    else:
                        results["failed"].append(job_id)
                else:
                    results["failed"].append(job_id)
                    
            except Exception:
                results["failed"].append(job_id)
        
        return {
            "operation": operation,
            "total_requested": len(job_ids),
            "successful_count": len(results["successful"]),
            "failed_count": len(results["failed"]),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to perform batch operations: {str(e)}")

@router.get("/status")
async def get_scheduler_status():
    """Get current scheduler status and health"""
    try:
        dashboard = scheduler.get_scheduler_dashboard()
        
        status_info = {
            "scheduler_running": dashboard.get("scheduler_status") == "running",
            "active_jobs": dashboard.get("current_resources", {}).get("active_jobs", 0),
            "queue_size": dashboard.get("current_resources", {}).get("queue_size", 0),
            "max_concurrent": dashboard.get("current_resources", {}).get("max_concurrent", 3),
            "performance_metrics": dashboard.get("performance_metrics", {}),
            "recent_activity": len(dashboard.get("recent_executions", [])),
            "system_health": "healthy" if dashboard.get("scheduler_status") == "running" else "stopped"
        }
        
        return status_info
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler status: {str(e)}")
