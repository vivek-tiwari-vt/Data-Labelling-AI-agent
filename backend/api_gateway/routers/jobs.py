from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
import json
import uuid
from pathlib import Path
from typing import Dict, Any, Optional
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.models import JobRequest, JobStatus
from services.job_service import JobService

router = APIRouter()
job_service = JobService()

@router.post("/jobs/")
async def create_job(job_request: JobRequest):
    """Create a new text labeling job."""
    try:
        job_id = await job_service.create_text_labeling_job(job_request.text_content)
        
        return {
            "job_id": job_id,
            "status": "pending",
            "message": "Job created and dispatched to Mother AI"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create job: {str(e)}")

@router.post("/submit-batch-job")
async def upload_file_for_labeling(
    file: UploadFile = File(...),
    labels: str = Form(...),
    instructions: str = Form(...),
    mother_ai_model: str = Form(...),
    child_ai_model: str = Form(...)
):
    """Upload a file (JSON, CSV, or XML) for batch text classification."""
    try:
        # Import file manager
        from common.file_manager import FileManager
        file_manager = FileManager()
        
        # Validate file type
        file_extension = file.filename.lower().split('.')[-1] if '.' in file.filename else ''
        supported_extensions = ['json', 'csv', 'xml']
        
        if file_extension not in supported_extensions:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported file type: .{file_extension}. Supported formats: {', '.join(supported_extensions)}"
            )
        
        # Read file content
        content = await file.read()
        
        # Save uploaded file temporarily
        temp_file_path = file_manager.save_uploaded_file(content, file.filename)
        
        try:
            # Parse file using unified parser
            file_data = file_manager.parse_uploaded_file(temp_file_path)
            
            print(f"📁 Successfully parsed {file.filename}:")
            print(f"   📊 Format: {file_data['source_format'].upper()}")
            print(f"   📝 Total texts: {file_data['total_texts']}")
            
            # Clean up temp file
            file_manager.delete_file(temp_file_path)
            
        except Exception as parse_error:
            # Clean up temp file on error
            if temp_file_path.exists():
                file_manager.delete_file(temp_file_path)
            raise HTTPException(status_code=400, detail=str(parse_error))
        
        # Parse labels
        available_labels = [label.strip() for label in labels.split(',') if label.strip()]
        if not available_labels:
            raise HTTPException(status_code=400, detail="At least one label must be provided")
        
        # Validate parsed data has texts
        if not file_data.get("test_texts"):
            raise HTTPException(status_code=400, detail="No valid text content found in file")
        
        # Dispatch job with comprehensive logging and model selection
        job_id = await job_service.dispatch_batch_job_to_mother_ai(
            file_data=file_data,
            available_labels=available_labels,
            instructions=instructions,
            original_filename=file.filename,
            mother_ai_model=mother_ai_model,
            child_ai_model=child_ai_model
        )
        
        return {
            "job_id": job_id,
            "status": "pending",
            "message": f"{file_data['source_format'].upper()} file uploaded and job dispatched to Mother AI",
            "file_details": {
                "filename": file.filename,
                "source_format": file_data["source_format"],
                "total_texts": file_data["total_texts"],
                "available_labels": available_labels,
                "labels_count": str(len(available_labels)),
                "mother_ai_model": mother_ai_model,
                "child_ai_model": child_ai_model,
                "file_size": file_data.get("file_size", 0),
                "parsed_metadata": {
                    k: v for k, v in file_data.items() 
                    if k not in ["test_texts", "total_texts", "source_format"]
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process file upload: {str(e)}")

@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Get the status of a specific job with detailed logging information."""
    try:
        job_status = await job_service.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return job_status
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job status: {str(e)}")

@router.get("/jobs/{job_id}/download")
async def download_job_result(job_id: str):
    """Download the result file for a completed job."""
    try:
        # Check if job is completed
        job_status = await job_service.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail="Job not found")
        
        if job_status.get("status") != "completed":
            raise HTTPException(status_code=400, detail="Job is not completed yet")
        
        # Get the result file
        file_path = await job_service.get_job_file(job_id)
        if not file_path or not file_path.exists():
            raise HTTPException(status_code=404, detail="Result file not found")
        
        # Determine file extension and media type
        file_extension = file_path.suffix.lower()
        if file_extension == '.csv':
            media_type = "text/csv"
            filename = f"job_{job_id}_labeled.csv"
        elif file_extension == '.xml':
            media_type = "application/xml"
            filename = f"job_{job_id}_labeled.xml"
        else:  # Default to JSON
            media_type = "application/json"
            filename = f"job_{job_id}_labeled.json"
        
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type=media_type
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to download result: {str(e)}")

@router.get("/jobs/{job_id}/file")
async def get_job_file_content(job_id: str):
    """Get the content of the result file."""
    try:
        # Check if job is completed
        job_status = await job_service.get_job_status(job_id)
        if not job_status:
            raise HTTPException(status_code=404, detail="Job not found")
        
        if job_status.get("status") != "completed":
            raise HTTPException(status_code=400, detail="Job is not completed yet")
        
        # Get the result file
        file_path = await job_service.get_job_file(job_id)
        if not file_path or not file_path.exists():
            raise HTTPException(status_code=404, detail="Result file not found")
        
        # Read and return file content based on file type
        file_extension = file_path.suffix.lower()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            if file_extension == '.json':
                content = json.load(f)
                return content
            else:
                # For CSV and XML, return as text
                content = f.read()
                return {"content": content, "format": file_extension[1:]}  # Remove the dot
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get file content: {str(e)}")

@router.get("/jobs/{job_id}/log")
async def get_job_detailed_log(job_id: str):
    """Get the detailed processing log for a job."""
    try:
        job_log = await job_service.get_detailed_job_log(job_id)
        if not job_log:
            raise HTTPException(status_code=404, detail="Job log not found")
        
        return job_log
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job log: {str(e)}")

@router.get("/jobs/{job_id}/summary")
async def get_job_summary(job_id: str):
    """Get a summary of job processing."""
    try:
        from common.job_logger import job_logger
        
        summary = job_logger.get_job_summary(job_id)
        if not summary:
            raise HTTPException(status_code=404, detail="Job summary not found")
        
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job summary: {str(e)}")

@router.get("/jobs/")
async def list_recent_jobs(limit: int = 10):
    """List recent jobs with their summaries."""
    try:
        jobs = await job_service.list_recent_jobs(limit)
        return {
            "jobs": jobs,
            "total": len(jobs)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")

@router.get("/analytics/")
async def get_job_analytics():
    """Get analytics about job processing."""
    try:
        analytics = await job_service.get_job_analytics()
        return analytics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analytics: {str(e)}")

