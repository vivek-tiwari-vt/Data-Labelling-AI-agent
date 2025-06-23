import asyncio
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.redis_client import RedisClient
from common.models import JobStatus, AgentTask
from common.job_logger import job_logger
from common.ai_client import AIClient

class JobService:
    def __init__(self):
        self.redis_client = RedisClient()
        self.ai_client = AIClient()
        
        # Set up outputs directory
        self.outputs_dir = Path(__file__).parent.parent.parent.parent / "data" / "outputs"
        self.outputs_dir.mkdir(parents=True, exist_ok=True)

    async def create_text_labeling_job(self, text_content: str) -> str:
        """Creates a text labeling job and dispatches it to Mother AI."""
        job_id = str(uuid.uuid4())
        
        # Create job log
        job_data = {
            "job_type": "text_labeling",
            "text_content": text_content,
            "file_data": {"test_texts": [{"content": text_content}]},
            "available_labels": [],
            "instructions": "Single text labeling task"
        }
        job_logger.create_job_log(job_id, job_data)
        
        # Create initial job status
        job_status = JobStatus(
            job_id=job_id,
            status="pending",
            progress=0.0
        )
        
        # Store in Redis
        self.redis_client.set_key(f"job:{job_id}", job_status.dict())
        
        # Dispatch to Mother AI
        task_message = {
            "job_id": job_id,
            "job_type": "text_labeling",
            "text_content": text_content
        }
        
        self.redis_client.publish_message("mother_ai_jobs", task_message)
        print(f"Dispatched text labeling job {job_id} to Mother AI")
        
        return job_id

    async def dispatch_batch_job_to_mother_ai(self, file_data: Dict[str, Any], available_labels: list, 
                                            instructions: str, original_filename: str, 
                                            mother_ai_model: str, child_ai_model: str) -> str:
        """Dispatches a batch text classification job to Mother AI with comprehensive logging."""
        job_id = str(uuid.uuid4())
        
        # Prepare job data for logging
        job_data = {
            "job_type": "batch_text_classification",
            "file_data": file_data,
            "available_labels": available_labels,
            "instructions": instructions,
            "original_filename": original_filename,
            "mother_ai_model": mother_ai_model,
            "child_ai_model": child_ai_model,
            "file_size": len(json.dumps(file_data))
        }
        
        # Create comprehensive job log
        log_entry = job_logger.create_job_log(job_id, job_data)
        
        # Log AI client information
        ai_client_info = {
            "available_models": [
                f"OpenRouter: {self.ai_client.config.DEFAULT_OPENROUTER_MODEL}",
                f"Gemini: {self.ai_client.config.DEFAULT_GEMINI_MODEL}",
                f"OpenAI: {self.ai_client.config.DEFAULT_OPENAI_MODEL}"
            ],
            "providers": ["OpenRouter", "Gemini", "OpenAI"],
            "key_counts": {
                "openrouter": len(self.ai_client.key_manager.openrouter_keys),
                "gemini": len(self.ai_client.key_manager.gemini_keys),
                "openai": len(self.ai_client.key_manager.openai_keys)
            }
        }
        
        # Create initial job status
        job_status = JobStatus(
            job_id=job_id,
            status="pending",
            progress=0.0
        )
        
        # Store in Redis
        self.redis_client.set_key(f"job:{job_id}", job_status.dict())
        
        # Dispatch to Mother AI with all details including model selections
        task_message = {
            "job_id": job_id,
            "job_type": "batch_text_classification",
            "file_data": file_data,
            "available_labels": available_labels,
            "instructions": instructions,
            "original_filename": original_filename,
            "mother_ai_model": mother_ai_model,
            "child_ai_model": child_ai_model,
            "ai_client_info": ai_client_info,
            "timestamp": datetime.now().isoformat()
        }
        
        self.redis_client.publish_message("mother_ai_jobs", task_message)
        
        print(f"📤 Dispatched batch job {job_id} to Mother AI")
        print(f"📁 File: {original_filename}")
        print(f"🏷️  Labels: {', '.join(available_labels)}")
        print(f"📝 Instructions: {instructions}")
        print(f"📊 Total texts: {len(file_data.get('test_texts', []))}")
        print(f"🧠 Mother AI Model: {mother_ai_model}")
        print(f"🤖 Child AI Model: {child_ai_model}")
        
        return job_id

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves the current status of a job with enhanced logging information."""
        # Get basic job status from Redis
        job_data = self.redis_client.get_key(f"job:{job_id}")
        if not job_data:
            return None
        
        # Get detailed log information
        job_log = job_logger.get_job_log(job_id)
        job_summary = job_logger.get_job_summary(job_id)
        
        # Combine Redis status with log information
        enhanced_status = {
            **job_data,
            "detailed_log_available": job_log is not None,
            "log_summary": job_summary
        }
        
        return enhanced_status

    async def get_job_result(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves the result of a completed job."""
        job_data = self.redis_client.get_key(f"job:{job_id}")
        if not job_data or job_data.get("status") != "completed":
            return None
        
        return job_data.get("result")

    async def get_job_file(self, job_id: str) -> Optional[Path]:
        """Gets the output file path for a completed job."""
        # Check different possible file locations
        possible_files = [
            self.outputs_dir / f"job_{job_id}_labeled.json",
            self.outputs_dir / f"job_{job_id}.json"
        ]
        
        for file_path in possible_files:
            if file_path.exists():
                return file_path
        
        return None

    async def list_recent_jobs(self, limit: int = 10) -> list:
        """Lists recent jobs with their summaries from the job logger."""
        return job_logger.list_recent_jobs(limit)

    async def get_detailed_job_log(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Gets the complete detailed log for a job."""
        return job_logger.get_job_log(job_id)

    async def get_job_analytics(self) -> Dict[str, Any]:
        """Gets analytics about job processing."""
        recent_jobs = job_logger.list_recent_jobs(50)  # Last 50 jobs
        
        if not recent_jobs:
            return {
                "total_jobs": 0,
                "success_rate": 0.0,
                "average_processing_time": 0,
                "most_common_labels": [],
                "error_rate": 0.0
            }
        
        # Calculate analytics
        total_jobs = len(recent_jobs)
        completed_jobs = [job for job in recent_jobs if job["status"] == "completed"]
        failed_jobs = [job for job in recent_jobs if job["status"] == "failed"]
        
        success_rate = len(completed_jobs) / total_jobs if total_jobs > 0 else 0
        error_rate = len(failed_jobs) / total_jobs if total_jobs > 0 else 0
        
        # Gather label usage statistics
        all_labels = []
        for job in recent_jobs:
            all_labels.extend(job.get("labels", []))
        
        label_counts = {}
        for label in all_labels:
            label_counts[label] = label_counts.get(label, 0) + 1
        
        most_common_labels = sorted(label_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            "total_jobs": total_jobs,
            "completed_jobs": len(completed_jobs),
            "failed_jobs": len(failed_jobs),
            "success_rate": round(success_rate * 100, 2),
            "error_rate": round(error_rate * 100, 2),
            "most_common_labels": most_common_labels,
            "recent_activity": recent_jobs[:10]
        }

