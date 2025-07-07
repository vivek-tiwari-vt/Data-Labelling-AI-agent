"""
Data Collector - Responsible for gathering job data from various sources
"""
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from .models import JobData

class DataCollector:
    """Collects job data from job logger and output files"""
    
    def __init__(self):
        self.data_dir = Path("/Volumes/DATA/Projects/data_label_agent/data")
        self.outputs_dir = self.data_dir / "outputs"
        
        # Import job logger
        try:
            from infrastructure.monitoring.job_logger import job_logger
            self.job_logger = job_logger
        except ImportError:
            print("Warning: Could not import job_logger, will use fallback methods")
            self.job_logger = None
    
    def get_jobs_for_period(self, time_period: str = "7d") -> List[JobData]:
        """Get jobs for the specified time period"""
        # Parse time period
        if time_period == "24h":
            start_date = datetime.now() - timedelta(hours=24)
        elif time_period == "7d":
            start_date = datetime.now() - timedelta(days=7)
        elif time_period == "30d":
            start_date = datetime.now() - timedelta(days=30)
        else:
            start_date = datetime.now() - timedelta(days=7)
        
        # Get jobs from job logger
        jobs = []
        if self.job_logger:
            try:
                recent_jobs = self.job_logger.list_recent_jobs(1000)
                jobs.extend(self._filter_jobs_by_period(recent_jobs, start_date))
            except Exception as e:
                print(f"Error getting jobs from job logger: {e}")
        
        # Fallback: Get jobs from output files if no jobs from logger
        if not jobs:
            jobs.extend(self._get_jobs_from_output_files())
        
        return jobs
    
    def _filter_jobs_by_period(self, jobs: List[Dict], start_date: datetime) -> List[JobData]:
        """Filter jobs by time period and convert to JobData"""
        filtered_jobs = []
        
        for job in jobs:
            try:
                job_date = datetime.fromisoformat(job.get("created", "2024-01-01")).replace(tzinfo=None)
                if job_date >= start_date:
                    job_data = self._convert_to_job_data(job)
                    if job_data:
                        filtered_jobs.append(job_data)
            except Exception as e:
                print(f"Error processing job {job.get('job_id', 'unknown')}: {e}")
                continue
        
        return filtered_jobs
    
    def _convert_to_job_data(self, job: Dict) -> Optional[JobData]:
        """Convert raw job dict to standardized JobData"""
        try:
            # Get additional data from job log if available
            job_log = None
            if self.job_logger:
                job_log = self.job_logger.get_job_log(job["job_id"])
            
            # Extract model information from multiple sources
            ai_model_used = None
            mother_ai_model = None
            child_ai_model = None
            models_used = []
            
            if job_log:
                ai_models = job_log.get("ai_models", {})
                mother_ai_model = (ai_models.get("mother_ai_model") or 
                                 job_log.get("mother_ai_model"))
                child_ai_model = (ai_models.get("child_ai_model") or 
                                job_log.get("child_ai_model"))
                ai_model_used = child_ai_model or mother_ai_model or job_log.get("ai_model_used")
                models_used = job_log.get("models_used", [])
                
                if ai_model_used and ai_model_used not in models_used:
                    models_used.append(ai_model_used)
            
            # Get processing metrics
            processing_time_ms = 0
            total_texts = job.get("total_texts", 0)
            success_rate = 0.0
            
            if job_log:
                perf_metrics = job_log.get("performance_metrics", {})
                processing_time_ms = perf_metrics.get("total_time_ms", 0)
                
                job_metadata = job_log.get("job_metadata", {})
                total_texts = job_metadata.get("total_texts", total_texts)
                
                results = job_log.get("results", {})
                success_rate = results.get("success_rate", 0.0)
            
            return JobData(
                job_id=job["job_id"],
                created=job.get("created", datetime.now().isoformat()),
                status=job.get("status", "unknown"),
                total_texts=total_texts,
                success_rate=success_rate,
                processing_time_ms=processing_time_ms,
                ai_model_used=ai_model_used,
                mother_ai_model=mother_ai_model,
                child_ai_model=child_ai_model,
                models_used=models_used
            )
            
        except Exception as e:
            print(f"Error converting job data: {e}")
            return None
    
    def _get_jobs_from_output_files(self) -> List[JobData]:
        """Get job data from output files as fallback"""
        jobs = []
        
        if not self.outputs_dir.exists():
            return jobs
        
        # Look for metadata files
        for metadata_file in self.outputs_dir.glob("*_metadata.log"):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                job_id = metadata.get("job_id")
                if not job_id:
                    continue
                
                # Try to load corresponding results file
                results_data = None
                results_file = self.outputs_dir / f"job_{job_id}_labeled.json"
                if results_file.exists():
                    try:
                        with open(results_file, 'r') as f:
                            results_data = json.load(f)
                    except Exception:
                        pass
                
                # Create JobData from metadata
                job_data = JobData(
                    job_id=job_id,
                    created=metadata.get("processing_timestamp", datetime.now().isoformat()),
                    status="completed",  # Files in outputs are assumed completed
                    total_texts=metadata.get("total_texts", 0),
                    success_rate=metadata.get("success_rate", 1.0),
                    processing_time_ms=int(metadata.get("processing_time_seconds", 0) * 1000),
                    ai_model_used=metadata.get("ai_model_used"),
                    mother_ai_model=metadata.get("mother_ai_model"),
                    child_ai_model=metadata.get("child_ai_model"),
                    models_used=metadata.get("models_used", []),
                    results_data=results_data
                )
                
                jobs.append(job_data)
                
            except Exception as e:
                print(f"Error loading metadata file {metadata_file}: {e}")
                continue
        
        return jobs
    
    def get_job_log(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed job log for a specific job"""
        if self.job_logger:
            try:
                return self.job_logger.get_job_log(job_id)
            except Exception as e:
                print(f"Error getting job log for {job_id}: {e}")
        return None
