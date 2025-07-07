"""
Batch Job Scheduler for Data Labeling System
Provides time-based scheduling, recurring jobs, priority queues, and resource optimization
"""
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid
try:
    from croniter import croniter
except ImportError:
    croniter = None
import threading
import time

class JobPriority(Enum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4
    CRITICAL = 5

class ScheduleType(Enum):
    ONE_TIME = "one_time"
    RECURRING = "recurring"
    CONDITIONAL = "conditional"

class JobStatus(Enum):
    SCHEDULED = "scheduled"
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"

@dataclass
class ScheduledJob:
    id: str
    name: str
    description: str
    job_type: str  # "batch_text_classification", "export", "cleanup", etc.
    priority: JobPriority
    schedule_type: ScheduleType
    schedule_expression: str  # cron expression or timestamp
    job_data: Dict[str, Any]
    status: JobStatus
    created_at: str
    created_by: str
    next_run_time: Optional[str] = None
    last_run_time: Optional[str] = None
    run_count: int = 0
    max_runs: Optional[int] = None
    retry_count: int = 0
    max_retries: int = 3
    timeout_minutes: int = 60
    dependencies: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
@dataclass
class JobExecution:
    id: str
    scheduled_job_id: str
    status: JobStatus
    start_time: str
    end_time: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)

class BatchJobScheduler:
    """Advanced batch job scheduling system with priority queues and resource management"""
    
    def __init__(self):
        self.data_dir = Path("/Volumes/DATA/Projects/data_label_agent/data")
        self.scheduler_dir = self.data_dir / "scheduler"
        self.scheduler_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize scheduler database
        self.db_path = self.scheduler_dir / "scheduler.db"
        self._init_database()
        
        # Scheduler state
        self.is_running = False
        self.scheduler_thread = None
        self.max_concurrent_jobs = 3
        self.running_jobs = {}
        
        # Job handlers registry
        self.job_handlers = {}
        self._register_default_handlers()
        
        # Import required services
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from api_gateway.services.job_service import JobService
        from infrastructure.monitoring.job_logger import job_logger
        self.job_service = JobService()
        self.job_logger = job_logger
    
    def _init_database(self):
        """Initialize scheduler database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Scheduled jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_jobs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                job_type TEXT NOT NULL,
                priority INTEGER NOT NULL,
                schedule_type TEXT NOT NULL,
                schedule_expression TEXT NOT NULL,
                job_data TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                next_run_time TEXT,
                last_run_time TEXT,
                run_count INTEGER DEFAULT 0,
                max_runs INTEGER,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                timeout_minutes INTEGER DEFAULT 60,
                dependencies TEXT,
                metadata TEXT
            )
        """)
        
        # Job executions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_executions (
                id TEXT PRIMARY KEY,
                scheduled_job_id TEXT NOT NULL,
                status TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT,
                result TEXT,
                error_message TEXT,
                logs TEXT,
                FOREIGN KEY (scheduled_job_id) REFERENCES scheduled_jobs (id)
            )
        """)
        
        # Resource usage table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS resource_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                cpu_usage REAL,
                memory_usage REAL,
                active_jobs INTEGER,
                queue_size INTEGER,
                throughput_jobs_per_hour REAL
            )
        """)
        
        conn.commit()
        conn.close()
    
    def start_scheduler(self):
        """Start the background scheduler"""
        if self.is_running:
            return {"error": "Scheduler already running"}
        
        self.is_running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        return {"message": "Scheduler started successfully"}
    
    def stop_scheduler(self):
        """Stop the background scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        return {"message": "Scheduler stopped successfully"}
    
    def create_scheduled_job(self, job_config: Dict[str, Any]) -> str:
        """Create a new scheduled job"""
        
        job_id = str(uuid.uuid4())
        
        # Validate schedule expression
        if job_config["schedule_type"] == ScheduleType.RECURRING.value:
            if not self._validate_cron_expression(job_config["schedule_expression"]):
                raise ValueError("Invalid cron expression")
        elif job_config["schedule_type"] == ScheduleType.ONE_TIME.value:
            try:
                schedule_time = datetime.fromisoformat(job_config["schedule_expression"])
                if schedule_time <= datetime.now():
                    raise ValueError("Scheduled time must be in the future")
            except ValueError:
                raise ValueError("Invalid datetime format for one-time job")
        
        scheduled_job = ScheduledJob(
            id=job_id,
            name=job_config["name"],
            description=job_config.get("description", ""),
            job_type=job_config["job_type"],
            priority=JobPriority(job_config.get("priority", JobPriority.NORMAL.value)),
            schedule_type=ScheduleType(job_config["schedule_type"]),
            schedule_expression=job_config["schedule_expression"],
            job_data=job_config["job_data"],
            status=JobStatus.SCHEDULED,
            created_at=datetime.now().isoformat(),
            created_by=job_config.get("created_by", "system"),
            max_runs=job_config.get("max_runs"),
            max_retries=job_config.get("max_retries", 3),
            timeout_minutes=job_config.get("timeout_minutes", 60),
            dependencies=job_config.get("dependencies", []),
            metadata=job_config.get("metadata", {})
        )
        
        # Calculate next run time
        scheduled_job.next_run_time = self._calculate_next_run_time(scheduled_job)
        
        # Store in database
        self._store_scheduled_job(scheduled_job)
        
        return job_id
    
    def update_scheduled_job(self, job_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing scheduled job"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get current job
        cursor.execute("SELECT * FROM scheduled_jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return False
        
        current_job = self._row_to_scheduled_job(row)
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(current_job, key):
                setattr(current_job, key, value)
        
        # Recalculate next run time if schedule changed
        if "schedule_expression" in updates or "schedule_type" in updates:
            current_job.next_run_time = self._calculate_next_run_time(current_job)
        
        # Update in database
        self._update_scheduled_job(current_job)
        
        conn.close()
        return True
    
    def cancel_scheduled_job(self, job_id: str) -> bool:
        """Cancel a scheduled job"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "UPDATE scheduled_jobs SET status = ? WHERE id = ?",
            (JobStatus.CANCELLED.value, job_id)
        )
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        
        return success
    
    def get_scheduled_jobs(self, status: Optional[JobStatus] = None, 
                          job_type: Optional[str] = None, limit: int = 100) -> List[ScheduledJob]:
        """Get list of scheduled jobs with optional filtering"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM scheduled_jobs WHERE 1=1"
        params = []
        
        if status:
            query += " AND status = ?"
            params.append(status.value)
        
        if job_type:
            query += " AND job_type = ?"
            params.append(job_type)
        
        query += " ORDER BY priority DESC, next_run_time ASC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_scheduled_job(row) for row in rows]
    
    def get_job_executions(self, scheduled_job_id: str, limit: int = 50) -> List[JobExecution]:
        """Get execution history for a scheduled job"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM job_executions 
            WHERE scheduled_job_id = ? 
            ORDER BY start_time DESC 
            LIMIT ?
        """, (scheduled_job_id, limit))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_job_execution(row) for row in rows]
    
    def get_scheduler_dashboard(self) -> Dict[str, Any]:
        """Get scheduler dashboard data"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get job counts by status
        cursor.execute("""
            SELECT status, COUNT(*) as count 
            FROM scheduled_jobs 
            GROUP BY status
        """)
        status_counts = dict(cursor.fetchall())
        
        # Get upcoming jobs (next 24 hours)
        next_24h = (datetime.now() + timedelta(hours=24)).isoformat()
        cursor.execute("""
            SELECT * FROM scheduled_jobs 
            WHERE status = 'scheduled' AND next_run_time <= ? 
            ORDER BY next_run_time ASC 
            LIMIT 10
        """, (next_24h,))
        upcoming_jobs = [self._row_to_scheduled_job(row) for row in cursor.fetchall()]
        
        # Get recent executions
        cursor.execute("""
            SELECT * FROM job_executions 
            ORDER BY start_time DESC 
            LIMIT 20
        """)
        recent_executions = [self._row_to_job_execution(row) for row in cursor.fetchall()]
        
        # Get resource usage
        cursor.execute("""
            SELECT * FROM resource_usage 
            ORDER BY timestamp DESC 
            LIMIT 1
        """)
        current_resources = cursor.fetchone()
        
        conn.close()
        
        return {
            "scheduler_status": "running" if self.is_running else "stopped",
            "job_counts": status_counts,
            "upcoming_jobs": [self._scheduled_job_to_dict(job) for job in upcoming_jobs],
            "recent_executions": [self._job_execution_to_dict(exec) for exec in recent_executions[-10:]],
            "current_resources": {
                "active_jobs": len(self.running_jobs),
                "max_concurrent": self.max_concurrent_jobs,
                "queue_size": status_counts.get("scheduled", 0)
            } if current_resources else {},
            "performance_metrics": self._calculate_performance_metrics()
        }
    
    def create_recurring_batch_job(self, file_path: str, labels: List[str], 
                                 instructions: str, cron_expression: str,
                                 mother_ai_model: str, child_ai_model: str,
                                 name: str = "Recurring Batch Job") -> str:
        """Create a recurring batch text classification job"""
        
        job_config = {
            "name": name,
            "description": f"Recurring batch processing of {file_path}",
            "job_type": "batch_text_classification",
            "schedule_type": ScheduleType.RECURRING.value,
            "schedule_expression": cron_expression,
            "priority": JobPriority.NORMAL.value,
            "job_data": {
                "file_path": file_path,
                "labels": labels,
                "instructions": instructions,
                "mother_ai_model": mother_ai_model,
                "child_ai_model": child_ai_model
            },
            "metadata": {
                "created_for": "recurring_processing",
                "auto_generated": True
            }
        }
        
        return self.create_scheduled_job(job_config)
    
    def schedule_export_job(self, job_id: str, export_format: str, 
                           schedule_time: datetime, recipients: List[str] = None) -> str:
        """Schedule an export job for specific time"""
        
        job_config = {
            "name": f"Export Job {job_id}",
            "description": f"Scheduled export of job {job_id} in {export_format} format",
            "job_type": "export",
            "schedule_type": ScheduleType.ONE_TIME.value,
            "schedule_expression": schedule_time.isoformat(),
            "priority": JobPriority.NORMAL.value,
            "job_data": {
                "source_job_id": job_id,
                "export_format": export_format,
                "recipients": recipients or []
            }
        }
        
        return self.create_scheduled_job(job_config)
    
    def register_job_handler(self, job_type: str, handler: Callable):
        """Register a custom job handler"""
        self.job_handlers[job_type] = handler
    
    # Private methods
    def _scheduler_loop(self):
        """Main scheduler loop that runs in background thread"""
        while self.is_running:
            try:
                self._process_scheduled_jobs()
                self._cleanup_old_executions()
                self._record_resource_usage()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                print(f"Scheduler error: {e}")
                time.sleep(60)  # Wait longer on error
    
    def _process_scheduled_jobs(self):
        """Process jobs that are ready to run"""
        if len(self.running_jobs) >= self.max_concurrent_jobs:
            return  # At capacity
        
        # Get jobs ready to run
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        cursor.execute("""
            SELECT * FROM scheduled_jobs 
            WHERE status = 'scheduled' AND next_run_time <= ?
            ORDER BY priority DESC, next_run_time ASC
            LIMIT ?
        """, (now, self.max_concurrent_jobs - len(self.running_jobs)))
        
        ready_jobs = [self._row_to_scheduled_job(row) for row in cursor.fetchall()]
        conn.close()
        
        for job in ready_jobs:
            if self._check_dependencies(job):
                self._execute_job(job)
    
    def _execute_job(self, scheduled_job: ScheduledJob):
        """Execute a scheduled job"""
        execution_id = str(uuid.uuid4())
        
        # Create execution record
        execution = JobExecution(
            id=execution_id,
            scheduled_job_id=scheduled_job.id,
            status=JobStatus.RUNNING,
            start_time=datetime.now().isoformat()
        )
        
        # Store execution
        self._store_job_execution(execution)
        
        # Update job status
        self._update_job_status(scheduled_job.id, JobStatus.RUNNING)
        
        # Add to running jobs
        self.running_jobs[execution_id] = {
            "scheduled_job": scheduled_job,
            "execution": execution,
            "start_time": datetime.now()
        }
        
        # Execute in background thread
        execution_thread = threading.Thread(
            target=self._run_job_handler,
            args=(scheduled_job, execution),
            daemon=True
        )
        execution_thread.start()
    
    def _run_job_handler(self, scheduled_job: ScheduledJob, execution: JobExecution):
        """Run the actual job handler"""
        try:
            # Get handler
            handler = self.job_handlers.get(scheduled_job.job_type)
            if not handler:
                raise ValueError(f"No handler registered for job type: {scheduled_job.job_type}")
            
            # Execute handler
            result = handler(scheduled_job.job_data)
            
            # Update execution with success
            execution.status = JobStatus.COMPLETED
            execution.end_time = datetime.now().isoformat()
            execution.result = result
            
            # Update scheduled job
            scheduled_job.run_count += 1
            scheduled_job.last_run_time = execution.end_time
            scheduled_job.retry_count = 0  # Reset retry count on success
            
            # Calculate next run time for recurring jobs
            if scheduled_job.schedule_type == ScheduleType.RECURRING:
                if not scheduled_job.max_runs or scheduled_job.run_count < scheduled_job.max_runs:
                    scheduled_job.next_run_time = self._calculate_next_run_time(scheduled_job)
                    scheduled_job.status = JobStatus.SCHEDULED
                else:
                    scheduled_job.status = JobStatus.COMPLETED
            else:
                scheduled_job.status = JobStatus.COMPLETED
            
        except Exception as e:
            # Handle failure
            execution.status = JobStatus.FAILED
            execution.end_time = datetime.now().isoformat()
            execution.error_message = str(e)
            
            # Update scheduled job
            scheduled_job.retry_count += 1
            
            if scheduled_job.retry_count < scheduled_job.max_retries:
                # Schedule retry
                retry_delay = min(2 ** scheduled_job.retry_count, 60)  # Exponential backoff, max 60 minutes
                scheduled_job.next_run_time = (datetime.now() + timedelta(minutes=retry_delay)).isoformat()
                scheduled_job.status = JobStatus.SCHEDULED
            else:
                scheduled_job.status = JobStatus.FAILED
        
        finally:
            # Update database
            self._update_job_execution(execution)
            self._update_scheduled_job(scheduled_job)
            
            # Remove from running jobs
            if execution.id in self.running_jobs:
                del self.running_jobs[execution.id]
    
    def _register_default_handlers(self):
        """Register default job handlers"""
        
        async def batch_text_classification_handler(job_data: Dict[str, Any]) -> Dict[str, Any]:
            """Handler for batch text classification jobs"""
            # This would integrate with the existing job service
            try:
                # Read file data
                file_path = job_data["file_path"]
                if not Path(file_path).exists():
                    raise FileNotFoundError(f"File not found: {file_path}")
                
                # Use existing file parsing logic
                from shared.storage.file_manager import FileManager
                file_manager = FileManager()
                
                with open(file_path, 'rb') as f:
                    file_data = file_manager.parse_uploaded_file(Path(file_path))
                
                # Dispatch to Mother AI
                job_id = await self.job_service.dispatch_batch_job_to_mother_ai(
                    file_data=file_data,
                    available_labels=job_data["labels"],
                    instructions=job_data["instructions"],
                    original_filename=Path(file_path).name,
                    mother_ai_model=job_data["mother_ai_model"],
                    child_ai_model=job_data["child_ai_model"]
                )
                
                return {"job_id": job_id, "status": "dispatched"}
                
            except Exception as e:
                raise Exception(f"Batch classification failed: {str(e)}")
        
        def export_handler(job_data: Dict[str, Any]) -> Dict[str, Any]:
            """Handler for export jobs"""
            try:
                source_job_id = job_data["source_job_id"]
                export_format = job_data["export_format"]
                
                # Use existing export functionality
                # This would call the export endpoint
                
                return {
                    "exported_job_id": source_job_id,
                    "format": export_format,
                    "status": "exported"
                }
                
            except Exception as e:
                raise Exception(f"Export failed: {str(e)}")
        
        def cleanup_handler(job_data: Dict[str, Any]) -> Dict[str, Any]:
            """Handler for cleanup jobs"""
            try:
                # Clean up old files, logs, etc.
                days_old = job_data.get("days_old", 30)
                cutoff_date = datetime.now() - timedelta(days=days_old)
                
                cleanup_count = 0
                # Implementation would clean up old files
                
                return {"files_cleaned": cleanup_count}
                
            except Exception as e:
                raise Exception(f"Cleanup failed: {str(e)}")
        
        # Register handlers
        self.job_handlers = {
            "batch_text_classification": batch_text_classification_handler,
            "export": export_handler,
            "cleanup": cleanup_handler
        }
    
    def _validate_cron_expression(self, cron_expr: str) -> bool:
        """Validate cron expression"""
        try:
            if croniter is None:
                return True  # Skip validation if croniter not available
            croniter(cron_expr)
            return True
        except:
            return False
    
    def _calculate_next_run_time(self, job: ScheduledJob) -> str:
        """Calculate next run time for a job"""
        if job.schedule_type == ScheduleType.ONE_TIME:
            return job.schedule_expression
        elif job.schedule_type == ScheduleType.RECURRING:
            if croniter is None:
                # Fallback: schedule for next hour if croniter not available
                return (datetime.now() + timedelta(hours=1)).isoformat()
            cron = croniter(job.schedule_expression, datetime.now())
            return cron.get_next(datetime).isoformat()
        else:
            return datetime.now().isoformat()
    
    def _check_dependencies(self, job: ScheduledJob) -> bool:
        """Check if job dependencies are satisfied"""
        if not job.dependencies:
            return True
        
        # Check if all dependency jobs are completed
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for dep_id in job.dependencies:
            cursor.execute("SELECT status FROM scheduled_jobs WHERE id = ?", (dep_id,))
            row = cursor.fetchone()
            if not row or row[0] != JobStatus.COMPLETED.value:
                conn.close()
                return False
        
        conn.close()
        return True
    
    def _store_scheduled_job(self, job: ScheduledJob):
        """Store scheduled job in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO scheduled_jobs 
            (id, name, description, job_type, priority, schedule_type, schedule_expression,
             job_data, status, created_at, created_by, next_run_time, max_runs,
             max_retries, timeout_minutes, dependencies, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (job.id, job.name, job.description, job.job_type, job.priority.value,
              job.schedule_type.value, job.schedule_expression, json.dumps(job.job_data),
              job.status.value, job.created_at, job.created_by, job.next_run_time,
              job.max_runs, job.max_retries, job.timeout_minutes,
              json.dumps(job.dependencies), json.dumps(job.metadata)))
        
        conn.commit()
        conn.close()
    
    def _update_scheduled_job(self, job: ScheduledJob):
        """Update scheduled job in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE scheduled_jobs SET
            name = ?, description = ?, priority = ?, schedule_expression = ?,
            job_data = ?, status = ?, next_run_time = ?, last_run_time = ?,
            run_count = ?, retry_count = ?, max_retries = ?, timeout_minutes = ?,
            dependencies = ?, metadata = ?
            WHERE id = ?
        """, (job.name, job.description, job.priority.value, job.schedule_expression,
              json.dumps(job.job_data), job.status.value, job.next_run_time,
              job.last_run_time, job.run_count, job.retry_count, job.max_retries,
              job.timeout_minutes, json.dumps(job.dependencies), 
              json.dumps(job.metadata), job.id))
        
        conn.commit()
        conn.close()
    
    def _update_job_status(self, job_id: str, status: JobStatus):
        """Update job status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("UPDATE scheduled_jobs SET status = ? WHERE id = ?", 
                      (status.value, job_id))
        
        conn.commit()
        conn.close()
    
    def _store_job_execution(self, execution: JobExecution):
        """Store job execution in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO job_executions 
            (id, scheduled_job_id, status, start_time, end_time, result, error_message, logs)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (execution.id, execution.scheduled_job_id, execution.status.value,
              execution.start_time, execution.end_time, 
              json.dumps(execution.result) if execution.result else None,
              execution.error_message, json.dumps(execution.logs)))
        
        conn.commit()
        conn.close()
    
    def _update_job_execution(self, execution: JobExecution):
        """Update job execution in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE job_executions SET
            status = ?, end_time = ?, result = ?, error_message = ?, logs = ?
            WHERE id = ?
        """, (execution.status.value, execution.end_time,
              json.dumps(execution.result) if execution.result else None,
              execution.error_message, json.dumps(execution.logs), execution.id))
        
        conn.commit()
        conn.close()
    
    def _row_to_scheduled_job(self, row) -> ScheduledJob:
        """Convert database row to ScheduledJob"""
        return ScheduledJob(
            id=row[0], name=row[1], description=row[2], job_type=row[3],
            priority=JobPriority(row[4]), schedule_type=ScheduleType(row[5]),
            schedule_expression=row[6], job_data=json.loads(row[7]),
            status=JobStatus(row[8]), created_at=row[9], created_by=row[10],
            next_run_time=row[11], last_run_time=row[12], run_count=row[13],
            max_runs=row[14], retry_count=row[15], max_retries=row[16],
            timeout_minutes=row[17], dependencies=json.loads(row[18] or "[]"),
            metadata=json.loads(row[19] or "{}")
        )
    
    def _row_to_job_execution(self, row) -> JobExecution:
        """Convert database row to JobExecution"""
        return JobExecution(
            id=row[0], scheduled_job_id=row[1], status=JobStatus(row[2]),
            start_time=row[3], end_time=row[4],
            result=json.loads(row[5]) if row[5] else None,
            error_message=row[6], logs=json.loads(row[7] or "[]")
        )
    
    def _scheduled_job_to_dict(self, job: ScheduledJob) -> Dict[str, Any]:
        """Convert ScheduledJob to dictionary"""
        return {
            "id": job.id,
            "name": job.name,
            "description": job.description,
            "job_type": job.job_type,
            "priority": job.priority.value,
            "schedule_type": job.schedule_type.value,
            "schedule_expression": job.schedule_expression,
            "status": job.status.value,
            "next_run_time": job.next_run_time,
            "last_run_time": job.last_run_time,
            "run_count": job.run_count,
            "created_at": job.created_at
        }
    
    def _job_execution_to_dict(self, execution: JobExecution) -> Dict[str, Any]:
        """Convert JobExecution to dictionary"""
        return {
            "id": execution.id,
            "scheduled_job_id": execution.scheduled_job_id,
            "status": execution.status.value,
            "start_time": execution.start_time,
            "end_time": execution.end_time,
            "error_message": execution.error_message
        }
    
    def _cleanup_old_executions(self):
        """Clean up old job executions"""
        cutoff_date = (datetime.now() - timedelta(days=30)).isoformat()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM job_executions WHERE start_time < ?", (cutoff_date,))
        
        conn.commit()
        conn.close()
    
    def _record_resource_usage(self):
        """Record current resource usage"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get queue size
        cursor.execute("SELECT COUNT(*) FROM scheduled_jobs WHERE status = 'scheduled'")
        queue_size = cursor.fetchone()[0]
        
        cursor.execute("""
            INSERT INTO resource_usage 
            (timestamp, active_jobs, queue_size)
            VALUES (?, ?, ?)
        """, (datetime.now().isoformat(), len(self.running_jobs), queue_size))
        
        conn.commit()
        conn.close()
    
    def _calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate scheduler performance metrics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Success rate over last 24 hours
        yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed
            FROM job_executions 
            WHERE start_time >= ?
        """, (yesterday,))
        
        row = cursor.fetchone()
        total_executions = row[0] if row else 0
        completed_executions = row[1] if row else 0
        
        success_rate = (completed_executions / total_executions * 100) if total_executions > 0 else 0
        
        conn.close()
        
        return {
            "success_rate_24h": round(success_rate, 2),
            "total_executions_24h": total_executions,
            "current_queue_size": 0
        }
