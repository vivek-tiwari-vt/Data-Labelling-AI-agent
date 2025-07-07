"""
Workflow Automation System
Automates actions and workflows based on labeling results, thresholds, and triggers.
"""

import json
import os
import sqlite3
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import logging
from enum import Enum
import asyncio
import uuid
from abc import ABC, abstractmethod

# For email notifications
try:
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    HAS_EMAIL = True
except ImportError:
    HAS_EMAIL = False

# For webhook notifications
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

class TriggerType(Enum):
    JOB_COMPLETED = "job_completed"
    CONFIDENCE_THRESHOLD = "confidence_threshold"
    LABEL_DISTRIBUTION = "label_distribution"
    ERROR_RATE = "error_rate"
    PROCESSING_TIME = "processing_time"
    SCHEDULE = "schedule"
    MANUAL = "manual"

class ActionType(Enum):
    EMAIL_NOTIFICATION = "email_notification"
    WEBHOOK = "webhook"
    EXPORT_DATA = "export_data"
    RETRAIN_MODEL = "retrain_model"
    ARCHIVE_DATA = "archive_data"
    GENERATE_REPORT = "generate_report"
    ESCALATE_REVIEW = "escalate_review"
    UPDATE_TEMPLATE = "update_template"

@dataclass
class WorkflowTrigger:
    """Defines when a workflow should be triggered"""
    type: TriggerType
    conditions: Dict[str, Any]  # e.g., {"threshold": 0.8, "operator": "less_than"}
    metadata: Dict[str, Any] = None

@dataclass
class WorkflowAction:
    """Defines what action to take when triggered"""
    type: ActionType
    parameters: Dict[str, Any]
    order: int = 0  # Execution order
    retry_count: int = 0
    max_retries: int = 3

@dataclass
class Workflow:
    """Complete workflow definition"""
    id: str
    name: str
    description: str
    triggers: List[WorkflowTrigger]
    actions: List[WorkflowAction]
    is_active: bool = True
    created_at: datetime = None
    last_executed: Optional[datetime] = None
    execution_count: int = 0
    success_count: int = 0
    failure_count: int = 0

@dataclass
class WorkflowExecution:
    """Record of a workflow execution"""
    execution_id: str
    workflow_id: str
    trigger_type: TriggerType
    trigger_data: Dict[str, Any]
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "running"  # "running", "completed", "failed", "partial"
    actions_executed: List[Dict[str, Any]] = None
    error_message: Optional[str] = None

class BaseAction(ABC):
    """Base class for workflow actions"""
    
    def __init__(self, action: WorkflowAction):
        self.action = action
        self.logger = logging.getLogger(f"workflow.action.{action.type.value}")
    
    @abstractmethod
    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the action with the given context"""
        pass

class EmailNotificationAction(BaseAction):
    """Send email notifications"""
    
    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        if not HAS_EMAIL:
            return {"status": "error", "message": "Email support not available"}
        
        try:
            params = self.action.parameters
            
            # Email configuration
            smtp_server = params.get('smtp_server', 'localhost')
            smtp_port = params.get('smtp_port', 587)
            username = params.get('username')
            password = params.get('password')
            
            # Email content
            to_emails = params.get('to', [])
            subject = params.get('subject', 'Workflow Notification').format(**context)
            body = params.get('body', 'Workflow triggered').format(**context)
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = params.get('from', username)
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(smtp_server, smtp_port)
            if username and password:
                server.starttls()
                server.login(username, password)
            
            text = msg.as_string()
            server.sendmail(msg['From'], to_emails, text)
            server.quit()
            
            return {"status": "success", "recipients": to_emails}
        
        except Exception as e:
            self.logger.error(f"Email notification failed: {e}")
            return {"status": "error", "message": str(e)}

class WebhookAction(BaseAction):
    """Send webhook notifications"""
    
    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        if not HAS_REQUESTS:
            return {"status": "error", "message": "HTTP support not available"}
        
        try:
            params = self.action.parameters
            
            url = params.get('url')
            method = params.get('method', 'POST')
            headers = params.get('headers', {})
            
            # Prepare payload
            payload = {
                "workflow_id": context.get('workflow_id'),
                "trigger": context.get('trigger_type'),
                "timestamp": datetime.now().isoformat(),
                "data": context.get('trigger_data', {}),
                **params.get('payload', {})
            }
            
            # Send request
            response = requests.request(
                method=method,
                url=url,
                json=payload,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            return {
                "status": "success",
                "status_code": response.status_code,
                "response": response.text[:500]  # Truncate response
            }
        
        except Exception as e:
            self.logger.error(f"Webhook action failed: {e}")
            return {"status": "error", "message": str(e)}

class ExportDataAction(BaseAction):
    """Export data to various formats"""
    
    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        try:
            from core.jobs.exports.export_manager import export_manager
            
            params = self.action.parameters
            job_id = context.get('job_id')
            if not job_id:
                return {"status": "error", "message": "No job ID provided"}
            
            # Export parameters
            export_format = params.get('format', 'json')
            include_metadata = params.get('include_metadata', True)
            
            # Get job data
            job_data = self._get_job_data(job_id, include_metadata)
            
            # Use the centralized export manager
            destination = await export_manager.export_results(
                job_id=job_id,
                results=job_data.get('results', []),
                job_metadata=job_data.get('metadata', {}),
                format_type=export_format
            )
            
            return {
                "status": "success",
                "destination": destination,
                "format": export_format,
                "records": len(job_data.get('results', []))
            }
        
        except Exception as e:
            self.logger.error(f"Export action failed: {e}")
            return {"status": "error", "message": str(e)}
    
    def _get_job_data(self, job_id: str, include_metadata: bool) -> Dict[str, Any]:
        """Get job data - integration with job system"""
        try:
            # Try to get from job logger first
            from infrastructure.monitoring.job_logger import job_logger
            job_log = job_logger.get_job_log(job_id)
            
            if job_log:
                return {
                    "job_id": job_id,
                    "timestamp": job_log.get('created', datetime.now().isoformat()),
                    "results": job_log.get('labeled_data', []),
                    "metadata": job_log if include_metadata else {}
                }
        except ImportError:
            pass
        
        # Fallback to basic structure
        return {
            "job_id": job_id,
            "timestamp": datetime.now().isoformat(),
            "results": [],
            "metadata": {} if include_metadata else None
        }

class GenerateReportAction(BaseAction):
    """Generate automated reports"""
    
    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        try:
            params = self.action.parameters
            
            report_type = params.get('type', 'summary')
            template = params.get('template', 'default')
            destination = params.get('destination', f'reports/{uuid.uuid4()}.html')
            
            # Generate report content
            report_content = self._generate_report_content(report_type, context)
            
            # Ensure reports directory exists
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            
            # Save report
            with open(destination, 'w') as f:
                f.write(report_content)
            
            return {
                "status": "success",
                "destination": destination,
                "type": report_type
            }
        
        except Exception as e:
            self.logger.error(f"Report generation failed: {e}")
            return {"status": "error", "message": str(e)}
    
    def _generate_report_content(self, report_type: str, context: Dict[str, Any]) -> str:
        """Generate report content based on type"""
        
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Workflow Report - {report_type.title()}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 10px; border-radius: 5px; }}
                .content {{ margin-top: 20px; }}
                .section {{ margin-bottom: 20px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Workflow Report: {report_type.title()}</h1>
                <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Workflow ID: {context.get('workflow_id', 'N/A')}</p>
            </div>
            
            <div class="content">
                <div class="section">
                    <h2>Trigger Information</h2>
                    <p><strong>Trigger Type:</strong> {context.get('trigger_type', 'N/A')}</p>
                    <p><strong>Trigger Data:</strong> {json.dumps(context.get('trigger_data', {}), indent=2)}</p>
                </div>
                
                <div class="section">
                    <h2>Context Data</h2>
                    <pre>{json.dumps(context, indent=2, default=str)}</pre>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html_template

class WorkflowEngine:
    """Main engine for managing and executing workflows"""
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            # Use existing scheduler database since workflows are related to scheduling
            db_path = os.path.join(os.path.dirname(__file__), '../../../../data/scheduler/scheduler.db')
        self.db_path = db_path
        self.logger = logging.getLogger("workflow_engine")
        self._init_database()
        
        # Map action types to classes
        self.action_classes = {
            ActionType.EMAIL_NOTIFICATION: EmailNotificationAction,
            ActionType.WEBHOOK: WebhookAction,
            ActionType.EXPORT_DATA: ExportDataAction,
            ActionType.GENERATE_REPORT: GenerateReportAction,
        }
        
        # Active workflows cache
        self._workflows_cache = {}
        self._load_workflows()
    
    def _init_database(self):
        """Initialize the SQLite database for workflows"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS workflows (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    triggers TEXT NOT NULL,
                    actions TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_executed TEXT,
                    execution_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    failure_count INTEGER DEFAULT 0
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS workflow_executions (
                    execution_id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL,
                    trigger_type TEXT NOT NULL,
                    trigger_data TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT DEFAULT 'running',
                    actions_executed TEXT,
                    error_message TEXT,
                    FOREIGN KEY (workflow_id) REFERENCES workflows (id)
                )
            ''')
    
    def _load_workflows(self):
        """Load active workflows into cache"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT * FROM workflows WHERE is_active = 1')
            
            for row in cursor.fetchall():
                # Deserialize triggers with Enum conversion
                triggers_data = json.loads(row[3])
                triggers = []
                for t in triggers_data:
                    trigger = WorkflowTrigger(
                        type=TriggerType(t['type']),
                        conditions=t['conditions'],
                        metadata=t.get('metadata', {})
                    )
                    triggers.append(trigger)
                
                # Deserialize actions with Enum conversion
                actions_data = json.loads(row[4])
                actions = []
                for a in actions_data:
                    action = WorkflowAction(
                        type=ActionType(a['type']),
                        parameters=a['parameters'],
                        order=a.get('order', 0),
                        retry_count=a.get('retry_count', 0),
                        max_retries=a.get('max_retries', 3)
                    )
                    actions.append(action)
                
                workflow = Workflow(
                    id=row[0],
                    name=row[1],
                    description=row[2],
                    triggers=triggers,
                    actions=actions,
                    is_active=bool(row[5]),
                    created_at=datetime.fromisoformat(row[6]),
                    last_executed=datetime.fromisoformat(row[7]) if row[7] else None,
                    execution_count=row[8],
                    success_count=row[9],
                    failure_count=row[10]
                )
                self._workflows_cache[workflow.id] = workflow
    
    def create_workflow(self, workflow: Workflow) -> Dict[str, Any]:
        """Create a new workflow"""
        try:
            # Custom serialization to handle Enums
            triggers_data = []
            for trigger in workflow.triggers:
                trigger_dict = {
                    'type': trigger.type.value,
                    'conditions': trigger.conditions,
                    'metadata': trigger.metadata or {}
                }
                triggers_data.append(trigger_dict)
            
            actions_data = []
            for action in workflow.actions:
                action_dict = {
                    'type': action.type.value,
                    'parameters': action.parameters,
                    'order': action.order,
                    'retry_count': action.retry_count,
                    'max_retries': action.max_retries
                }
                actions_data.append(action_dict)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO workflows 
                    (id, name, description, triggers, actions, is_active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    workflow.id,
                    workflow.name,
                    workflow.description,
                    json.dumps(triggers_data),
                    json.dumps(actions_data),
                    workflow.is_active,
                    workflow.created_at.isoformat() if workflow.created_at else datetime.now().isoformat()
                ))
            
            if workflow.is_active:
                self._workflows_cache[workflow.id] = workflow
            
            self.logger.info(f"Created workflow: {workflow.name}")
            return {"status": "success", "workflow_id": workflow.id}
        
        except Exception as e:
            self.logger.error(f"Failed to create workflow: {e}")
            return {"status": "error", "message": str(e)}
    
    async def trigger_workflows(self, trigger_type: TriggerType, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Trigger workflows based on type and data"""
        results = []
        
        for workflow in self._workflows_cache.values():
            for trigger in workflow.triggers:
                if trigger.type == trigger_type and self._check_trigger_conditions(trigger, data):
                    result = await self._execute_workflow(workflow, trigger_type, data)
                    results.append(result)
        
        return results
    
    def _check_trigger_conditions(self, trigger: WorkflowTrigger, data: Dict[str, Any]) -> bool:
        """Check if trigger conditions are met"""
        conditions = trigger.conditions
        
        for key, condition in conditions.items():
            if key not in data:
                return False
            
            value = data[key]
            operator = condition.get('operator', 'equals')
            threshold = condition.get('value')
            
            if operator == 'equals' and value != threshold:
                return False
            elif operator == 'not_equals' and value == threshold:
                return False
            elif operator == 'greater_than' and value <= threshold:
                return False
            elif operator == 'less_than' and value >= threshold:
                return False
            elif operator == 'greater_equal' and value < threshold:
                return False
            elif operator == 'less_equal' and value > threshold:
                return False
            elif operator == 'contains' and threshold not in str(value):
                return False
        
        return True
    
    async def _execute_workflow(self, workflow: Workflow, trigger_type: TriggerType, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a workflow"""
        execution_id = str(uuid.uuid4())
        started_at = datetime.now()
        
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow.id,
            trigger_type=trigger_type,
            trigger_data=data,
            started_at=started_at
        )
        
        # Log execution start
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO workflow_executions 
                (execution_id, workflow_id, trigger_type, trigger_data, started_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                execution_id,
                workflow.id,
                trigger_type.value,
                json.dumps(data),
                started_at.isoformat()
            ))
        
        actions_executed = []
        context = {
            "workflow_id": workflow.id,
            "execution_id": execution_id,
            "trigger_type": trigger_type.value,
            "trigger_data": data,
            **data
        }
        
        try:
            # Execute actions in order
            sorted_actions = sorted(workflow.actions, key=lambda a: a.order)
            
            for action in sorted_actions:
                action_class = self.action_classes.get(action.type)
                if action_class:
                    action_instance = action_class(action)
                    result = await action_instance.execute(context)
                    
                    actions_executed.append({
                        "type": action.type.value,
                        "parameters": action.parameters,
                        "result": result,
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    # If action failed and has retries, attempt retry
                    if result.get("status") == "error" and action.retry_count < action.max_retries:
                        await asyncio.sleep(1)  # Brief delay before retry
                        action.retry_count += 1
                        result = await action_instance.execute(context)
                        actions_executed[-1]["result"] = result
                        actions_executed[-1]["retry_count"] = action.retry_count
            
            # Update execution success
            completed_at = datetime.now()
            status = "completed"
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE workflow_executions 
                    SET completed_at = ?, status = ?, actions_executed = ?
                    WHERE execution_id = ?
                ''', (
                    completed_at.isoformat(),
                    status,
                    json.dumps(actions_executed),
                    execution_id
                ))
                
                # Update workflow stats
                conn.execute('''
                    UPDATE workflows 
                    SET last_executed = ?, execution_count = execution_count + 1,
                        success_count = success_count + 1
                    WHERE id = ?
                ''', (completed_at.isoformat(), workflow.id))
            
            return {
                "status": "success",
                "execution_id": execution_id,
                "workflow_id": workflow.id,
                "actions_executed": len(actions_executed)
            }
        
        except Exception as e:
            # Update execution failure
            completed_at = datetime.now()
            error_message = str(e)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    UPDATE workflow_executions 
                    SET completed_at = ?, status = ?, error_message = ?, actions_executed = ?
                    WHERE execution_id = ?
                ''', (
                    completed_at.isoformat(),
                    "failed",
                    error_message,
                    json.dumps(actions_executed),
                    execution_id
                ))
                
                # Update workflow stats
                conn.execute('''
                    UPDATE workflows 
                    SET failure_count = failure_count + 1
                    WHERE id = ?
                ''', (workflow.id,))
            
            self.logger.error(f"Workflow execution failed: {e}")
            return {
                "status": "error",
                "execution_id": execution_id,
                "message": error_message
            }
    
    def get_workflows(self) -> List[Dict[str, Any]]:
        """Get all workflows"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT * FROM workflows ORDER BY created_at DESC')
            workflows = []
            
            for row in cursor.fetchall():
                workflows.append({
                    "id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "triggers": json.loads(row[3]),
                    "actions": json.loads(row[4]),
                    "is_active": bool(row[5]),
                    "created_at": row[6],
                    "last_executed": row[7],
                    "execution_count": row[8],
                    "success_count": row[9],
                    "failure_count": row[10]
                })
            
            return workflows
    
    def get_workflow_runs(self, workflow_id: Optional[str] = None, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Get workflow execution runs with optional filters"""
        with sqlite3.connect(self.db_path) as conn:
            # Build query with filters
            conditions = []
            params = []
            
            if workflow_id:
                conditions.append("workflow_id = ?")
                params.append(workflow_id)
            
            if status:
                conditions.append("status = ?")
                params.append(status)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            cursor = conn.execute(f'''
                SELECT * FROM workflow_executions 
                WHERE {where_clause}
                ORDER BY started_at DESC 
                LIMIT ?
            ''', params + [limit])
            
            runs = []
            for row in cursor.fetchall():
                runs.append({
                    "execution_id": row[0],
                    "workflow_id": row[1],
                    "trigger_type": row[2],
                    "trigger_data": json.loads(row[3]),
                    "started_at": row[4],
                    "completed_at": row[5],
                    "status": row[6],
                    "actions_executed": json.loads(row[7]) if row[7] else [],
                    "error_message": row[8]
                })
            
            return runs
    
    def get_workflow_analytics(self, days: int = 7) -> Dict[str, Any]:
        """Get workflow execution analytics"""
        with sqlite3.connect(self.db_path) as conn:
            # Get execution statistics
            cursor = conn.execute('''
                SELECT status, COUNT(*) as count
                FROM workflow_executions 
                WHERE started_at > datetime('now', '-{} days')
                GROUP BY status
            '''.format(days))
            
            status_counts = dict(cursor.fetchall())
            
            # Get most active workflows
            cursor = conn.execute('''
                SELECT w.name, w.execution_count, w.success_count, w.failure_count
                FROM workflows w
                ORDER BY w.execution_count DESC
                LIMIT 10
            ''')
            
            active_workflows = []
            for row in cursor.fetchall():
                active_workflows.append({
                    "name": row[0],
                    "execution_count": row[1],
                    "success_count": row[2],
                    "failure_count": row[3],
                    "success_rate": row[2] / row[1] if row[1] > 0 else 0
                })
            
            # Get trigger type distribution
            cursor = conn.execute('''
                SELECT trigger_type, COUNT(*) as count
                FROM workflow_executions 
                WHERE started_at > datetime('now', '-{} days')
                GROUP BY trigger_type
            '''.format(days))
            
            trigger_distribution = dict(cursor.fetchall())
            
            return {
                "status_counts": status_counts,
                "active_workflows": active_workflows,
                "trigger_distribution": trigger_distribution,
                "total_workflows": len(self.get_workflows()),
                "active_workflows_count": len(self._workflows_cache),
                "period_days": days
            }
    
    def delete_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """Delete a workflow and its execution history"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if workflow exists
                cursor = conn.execute('SELECT id, name FROM workflows WHERE id = ?', (workflow_id,))
                workflow = cursor.fetchone()
                
                if not workflow:
                    return {"status": "error", "message": f"Workflow with id {workflow_id} not found"}
                
                workflow_name = workflow[1]
                
                # Delete workflow executions first (foreign key constraint)
                cursor = conn.execute('DELETE FROM workflow_executions WHERE workflow_id = ?', (workflow_id,))
                executions_deleted = cursor.rowcount
                
                # Delete the workflow
                cursor = conn.execute('DELETE FROM workflows WHERE id = ?', (workflow_id,))
                workflows_deleted = cursor.rowcount
                
                # Remove from cache
                if workflow_id in self._workflows_cache:
                    del self._workflows_cache[workflow_id]
                
                conn.commit()
                
                self.logger.info(f"Deleted workflow '{workflow_name}' (ID: {workflow_id}) and {executions_deleted} execution records")
                
                return {
                    "status": "success",
                    "message": f"Workflow '{workflow_name}' deleted successfully",
                    "workflow_id": workflow_id,
                    "workflow_name": workflow_name,
                    "executions_deleted": executions_deleted
                }
                
        except Exception as e:
            self.logger.error(f"Error deleting workflow {workflow_id}: {str(e)}")
            return {"status": "error", "message": f"Failed to delete workflow: {str(e)}"}
