"""
Workflow Automation API Router
Provides endpoints for managing and executing automated workflows
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import sys
import os
from datetime import datetime
import uuid
import asyncio

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from core.jobs.workflows.workflow_automation import (
    WorkflowEngine, Workflow, WorkflowTrigger, WorkflowAction,
    TriggerType, ActionType
)

router = APIRouter(tags=["workflows"])

# Initialize workflow engine
workflow_engine = WorkflowEngine()

@router.get("/workflows")
async def list_workflows_alias():
    """Get all workflows (alias for root endpoint)"""
    try:
        workflows = workflow_engine.get_workflows()
        return {"status": "success", "workflows": workflows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list workflows: {str(e)}")

@router.get("/runs")
async def list_workflow_runs(
    workflow_id: Optional[str] = Query(default=None, description="Filter by workflow ID"),
    status: Optional[str] = Query(default=None, description="Filter by status"),
    limit: int = Query(default=50, description="Maximum number of runs to return")
):
    """Get workflow execution runs"""
    try:
        runs = workflow_engine.get_workflow_runs(workflow_id, status, limit)
        return {"status": "success", "runs": runs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list workflow runs: {str(e)}")

@router.get("/")
async def list_workflows():
    """Get all workflows"""
    try:
        workflows = workflow_engine.get_workflows()
        return {"status": "success", "workflows": workflows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list workflows: {str(e)}")

@router.post("/")
async def create_workflow(workflow_data: Dict[str, Any]):
    """Create a new workflow"""
    try:
        # Validate required fields
        required_fields = ['name', 'description', 'triggers', 'actions']
        for field in required_fields:
            if field not in workflow_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Parse triggers
        triggers = []
        for trigger_data in workflow_data['triggers']:
            trigger_type = TriggerType(trigger_data['type'])
            trigger = WorkflowTrigger(
                type=trigger_type,
                conditions=trigger_data.get('conditions', {}),
                metadata=trigger_data.get('metadata', {})
            )
            triggers.append(trigger)
        
        # Parse actions
        actions = []
        for action_data in workflow_data['actions']:
            action_type = ActionType(action_data['type'])
            action = WorkflowAction(
                type=action_type,
                parameters=action_data.get('parameters', {}),
                order=action_data.get('order', 0),
                max_retries=action_data.get('max_retries', 3)
            )
            actions.append(action)
        
        # Create workflow
        workflow = Workflow(
            id=str(uuid.uuid4()),
            name=workflow_data['name'],
            description=workflow_data['description'],
            triggers=triggers,
            actions=actions,
            is_active=workflow_data.get('is_active', True),
            created_at=datetime.now()
        )
        
        result = workflow_engine.create_workflow(workflow)
        return result
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid trigger or action type: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create workflow: {str(e)}")

@router.post("/trigger")
async def trigger_workflows(trigger_data: Dict[str, Any]):
    """Manually trigger workflows"""
    try:
        # Validate required fields
        if 'trigger_type' not in trigger_data:
            raise HTTPException(status_code=400, detail="Missing required field: trigger_type")
        
        trigger_type = TriggerType(trigger_data['trigger_type'])
        data = trigger_data.get('data', {})
        
        results = await workflow_engine.trigger_workflows(trigger_type, data)
        
        return {
            "status": "success",
            "triggered_workflows": len(results),
            "results": results
        }
    
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid trigger type: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger workflows: {str(e)}")

@router.get("/analytics")
async def get_workflow_analytics(days: int = Query(default=7, description="Number of days for analytics")):
    """Get workflow execution analytics"""
    try:
        analytics = workflow_engine.get_workflow_analytics(days)
        return {"status": "success", "analytics": analytics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get analytics: {str(e)}")

@router.get("/trigger-types")
async def get_trigger_types():
    """Get available trigger types"""
    return {
        "status": "success",
        "trigger_types": [
            {
                "type": "job_completed",
                "name": "Job Completed",
                "description": "Triggered when a labeling job is completed",
                "example_conditions": {
                    "job_status": {"value": "completed", "operator": "equals"}
                }
            },
            {
                "type": "confidence_threshold",
                "name": "Confidence Threshold",
                "description": "Triggered when average confidence falls below threshold",
                "example_conditions": {
                    "avg_confidence": {"value": 0.8, "operator": "less_than"}
                }
            },
            {
                "type": "label_distribution",
                "name": "Label Distribution",
                "description": "Triggered based on label distribution patterns",
                "example_conditions": {
                    "dominant_label_percentage": {"value": 0.9, "operator": "greater_than"}
                }
            },
            {
                "type": "error_rate",
                "name": "Error Rate",
                "description": "Triggered when error rate exceeds threshold",
                "example_conditions": {
                    "error_rate": {"value": 0.1, "operator": "greater_than"}
                }
            },
            {
                "type": "processing_time",
                "name": "Processing Time",
                "description": "Triggered based on processing time metrics",
                "example_conditions": {
                    "processing_time_ms": {"value": 30000, "operator": "greater_than"}
                }
            },
            {
                "type": "schedule",
                "name": "Schedule",
                "description": "Triggered on a schedule (handled externally)",
                "example_conditions": {
                    "schedule": {"value": "daily", "operator": "equals"}
                }
            },
            {
                "type": "manual",
                "name": "Manual",
                "description": "Manually triggered via API",
                "example_conditions": {}
            }
        ]
    }

@router.get("/action-types")
async def get_action_types():
    """Get available action types"""
    return {
        "status": "success",
        "action_types": [
            {
                "type": "email_notification",
                "name": "Email Notification",
                "description": "Send email notifications",
                "example_parameters": {
                    "smtp_server": "smtp.gmail.com",
                    "smtp_port": 587,
                    "username": "your-email@gmail.com",
                    "password": "your-password",
                    "to": ["recipient@example.com"],
                    "subject": "Workflow Alert: {trigger_type}",
                    "body": "Workflow triggered with data: {trigger_data}"
                }
            },
            {
                "type": "webhook",
                "name": "Webhook",
                "description": "Send HTTP webhook notifications",
                "example_parameters": {
                    "url": "https://hooks.slack.com/services/...",
                    "method": "POST",
                    "headers": {"Content-Type": "application/json"},
                    "payload": {"text": "Workflow triggered: {trigger_type}"}
                }
            },
            {
                "type": "export_data",
                "name": "Export Data",
                "description": "Export job data to files",
                "example_parameters": {
                    "format": "json",
                    "destination": "exports/{job_id}.json",
                    "include_metadata": True
                }
            },
            {
                "type": "generate_report",
                "name": "Generate Report",
                "description": "Generate automated reports",
                "example_parameters": {
                    "type": "summary",
                    "template": "default",
                    "destination": "reports/auto_report_{execution_id}.html"
                }
            },
            {
                "type": "retrain_model",
                "name": "Retrain Model",
                "description": "Trigger model retraining (placeholder)",
                "example_parameters": {
                    "model_id": "model_123",
                    "training_data_path": "data/training_set.json"
                }
            },
            {
                "type": "archive_data",
                "name": "Archive Data",
                "description": "Archive old data (placeholder)",
                "example_parameters": {
                    "age_threshold_days": 30,
                    "archive_location": "archive/"
                }
            },
            {
                "type": "escalate_review",
                "name": "Escalate Review",
                "description": "Flag items for human review (placeholder)",
                "example_parameters": {
                    "priority": "high",
                    "reviewer_group": "quality_team"
                }
            },
            {
                "type": "update_template",
                "name": "Update Template",
                "description": "Update labeling templates (placeholder)",
                "example_parameters": {
                    "template_id": "template_123",
                    "updates": {}
                }
            }
        ]
    }

@router.get("/templates")
async def get_workflow_templates():
    """Get predefined workflow templates"""
    templates = [
        {
            "name": "Low Confidence Alert",
            "description": "Send alert when confidence drops below threshold",
            "template": {
                "name": "Low Confidence Alert",
                "description": "Alert when labeling confidence is too low",
                "triggers": [
                    {
                        "type": "confidence_threshold",
                        "conditions": {
                            "avg_confidence": {"value": 0.8, "operator": "less_than"}
                        }
                    }
                ],
                "actions": [
                    {
                        "type": "email_notification",
                        "order": 0,
                        "parameters": {
                            "to": ["admin@example.com"],
                            "subject": "Low Confidence Alert",
                            "body": "Average confidence has dropped to {avg_confidence}"
                        }
                    }
                ]
            }
        },
        {
            "name": "Job Completion Export",
            "description": "Export data when job completes",
            "template": {
                "name": "Auto Export on Completion",
                "description": "Automatically export data when a job completes",
                "triggers": [
                    {
                        "type": "job_completed",
                        "conditions": {
                            "job_status": {"value": "completed", "operator": "equals"}
                        }
                    }
                ],
                "actions": [
                    {
                        "type": "export_data",
                        "order": 0,
                        "parameters": {
                            "format": "json",
                            "destination": "exports/{job_id}_results.json",
                            "include_metadata": True
                        }
                    },
                    {
                        "type": "generate_report",
                        "order": 1,
                        "parameters": {
                            "type": "summary",
                            "destination": "reports/{job_id}_report.html"
                        }
                    }
                ]
            }
        },
        {
            "name": "Error Rate Monitor",
            "description": "Monitor and alert on high error rates",
            "template": {
                "name": "Error Rate Monitor",
                "description": "Monitor system for high error rates",
                "triggers": [
                    {
                        "type": "error_rate",
                        "conditions": {
                            "error_rate": {"value": 0.05, "operator": "greater_than"}
                        }
                    }
                ],
                "actions": [
                    {
                        "type": "webhook",
                        "order": 0,
                        "parameters": {
                            "url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
                            "method": "POST",
                            "payload": {
                                "text": "⚠️ High error rate detected: {error_rate}",
                                "channel": "#alerts"
                            }
                        }
                    },
                    {
                        "type": "escalate_review",
                        "order": 1,
                        "parameters": {
                            "priority": "high",
                            "reviewer_group": "ops_team"
                        }
                    }
                ]
            }
        },
        {
            "name": "Daily Summary Report",
            "description": "Generate daily summary reports",
            "template": {
                "name": "Daily Summary Report",
                "description": "Generate and send daily summary reports",
                "triggers": [
                    {
                        "type": "schedule",
                        "conditions": {
                            "schedule": {"value": "daily", "operator": "equals"}
                        }
                    }
                ],
                "actions": [
                    {
                        "type": "generate_report",
                        "order": 0,
                        "parameters": {
                            "type": "daily_summary",
                            "destination": "reports/daily_{date}.html"
                        }
                    },
                    {
                        "type": "email_notification",
                        "order": 1,
                        "parameters": {
                            "to": ["team@example.com"],
                            "subject": "Daily Summary Report - {date}",
                            "body": "Please find attached the daily summary report."
                        }
                    }
                ]
            }
        }
    ]
    
    return {"status": "success", "templates": templates}

@router.delete("/{workflow_id}")
async def delete_workflow(workflow_id: str):
    """Delete a workflow"""
    try:
        result = workflow_engine.delete_workflow(workflow_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete workflow: {str(e)}")
