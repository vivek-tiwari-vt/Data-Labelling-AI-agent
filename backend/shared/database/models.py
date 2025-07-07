from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class JobRequest(BaseModel):
    text_content: str
    job_type: str = "text_labeling"
    # Add more fields as per your plan, e.g., schema, output_format

class FileUploadJobRequest(BaseModel):
    job_type: str = "batch_text_classification"
    available_labels: List[str]  # User-provided labels to choose from
    instructions: Optional[str] = "Analyze each text and assign the most appropriate label"

class BatchTextItem(BaseModel):
    id: str
    content: str
    category: Optional[str] = None
    expected_labels: Optional[List[str]] = None
    ai_assigned_label: Optional[str] = None  # Single label assigned by AI

class JobStatus(BaseModel):
    job_id: str
    status: str  # e.g., 'pending', 'processing', 'completed', 'failed'
    progress: float = 0.0  # 0.0 to 1.0
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class AgentTask(BaseModel):
    job_id: str
    task_id: str
    content: str
    task_type: str
    available_labels: Optional[List[str]] = None  # For classification tasks
    # Add more fields for agent-specific instructions

class AgentResult(BaseModel):
    job_id: str
    task_id: str
    result: Dict[str, Any]
    status: str # 'completed', 'failed'
    message: Optional[str] = None


