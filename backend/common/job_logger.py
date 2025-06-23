import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid

class JobLogger:
    def __init__(self):
        # Create logs directory
        self.logs_dir = Path(__file__).parent.parent.parent / "data" / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Job logs will be stored in individual files and a master log
        self.master_log_file = self.logs_dir / "master_job_log.jsonl"
        
    def create_job_log(self, job_id: str, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create initial job log entry with all submission details."""
        
        log_entry = {
            "job_id": job_id,
            "log_version": "1.0",
            "timestamps": {
                "job_created": datetime.now().isoformat(),
                "job_started": None,
                "job_completed": None
            },
            "job_metadata": {
                "original_filename": job_data.get("original_filename"),
                "file_size_bytes": job_data.get("file_size", 0),
                "total_texts": len(job_data.get("file_data", {}).get("test_texts", [])),
                "job_type": job_data.get("job_type"),
                "status": "created"
            },
            "user_input": {
                "available_labels": job_data.get("available_labels", []),
                "user_instructions": job_data.get("instructions", ""),
                "labels_count": len(job_data.get("available_labels", []))
            },
            "mother_ai": {
                "instructions_created": None,
                "instructions_length": 0,
                "content_analysis": None,
                "label_strategies": None,
                "classification_rules": None,
                "processing_timestamp": None
            },
            "text_agent": {
                "instructions_received": None,
                "processing_started": None,
                "processing_completed": None,
                "classification_strategy_parsed": None,
                "texts_processed": 0,
                "processing_details": []
            },
            "ai_models": {
                "models_available": [],
                "models_used": [],
                "api_providers": []
            },
            "results": {
                "output_file": None,
                "classification_summary": {},
                "processing_time_seconds": 0,
                "success_rate": 0.0
            },
            "sample_texts": [],
            "errors": [],
            "performance_metrics": {
                "queue_time_ms": 0,
                "processing_time_ms": 0,
                "total_time_ms": 0
            }
        }
        
        # Add sample texts for analysis
        file_data = job_data.get("file_data", {})
        texts = file_data.get("test_texts", [])
        if texts:
            # Store first 3 texts as samples
            for i, text_item in enumerate(texts[:3]):
                sample = {
                    "text_id": text_item.get("id", f"sample_{i+1}"),
                    "content": text_item.get("content", "")[:200] + "..." if len(text_item.get("content", "")) > 200 else text_item.get("content", ""),
                    "content_length": len(text_item.get("content", "")),
                    "expected_labels": text_item.get("expected_labels", []),
                    "assigned_label": None,
                    "classification_reasoning": None
                }
                log_entry["sample_texts"].append(sample)
        
        # Save initial log
        self._save_job_log(job_id, log_entry)
        self._append_to_master_log(log_entry)
        
        print(f"📝 Job log created for {job_id}: {self.logs_dir / f'job_{job_id}.json'}")
        return log_entry
    
    def update_mother_ai_processing(self, job_id: str, mother_ai_data: Dict[str, Any]):
        """Update log with Mother AI processing details."""
        
        log_entry = self._load_job_log(job_id)
        if not log_entry:
            print(f"❌ Could not find log for job {job_id}")
            return
        
        log_entry["timestamps"]["job_started"] = datetime.now().isoformat()
        log_entry["job_metadata"]["status"] = "processing_by_mother_ai"
        
        log_entry["mother_ai"] = {
            "instructions_created": mother_ai_data.get("enhanced_instructions", ""),
            "instructions_length": len(mother_ai_data.get("enhanced_instructions", "")),
            "content_analysis": mother_ai_data.get("content_analysis", ""),
            "label_strategies": mother_ai_data.get("label_strategies", ""),
            "classification_rules": mother_ai_data.get("classification_rules", ""),
            "processing_timestamp": datetime.now().isoformat(),
            "analysis_details": {
                "total_texts_analyzed": mother_ai_data.get("total_texts", 0),
                "sample_size_analyzed": mother_ai_data.get("sample_size", 0),
                "dominant_themes": mother_ai_data.get("dominant_themes", []),
                "content_types_found": mother_ai_data.get("content_types", [])
            }
        }
        
        # Update AI models info
        if "ai_client_info" in mother_ai_data:
            ai_info = mother_ai_data["ai_client_info"]
            log_entry["ai_models"]["models_available"] = ai_info.get("available_models", [])
            log_entry["ai_models"]["api_providers"] = ai_info.get("providers", [])
        
        self._save_job_log(job_id, log_entry)
        print(f"📝 Updated job log {job_id}: Mother AI processing completed")
    
    def update_text_agent_start(self, job_id: str, text_agent_data: Dict[str, Any]):
        """Update log when Text Agent starts processing."""
        
        log_entry = self._load_job_log(job_id)
        if not log_entry:
            return
        
        log_entry["job_metadata"]["status"] = "processing_by_text_agent"
        log_entry["text_agent"]["instructions_received"] = text_agent_data.get("enhanced_instructions", "")
        log_entry["text_agent"]["processing_started"] = datetime.now().isoformat()
        log_entry["text_agent"]["classification_strategy_parsed"] = text_agent_data.get("strategy_summary", "")
        
        self._save_job_log(job_id, log_entry)
        print(f"📝 Updated job log {job_id}: Text Agent started processing")
    
    def log_text_classification(self, job_id: str, text_id: str, classification_data: Dict[str, Any]):
        """Log individual text classification details."""
        
        log_entry = self._load_job_log(job_id)
        if not log_entry:
            return
        
        classification_detail = {
            "text_id": text_id,
            "content_preview": classification_data.get("content", "")[:100] + "..." if len(classification_data.get("content", "")) > 100 else classification_data.get("content", ""),
            "assigned_label": classification_data.get("assigned_label"),
            "classification_reasoning": classification_data.get("reasoning", ""),
            "confidence_score": classification_data.get("confidence", 0.0),
            "keyword_matches": classification_data.get("keyword_matches", []),
            "semantic_score": classification_data.get("semantic_score", 0),
            "processing_time_ms": classification_data.get("processing_time_ms", 0),
            "timestamp": datetime.now().isoformat()
        }
        
        log_entry["text_agent"]["processing_details"].append(classification_detail)
        log_entry["text_agent"]["texts_processed"] += 1
        
        # Update sample texts if this is one of them
        for sample in log_entry["sample_texts"]:
            if sample["text_id"] == text_id:
                sample["assigned_label"] = classification_data.get("assigned_label")
                sample["classification_reasoning"] = classification_data.get("reasoning", "")
                break
        
        self._save_job_log(job_id, log_entry)
    
    def complete_job_log(self, job_id: str, completion_data: Dict[str, Any]):
        """Finalize job log with completion details."""
        
        log_entry = self._load_job_log(job_id)
        if not log_entry:
            return
        
        completion_time = datetime.now().isoformat()
        log_entry["timestamps"]["job_completed"] = completion_time
        log_entry["job_metadata"]["status"] = completion_data.get("status", "completed")
        
        log_entry["text_agent"]["processing_completed"] = completion_time
        
        # Calculate processing times
        if log_entry["timestamps"]["job_started"]:
            start_time = datetime.fromisoformat(log_entry["timestamps"]["job_started"])
            end_time = datetime.fromisoformat(completion_time)
            total_time = (end_time - start_time).total_seconds()
            log_entry["performance_metrics"]["total_time_ms"] = int(total_time * 1000)
        
        # Update results
        log_entry["results"] = {
            "output_file": completion_data.get("output_file"),
            "classification_summary": completion_data.get("classification_summary", {}),
            "processing_time_seconds": completion_data.get("processing_time_seconds", 0),
            "success_rate": completion_data.get("success_rate", 1.0),
            "total_texts_processed": completion_data.get("total_processed", 0)
        }
        
        # Calculate classification distribution
        if log_entry["text_agent"]["processing_details"]:
            label_counts = {}
            for detail in log_entry["text_agent"]["processing_details"]:
                label = detail["assigned_label"]
                label_counts[label] = label_counts.get(label, 0) + 1
            log_entry["results"]["classification_summary"] = label_counts
        
        # Update AI models used
        if "models_used" in completion_data:
            log_entry["ai_models"]["models_used"] = completion_data["models_used"]
        
        self._save_job_log(job_id, log_entry)
        self._update_master_log(log_entry)
        
        print(f"📝 Job log completed for {job_id}")
        print(f"📊 Summary: {log_entry['results']['total_texts_processed']} texts processed in {log_entry['performance_metrics']['total_time_ms']}ms")
    
    def log_error(self, job_id: str, error_data: Dict[str, Any]):
        """Log error information."""
        
        log_entry = self._load_job_log(job_id)
        if not log_entry:
            return
        
        error_detail = {
            "timestamp": datetime.now().isoformat(),
            "error_type": error_data.get("error_type", "unknown"),
            "error_message": error_data.get("error_message", ""),
            "component": error_data.get("component", "unknown"),
            "stack_trace": error_data.get("stack_trace", "")
        }
        
        log_entry["errors"].append(error_detail)
        log_entry["job_metadata"]["status"] = "failed"
        log_entry["timestamps"]["job_completed"] = datetime.now().isoformat()
        
        self._save_job_log(job_id, log_entry)
        print(f"❌ Error logged for job {job_id}: {error_data.get('error_message', 'Unknown error')}")
    
    def get_job_log(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve complete job log."""
        return self._load_job_log(job_id)
    
    def get_job_summary(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get a summary of job processing."""
        log_entry = self._load_job_log(job_id)
        if not log_entry:
            return None
        
        return {
            "job_id": job_id,
            "status": log_entry["job_metadata"]["status"],
            "total_texts": log_entry["job_metadata"]["total_texts"],
            "texts_processed": log_entry["text_agent"]["texts_processed"],
            "classification_summary": log_entry["results"]["classification_summary"],
            "processing_time_ms": log_entry["performance_metrics"]["total_time_ms"],
            "success_rate": log_entry["results"]["success_rate"],
            "output_file": log_entry["results"]["output_file"]
        }
    
    def list_recent_jobs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """List recent jobs with summaries."""
        summaries = []
        
        # Read from master log file
        if self.master_log_file.exists():
            with open(self.master_log_file, 'r') as f:
                lines = f.readlines()
                for line in lines[-limit:]:
                    try:
                        log_entry = json.loads(line.strip())
                        summary = {
                            "job_id": log_entry["job_id"],
                            "status": log_entry["job_metadata"]["status"],
                            "created": log_entry["timestamps"]["job_created"],
                            "total_texts": log_entry["job_metadata"]["total_texts"],
                            "labels": log_entry["user_input"]["available_labels"]
                        }
                        summaries.append(summary)
                    except json.JSONDecodeError:
                        continue
        
        return list(reversed(summaries))  # Most recent first
    
    def _save_job_log(self, job_id: str, log_entry: Dict[str, Any]):
        """Save job log to individual file."""
        log_file = self.logs_dir / f"job_{job_id}.json"
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(log_entry, f, indent=2, ensure_ascii=False)
    
    def _load_job_log(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Load job log from individual file."""
        log_file = self.logs_dir / f"job_{job_id}.json"
        if not log_file.exists():
            return None
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return None
    
    def _append_to_master_log(self, log_entry: Dict[str, Any]):
        """Append log entry to master log file (JSONL format)."""
        with open(self.master_log_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
    
    def _update_master_log(self, log_entry: Dict[str, Any]):
        """Update existing entry in master log file."""
        if not self.master_log_file.exists():
            self._append_to_master_log(log_entry)
            return
        
        # Read all lines
        with open(self.master_log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Update the matching job entry
        job_id = log_entry["job_id"]
        updated = False
        
        for i, line in enumerate(lines):
            try:
                existing_entry = json.loads(line.strip())
                if existing_entry["job_id"] == job_id:
                    lines[i] = json.dumps(log_entry, ensure_ascii=False) + '\n'
                    updated = True
                    break
            except json.JSONDecodeError:
                continue
        
        # If not found, append
        if not updated:
            lines.append(json.dumps(log_entry, ensure_ascii=False) + '\n')
        
        # Write back
        with open(self.master_log_file, 'w', encoding='utf-8') as f:
            f.writelines(lines)

# Global logger instance
job_logger = JobLogger() 