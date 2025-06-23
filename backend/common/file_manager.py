

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any
from .file_parsers import parse_file

class FileManager:
    def __init__(self, base_data_dir: str = "./data"):
        self.base_data_dir = Path(base_data_dir)
        self.uploads_dir = self.base_data_dir / "uploads"
        self.processing_dir = self.base_data_dir / "processing"
        self.outputs_dir = self.base_data_dir / "outputs"
        self._create_directories()

    def _create_directories(self):
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.processing_dir.mkdir(parents=True, exist_ok=True)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)

    def save_uploaded_file(self, file_content: bytes, filename: str) -> Path:
        file_path = self.uploads_dir / filename
        with open(file_path, "wb") as f:
            f.write(file_content)
        return file_path

    def read_file_content(self, file_path: Path) -> str:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    def write_output_file(self, content: str, filename: str) -> Path:
        file_path = self.outputs_dir / filename
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        return file_path

    def move_to_processing(self, file_path: Path) -> Path:
        new_path = self.processing_dir / file_path.name
        os.rename(file_path, new_path)
        return new_path

    def delete_file(self, file_path: Path):
        os.remove(file_path)
    
    def parse_uploaded_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Parse uploaded file (JSON, CSV, or XML) and return standardized structure
        
        Returns:
            Dict with 'test_texts' array and metadata about the file
        """
        # Read file as bytes
        with open(file_path, "rb") as f:
            file_content = f.read()
        
        # Parse using the unified parser
        try:
            parsed_data = parse_file(str(file_path), file_content)
            
            # Add file metadata
            parsed_data["original_filename"] = file_path.name
            parsed_data["file_size"] = file_path.stat().st_size
            parsed_data["file_extension"] = file_path.suffix.lower()
            
            print(f"📁 Parsed {file_path.name}: {parsed_data['total_texts']} texts from {parsed_data['source_format'].upper()} format")
            
            return parsed_data
            
        except Exception as e:
            error_msg = f"Failed to parse {file_path.name}: {str(e)}"
            print(f"❌ {error_msg}")
            raise ValueError(error_msg)
    
    def save_parsed_data_as_json(self, parsed_data: Dict[str, Any], job_id: str) -> Path:
        """
        Save parsed data as JSON for internal processing
        This ensures all formats go through the same processing pipeline
        """
        filename = f"parsed_{job_id}.json"
        file_path = self.processing_dir / filename
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(parsed_data, f, indent=2, ensure_ascii=False)
        
        return file_path


