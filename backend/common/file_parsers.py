"""
File parsers for different input formats (CSV, XML, JSON)
Converts all formats to the standard internal JSON structure with test_texts array
"""

import json
import csv
import xml.etree.ElementTree as ET
from typing import Dict, List, Any
from pathlib import Path
import io

class FileParser:
    """Unified file parser for JSON, CSV, and XML formats"""
    
    def __init__(self):
        self.supported_formats = ['.json', '.csv', '.xml']
    
    def parse_file(self, file_path: str, file_content: bytes) -> Dict[str, Any]:
        """
        Parse file based on extension and return standardized JSON structure
        
        Returns:
            Dict with 'test_texts' array containing text objects for classification
        """
        file_extension = Path(file_path).suffix.lower()
        
        if file_extension == '.json':
            return self._parse_json(file_content)
        elif file_extension == '.csv':
            return self._parse_csv(file_content)
        elif file_extension == '.xml':
            return self._parse_xml(file_content)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}. Supported: {self.supported_formats}")
    
    def _parse_json(self, file_content: bytes) -> Dict[str, Any]:
        """Parse JSON file - existing functionality unchanged"""
        try:
            content_str = file_content.decode('utf-8')
            data = json.loads(content_str)
            
            # Validate structure
            if 'test_texts' not in data:
                raise ValueError("JSON file must contain 'test_texts' array")
            
            if not isinstance(data['test_texts'], list):
                raise ValueError("'test_texts' must be an array")
            
            # Ensure each text has required structure
            standardized_texts = []
            for i, item in enumerate(data['test_texts']):
                if isinstance(item, str):
                    # Simple string, convert to object
                    standardized_texts.append({
                        "id": f"text_{i+1:03d}",
                        "content": item
                    })
                elif isinstance(item, dict):
                    # Object, ensure it has id and content
                    text_obj = {
                        "id": item.get("id", f"text_{i+1:03d}"),
                        "content": item.get("content", item.get("text", str(item)))
                    }
                    standardized_texts.append(text_obj)
                else:
                    # Other type, convert to string
                    standardized_texts.append({
                        "id": f"text_{i+1:03d}",
                        "content": str(item)
                    })
            
            return {
                "test_texts": standardized_texts,
                "source_format": "json",
                "total_texts": len(standardized_texts)
            }
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {e}")
        except UnicodeDecodeError as e:
            raise ValueError(f"Invalid file encoding: {e}")
    
    def _parse_csv(self, file_content: bytes) -> Dict[str, Any]:
        """Parse CSV file and convert to standard JSON structure"""
        try:
            content_str = file_content.decode('utf-8')
            
            # Try different CSV dialects
            sample = content_str[:1024]
            sniffer = csv.Sniffer()
            
            try:
                dialect = sniffer.sniff(sample, delimiters=',;\t')
            except csv.Error:
                # Default to comma-separated
                dialect = csv.excel
            
            # Parse CSV
            csv_reader = csv.DictReader(io.StringIO(content_str), dialect=dialect)
            rows = list(csv_reader)
            
            if not rows:
                raise ValueError("CSV file is empty or has no data rows")
            
            # Find text column (try common names)
            text_columns = ['text', 'content', 'message', 'description', 'comment', 'review', 'body']
            text_column = None
            
            # Check headers
            headers = list(rows[0].keys()) if rows else []
            
            # Find the best text column
            for col_name in text_columns:
                if col_name in headers:
                    text_column = col_name
                    break
            
            # If no standard column found, use the first column with text-like content
            if not text_column:
                for col_name in headers:
                    # Check if column has substantial text content
                    sample_values = [row.get(col_name, '') for row in rows[:5]]
                    avg_length = sum(len(str(val)) for val in sample_values) / len(sample_values)
                    if avg_length > 10:  # Assume columns with average >10 chars contain text
                        text_column = col_name
                        break
            
            if not text_column:
                raise ValueError(f"Could not find text column in CSV. Available columns: {headers}")
            
            # Convert to standard format
            standardized_texts = []
            for i, row in enumerate(rows):
                text_content = str(row.get(text_column, '')).strip()
                if text_content:  # Skip empty rows
                    text_obj = {
                        "id": row.get('id', f"csv_text_{i+1:03d}"),
                        "content": text_content
                    }
                    
                    # Add any additional metadata
                    metadata = {}
                    for key, value in row.items():
                        if key != text_column and key != 'id' and value:
                            metadata[key] = value
                    
                    if metadata:
                        text_obj["metadata"] = metadata
                    
                    standardized_texts.append(text_obj)
            
            if not standardized_texts:
                raise ValueError("No valid text content found in CSV file")
            
            return {
                "test_texts": standardized_texts,
                "source_format": "csv",
                "text_column": text_column,
                "total_texts": len(standardized_texts),
                "csv_headers": headers
            }
            
        except UnicodeDecodeError as e:
            raise ValueError(f"Invalid file encoding for CSV: {e}")
        except csv.Error as e:
            raise ValueError(f"CSV parsing error: {e}")
    
    def _parse_xml(self, file_content: bytes) -> Dict[str, Any]:
        """Parse XML file and convert to standard JSON structure"""
        try:
            content_str = file_content.decode('utf-8')
            root = ET.fromstring(content_str)
            
            # Common XML structures to look for
            text_elements = []
            
            # Strategy 1: Look for common text element names
            text_tags = ['text', 'content', 'message', 'description', 'comment', 'review', 'body', 'item', 'entry']
            
            for tag_name in text_tags:
                elements = root.findall(f".//{tag_name}")
                if elements:
                    text_elements = elements
                    break
            
            # Strategy 2: If no common tags found, look for elements with substantial text
            if not text_elements:
                for elem in root.iter():
                    if elem.text and len(elem.text.strip()) > 10:
                        text_elements.append(elem)
            
            # Strategy 3: If still no elements, look for direct children with text
            if not text_elements:
                for child in root:
                    if child.text and len(child.text.strip()) > 5:
                        text_elements.append(child)
            
            if not text_elements:
                raise ValueError("No text content found in XML file")
            
            # Convert to standard format
            standardized_texts = []
            for i, elem in enumerate(text_elements):
                text_content = elem.text.strip() if elem.text else ""
                
                if text_content:  # Skip empty elements
                    text_obj = {
                        "id": elem.get('id', f"xml_text_{i+1:03d}"),
                        "content": text_content
                    }
                    
                    # Add attributes as metadata
                    if elem.attrib:
                        text_obj["metadata"] = dict(elem.attrib)
                    
                    # Add tag name
                    text_obj["xml_tag"] = elem.tag
                    
                    standardized_texts.append(text_obj)
            
            if not standardized_texts:
                raise ValueError("No valid text content found in XML elements")
            
            return {
                "test_texts": standardized_texts,
                "source_format": "xml",
                "total_texts": len(standardized_texts),
                "root_tag": root.tag
            }
            
        except ET.ParseError as e:
            raise ValueError(f"XML parsing error: {e}")
        except UnicodeDecodeError as e:
            raise ValueError(f"Invalid file encoding for XML: {e}")

# Convenience function for easy usage
def parse_file(file_path: str, file_content: bytes) -> Dict[str, Any]:
    """Parse file and return standardized JSON structure"""
    parser = FileParser()
    return parser.parse_file(file_path, file_content) 