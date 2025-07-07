"""
Enhanced export manager for classification results in different file formats
Focused on job result exports - analytics exports handled by visual_creator system
"""
import json
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd

class ExportManager:
    """Manages export of classification results to various formats"""
    
    def __init__(self):
        self.supported_formats = ['json', 'csv', 'xml', 'xlsx']
    
    async def export_results(self, job_id: str, results: List[Dict], 
                           job_metadata: Dict, format_type: str = 'json') -> str:
        """Export classification results in the specified format"""
        
        if format_type not in self.supported_formats:
            raise ValueError(f"Unsupported format: {format_type}")
        
        output_dir = Path("/Volumes/DATA/Projects/data_label_agent/data/outputs")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if format_type == 'xlsx':
            return await self._export_to_excel(job_id, results, job_metadata, output_dir)
        elif format_type == 'csv':
            return await self._export_to_enhanced_csv(job_id, results, job_metadata, output_dir)
        elif format_type == 'json':
            return await self._export_to_enhanced_json(job_id, results, job_metadata, output_dir)
        else:
            raise ValueError(f"Format {format_type} not implemented yet")
    
    async def _export_to_excel(self, job_id: str, results: List[Dict], 
                             job_metadata: Dict, output_dir: Path) -> str:
        """Export results to Excel with multiple sheets and analytics"""
        
        output_file = output_dir / f"job_{job_id}_detailed_report.xlsx"
        
        # Create Excel writer
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Sheet 1: Main Results
            results_df = pd.DataFrame(results)
            if not results_df.empty:
                # Clean up the dataframe
                if 'metadata' in results_df.columns:
                    # Expand metadata into separate columns
                    metadata_df = pd.json_normalize(results_df['metadata'])
                    results_df = pd.concat([results_df.drop('metadata', axis=1), metadata_df], axis=1)
                
                results_df.to_excel(writer, sheet_name='Classifications', index=False)
            
            # Sheet 2: Job Summary
            summary_data = {
                'Metric': [
                    'Job ID',
                    'Processing Date',
                    'Total Texts Processed',
                    'Available Labels',
                    'Success Rate',
                    'Average Processing Time (ms)',
                    'Mother AI Model',
                    'Child AI Model'
                ],
                'Value': [
                    job_id,
                    job_metadata.get('processing_timestamp', datetime.now().isoformat()),
                    len(results),
                    ', '.join(job_metadata.get('available_labels', [])),
                    f"{job_metadata.get('success_rate', 100):.1f}%",
                    job_metadata.get('processing_time_seconds', 0) * 1000,
                    job_metadata.get('mother_ai_model', 'Unknown'),
                    job_metadata.get('child_ai_model', 'Unknown')
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Job Summary', index=False)
            
            # Sheet 3: Label Distribution
            if results:
                labels = [r.get('ai_assigned_label', 'Unknown') for r in results]
                label_counts = pd.Series(labels).value_counts()
                label_stats = pd.DataFrame({
                    'Label': label_counts.index,
                    'Count': label_counts.values,
                    'Percentage': (label_counts.values / len(results) * 100).round(2)
                })
                label_stats.to_excel(writer, sheet_name='Label Distribution', index=False)
            
            # Sheet 4: Confidence Analysis if available
            confidence_data = []
            for result in results:
                if 'confidence' in result and result['confidence'] is not None:
                    confidence_data.append({
                        'ID': result.get('id', ''),
                        'Content Preview': result.get('content', '')[:100] + '...' if len(result.get('content', '')) > 100 else result.get('content', ''),
                        'Label': result.get('ai_assigned_label', ''),
                        'Confidence': result.get('confidence', 0)
                    })
            
            if confidence_data:
                confidence_df = pd.DataFrame(confidence_data)
                confidence_df = confidence_df.sort_values('Confidence', ascending=False)
                confidence_df.to_excel(writer, sheet_name='Confidence Analysis', index=False)
        
        return str(output_file)
    
    async def _export_to_enhanced_csv(self, job_id: str, results: List[Dict], 
                                    job_metadata: Dict, output_dir: Path) -> str:
        """Export to CSV with additional metadata columns"""
        
        output_file = output_dir / f"job_{job_id}_enhanced.csv"
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            if not results:
                return str(output_file)
            
            # Determine all possible fieldnames
            fieldnames = set()
            for result in results:
                fieldnames.update(result.keys())
                if 'metadata' in result:
                    fieldnames.update(result['metadata'].keys())
            
            # Remove metadata from fieldnames and add individual metadata fields
            fieldnames.discard('metadata')
            fieldnames = list(fieldnames)
            
            # Add job metadata as additional columns
            fieldnames.extend(['job_id', 'processing_date', 'job_success_rate'])
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                row = result.copy()
                
                # Flatten metadata
                if 'metadata' in row:
                    metadata = row.pop('metadata')
                    row.update(metadata)
                
                # Add job-level information
                row['job_id'] = job_id
                row['processing_date'] = job_metadata.get('processing_timestamp', datetime.now().isoformat())
                row['job_success_rate'] = job_metadata.get('success_rate', 100)
                
                writer.writerow(row)
        
        return str(output_file)
    
    async def _export_to_enhanced_json(self, job_id: str, results: List[Dict], 
                                     job_metadata: Dict, output_dir: Path) -> str:
        """Export to JSON with comprehensive metadata"""
        
        output_file = output_dir / f"job_{job_id}_enhanced.json"
        
        # Create enhanced data structure
        enhanced_data = {
            "job_metadata": {
                "job_id": job_id,
                "export_timestamp": datetime.now().isoformat(),
                "total_results": len(results),
                **job_metadata
            },
            "results": results,
            "analytics": {
                "summary": {
                    "total_classifications": len(results),
                    "export_format": "json",
                    "processing_completed": datetime.now().isoformat()
                }
            }
        }
        
        # Calculate label distribution
        if results:
            enhanced_data["analytics"]["label_distribution"] = {}
            labels = [r.get('ai_assigned_label', 'Unknown') for r in results]
            for label in set(labels):
                count = labels.count(label)
                enhanced_data["analytics"]["label_distribution"][label] = {
                    "count": count,
                    "percentage": round(count / len(results) * 100, 2)
                }
        
        # Calculate confidence statistics if available
        confidences = [r.get('confidence', 0) for r in results if 'confidence' in r]
        if confidences:
            enhanced_data["analytics"]["confidence_statistics"] = {
                "average": round(sum(confidences) / len(confidences), 3),
                "min": min(confidences),
                "max": max(confidences),
                "high_confidence_count": len([c for c in confidences if c >= 0.8]),
                "low_confidence_count": len([c for c in confidences if c < 0.6])
            }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_data, f, indent=2, ensure_ascii=False)
        
        return str(output_file)


# Global instance for backward compatibility
export_manager = ExportManager()
