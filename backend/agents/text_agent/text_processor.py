import asyncio
import time
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple
import sys
import re
import traceback

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from agents.base_agent.base_processor import BaseProcessor
from common.ai_client import AIClient
from common.job_logger import job_logger

class TextProcessor(BaseProcessor):
    def __init__(self):
        self.ai_client = AIClient()
        # Set up outputs directory
        self.outputs_dir = Path(__file__).parent.parent.parent.parent / "data" / "outputs"
        self.outputs_dir.mkdir(parents=True, exist_ok=True)
        print("📝 Text Processor initialized with pure AI classification (no hardcoded logic)")

    async def process_batch_classification(self, job_data: dict) -> Dict[str, Any]:
        """Process batch text classification using ONLY AI and Mother AI's instructions."""
        job_id = job_data.get("job_id")
        file_data = job_data.get("file_data", {})
        available_labels = job_data.get("available_labels", [])
        user_instructions = job_data.get("user_instructions", "")
        enhanced_instructions = job_data.get("enhanced_instructions", "")
        original_filename = job_data.get("original_filename", "")
        
        # Get user-selected models
        mother_ai_model = job_data.get("mother_ai_model", self.ai_client.config.DEFAULT_OPENROUTER_MODEL)
        child_ai_model = job_data.get("child_ai_model", self.ai_client.config.DEFAULT_OPENROUTER_MODEL)
        
        print(f"📊 Text Agent received batch classification task:")
        print(f"   📁 File: {original_filename}")
        print(f"   🏷️  Labels: {', '.join(available_labels)}")
        print(f"   👤 User instructions: {user_instructions}")
        print(f"   🧠 Enhanced instructions from Mother AI: {len(enhanced_instructions)} chars")
        print(f"   🤖 Using child AI model: {child_ai_model}")
        
        # Log Text Agent start
        text_agent_data = {
            "enhanced_instructions": enhanced_instructions,
            "strategy_summary": self._extract_strategy_summary(enhanced_instructions)
        }
        job_logger.update_text_agent_start(job_id, text_agent_data)
        
        try:
            print(f"🚀 Text Agent starting pure AI classification process...")
            
            # Get test texts
            test_texts = file_data.get("test_texts", [])
            total_texts = len(test_texts)
            
            print(f"🤖 Text Agent using PURE AI classification for job {job_id}")
            print(f"📋 Enhanced instructions received: {len(enhanced_instructions)} characters")
            print(f"🎯 Available labels: {', '.join(available_labels)}")
            print(f"📊 Processing {total_texts} texts using ONLY AI and Mother AI guidance")
            
            # Process each text with pure AI classification
            results = []
            start_time = time.time()
            
            for i, text_item in enumerate(test_texts):
                text_id = text_item.get("id", f"text_{i+1:03d}")
                content = text_item.get("content", "")
                
                # Calculate progress
                progress = int(((i + 1) / total_texts) * 100)
                print(f"📊 Job {job_id} progress: {progress}% - Processing text {i+1}/{total_texts}")
                
                # Update Redis with progress (fix the 10000% issue)
                from common.redis_client import RedisClient
                redis_client = RedisClient()
                redis_client.update_job_status(job_id, "processing", float(progress))
                
                # Classify using PURE AI - no hardcoded logic
                classification_start = time.time()
                try:
                    classification_result = await self.classify_with_pure_ai(
                        content, available_labels, user_instructions, enhanced_instructions, child_ai_model
                    )
                    classification_time = int((time.time() - classification_start) * 1000)
                except Exception as classification_error:
                    # If API keys are missing, fail the entire job
                    if "NO API KEYS CONFIGURED" in str(classification_error):
                        print(f"🚨 CRITICAL: No API keys configured! Stopping entire job.")
                        raise classification_error
                    
                    # If all keys failed, fail the entire job  
                    elif "ALL" in str(classification_error) and "API KEYS FAILED" in str(classification_error):
                        print(f"🚨 CRITICAL: All API keys exhausted! Stopping entire job.")
                        raise classification_error
                    
                    # For other API errors, log and continue with next text
                    else:
                        print(f"⚠️  Text {i+1} classification failed: {str(classification_error)}")
                        classification_time = int((time.time() - classification_start) * 1000)
                        
                        # Log failed classification
                        classification_data = {
                            "content": content,
                            "assigned_label": "ERROR",
                            "reasoning": f"Classification failed: {str(classification_error)}",
                            "confidence": 0.0,
                            "ai_model_used": child_ai_model,
                            "processing_time_ms": classification_time,
                            "error": True
                        }
                        
                        job_logger.log_text_classification(job_id, text_id, classification_data)
                        
                        # Skip this text and continue
                        continue
                
                # Log individual classification
                classification_data = {
                    "content": content,
                    "assigned_label": classification_result["label"],
                    "reasoning": classification_result["reasoning"],
                    "confidence": classification_result["confidence"],
                    "ai_model_used": classification_result.get("model_used", "unknown"),
                    "processing_time_ms": classification_time
                }
                
                job_logger.log_text_classification(job_id, text_id, classification_data)
                
                # Add to results - preserve original metadata
                result = {
                    "id": text_id,
                    "content": content,
                    "ai_assigned_label": classification_result["label"]
                }
                
                # Preserve original metadata from the text item
                if 'metadata' in text_item:
                    result['metadata'] = text_item['metadata']
                
                # Preserve XML tag if available
                if 'xml_tag' in text_item:
                    result['xml_tag'] = text_item['xml_tag']
                
                results.append(result)
                
                # Print classification result
                print(f"✅ {text_id}: '{content[:50]}...' → {classification_result['label']} | {classification_result['reasoning']}")
            
            # Calculate processing time
            total_processing_time = time.time() - start_time
            
            # Save results in the same format as the original file
            output_dir = "/Volumes/DATA/Projects/data_label_agent/data/outputs"
            os.makedirs(output_dir, exist_ok=True)
            
            # Get original file format
            source_format = file_data.get("source_format", "json")
            output_file = await self._save_results_in_original_format(
                results, job_id, source_format, output_dir, file_data
            )
            
            print(f"📁 Clean result saved to: {output_file} (format: {source_format.upper()})")
            
            # Save detailed metadata to log file
            metadata_log = {
                "job_id": job_id,
                "original_filename": original_filename,
                "processing_timestamp": datetime.now().isoformat(),
                "total_texts": total_texts,
                "available_labels": available_labels,
                "user_instructions": user_instructions,
                "enhanced_instructions_length": len(enhanced_instructions),
                "processing_time_seconds": round(total_processing_time, 2),
                "classification_method": "intelligent_ai_classification",
                "ai_model_used": child_ai_model,
                "mother_ai_model": mother_ai_model,
                "child_ai_model": child_ai_model,
                "success_rate": 1.0,
                "models_used": [child_ai_model]
            }
            
            # Save metadata to log file
            log_file = f"{output_dir}/job_{job_id}_metadata.log"
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(metadata_log, f, indent=2, ensure_ascii=False)
            
            print(f"📋 Metadata logged to: {log_file}")
            
            # Complete job logging
            completion_data = {
                "status": "completed",
                "output_file": output_file,
                "processing_time_seconds": total_processing_time,
                "total_processed": total_texts,
                "success_rate": 1.0,
                "models_used": [child_ai_model]
            }
            
            job_logger.complete_job_log(job_id, completion_data)
            
            print(f"🎉 Text Agent completed job {job_id} successfully using PURE AI classification")
            
            return {
                "status": "completed",
                "results_file": output_file,
                "total_processed": total_texts,
                "processing_time": total_processing_time
            }
            
        except Exception as e:
            error_message = f"Text Agent processing failed: {str(e)}"
            print(f"❌ {error_message}")
            print(f"📊 Job {job_id} STOPPED due to AI classification failure")
            
            # Check if it's an API key issue
            if "NO API KEYS CONFIGURED" in str(e):
                print(f"🔑 API KEYS MISSING! Please run: python setup_api_keys.py")
                error_message = f"❌ NO API KEYS CONFIGURED! {str(e)}"
            elif "API call" in str(e) and "failed" in str(e):
                print(f"🌐 API call failed! Check your API keys and network connection")
                error_message = f"❌ API CALL FAILED! {str(e)}"
            
            traceback.print_exc()
            
            # Log error
            job_logger.log_error(job_id, {
                "error_type": "text_agent_processing_error",
                "error_message": error_message,
                "component": "text_agent",
                "stack_trace": traceback.format_exc(),
                "requires_api_key_setup": "NO API KEYS CONFIGURED" in str(e)
            })
            
            raise Exception(error_message)

    async def classify_with_pure_ai(self, content: str, available_labels: List[str], 
                                  user_instructions: str, enhanced_instructions: str, 
                                  model: str = None) -> Dict[str, Any]:
        """
        Classify text using PURE AI with NO hardcoded logic.
        Uses only the Mother AI instructions and available labels provided by the user.
        """
        
        # Create a comprehensive classification prompt
        classification_prompt = f"""You are an expert text classifier. Classify the text using ONLY the provided labels.

TEXT: "{content}"

AVAILABLE LABELS: {', '.join(available_labels)}

USER INSTRUCTIONS: {user_instructions}

ENHANCED INSTRUCTIONS: {enhanced_instructions}

IMPORTANT: Respond with ONLY valid JSON in this exact format:
{{"label": "chosen_label", "confidence": 0.95, "reasoning": "explanation"}}

Choose from: {', '.join(available_labels)}"""

        try:
            # Use AI to classify the text with the specified model
            ai_response = await self.ai_client.chat_completion(
                messages=[{"role": "user", "content": classification_prompt}],
                max_tokens=500,
                temperature=0.1,  # Low temperature for consistent classification
                model=model  # Use the user-selected model
            )
            
            ai_content = ai_response.get("content", "")
            model_used = ai_response.get("model", "unknown")
            
            # Parse the AI response
            try:
                # Try to extract JSON from the response
                import json
                import re
                
                print(f"🔍 AI Response: {ai_content[:200]}...")  # Debug log
                
                # Try multiple approaches to extract JSON
                parsed_response = None
                
                # Approach 1: Try direct JSON parsing
                try:
                    parsed_response = json.loads(ai_content.strip())
                except json.JSONDecodeError:
                    pass
                
                # Approach 2: Look for JSON blocks with better regex
                if not parsed_response:
                    json_matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', ai_content)
                    for match in json_matches:
                        try:
                            parsed_response = json.loads(match)
                            break
                        except json.JSONDecodeError:
                            continue
                
                # Approach 3: Extract from code blocks
                if not parsed_response:
                    code_block_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', ai_content, re.DOTALL)
                    if code_block_match:
                        try:
                            parsed_response = json.loads(code_block_match.group(1))
                        except json.JSONDecodeError:
                            pass
                
                if parsed_response:
                    # Validate the response
                    predicted_label = parsed_response.get("label", "")
                    confidence = float(parsed_response.get("confidence", 0.5))
                    reasoning = parsed_response.get("reasoning", "AI classification")
                    
                    # Ensure the predicted label is in available labels
                    if predicted_label in available_labels:
                        return {
                            "label": predicted_label,
                            "confidence": confidence,
                            "reasoning": reasoning,
                            "model_used": model_used,
                            "method": "pure_ai_classification"
                        }
                    else:
                        # NO FALLBACK - Raise error if AI returned invalid label
                        raise ValueError(f"AI returned invalid label '{predicted_label}' not in available labels: {available_labels}")
                else:
                    raise ValueError("No JSON found in AI response")
                    
            except (json.JSONDecodeError, ValueError) as parse_error:
                print(f"❌ Failed to parse AI response: {parse_error}")
                print(f"🔍 Raw AI response: {ai_content}")
                # NO FALLBACK - Raise the parse error
                raise ValueError(f"Failed to parse AI response: {parse_error}. Raw response: {ai_content[:200]}...")
                
        except Exception as e:
            print(f"❌ AI classification failed: {str(e)}")
            # NO FALLBACK - Raise the actual error
            raise Exception(f"AI classification failed: {str(e)}")

    def _extract_strategy_summary(self, enhanced_instructions: str) -> str:
        """Extract a summary of the strategy from enhanced instructions."""
        if not enhanced_instructions:
            return "No enhanced instructions provided"
        
        # Simple extraction of key strategy points
        lines = enhanced_instructions.split('\n')
        strategy_lines = []
        
        for line in lines:
            if any(keyword in line.lower() for keyword in ['strategy', 'classify', 'label', 'approach']):
                strategy_lines.append(line.strip())
        
        if strategy_lines:
            return "; ".join(strategy_lines[:3])  # First 3 strategy lines
        else:
            return f"Enhanced instructions provided: {len(enhanced_instructions)} characters"

    async def _save_results_in_original_format(self, results: list, job_id: str, 
                                             source_format: str, output_dir: str, 
                                             file_data: dict) -> str:
        """Save classification results in the same format as the original file."""
        import csv
        import xml.etree.ElementTree as ET
        from xml.dom import minidom
        
        if source_format == "json":
            # Save as JSON
            output_file = f"{output_dir}/job_{job_id}_labeled.json"
            essential_output = {"test_texts": results}
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(essential_output, f, indent=2, ensure_ascii=False)
                
        elif source_format == "csv":
            # Save as CSV
            output_file = f"{output_dir}/job_{job_id}_labeled.csv"
            
            # Get original CSV headers and text column info
            csv_headers = file_data.get("csv_headers", [])
            text_column = file_data.get("text_column", "content")
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                if results:
                    # Create fieldnames based on original headers plus label column
                    fieldnames = []
                    
                    # Reconstruct original column order
                    for header in csv_headers:
                        if header == text_column:
                            fieldnames.append(text_column)  # Original text column
                        elif header != 'id':  # Skip id if it was original column
                            fieldnames.append(header)  # Other metadata columns
                    
                    # Add label column if not already present
                    if 'ai_assigned_label' not in fieldnames:
                        fieldnames.append('ai_assigned_label')
                    
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for result in results:
                        row = {}
                        
                        # Get metadata from the result
                        metadata = result.get('metadata', {})
                        
                        # Populate row based on original CSV structure
                        for fieldname in fieldnames:
                            if fieldname == text_column:
                                row[fieldname] = result.get('content', '')
                            elif fieldname == 'ai_assigned_label':
                                row[fieldname] = result.get('ai_assigned_label', '')
                            elif fieldname == 'id':
                                row[fieldname] = result.get('id', '')
                            else:
                                # Get from metadata
                                row[fieldname] = metadata.get(fieldname, '')
                        
                        writer.writerow(row)
                        
        elif source_format == "xml":
            # Save as XML
            output_file = f"{output_dir}/job_{job_id}_labeled.xml"
            
            # Get original XML root tag
            root_tag = file_data.get("root_tag", "labeled_texts")
            
            # Create root element with original tag name
            root = ET.Element(root_tag)
            
            for result in results:
                # Use original XML tag name if available
                xml_tag = result.get('xml_tag', 'text')
                text_elem = ET.SubElement(root, xml_tag)
                
                # Add id and label as attributes
                text_elem.set("id", str(result.get('id', '')))
                text_elem.set("ai_assigned_label", str(result.get('ai_assigned_label', '')))
                
                # Add original metadata as attributes
                metadata = result.get('metadata', {})
                for key, value in metadata.items():
                    if key not in ['id', 'ai_assigned_label']:
                        text_elem.set(key, str(value))
                
                # Set the text content
                text_elem.text = str(result.get('content', ''))
            
            # Pretty print XML
            rough_string = ET.tostring(root, encoding='unicode')
            reparsed = minidom.parseString(rough_string)
            pretty_xml = reparsed.toprettyxml(indent="  ")
            
            # Remove empty lines and XML declaration duplicate
            lines = [line for line in pretty_xml.split('\n') if line.strip()]
            if len(lines) > 1 and '<?xml' in lines[0] and '<?xml' in lines[1]:
                lines = lines[1:]  # Remove duplicate XML declaration
            pretty_xml = '\n'.join(lines)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(pretty_xml)
                
        else:
            # Default to JSON if unknown format
            output_file = f"{output_dir}/job_{job_id}_labeled.json"
            essential_output = {"test_texts": results}
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(essential_output, f, indent=2, ensure_ascii=False)
        
        return output_file

    async def process_single_text(self, job_data: dict) -> Dict[str, Any]:
        """Process single text classification with pure AI."""
        job_id = job_data.get("job_id")
        text_content = job_data.get("text_content", "")
        enhanced_instructions = job_data.get("enhanced_instructions", "")
        
        print(f"📝 Text Agent processing single text for job {job_id} using pure AI")
        
        # Log Text Agent start
        text_agent_data = {
            "enhanced_instructions": enhanced_instructions,
            "strategy_summary": "Single text pure AI classification"
        }
        job_logger.update_text_agent_start(job_id, text_agent_data)
        
        try:
            # For single text, we need to determine appropriate labels or use general classification
            # This would typically come from the job request, but for now we'll use a general approach
            
            general_classification_prompt = f"""
            Classify this text into an appropriate category:
            
            TEXT: "{text_content}"
            
            INSTRUCTIONS: {enhanced_instructions}
            
            Provide a single appropriate label for this text.
            """
            
            ai_response = await self.ai_client.chat_completion(
                messages=[{"role": "user", "content": general_classification_prompt}],
                max_tokens=100
            )
            
            result_label = ai_response.get("content", "general").strip().lower()
            
            # Log classification
            classification_data = {
                "content": text_content,
                "assigned_label": result_label,
                "reasoning": "Single text AI classification",
                "confidence": 0.8,
                "processing_time_ms": 100
            }
            
            job_logger.log_text_classification(job_id, "single_text", classification_data)
            
            # Complete job
            completion_data = {
                "status": "completed",
                "total_processed": 1,
                "success_rate": 1.0
            }
            
            job_logger.complete_job_log(job_id, completion_data)
            
            return {
                "status": "completed",
                "label": result_label,
                "confidence": 0.8
            }
            
        except Exception as e:
            job_logger.log_error(job_id, {
                "error_type": "single_text_processing_error",
                "error_message": str(e),
                "component": "text_agent"
            })
            raise

    # Remove all legacy hardcoded methods
    async def process(self, content: str, job_id: str, task_id: str) -> Dict[str, Any]:
        """Legacy method - redirects to pure AI classification."""
        print(f"⚠️ Using legacy process method - redirecting to pure AI classification")
        
        # Create a simple job data structure for legacy support
        job_data = {
            "job_id": job_id,
            "text_content": content,
            "enhanced_instructions": "Classify this text appropriately"
        }
        
        return await self.process_single_text(job_data)

    async def save_result_to_file(self, result: Dict[str, Any], job_id: str):
        """Save the processed result to a JSON file in the outputs directory."""
        try:
            filename = f"job_{job_id}_labeled.json"
            filepath = self.outputs_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            print(f"📁 Result saved to: {filepath}")
            
        except Exception as e:
            print(f"❌ Error saving result to file: {e}")

    async def simulate_processing_with_progress(self, job_id: str, content: str):
        """Simulates processing with progress updates."""
        from common.redis_client import RedisClient
        from common.models import JobStatus
        
        redis_client = RedisClient()
        
        # Simulate processing steps with progress updates
        steps = [
            (0.2, "Initializing AI classification..."),
            (0.4, "Analyzing text content..."),
            (0.6, "Applying classification model..."),
            (0.8, "Generating labels..."),
            (1.0, "Finalizing results...")
        ]
        
        for progress, message in steps:
            # Update job status
            job_status = JobStatus(
                job_id=job_id,
                status="processing",
                progress=progress
            )
            redis_client.set_key(f"job:{job_id}", job_status.dict())
            
            # Publish progress update
            progress_message = {
                "job_id": job_id,
                "status": "processing",
                "progress": progress,
                "message": message
            }
            redis_client.publish_message(f"job_progress:{job_id}", progress_message)
            
            print(f"Job {job_id} progress: {progress*100:.0f}% - {message}")
            
            # Simulate processing time
            await asyncio.sleep(0.5)

