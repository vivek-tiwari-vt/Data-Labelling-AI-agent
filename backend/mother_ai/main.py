import asyncio
import json
import sys
import os
from datetime import datetime
import traceback

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from shared.messaging.redis_client import RedisClient
from shared.database.models import JobStatus
from shared.utils.ai_client import AIClient
from infrastructure.monitoring.job_logger import job_logger

class MotherAI:
    def __init__(self):
        self.redis_client = RedisClient()
        self.ai_client = AIClient()
        print("🤖 Mother AI initialized with enhanced logging")
        print(f"📊 Available AI models: OpenRouter={len(self.ai_client.key_manager.openrouter_keys)}, Gemini={len(self.ai_client.key_manager.gemini_keys)}, OpenAI={len(self.ai_client.key_manager.openai_keys)}")

    async def process_job(self, job_data: dict):
        """Process incoming job with comprehensive logging."""
        job_id = job_data.get("job_id")
        job_type = job_data.get("job_type")
        
        try:
            print(f"\n🤖 Mother AI received job: {job_id}")
            print(f"📋 Job type: {job_type}")
            
            # Update job status to processing
            self.redis_client.update_job_status(job_id, "processing", 10.0)
            
            if job_type == "batch_text_classification":
                await self.handle_batch_text_classification(job_data)
            elif job_type == "text_labeling":
                await self.handle_text_labeling(job_data)
            else:
                raise ValueError(f"Unknown job type: {job_type}")
                
        except Exception as e:
            error_message = f"Mother AI processing failed: {str(e)}"
            print(f"❌ {error_message}")
            traceback.print_exc()
            
            # Log error
            job_logger.log_error(job_id, {
                "error_type": "mother_ai_processing_error",
                "error_message": error_message,
                "component": "mother_ai",
                "stack_trace": traceback.format_exc()
            })
            
            # Update job status to failed
            self.redis_client.update_job_status(job_id, "failed", 0.0, {"error": error_message})

    async def handle_batch_text_classification(self, job_data: dict):
        """Handle batch text classification with comprehensive logging."""
        job_id = job_data.get("job_id")
        file_data = job_data.get("file_data", {})
        available_labels = job_data.get("available_labels", [])
        user_instructions = job_data.get("instructions", "")
        original_filename = job_data.get("original_filename", "")
        
        # Get user-selected models
        mother_ai_model = job_data.get("mother_ai_model", self.ai_client.config.DEFAULT_OPENROUTER_MODEL)
        child_ai_model = job_data.get("child_ai_model", self.ai_client.config.DEFAULT_OPENROUTER_MODEL)
        
        print(f"🧠 Mother AI analyzing content for job {job_id}")
        print(f"📁 File: {original_filename}")
        print(f"🏷️  Available labels: {', '.join(available_labels)}")
        print(f"📝 User instructions: {user_instructions}")
        print(f"🤖 Mother AI model: {mother_ai_model} (will be used for content analysis & instruction generation)")
        print(f"👶 Child AI model: {child_ai_model} (will be used for individual text classification)")
        
        # Perform content analysis using the selected Mother AI model
        content_analysis = await self.perform_content_analysis(file_data, available_labels, mother_ai_model)
        
        # Create intelligent instructions using the selected Mother AI model
        enhanced_instructions = await self.create_intelligent_instructions(
            file_data, available_labels, user_instructions, content_analysis, mother_ai_model
        )
        
        # Log Mother AI processing details
        mother_ai_data = {
            "enhanced_instructions": enhanced_instructions,
            "total_texts": len(file_data.get("test_texts", [])),
            "available_labels": available_labels,
            "classification_methodology": content_analysis.get("classification_methodology", ""),
        }
        
        job_logger.update_mother_ai_processing(job_id, mother_ai_data)
        
        print(f"📊 Mother AI created {len(enhanced_instructions)} character instruction for Text Agent")
        print(f"🎯 Enhanced instructions include content analysis and classification strategies")
        
        # Update job status to dispatching
        self.redis_client.update_job_status(job_id, "dispatching_to_text_agent", 30.0)
        
        # Send enhanced task to Text Agent
        text_agent_task = {
            "job_id": job_id,
            "job_type": "batch_text_classification",
            "file_data": file_data,
            "available_labels": available_labels,
            "original_filename": original_filename,
            "user_instructions": user_instructions,
            "enhanced_instructions": enhanced_instructions,
            "content_analysis": content_analysis,
            "timestamp": datetime.now().isoformat(),
            "mother_ai_model": mother_ai_model,
            "child_ai_model": child_ai_model,
            "models_available": {
                "openrouter": len(self.ai_client.key_manager.openrouter_keys),
                "gemini": len(self.ai_client.key_manager.gemini_keys),
                "openai": len(self.ai_client.key_manager.openai_keys)
            }
        }
        
        # Dispatch to Text Agent
        self.redis_client.publish_message("text_agent_jobs", text_agent_task)
        
        print(f"📤 Mother AI dispatched enhanced task to Text Agent for job {job_id}")
        print(f"🔍 Enhanced instructions length: {len(enhanced_instructions)} characters")

    async def handle_text_labeling(self, job_data: dict):
        """Handle single text labeling with logging."""
        job_id = job_data.get("job_id")
        text_content = job_data.get("text_content", "")
        
        print(f"🧠 Mother AI processing single text for job {job_id}")
        
        # Log Mother AI processing
        mother_ai_data = {
            "enhanced_instructions": f"Process single text: {text_content[:100]}...",
            "content_analysis": f"Single text analysis for: {text_content[:50]}...",
            "total_texts": 1,
            "sample_size": 1
        }
        
        job_logger.update_mother_ai_processing(job_id, mother_ai_data)
        
        # For single text, we can process directly or still send to Text Agent
        # For now, let's send to Text Agent for consistency
        text_agent_task = {
            "job_id": job_id,
            "job_type": "text_labeling",
            "text_content": text_content,
            "enhanced_instructions": "Analyze and label this single text",
            "timestamp": datetime.now().isoformat()
        }
        
        self.redis_client.publish_message("text_agent_jobs", text_agent_task)
        print(f"📤 Mother AI dispatched single text task to Text Agent for job {job_id}")

    async def perform_content_analysis(self, file_data: dict, available_labels: list, mother_ai_model: str) -> dict:
        """Perform AI-powered content analysis using the selected Mother AI model."""
        test_texts = file_data.get("test_texts", [])
        
        # Sample a few texts for analysis (to keep costs reasonable)
        sample_size = min(5, len(test_texts))
        sample_texts = test_texts[:sample_size]
        sample_content = "\n\n".join([f"Text {i+1}: {text.get('content', '')[:200]}..." 
                                     for i, text in enumerate(sample_texts)])
        
        analysis_prompt = f"""Analyze this sample of {sample_size} texts from a dataset of {len(test_texts)} texts that need to be classified into these labels: {', '.join(available_labels)}

SAMPLE TEXTS:
{sample_content}

AVAILABLE LABELS: {', '.join(available_labels)}

Provide a JSON response with:
1. "content_patterns": What types of content patterns do you see?
2. "label_strategies": For each label, what specific characteristics should guide classification?
3. "classification_methodology": What approach should be used for accurate classification?
4. "key_indicators": What are the key indicators that distinguish between labels?

Respond with valid JSON only."""

        try:
            print(f"🧠 Mother AI ({mother_ai_model}) analyzing content sample...")
            
            ai_response = await self.ai_client.chat_completion(
                messages=[{"role": "user", "content": analysis_prompt}],
                max_tokens=1000,
                temperature=0.3,
                model=mother_ai_model
            )
            
            ai_content = ai_response.get("content", "")
            print(f"✅ Mother AI analysis received: {len(ai_content)} characters")
            
            # Try to parse AI response as JSON
            try:
                import json
                import re
                
                # Try to extract JSON from the response
                json_match = re.search(r'\{.*\}', ai_content, re.DOTALL)
                if json_match:
                    analysis_result = json.loads(json_match.group())
                else:
                    analysis_result = json.loads(ai_content)
                
                # Add metadata
                analysis_result.update({
                    "total_texts": len(test_texts),
                    "sample_size": sample_size,
                    "available_labels": available_labels,
                    "analysis_timestamp": datetime.now().isoformat(),
                    "mother_ai_model_used": mother_ai_model
                })
                
                print(f"✅ Content analysis completed using {mother_ai_model}")
                return analysis_result
                
            except (json.JSONDecodeError, AttributeError) as e:
                print(f"⚠️  Failed to parse AI analysis, using fallback: {e}")
                # Fallback to basic analysis if AI response parsing fails
                pass
                
        except Exception as e:
            print(f"⚠️  AI content analysis failed, using fallback: {e}")
            # Continue with fallback if AI call fails
            pass
        
        # Fallback analysis
        analysis_data = {
            "content_patterns": "Mixed content types requiring intelligent classification",
            "label_strategies": {label: f"Classify content as '{label}' based on semantic meaning and context" for label in available_labels},
            "classification_methodology": "Pure AI semantic classification with contextual understanding",
            "key_indicators": "Text purpose, content type, authorial intent, and semantic context",
            "total_texts": len(test_texts),
            "sample_size": sample_size,
            "available_labels": available_labels,
            "analysis_timestamp": datetime.now().isoformat(),
            "mother_ai_model_used": f"{mother_ai_model} (fallback used)"
        }
        
        print(f"✅ Content analysis completed (fallback mode)")
        return analysis_data

    async def create_intelligent_instructions(self, file_data: dict, available_labels: list, 
                                            user_instructions: str, content_analysis: dict, mother_ai_model: str) -> str:
        """Create AI-enhanced classification instructions using the selected Mother AI model."""
        
        # Try to create AI-enhanced instructions first
        instruction_prompt = f"""You are creating detailed classification instructions for another AI agent that will classify {len(file_data.get('test_texts', []))} texts into these labels: {', '.join(available_labels)}

USER INSTRUCTIONS: {user_instructions}

CONTENT ANALYSIS RESULTS:
- Content patterns found: {content_analysis.get('content_patterns', 'Mixed content')}
- Classification methodology: {content_analysis.get('classification_methodology', 'Semantic analysis')}
- Key indicators: {content_analysis.get('key_indicators', 'Context and purpose')}

Create comprehensive classification instructions that will guide the Child AI to make accurate decisions. Include:
1. Clear decision criteria for each label
2. Examples of what qualifies for each category
3. How to handle edge cases and ambiguous content
4. Specific reasoning patterns to follow

The instructions should be detailed enough that another AI can consistently apply them across the entire dataset."""

        try:
            print(f"🧠 Mother AI ({mother_ai_model}) creating enhanced instructions...")
            
            ai_response = await self.ai_client.chat_completion(
                messages=[{"role": "user", "content": instruction_prompt}],
                max_tokens=2000,
                temperature=0.2,
                model=mother_ai_model
            )
            
            ai_instructions = ai_response.get("content", "")
            print(f"✅ AI-enhanced instructions created: {len(ai_instructions)} characters")
            
            # Combine AI instructions with metadata
            enhanced_instructions = f"""
AI-Enhanced Classification Instructions (Generated by {mother_ai_model}):

{ai_instructions}

METADATA:
- Total texts to process: {len(file_data.get('test_texts', []))}
- Available labels: {', '.join(available_labels)}
- User instructions: {user_instructions}
- Analysis model: {mother_ai_model}
- Generated: {datetime.now().isoformat()}
"""
            
            return enhanced_instructions
            
        except Exception as e:
            print(f"⚠️  AI instruction generation failed, using fallback: {e}")
            # Continue with fallback instructions
            pass
        
        # Fallback to static instructions if AI call fails
        instructions = f"""
Enhanced Classification Instructions for Child AI Agent (Fallback Mode):

USER INSTRUCTIONS: {user_instructions}
AVAILABLE LABELS: {', '.join(available_labels)}

You are an expert human data labeler with decades of experience in content classification. Your classification decisions must demonstrate the same nuanced understanding and contextual awareness that a seasoned human expert would apply.

CORE CLASSIFICATION PHILOSOPHY:

1. **Three-Layer Content Analysis**:
   - **Surface Layer**: What is explicitly stated?
   - **Intent Layer**: Why was this written? What's the author's goal?
   - **Function Layer**: How would this be used/organized by readers?
   
   Always prioritize intent and function over surface-level keywords.

2. **Expert Decision Process**:
   For each text, determine:
   a) **Primary Purpose**: What is the author's main communicative goal?
   b) **Functional Role**: Where would this naturally belong in an organized system?
   c) **Best Label**: Which captures the essential nature, not secondary elements?

3. **Quality Verification**:
   Apply the "filing cabinet test": In a well-organized system, where would this naturally belong?
   Use the "expert consensus test": Would other experts immediately understand this choice?

4. **Advanced Pattern Recognition**:
   **Avoid Common Traps**:
   - Content ABOUT topic X ≠ content OF category X
   - Format similarity ≠ content category similarity
   - Emotional expressions should be classified by communicative function
   - Personal experiences ≠ reviews (even when opinion-based)
   
   **When Multiple Labels Apply**:
   - Choose the PRIMARY function, not secondary elements
   - Focus on authorial intent over reader interpretation
   - Consider conventional human categorization patterns

EXECUTION STANDARD:
Your label should represent what expert human annotators would consensus-choose based on the text's primary communicative function and practical organizational value. Think meaning and purpose, not keyword matching.

Total texts to process: {content_analysis.get('total_texts', 0)}
"""
        
        print(f"✅ Enhanced human-like instructions created for {len(available_labels)} labels")
        return instructions



    async def handle_completion(self, completion_data: dict):
        """Handle completion messages from Text Agent."""
        job_id = completion_data.get("job_id")
        status = completion_data.get("status")
        result = completion_data.get("result")
        
        print(f"✅ Mother AI received completion for job {job_id}: {status}")
        
        if status == "completed":
            # Update job status to completed
            self.redis_client.update_job_status(job_id, "completed", 100.0, {"result": result})
            print(f"🎉 Job {job_id} completed successfully")
        else:
            # Handle failed status
            error_info = completion_data.get("error", "Unknown error")
            self.redis_client.update_job_status(job_id, "failed", 0.0, {"error": error_info})
            print(f"❌ Job {job_id} failed: {error_info}")

    async def handle_cancellation(self, cancellation_data: dict):
        """Handle job cancellation messages."""
        try:
            job_id = cancellation_data.get("job_id")
            print(f"🚫 Mother AI processing cancellation for job {job_id}")
            
            # Update job status to cancelled if still processing
            self.redis_client.update_job_status(job_id, "cancelled", 0.0, {"cancelled_by": "user"})
            
            # Forward cancellation to Text Agent
            text_agent_cancellation = {
                "job_id": job_id,
                "action": "cancel",
                "source": "mother_ai",
                "timestamp": datetime.now().isoformat()
            }
            self.redis_client.publish_message("text_agent_cancellations", text_agent_cancellation)
            
            print(f"🚫 Job {job_id} cancellation processed by Mother AI")
            
        except Exception as e:
            print(f"❌ Error handling cancellation: {e}")
            import traceback
            traceback.print_exc()

    async def listen_for_jobs(self):
        """Listen for incoming jobs and completion messages."""
        print("🎧 Mother AI listening for jobs and completions...")
        
        try:
            # Subscribe to all channels
            jobs_pubsub = self.redis_client.subscribe_channel("mother_ai_jobs")
            completion_pubsub = self.redis_client.subscribe_channel("mother_ai_queue")
            cancellation_pubsub = self.redis_client.subscribe_channel("job_cancellations")
            print("✅ Mother AI subscribed to mother_ai_jobs, mother_ai_queue, and job_cancellations channels")
            
            while True:
                try:
                    # Check for new jobs
                    job_message = self.redis_client.get_message(jobs_pubsub)
                    if job_message:
                        print(f"📨 Mother AI received job: {str(job_message)[:100]}...")
                        await self.process_job(job_message)
                    
                    # Check for completion messages
                    completion_message = self.redis_client.get_message(completion_pubsub)
                    if completion_message:
                        print(f"📨 Mother AI received completion: {str(completion_message)[:100]}...")
                        await self.handle_completion(completion_message)
                    
                    # Check for cancellation messages
                    cancellation_message = self.redis_client.get_message(cancellation_pubsub)
                    if cancellation_message:
                        print(f"🚫 Mother AI received cancellation: {str(cancellation_message)[:100]}...")
                        await self.handle_cancellation(cancellation_message)
                    
                    # Small delay to prevent busy waiting
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"❌ Error in Mother AI listening loop: {e}")
                    import traceback
                    traceback.print_exc()
                    # Continue listening even after errors
                    await asyncio.sleep(1)
                    
        except Exception as e:
            print(f"❌ Fatal error in Mother AI: {e}")
            import traceback
            traceback.print_exc()
        except KeyboardInterrupt:
            print("🛑 Mother AI shutting down...")
        finally:
            try:
                jobs_pubsub.close()
                completion_pubsub.close()
                cancellation_pubsub.close()
            except:
                pass

async def main():
    mother_ai = MotherAI()
    await mother_ai.listen_for_jobs()

if __name__ == "__main__":
    asyncio.run(main())

