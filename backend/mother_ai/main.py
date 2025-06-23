import asyncio
import json
import sys
import os
from datetime import datetime
import traceback

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from common.redis_client import RedisClient
from common.models import JobStatus
from common.ai_client import AIClient
from common.job_logger import job_logger

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
        print(f"🤖 Mother AI model: {mother_ai_model}")
        print(f"👶 Child AI model: {child_ai_model}")
        
        # Perform content analysis
        content_analysis = await self.perform_content_analysis(file_data, available_labels)
        
        # Create intelligent instructions
        enhanced_instructions = await self.create_intelligent_instructions(
            file_data, available_labels, user_instructions, content_analysis
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

    async def perform_content_analysis(self, file_data: dict, available_labels: list) -> dict:
        """Simple content analysis for classification guidance."""
        test_texts = file_data.get("test_texts", [])
        
        # Basic analysis without AI call for performance
        analysis_data = {
            "total_texts": len(test_texts),
            "available_labels": available_labels,
            "label_strategies": {label: f"Classify content as '{label}' based on semantic meaning" for label in available_labels},
            "classification_methodology": "Pure AI semantic classification",
            "analysis_timestamp": datetime.now().isoformat()
        }
        
        print(f"✅ Content analysis completed for {len(test_texts)} texts")
        return analysis_data

    async def create_intelligent_instructions(self, file_data: dict, available_labels: list, 
                                            user_instructions: str, content_analysis: dict) -> str:
        """Create enhanced human-like classification instructions for Text Agent."""
        
        instructions = f"""
Enhanced Classification Instructions for Child AI Agent:

USER INSTRUCTIONS: {user_instructions}
AVAILABLE LABELS: {', '.join(available_labels)}

You are an expert human data labeler with decades of experience in content classification. Your classification decisions must demonstrate the same nuanced understanding, contextual awareness, and thoughtful reasoning that a seasoned human expert would apply.

CORE CLASSIFICATION PHILOSOPHY:

1. **Multi-Layer Content Understanding**:
   - **Surface Layer**: What words and phrases are present?
   - **Intent Layer**: Why was this text written? What is the author trying to accomplish?
   - **Function Layer**: How would this text be used or consumed by readers?
   - **Context Layer**: What situational factors influence the meaning?
   
   Always prioritize deeper layers over surface-level keyword matching.

2. **Human Expert Reasoning Process**:
   Before labeling, mentally walk through this thought process:
   
   a) **Initial Impression**: "What is my immediate sense of what this text is about?"
   b) **Purpose Analysis**: "What is the author's primary goal in writing this?"
   c) **Audience Consideration**: "Who is this intended for and why?"
   d) **Content vs Container**: "What is the substance versus the format/medium?"
   e) **Label Justification**: "Can I explain why this label fits better than any other?"

3. **Advanced Decision Framework**:
   
   **When Multiple Labels Seem Applicable**:
   - Identify the text's PRIMARY function (what would happen if you removed secondary elements?)
   - Ask: "If I could only communicate ONE thing about this text's purpose, what would it be?"
   - Consider temporal context: What is the immediate vs. long-term purpose?
   
   **For Ambiguous Content**:
   - Focus on authorial intent over reader interpretation
   - Consider the most specific applicable label rather than generic ones
   - Think about conventional human categorization patterns
   
   **For Complex Multi-Topic Content**:
   - Identify the organizational structure: Is this primarily X with elements of Y, or vice versa?
   - Determine what would be lost if you moved this text to a different category
   - Consider what aspects would matter most to someone organizing or searching for this content

4. **Expert-Level Quality Controls**:
   
   **Before Final Decision**:
   - Perform a "mental file test": In a well-organized filing system, where would this naturally belong?
   - Apply the "colleague test": Would another expert immediately understand why you chose this label?
   - Use the "utility test": Does this labeling serve the end user's likely needs and expectations?
   
   **Consistency Verification**:
   - Are you applying the same decision criteria across similar texts?
   - Are you maintaining appropriate granularity (not too broad, not over-specific)?
   - Are you avoiding classification drift as you progress through the dataset?

5. **Sophisticated Pattern Recognition**:
   
   **Recognize Common Misclassification Traps**:
   - Content ABOUT topic X is not always OF category X (discussing technology ≠ technology content)
   - Emotional expressions about subjects should be classified by their communicative function, not their emotional content
   - Format similarities don't determine content categories (questions can serve many different purposes)
   - Context matters more than vocabulary overlap
   
   **Apply Contextual Intelligence**:
   - Personal experiences are different from reviews, even when they express opinions
   - Informational content serves different purposes than experiential sharing
   - Requests and questions have different intents than statements and declarations
   - Professional communication follows different patterns than casual expression

6. **Meta-Cognitive Approach**:
   
   **Continuous Self-Monitoring**:
   - Am I getting trapped in keyword matching rather than meaning analysis?
   - Am I considering the full spectrum of available labels or gravitating toward familiar ones?
   - Am I maintaining appropriate confidence levels in my decisions?
   - Am I adapting my approach based on the specific label set and domain?

EXECUTION PROTOCOL:

For each text, document your reasoning process internally:
1. **Content Summary**: What is this text fundamentally about?
2. **Purpose Identification**: Why does this text exist?
3. **Label Consideration**: Which labels are potentially applicable and why?
4. **Primary Selection**: Which label best captures the essential nature?
5. **Verification**: Does this choice align with human expert judgment patterns?

FINAL CLASSIFICATION STANDARD:
Your label should represent what a consensus of human experts would choose after careful consideration. The label should capture the text's primary communicative function and content focus in a way that serves practical organizational and retrieval purposes.

Remember: Excellence in classification comes from understanding meaning, context, and purpose - not from pattern matching or keyword detection. Think like a human expert who deeply understands both content and categorical systems.

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

    async def listen_for_jobs(self):
        """Listen for incoming jobs and completion messages."""
        print("🎧 Mother AI listening for jobs and completions...")
        
        try:
            # Subscribe to both channels
            jobs_pubsub = self.redis_client.subscribe_channel("mother_ai_jobs")
            completion_pubsub = self.redis_client.subscribe_channel("mother_ai_queue")
            print("✅ Mother AI subscribed to mother_ai_jobs and mother_ai_queue channels")
            
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
            except:
                pass

async def main():
    mother_ai = MotherAI()
    await mother_ai.listen_for_jobs()

if __name__ == "__main__":
    asyncio.run(main())

