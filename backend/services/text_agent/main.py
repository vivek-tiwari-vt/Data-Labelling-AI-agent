import asyncio
import json
import sys
import os
from datetime import datetime

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.messaging.redis_client import RedisClient
from text_processor import TextProcessor
from infrastructure.monitoring.job_logger import job_logger

class TextAgent:
    def __init__(self):
        self.redis_client = RedisClient()
        self.processor = TextProcessor()
        self.agent_name = "text_agent"
        self.cancelled_jobs = set()  # Track cancelled jobs
        print("🤖 Text Agent initialized with pure AI classification")

    async def handle_task(self, task_data: dict):
        """Handle incoming tasks from Mother AI with pure AI classification."""
        try:
            job_type = task_data.get("job_type")
            job_id = task_data.get("job_id")
            
            # Check if job is cancelled before processing
            if job_id in self.cancelled_jobs:
                print(f"🚫 Skipping cancelled job {job_id}")
                return
            
            print(f"🤖 Text Agent received task from Mother AI:")
            print(f"   📋 Job ID: {job_id}")
            print(f"   🎯 Job Type: {job_type}")
            
            if job_type == "batch_text_classification":
                # Handle batch text classification with pure AI
                result = await self.processor.process_batch_classification(task_data)
                
                # Check if job was cancelled during processing
                if job_id in self.cancelled_jobs:
                    print(f"🚫 Job {job_id} was cancelled during processing")
                    return
                
                print(f"✅ Text Agent completed job {job_id} successfully")
                
                # Send completion message back to Mother AI
                completion_message = {
                    "job_id": job_id,
                    "status": "completed",
                    "agent": "text_agent",
                    "result": result,
                    "timestamp": datetime.now().isoformat()
                }
                
                self.redis_client.publish_message("mother_ai_queue", completion_message)
                
            elif job_type == "text_labeling":
                # Check if job is cancelled before processing
                if job_id in self.cancelled_jobs:
                    print(f"🚫 Skipping cancelled single text job {job_id}")
                    return
                    
                # Handle single text classification
                result = await self.processor.process_single_text(task_data)
                
                print(f"✅ Text Agent completed single text classification for job {job_id}")
                
                # Send completion message back to Mother AI
                completion_message = {
                    "job_id": job_id,
                    "status": "completed",
                    "agent": "text_agent",
                    "result": result,
                    "timestamp": datetime.now().isoformat()
                }
                
                self.redis_client.publish_message("mother_ai_queue", completion_message)
                
            else:
                print(f"❌ Unknown job type: {job_type}")
                
                # Log error
                job_logger.log_error(job_id, {
                    "error_type": "unknown_job_type",
                    "error_message": f"Unknown job type: {job_type}",
                    "component": "text_agent"
                })
                
        except Exception as e:
            print(f"❌ Text Agent error handling task: {str(e)}")
            import traceback
            traceback.print_exc()
            
            job_id = task_data.get("job_id", "unknown")
            job_logger.log_error(job_id, {
                "error_type": "text_agent_task_error",
                "error_message": str(e),
                "component": "text_agent",
                "stack_trace": traceback.format_exc()
            })

    async def handle_cancellation(self, cancellation_data: dict):
        """Handle job cancellation messages."""
        try:
            job_id = cancellation_data.get("job_id")
            print(f"🚫 Text Agent processing cancellation for job {job_id}")
            
            # Add to cancelled jobs set
            self.cancelled_jobs.add(job_id)
            
            # Update job status
            self.redis_client.update_job_status(job_id, "cancelled", 0.0, {"cancelled_by": "user", "agent": "text_agent"})
            
            print(f"🚫 Job {job_id} marked as cancelled in Text Agent")
            
        except Exception as e:
            print(f"❌ Error handling cancellation in Text Agent: {e}")

    async def start_listening(self):
        """Start listening for tasks from Mother AI using pure AI classification."""
        print("🎧 Text Agent starting to listen for Mother AI tasks...")
        print("🧠 Using PURE AI classification - no hardcoded logic")
        
        try:
            # Subscribe to both text agent jobs and cancellations
            pubsub = self.redis_client.subscribe_channel("text_agent_jobs")
            cancellation_pubsub = self.redis_client.subscribe_channel("text_agent_cancellations")
            print("✅ Text Agent successfully subscribed to text_agent_jobs and text_agent_cancellations channels")
            
            while True:
                try:
                    # Check for new tasks
                    message = self.redis_client.get_message(pubsub)
                    if message:
                        print(f"📨 Text Agent received message: {str(message)[:100]}...")
                        await self.handle_task(message)
                    
                    # Check for cancellation messages
                    cancellation_message = self.redis_client.get_message(cancellation_pubsub)
                    if cancellation_message:
                        print(f"🚫 Text Agent received cancellation: {str(cancellation_message)[:100]}...")
                        await self.handle_cancellation(cancellation_message)
                    
                    # Small delay to prevent busy waiting
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"❌ Error in Text Agent listening loop: {e}")
                    import traceback
                    traceback.print_exc()
                    # Continue listening even after errors
                    await asyncio.sleep(1)
                    
        except Exception as e:
            print(f"❌ Fatal error in Text Agent: {e}")
            import traceback
            traceback.print_exc()
        except KeyboardInterrupt:
            print("🛑 Text Agent shutting down...")
        finally:
            try:
                pubsub.close()
                cancellation_pubsub.close()
            except:
                pass

def main():
    """Main function to start the Text Agent with pure AI classification."""
    print("🚀 Starting Text Agent with Pure AI Classification...")
    
    agent = TextAgent()
    
    # Run the event loop
    try:
        asyncio.run(agent.start_listening())
    except KeyboardInterrupt:
        print("👋 Text Agent shutting down...")
    except Exception as e:
        print(f"❌ Text Agent error: {e}")

if __name__ == "__main__":
    main()

