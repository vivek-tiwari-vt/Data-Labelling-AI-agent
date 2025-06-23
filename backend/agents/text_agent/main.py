import asyncio
import json
import sys
import os
from datetime import datetime

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.redis_client import RedisClient
from text_processor import TextProcessor
from common.job_logger import job_logger

class TextAgent:
    def __init__(self):
        self.redis_client = RedisClient()
        self.processor = TextProcessor()
        self.agent_name = "text_agent"
        print("🤖 Text Agent initialized with pure AI classification")

    async def handle_task(self, task_data: dict):
        """Handle incoming tasks from Mother AI with pure AI classification."""
        try:
            job_type = task_data.get("job_type")
            job_id = task_data.get("job_id")
            
            print(f"🤖 Text Agent received task from Mother AI:")
            print(f"   📋 Job ID: {job_id}")
            print(f"   🎯 Job Type: {job_type}")
            
            if job_type == "batch_text_classification":
                # Handle batch text classification with pure AI
                result = await self.processor.process_batch_classification(task_data)
                
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

    async def start_listening(self):
        """Start listening for tasks from Mother AI using pure AI classification."""
        print("🎧 Text Agent starting to listen for Mother AI tasks...")
        print("🧠 Using PURE AI classification - no hardcoded logic")
        
        try:
            # Subscribe to the text agent jobs channel
            pubsub = self.redis_client.subscribe_channel("text_agent_jobs")
            print("✅ Text Agent successfully subscribed to text_agent_jobs channel")
            
            while True:
                try:
                    message = self.redis_client.get_message(pubsub)
                    if message:
                        print(f"📨 Text Agent received message: {str(message)[:100]}...")
                        await self.handle_task(message)
                    
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

