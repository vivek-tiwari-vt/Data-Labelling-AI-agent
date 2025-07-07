

import redis
import os
import json
from typing import Dict, Any

class RedisClient:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.client = redis.from_url(self.redis_url)

    def publish_message(self, channel: str, message: Dict[str, Any]):
        """Publishes a message to a Redis channel."""
        self.client.publish(channel, json.dumps(message))

    def subscribe_channel(self, channel: str):
        """Subscribes to a Redis channel and returns a PubSub object."""
        pubsub = self.client.pubsub()
        pubsub.subscribe(channel)
        return pubsub

    def get_message(self, pubsub):
        """Gets a message from the PubSub object."""
        message = pubsub.get_message(timeout=0.1)
        if message and message["type"] == "message" and message["data"]:
            try:
                return json.loads(message["data"])
            except json.JSONDecodeError:
                print(f"❌ Failed to decode message: {message['data']}")
                return None
        return None

    def set_key(self, key: str, value: Any):
        """Sets a key-value pair in Redis."""
        self.client.set(key, json.dumps(value))

    def get_key(self, key: str):
        """Gets a value from Redis by key."""
        value = self.client.get(key)
        if value:
            return json.loads(value)
        return None

    def delete_key(self, key: str):
        """Deletes a key from Redis."""
        self.client.delete(key)
    
    def update_job_status(self, job_id: str, status: str, progress: float, additional_data: Dict[str, Any] = None):
        """Updates job status in Redis."""
        job_data = {
            "job_id": job_id,
            "status": status,
            "progress": progress,
            "timestamp": json.dumps({"$date": {"$numberLong": str(int(__import__('time').time() * 1000))}}),
        }
        
        if additional_data:
            job_data.update(additional_data)
        
        # Store job status
        self.set_key(f"job:{job_id}", job_data)
        
        # Publish status update
        self.publish_message("job_status_updates", job_data)


