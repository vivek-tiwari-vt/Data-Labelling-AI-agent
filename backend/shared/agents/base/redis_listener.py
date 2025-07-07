

import asyncio
import json
from typing import Callable, Dict, Any
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.messaging.redis_client import RedisClient

class RedisListener:
    def __init__(self, redis_client: RedisClient, channel: str, handler: Callable[[Dict[str, Any]], None]):
        self.redis_client = redis_client
        self.channel = channel
        self.handler = handler
        self.pubsub = self.redis_client.subscribe_channel(self.channel)

    async def listen_for_messages(self):
        print(f"Listening for messages on channel: {self.channel}")
        while True:
            message = self.redis_client.get_message(self.pubsub)
            if message:
                print(f"Received message on {self.channel}: {message}")
                await self.handler(message)
            await asyncio.sleep(0.1)  # Small delay to prevent busy-waiting


