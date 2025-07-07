from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import asyncio
import json
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.messaging.redis_client import RedisClient

router = APIRouter()

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                # Remove disconnected connections
                self.active_connections.remove(connection)

manager = ConnectionManager()

@router.websocket("/progress/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await manager.connect(websocket)
    redis_client = RedisClient()
    
    try:
        # Subscribe to job progress updates
        pubsub = redis_client.subscribe_channel(f"job_progress:{job_id}")
        
        while True:
            # Check for Redis messages
            message = redis_client.get_message(pubsub)
            if message:
                await manager.send_personal_message(json.dumps(message), websocket)
            
            # Small delay to prevent busy-waiting
            await asyncio.sleep(0.1)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print(f"WebSocket disconnected for job {job_id}")

