from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.websockets import WebSocket, WebSocketDisconnect
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from common.config import settings
from common.redis_client import RedisClient
from routers import jobs, websocket_router, health, ai_status, logs

app = FastAPI(title="Multi-Agent Labeling System API Gateway")

# Enable CORS for frontend-backend interaction
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(jobs.router, prefix="/api/v1")
app.include_router(websocket_router.router, prefix="/ws")
app.include_router(health.router, prefix="/health")
app.include_router(ai_status.router, prefix="/api/v1/ai", tags=["AI Management"])
app.include_router(logs.router, prefix="/api/v1", tags=["Job Logs & Analytics"])

@app.get("/")
async def root():
    return {"message": "Multi-Agent Labeling System API Gateway"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

