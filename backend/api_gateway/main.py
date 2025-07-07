from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.websockets import WebSocket, WebSocketDisconnect
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Temporarily comment out problematic imports
# from common.config import settings
# from common.redis_client import RedisClient
from routers import analytics, workflow_automation, integration_hub, advanced_validation, data_versioning

app = FastAPI(title="Multi-Agent Labeling System API Gateway")

# Enable CORS for frontend-backend interaction
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers - Only analytics for testing PDF export and workflow automation
app.include_router(analytics.router, prefix="/api/v1", tags=["Advanced Analytics"])
app.include_router(workflow_automation.router, prefix="/api/v1/workflows", tags=["Workflow Automation"])
app.include_router(integration_hub.router, prefix="/api/v1/integration", tags=["Integration Hub"])
app.include_router(advanced_validation.router, prefix="/api/v1/validation", tags=["Advanced Validation"])
app.include_router(data_versioning.router, prefix="/api/v1/versioning", tags=["Data Versioning"])

# Commented out other routers due to missing dependencies
# app.include_router(jobs.router, prefix="/api/v1")
# app.include_router(websocket_router.router, prefix="/ws")
# app.include_router(health.router, prefix="/health")
# app.include_router(ai_status.router, prefix="/api/v1/ai", tags=["AI Management"])
# app.include_router(logs.router, prefix="/api/v1", tags=["Job Logs & Analytics"])
# app.include_router(templates.router, prefix="/api/v1", tags=["Label Templates"])
# app.include_router(quality_assurance.router, prefix="/api/v1", tags=["Quality Assurance"])
# app.include_router(scheduler.router, prefix="/api/v1", tags=["Batch Scheduler"])
# app.include_router(model_comparison.router, prefix="/api/v1", tags=["Model Comparison"])
# app.include_router(integration_hub.router, prefix="/api/v1/integration", tags=["Integration Hub"])
# app.include_router(workflow_automation.router, prefix="/api/v1/workflows", tags=["Workflow Automation"])
# app.include_router(active_learning.router, prefix="/api/v1/active-learning", tags=["Active Learning"])
# app.include_router(data_versioning.router, prefix="/api/v1/versioning", tags=["Data Versioning"])
# app.include_router(advanced_validation.router, prefix="/api/v1/validation", tags=["Advanced Validation"])

@app.get("/")
async def root():
    return {"message": "Multi-Agent Labeling System API Gateway"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

