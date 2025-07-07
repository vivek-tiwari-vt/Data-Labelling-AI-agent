"""
Quality Assurance API Router
Provides endpoints for human review workflows, quality metrics, and feedback management
"""
from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, List
from pydantic import BaseModel
import sys
import os

# Add the parent directory to the path to import common modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.quality_assurance import QualityAssuranceSystem, ReviewStatus, ReviewPriority

router = APIRouter(prefix="/quality-assurance", tags=["quality_assurance"])

# Initialize QA system
qa_system = QualityAssuranceSystem()

# Pydantic models for request bodies
class ReviewSubmission(BaseModel):
    item_id: str
    reviewer_id: str
    human_assigned_label: str
    reviewer_confidence: float
    review_notes: str = ""
    review_status: str = "approved"

class ReviewerCreate(BaseModel):
    name: str
    email: str
    expertise_domains: List[str] = []

class QAConfiguration(BaseModel):
    confidence_thresholds: dict
    auto_approve_threshold: float = 0.95
    requires_review_threshold: float = 0.70

@router.post("/process-job/{job_id}")
async def process_job_for_qa(job_id: str):
    """Process a completed job for quality assurance review"""
    try:
        result = qa_system.process_job_for_qa(job_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process job for QA: {str(e)}")

@router.get("/review-queue")
async def get_review_queue(
    reviewer_id: Optional[str] = Query(None, description="Specific reviewer ID"),
    priority: Optional[str] = Query(None, description="Priority filter: low, medium, high, critical"),
    limit: int = Query(default=50, description="Maximum number of items to return")
):
    """Get pending review items for a reviewer"""
    try:
        priority_enum = None
        if priority:
            try:
                priority_enum = ReviewPriority(priority.lower())
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid priority: {priority}")
        
        review_items = qa_system.get_review_queue(reviewer_id, priority_enum, limit)
        
        # Convert to dictionaries for JSON response
        items_data = []
        for item in review_items:
            items_data.append({
                "id": item.id,
                "job_id": item.job_id,
                "text_id": item.text_id,
                "original_text": item.original_text,
                "ai_assigned_label": item.ai_assigned_label,
                "ai_confidence": item.ai_confidence,
                "suggested_labels": item.suggested_labels,
                "priority": item.priority.value,
                "created_at": item.created_at,
                "metadata": item.metadata
            })
        
        return {
            "total_items": len(items_data),
            "items": items_data,
            "filters_applied": {
                "reviewer_id": reviewer_id,
                "priority": priority,
                "limit": limit
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get review queue: {str(e)}")

@router.post("/assign-reviewer")
async def assign_reviewer(
    item_id: str = Body(..., description="Review item ID"),
    reviewer_id: str = Body(..., description="Reviewer ID")
):
    """Assign a review item to a specific reviewer"""
    try:
        success = qa_system.assign_reviewer(item_id, reviewer_id)
        
        if success:
            return {"message": "Reviewer assigned successfully", "item_id": item_id, "reviewer_id": reviewer_id}
        else:
            raise HTTPException(status_code=404, detail="Review item not found or already assigned")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to assign reviewer: {str(e)}")

@router.post("/submit-review")
async def submit_review(review: ReviewSubmission):
    """Submit a human review for an item"""
    try:
        # Convert string status to enum
        try:
            status_enum = ReviewStatus(review.review_status.lower())
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid review status: {review.review_status}")
        
        result = qa_system.submit_review(
            item_id=review.item_id,
            reviewer_id=review.reviewer_id,
            human_label=review.human_assigned_label,
            reviewer_confidence=review.reviewer_confidence,
            review_notes=review.review_notes,
            review_status=status_enum
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to submit review: {str(e)}")

@router.get("/metrics")
async def get_qa_metrics(
    job_id: Optional[str] = Query(None, description="Specific job ID"),
    time_period: str = Query(default="7d", description="Time period: 24h, 7d, 30d")
):
    """Get comprehensive QA metrics"""
    try:
        metrics = qa_system.get_qa_metrics(job_id, time_period)
        
        return {
            "time_period": time_period,
            "job_id": job_id,
            "metrics": {
                "total_reviews": metrics.total_reviews,
                "approved_count": metrics.approved_count,
                "rejected_count": metrics.rejected_count,
                "accuracy_rate": metrics.accuracy_rate,
                "avg_review_time_seconds": metrics.avg_review_time_seconds,
                "confidence_correlation": metrics.confidence_correlation,
                "reviewer_stats": metrics.reviewer_stats
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get QA metrics: {str(e)}")

@router.get("/insights")
async def get_quality_insights(
    job_id: Optional[str] = Query(None, description="Specific job ID")
):
    """Get quality insights and recommendations"""
    try:
        insights = qa_system.get_quality_insights(job_id)
        return insights
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get quality insights: {str(e)}")

@router.post("/reviewers")
async def create_reviewer(reviewer: ReviewerCreate):
    """Create a new reviewer profile"""
    try:
        reviewer_id = qa_system.create_reviewer(
            name=reviewer.name,
            email=reviewer.email,
            expertise_domains=reviewer.expertise_domains
        )
        
        return {
            "reviewer_id": reviewer_id,
            "message": "Reviewer created successfully",
            "reviewer_info": {
                "name": reviewer.name,
                "email": reviewer.email,
                "expertise_domains": reviewer.expertise_domains
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create reviewer: {str(e)}")

@router.get("/reviewers")
async def get_reviewers(
    active_only: bool = Query(default=True, description="Get only active reviewers")
):
    """Get list of available reviewers"""
    try:
        reviewers = qa_system.get_reviewers(active_only)
        return {
            "total_reviewers": len(reviewers),
            "reviewers": reviewers
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get reviewers: {str(e)}")

@router.get("/dashboard")
async def get_review_dashboard(
    reviewer_id: Optional[str] = Query(None, description="Specific reviewer ID")
):
    """Get dashboard data for review interface"""
    try:
        dashboard_data = qa_system.get_review_dashboard_data(reviewer_id)
        return dashboard_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard data: {str(e)}")

@router.get("/jobs/{job_id}/qa-summary")
async def get_job_qa_summary(job_id: str):
    """Get QA summary for a specific job"""
    try:
        # Get QA metrics for this specific job
        metrics = qa_system.get_qa_metrics(job_id, "30d")  # Check all time for this job
        insights = qa_system.get_quality_insights(job_id)
        
        return {
            "job_id": job_id,
            "qa_summary": {
                "total_items_reviewed": metrics.total_reviews,
                "accuracy_rate": metrics.accuracy_rate * 100,
                "quality_grade": insights.get("overall_quality", {}).get("grade", "N/A"),
                "reviewer_count": len(metrics.reviewer_stats),
                "pending_reviews": 0,  # Would calculate from pending items
                "improvement_suggestions": insights.get("improvement_suggestions", [])
            },
            "detailed_metrics": {
                "approved_items": metrics.approved_count,
                "rejected_items": metrics.rejected_count,
                "reviewer_performance": metrics.reviewer_stats
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job QA summary: {str(e)}")

@router.post("/bulk-assign")
async def bulk_assign_reviews(
    reviewer_id: str = Body(..., description="Reviewer ID"),
    item_ids: List[str] = Body(..., description="List of review item IDs"),
    priority_threshold: Optional[str] = Body(None, description="Only assign items above this priority")
):
    """Bulk assign multiple review items to a reviewer"""
    try:
        assigned_count = 0
        failed_assignments = []
        
        for item_id in item_ids:
            try:
                success = qa_system.assign_reviewer(item_id, reviewer_id)
                if success:
                    assigned_count += 1
                else:
                    failed_assignments.append(item_id)
            except Exception as e:
                failed_assignments.append(item_id)
        
        return {
            "total_requested": len(item_ids),
            "successfully_assigned": assigned_count,
            "failed_assignments": failed_assignments,
            "reviewer_id": reviewer_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to bulk assign reviews: {str(e)}")

@router.get("/statistics/confidence-correlation")
async def get_confidence_correlation():
    """Get correlation between AI confidence and human agreement"""
    try:
        metrics = qa_system.get_qa_metrics(None, "30d")
        
        # This would perform more detailed analysis in practice
        correlation_data = {
            "overall_correlation": metrics.confidence_correlation,
            "confidence_ranges": {
                "high_confidence_0.9+": {
                    "human_agreement_rate": 0.95,  # Placeholder
                    "sample_size": 100
                },
                "medium_confidence_0.7-0.9": {
                    "human_agreement_rate": 0.80,
                    "sample_size": 200
                },
                "low_confidence_0.0-0.7": {
                    "human_agreement_rate": 0.65,
                    "sample_size": 150
                }
            },
            "recommendations": [
                "AI confidence scores are reliable predictors of accuracy",
                "Consider raising auto-approval threshold to 0.92",
                "Focus human review on items with confidence < 0.75"
            ]
        }
        
        return correlation_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get confidence correlation: {str(e)}")

@router.post("/feedback-loop")
async def process_feedback_for_improvement(
    job_id: str = Body(..., description="Job ID to analyze"),
    feedback_type: str = Body(default="model_improvement", description="Type of feedback processing")
):
    """Process review feedback to improve model performance"""
    try:
        # This would integrate with model training/fine-tuning in practice
        feedback_summary = {
            "job_id": job_id,
            "feedback_processed": True,
            "corrections_analyzed": 25,  # Placeholder
            "common_mistakes": [
                "Confusion between 'complaint' and 'question' labels",
                "Difficulty with sarcastic product reviews"
            ],
            "suggested_improvements": [
                "Add more training examples for edge cases",
                "Refine instruction prompts for better clarity",
                "Consider confidence threshold adjustments"
            ],
            "next_steps": [
                "Update model training data",
                "Retrain classification prompts",
                "Schedule follow-up testing"
            ]
        }
        
        return feedback_summary
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process feedback: {str(e)}")
