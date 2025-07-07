"""
Quality Assurance System for Data Labeling
Provides human review workflows, confidence-based routing, and quality metrics
"""
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import uuid

class ReviewStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_REVISION = "needs_revision"
    SKIPPED = "skipped"

class ReviewPriority(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class QualityReviewItem:
    id: str
    job_id: str
    text_id: str
    original_text: str
    ai_assigned_label: str
    ai_confidence: float
    suggested_labels: List[str]
    review_status: ReviewStatus
    priority: ReviewPriority
    reviewer_id: Optional[str] = None
    human_assigned_label: Optional[str] = None
    reviewer_confidence: Optional[float] = None
    review_notes: Optional[str] = None
    review_time: Optional[str] = None
    created_at: str = ""
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class QualityMetrics:
    total_reviews: int
    approved_count: int
    rejected_count: int
    accuracy_rate: float
    avg_review_time_seconds: float
    confidence_correlation: float
    reviewer_stats: Dict[str, Any]

class QualityAssuranceSystem:
    """Advanced quality assurance system for human review workflows"""
    
    def __init__(self):
        self.data_dir = Path("/Volumes/DATA/Projects/data_label_agent/data")
        self.qa_dir = self.data_dir / "quality_assurance"
        self.qa_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize QA database
        self.db_path = self.qa_dir / "quality_assurance.db"
        self._init_database()
        
        # Configuration
        self.confidence_thresholds = {
            "auto_approve": 0.95,  # Auto-approve if confidence > 95%
            "requires_review": 0.70,  # Requires review if confidence < 70%
            "critical_review": 0.50   # Critical review if confidence < 50%
        }
        
        # Import job logger
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from common.job_logger import job_logger
        self.job_logger = job_logger
    
    def _init_database(self):
        """Initialize QA database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Review items table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS review_items (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                text_id TEXT NOT NULL,
                original_text TEXT NOT NULL,
                ai_assigned_label TEXT NOT NULL,
                ai_confidence REAL NOT NULL,
                suggested_labels TEXT NOT NULL,
                review_status TEXT NOT NULL,
                priority TEXT NOT NULL,
                reviewer_id TEXT,
                human_assigned_label TEXT,
                reviewer_confidence REAL,
                review_notes TEXT,
                review_time TEXT,
                created_at TEXT NOT NULL,
                metadata TEXT
            )
        """)
        
        # Reviewers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviewers (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                expertise_domains TEXT,
                active BOOLEAN DEFAULT TRUE,
                total_reviews INTEGER DEFAULT 0,
                accuracy_score REAL DEFAULT 0.0,
                avg_review_time REAL DEFAULT 0.0,
                created_at TEXT NOT NULL,
                last_active TEXT
            )
        """)
        
        # Quality metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quality_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                total_items INTEGER NOT NULL,
                reviewed_items INTEGER NOT NULL,
                approved_items INTEGER NOT NULL,
                rejected_items INTEGER NOT NULL,
                accuracy_rate REAL NOT NULL,
                avg_confidence REAL NOT NULL,
                review_completion_rate REAL NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        
        # Feedback and corrections table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS feedback_corrections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_item_id TEXT NOT NULL,
                original_label TEXT NOT NULL,
                corrected_label TEXT NOT NULL,
                correction_reason TEXT,
                model_used TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (review_item_id) REFERENCES review_items (id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    def process_job_for_qa(self, job_id: str) -> Dict[str, Any]:
        """Process completed job and route items for quality assurance"""
        
        job_log = self.job_logger.get_job_log(job_id)
        if not job_log:
            return {"error": "Job log not found"}
        
        if job_log.get("status") != "completed":
            return {"error": "Job must be completed for QA processing"}
        
        # Extract processing details
        text_agent_data = job_log.get("text_agent", {})
        processing_details = text_agent_data.get("processing_details", [])
        available_labels = job_log.get("user_input", {}).get("available_labels", [])
        
        review_items = []
        auto_approved = 0
        requires_review = 0
        critical_review = 0
        
        for detail in processing_details:
            text_id = detail.get("text_id", str(uuid.uuid4()))
            original_text = detail.get("content_preview", "")
            ai_label = detail.get("assigned_label", "")
            ai_confidence = detail.get("confidence_score", 0.0)
            
            # Determine review priority based on confidence
            priority = self._determine_review_priority(ai_confidence)
            
            # Route based on confidence thresholds
            if ai_confidence >= self.confidence_thresholds["auto_approve"]:
                review_status = ReviewStatus.APPROVED
                auto_approved += 1
            elif ai_confidence < self.confidence_thresholds["critical_review"]:
                review_status = ReviewStatus.PENDING
                priority = ReviewPriority.CRITICAL
                critical_review += 1
            elif ai_confidence < self.confidence_thresholds["requires_review"]:
                review_status = ReviewStatus.PENDING
                requires_review += 1
            else:
                review_status = ReviewStatus.APPROVED
                auto_approved += 1
            
            review_item = QualityReviewItem(
                id=str(uuid.uuid4()),
                job_id=job_id,
                text_id=text_id,
                original_text=original_text,
                ai_assigned_label=ai_label,
                ai_confidence=ai_confidence,
                suggested_labels=available_labels,
                review_status=review_status,
                priority=priority,
                created_at=datetime.now().isoformat(),
                metadata={
                    "processing_time_ms": detail.get("processing_time_ms", 0),
                    "classification_reasoning": detail.get("classification_reasoning", ""),
                    "model_used": job_log.get("ai_models", {}).get("child_ai_model", "unknown")
                }
            )
            
            review_items.append(review_item)
        
        # Store review items in database
        self._store_review_items(review_items)
        
        # Generate QA summary
        qa_summary = {
            "job_id": job_id,
            "total_items": len(review_items),
            "auto_approved": auto_approved,
            "requires_review": requires_review,
            "critical_review": critical_review,
            "review_completion_rate": 0.0,
            "estimated_review_time_minutes": self._estimate_review_time(review_items),
            "quality_score": self._calculate_initial_quality_score(review_items),
            "routing_summary": {
                "auto_approved_rate": auto_approved / len(review_items) * 100 if review_items else 0,
                "review_required_rate": (requires_review + critical_review) / len(review_items) * 100 if review_items else 0
            }
        }
        
        # Store QA metrics
        self._store_qa_metrics(qa_summary)
        
        return qa_summary
    
    def get_review_queue(self, reviewer_id: Optional[str] = None, priority: Optional[ReviewPriority] = None, limit: int = 50) -> List[QualityReviewItem]:
        """Get pending review items for a reviewer"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = """
            SELECT * FROM review_items 
            WHERE review_status = ? 
        """
        params = [ReviewStatus.PENDING.value]
        
        if reviewer_id:
            query += " AND (reviewer_id IS NULL OR reviewer_id = ?)"
            params.append(reviewer_id)
        else:
            query += " AND reviewer_id IS NULL"
        
        if priority:
            query += " AND priority = ?"
            params.append(priority.value)
        
        query += " ORDER BY priority DESC, ai_confidence ASC, created_at ASC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        review_items = []
        for row in rows:
            review_items.append(self._row_to_review_item(row))
        
        return review_items
    
    def assign_reviewer(self, item_id: str, reviewer_id: str) -> bool:
        """Assign a review item to a specific reviewer"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "UPDATE review_items SET reviewer_id = ? WHERE id = ? AND reviewer_id IS NULL",
            (reviewer_id, item_id)
        )
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        
        return success
    
    def submit_review(self, item_id: str, reviewer_id: str, human_label: str, 
                     reviewer_confidence: float, review_notes: str = "", 
                     review_status: ReviewStatus = ReviewStatus.APPROVED) -> Dict[str, Any]:
        """Submit a human review for an item"""
        
        review_time = datetime.now().isoformat()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Update review item
        cursor.execute("""
            UPDATE review_items 
            SET human_assigned_label = ?, reviewer_confidence = ?, 
                review_notes = ?, review_status = ?, review_time = ?
            WHERE id = ? AND reviewer_id = ?
        """, (human_label, reviewer_confidence, review_notes, 
              review_status.value, review_time, item_id, reviewer_id))
        
        if cursor.rowcount == 0:
            conn.close()
            return {"error": "Review item not found or not assigned to reviewer"}
        
        # Get the original item for comparison
        cursor.execute("SELECT * FROM review_items WHERE id = ?", (item_id,))
        row = cursor.fetchone()
        
        if row:
            original_item = self._row_to_review_item(row)
            
            # Record feedback correction if labels differ
            if original_item.ai_assigned_label != human_label:
                cursor.execute("""
                    INSERT INTO feedback_corrections 
                    (review_item_id, original_label, corrected_label, correction_reason, model_used, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (item_id, original_item.ai_assigned_label, human_label, 
                      review_notes, original_item.metadata.get("model_used", "unknown"), review_time))
        
        # Update reviewer stats
        self._update_reviewer_stats(reviewer_id, review_time)
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "review_submitted": True,
            "feedback_recorded": original_item.ai_assigned_label != human_label if row else False
        }
    
    def get_qa_metrics(self, job_id: Optional[str] = None, time_period: str = "7d") -> QualityMetrics:
        """Get comprehensive QA metrics"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Time filter
        if time_period == "24h":
            start_date = datetime.now() - timedelta(hours=24)
        elif time_period == "7d":
            start_date = datetime.now() - timedelta(days=7)
        elif time_period == "30d":
            start_date = datetime.now() - timedelta(days=30)
        else:
            start_date = datetime.now() - timedelta(days=7)
        
        query_conditions = ["created_at >= ?"]
        params = [start_date.isoformat()]
        
        if job_id:
            query_conditions.append("job_id = ?")
            params.append(job_id)
        
        where_clause = " WHERE " + " AND ".join(query_conditions)
        
        # Get review statistics
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_reviews,
                SUM(CASE WHEN review_status = 'approved' THEN 1 ELSE 0 END) as approved_count,
                SUM(CASE WHEN review_status = 'rejected' THEN 1 ELSE 0 END) as rejected_count,
                AVG(ai_confidence) as avg_ai_confidence,
                AVG(reviewer_confidence) as avg_reviewer_confidence
            FROM review_items
            {where_clause}
        """, params)
        
        stats = cursor.fetchone()
        
        # Calculate accuracy rate (proportion where human agreed with AI)
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_reviewed,
                SUM(CASE WHEN ai_assigned_label = human_assigned_label THEN 1 ELSE 0 END) as ai_correct
            FROM review_items 
            {where_clause} AND review_status IN ('approved', 'rejected') AND human_assigned_label IS NOT NULL
        """, params)
        
        accuracy_stats = cursor.fetchone()
        
        # Get reviewer performance
        cursor.execute(f"""
            SELECT 
                reviewer_id,
                COUNT(*) as reviews_count,
                AVG(reviewer_confidence) as avg_confidence,
                COUNT(DISTINCT DATE(review_time)) as active_days
            FROM review_items 
            {where_clause} AND reviewer_id IS NOT NULL
            GROUP BY reviewer_id
        """, params)
        
        reviewer_stats = {}
        for row in cursor.fetchall():
            reviewer_id, count, avg_conf, active_days = row
            reviewer_stats[reviewer_id] = {
                "reviews_count": count,
                "avg_confidence": avg_conf or 0,
                "active_days": active_days
            }
        
        conn.close()
        
        total_reviews = stats[0] if stats else 0
        approved_count = stats[1] if stats else 0
        rejected_count = stats[2] if stats else 0
        
        accuracy_rate = 0.0
        if accuracy_stats and accuracy_stats[0] > 0:
            accuracy_rate = accuracy_stats[1] / accuracy_stats[0]
        
        return QualityMetrics(
            total_reviews=total_reviews,
            approved_count=approved_count,
            rejected_count=rejected_count,
            accuracy_rate=accuracy_rate,
            avg_review_time_seconds=0.0,  # Would need to track review start times
            confidence_correlation=0.0,  # Would need more complex calculation
            reviewer_stats=reviewer_stats
        )
    
    def get_quality_insights(self, job_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate quality insights and recommendations"""
        
        metrics = self.get_qa_metrics(job_id)
        
        insights = {
            "overall_quality": {
                "score": metrics.accuracy_rate * 100,
                "grade": self._calculate_quality_grade(metrics.accuracy_rate),
                "total_reviews": metrics.total_reviews
            },
            "confidence_analysis": self._analyze_confidence_patterns(job_id),
            "model_performance": self._analyze_model_performance(job_id),
            "reviewer_performance": metrics.reviewer_stats,
            "improvement_suggestions": self._generate_improvement_suggestions(metrics),
            "quality_trends": self._analyze_quality_trends(job_id)
        }
        
        return insights
    
    def create_reviewer(self, name: str, email: str, expertise_domains: List[str] = None) -> str:
        """Create a new reviewer profile"""
        
        reviewer_id = str(uuid.uuid4())
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO reviewers 
            (id, name, email, expertise_domains, created_at, last_active)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (reviewer_id, name, email, 
              json.dumps(expertise_domains or []), 
              datetime.now().isoformat(),
              datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return reviewer_id
    
    def get_reviewers(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get list of available reviewers"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM reviewers"
        if active_only:
            query += " WHERE active = TRUE"
        query += " ORDER BY name"
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        
        reviewers = []
        for row in rows:
            reviewers.append({
                "id": row[0],
                "name": row[1],
                "email": row[2],
                "expertise_domains": json.loads(row[3] or "[]"),
                "active": bool(row[4]),
                "total_reviews": row[5],
                "accuracy_score": row[6],
                "avg_review_time": row[7],
                "created_at": row[8],
                "last_active": row[9]
            })
        
        return reviewers
    
    def get_review_dashboard_data(self, reviewer_id: Optional[str] = None) -> Dict[str, Any]:
        """Get dashboard data for review interface"""
        
        pending_items = self.get_review_queue(reviewer_id, limit=100)
        metrics = self.get_qa_metrics()
        
        # Priority distribution
        priority_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for item in pending_items:
            priority_counts[item.priority.value] += 1
        
        # Confidence distribution for pending items
        confidence_ranges = {"<0.5": 0, "0.5-0.7": 0, "0.7-0.9": 0, "0.9+": 0}
        for item in pending_items:
            conf = item.ai_confidence
            if conf < 0.5:
                confidence_ranges["<0.5"] += 1
            elif conf < 0.7:
                confidence_ranges["0.5-0.7"] += 1
            elif conf < 0.9:
                confidence_ranges["0.7-0.9"] += 1
            else:
                confidence_ranges["0.9+"] += 1
        
        return {
            "pending_reviews": len(pending_items),
            "priority_distribution": priority_counts,
            "confidence_distribution": confidence_ranges,
            "recent_items": pending_items[:10],  # Most urgent 10
            "quality_metrics": {
                "total_reviews": metrics.total_reviews,
                "accuracy_rate": metrics.accuracy_rate * 100,
                "approved_rate": (metrics.approved_count / metrics.total_reviews * 100) if metrics.total_reviews > 0 else 0
            },
            "reviewer_stats": metrics.reviewer_stats.get(reviewer_id, {}) if reviewer_id else {}
        }
    
    # Helper methods
    def _determine_review_priority(self, confidence: float) -> ReviewPriority:
        """Determine review priority based on confidence score"""
        if confidence < 0.5:
            return ReviewPriority.CRITICAL
        elif confidence < 0.7:
            return ReviewPriority.HIGH
        elif confidence < 0.85:
            return ReviewPriority.MEDIUM
        else:
            return ReviewPriority.LOW
    
    def _store_review_items(self, review_items: List[QualityReviewItem]):
        """Store review items in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for item in review_items:
            cursor.execute("""
                INSERT INTO review_items 
                (id, job_id, text_id, original_text, ai_assigned_label, ai_confidence,
                 suggested_labels, review_status, priority, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (item.id, item.job_id, item.text_id, item.original_text,
                  item.ai_assigned_label, item.ai_confidence,
                  json.dumps(item.suggested_labels), item.review_status.value,
                  item.priority.value, item.created_at,
                  json.dumps(item.metadata or {})))
        
        conn.commit()
        conn.close()
    
    def _store_qa_metrics(self, qa_summary: Dict[str, Any]):
        """Store QA metrics in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO quality_metrics 
            (job_id, total_items, reviewed_items, approved_items, rejected_items,
             accuracy_rate, avg_confidence, review_completion_rate, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (qa_summary["job_id"], qa_summary["total_items"], 0,
              qa_summary["auto_approved"], 0, 0.0, 0.0,
              qa_summary["review_completion_rate"], datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    def _row_to_review_item(self, row) -> QualityReviewItem:
        """Convert database row to QualityReviewItem"""
        return QualityReviewItem(
            id=row[0],
            job_id=row[1],
            text_id=row[2],
            original_text=row[3],
            ai_assigned_label=row[4],
            ai_confidence=row[5],
            suggested_labels=json.loads(row[6] or "[]"),
            review_status=ReviewStatus(row[7]),
            priority=ReviewPriority(row[8]),
            reviewer_id=row[9],
            human_assigned_label=row[10],
            reviewer_confidence=row[11],
            review_notes=row[12],
            review_time=row[13],
            created_at=row[14],
            metadata=json.loads(row[15] or "{}")
        )
    
    def _estimate_review_time(self, review_items: List[QualityReviewItem]) -> float:
        """Estimate total review time in minutes"""
        # Rough estimates: critical=5min, high=3min, medium=2min, low=1min
        time_estimates = {
            ReviewPriority.CRITICAL: 5.0,
            ReviewPriority.HIGH: 3.0,
            ReviewPriority.MEDIUM: 2.0,
            ReviewPriority.LOW: 1.0
        }
        
        total_time = 0.0
        for item in review_items:
            if item.review_status == ReviewStatus.PENDING:
                total_time += time_estimates.get(item.priority, 2.0)
        
        return total_time
    
    def _calculate_initial_quality_score(self, review_items: List[QualityReviewItem]) -> float:
        """Calculate initial quality score based on confidence distribution"""
        if not review_items:
            return 0.0
        
        total_confidence = sum(item.ai_confidence for item in review_items)
        return (total_confidence / len(review_items)) * 100
    
    def _update_reviewer_stats(self, reviewer_id: str, review_time: str):
        """Update reviewer statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE reviewers 
            SET total_reviews = total_reviews + 1, last_active = ?
            WHERE id = ?
        """, (review_time, reviewer_id))
        
        conn.commit()
        conn.close()
    
    def _calculate_quality_grade(self, accuracy_rate: float) -> str:
        """Calculate quality grade based on accuracy rate"""
        if accuracy_rate >= 0.95:
            return "A+"
        elif accuracy_rate >= 0.90:
            return "A"
        elif accuracy_rate >= 0.85:
            return "B+"
        elif accuracy_rate >= 0.80:
            return "B"
        elif accuracy_rate >= 0.75:
            return "C+"
        elif accuracy_rate >= 0.70:
            return "C"
        else:
            return "D"
    
    def _analyze_confidence_patterns(self, job_id: Optional[str]) -> Dict[str, Any]:
        """Analyze confidence score patterns"""
        # Simplified implementation
        return {
            "avg_confidence": 0.75,
            "confidence_trend": "stable",
            "low_confidence_rate": 15.0
        }
    
    def _analyze_model_performance(self, job_id: Optional[str]) -> Dict[str, Any]:
        """Analyze model performance in QA context"""
        # Simplified implementation
        return {
            "accuracy_by_model": {},
            "confidence_by_model": {},
            "best_performing_model": "unknown"
        }
    
    def _generate_improvement_suggestions(self, metrics: QualityMetrics) -> List[str]:
        """Generate improvement suggestions based on metrics"""
        suggestions = []
        
        if metrics.accuracy_rate < 0.8:
            suggestions.append("Consider refining classification instructions")
            suggestions.append("Review and improve training examples")
        
        if metrics.total_reviews > 100 and metrics.accuracy_rate > 0.9:
            suggestions.append("Consider increasing auto-approval threshold")
        
        return suggestions
    
    def _analyze_quality_trends(self, job_id: Optional[str]) -> Dict[str, Any]:
        """Analyze quality trends over time"""
        # Simplified implementation
        return {
            "trend": "improving",
            "confidence": 0.8,
            "weekly_change": 5.0
        }
