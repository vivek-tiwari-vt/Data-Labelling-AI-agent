"""
Active Learning System for Data Labeling
Automatically identifies uncertain predictions for human review and optimization
"""
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import numpy as np
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import uuid

class UncertaintyStrategy(Enum):
    CONFIDENCE_BASED = "confidence_based"
    DIVERSITY_BASED = "diversity_based"
    UNCERTAINTY_SAMPLING = "uncertainty_sampling"
    QUERY_BY_COMMITTEE = "query_by_committee"

class LearningPriority(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

@dataclass
class ActiveLearningItem:
    id: str
    job_id: str
    text_id: str
    original_text: str
    predicted_label: str
    confidence_score: float
    uncertainty_score: float
    diversity_score: float
    priority: LearningPriority
    strategy_used: UncertaintyStrategy
    created_at: str
    reviewed: bool = False
    human_label: Optional[str] = None
    human_confidence: Optional[float] = None
    review_time: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class LearningInsights:
    total_uncertain_items: int
    avg_uncertainty_score: float
    priority_distribution: Dict[str, int]
    strategy_effectiveness: Dict[str, float]
    improvement_recommendations: List[str]
    confidence_gaps: List[Dict[str, Any]]

class ActiveLearningSystem:
    """Advanced active learning system for optimizing human annotation efforts"""
    
    def __init__(self):
        self.data_dir = Path("/Volumes/DATA/Projects/data_label_agent/data/active_learning")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "active_learning.db"
        self._init_database()
        
        # Initialize TF-IDF vectorizer for text similarity
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.text_vectors = None
        
    def _init_database(self):
        """Initialize active learning database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Active learning items table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS active_learning_items (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                text_id TEXT NOT NULL,
                original_text TEXT NOT NULL,
                predicted_label TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                uncertainty_score REAL NOT NULL,
                diversity_score REAL NOT NULL,
                priority TEXT NOT NULL,
                strategy_used TEXT NOT NULL,
                created_at TEXT NOT NULL,
                reviewed BOOLEAN DEFAULT FALSE,
                human_label TEXT,
                human_confidence REAL,
                review_time TEXT,
                metadata TEXT
            )
        """)
        
        # Learning strategies performance table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS strategy_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_name TEXT NOT NULL,
                job_id TEXT NOT NULL,
                items_reviewed INTEGER NOT NULL,
                accuracy_improvement REAL NOT NULL,
                avg_review_time REAL NOT NULL,
                human_agreement_rate REAL NOT NULL,
                effectiveness_score REAL NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        
        # Learning insights table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS learning_insights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                insight_type TEXT NOT NULL,
                insight_data TEXT NOT NULL,
                confidence REAL NOT NULL,
                created_at TEXT NOT NULL,
                is_actioned BOOLEAN DEFAULT FALSE
            )
        """)
        
        conn.commit()
        conn.close()
        
    def analyze_job_for_active_learning(self, job_id: str, 
                                      strategy: UncertaintyStrategy = UncertaintyStrategy.CONFIDENCE_BASED,
                                      max_items: int = 50) -> Dict[str, Any]:
        """Analyze completed job and identify items for active learning"""
        
        # Get job results from job logger
        from common.job_logger import job_logger
        job_log = job_logger.get_job_log(job_id)
        
        if not job_log:
            raise ValueError(f"Job {job_id} not found")
        
        # Try to get processing details from output file first
        processing_details = []
        try:
            from pathlib import Path
            import json
            
            # Find project root by looking for key directories
            current_path = Path(__file__).parent
            while current_path.name != 'data_label_agent' and current_path.parent != current_path:
                current_path = current_path.parent
            
            if current_path.name == 'data_label_agent':
                outputs_dir = current_path / "data" / "outputs"
            else:
                # Fallback to relative path from backend
                outputs_dir = Path(__file__).parent.parent.parent / "data" / "outputs"
            
            output_file = outputs_dir / f"job_{job_id}_labeled.json"
            
            if output_file.exists():
                with open(output_file, 'r') as f:
                    output_data = json.load(f)
                
                test_texts = output_data.get("test_texts", [])
                if test_texts:
                    # Convert output data to processing details format
                    for i, text_item in enumerate(test_texts):
                        detail = {
                            "text_id": text_item.get("id", f"text_{i+1:03d}"),
                            "content_preview": text_item.get("content", ""),
                            "assigned_label": text_item.get("ai_assigned_label", ""),
                            "confidence_score": text_item.get("confidence", 0.8),  # Default confidence
                            "classification_reasoning": text_item.get("reasoning", "AI classification")
                        }
                        processing_details.append(detail)
        except Exception:
            # Fallback to job log processing details
            processing_details = job_log.get("text_agent", {}).get("processing_details", [])
        
        if not processing_details:
            return {"message": "No processing details found for active learning analysis"}
        
        # Extract text and prediction data
        texts = []
        predictions = []
        confidences = []
        
        for detail in processing_details:
            texts.append(detail.get("content_preview", ""))
            predictions.append(detail.get("assigned_label", ""))
            confidences.append(detail.get("confidence_score", 0.0))
        
        # Apply active learning strategy
        if strategy == UncertaintyStrategy.CONFIDENCE_BASED:
            learning_items = self._confidence_based_selection(
                job_id, processing_details, max_items
            )
        elif strategy == UncertaintyStrategy.DIVERSITY_BASED:
            learning_items = self._diversity_based_selection(
                job_id, processing_details, texts, max_items
            )
        elif strategy == UncertaintyStrategy.UNCERTAINTY_SAMPLING:
            learning_items = self._uncertainty_sampling_selection(
                job_id, processing_details, max_items
            )
        else:  # QUERY_BY_COMMITTEE
            learning_items = self._query_by_committee_selection(
                job_id, processing_details, max_items
            )
        
        # Store learning items
        self._store_learning_items(learning_items)
        
        # Generate insights
        insights = self._generate_learning_insights(job_id, learning_items)
        
        return {
            "job_id": job_id,
            "strategy_used": strategy.value,
            "total_items_identified": len(learning_items),
            "priority_breakdown": self._calculate_priority_breakdown(learning_items),
            "avg_uncertainty_score": np.mean([item.uncertainty_score for item in learning_items]) if learning_items else 0,
            "insights": insights,
            "learning_items": [self._learning_item_to_dict(item) for item in learning_items[:10]]  # Return first 10
        }
    
    def _confidence_based_selection(self, job_id: str, processing_details: List[Dict], 
                                  max_items: int) -> List[ActiveLearningItem]:
        """Select items based on low confidence scores"""
        learning_items = []
        
        # Sort by confidence score (lowest first)
        sorted_details = sorted(processing_details, key=lambda x: x.get("confidence_score", 1.0))
        
        for i, detail in enumerate(sorted_details[:max_items]):
            confidence = detail.get("confidence_score", 0.0)
            uncertainty_score = 1.0 - confidence
            
            # Determine priority based on confidence
            if confidence < 0.3:
                priority = LearningPriority.CRITICAL
            elif confidence < 0.5:
                priority = LearningPriority.HIGH
            elif confidence < 0.7:
                priority = LearningPriority.MEDIUM
            else:
                priority = LearningPriority.LOW
            
            item = ActiveLearningItem(
                id=str(uuid.uuid4()),
                job_id=job_id,
                text_id=detail.get("text_id", f"item_{i}"),
                original_text=detail.get("content_preview", ""),
                predicted_label=detail.get("assigned_label", ""),
                confidence_score=confidence,
                uncertainty_score=uncertainty_score,
                diversity_score=0.0,  # Not used in this strategy
                priority=priority,
                strategy_used=UncertaintyStrategy.CONFIDENCE_BASED,
                created_at=datetime.now().isoformat(),
                metadata={
                    "processing_time_ms": detail.get("processing_time_ms", 0),
                    "classification_reasoning": detail.get("classification_reasoning", "")
                }
            )
            learning_items.append(item)
        
        return learning_items
    
    def _diversity_based_selection(self, job_id: str, processing_details: List[Dict], 
                                 texts: List[str], max_items: int) -> List[ActiveLearningItem]:
        """Select diverse items to maximize coverage of different text types"""
        learning_items = []
        
        if len(texts) < max_items:
            return self._confidence_based_selection(job_id, processing_details, max_items)
        
        try:
            # Vectorize texts
            text_vectors = self.vectorizer.fit_transform(texts)
            
            # Use K-means clustering to find diverse examples
            n_clusters = min(max_items, len(texts))
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            cluster_labels = kmeans.fit_predict(text_vectors)
            
            # Select one example from each cluster (closest to centroid)
            selected_indices = []
            for cluster_id in range(n_clusters):
                cluster_indices = np.where(cluster_labels == cluster_id)[0]
                if len(cluster_indices) > 0:
                    # Find closest to centroid
                    cluster_vectors = text_vectors[cluster_indices]
                    centroid = kmeans.cluster_centers_[cluster_id]
                    distances = cosine_similarity(cluster_vectors, centroid.reshape(1, -1)).flatten()
                    closest_idx = cluster_indices[np.argmax(distances)]
                    selected_indices.append(closest_idx)
            
            # Create learning items for selected indices
            for i, idx in enumerate(selected_indices):
                detail = processing_details[idx]
                confidence = detail.get("confidence_score", 0.0)
                
                # Calculate diversity score based on distance from other selected items
                diversity_score = self._calculate_diversity_score(idx, selected_indices, text_vectors)
                
                priority = LearningPriority.MEDIUM  # Default for diversity-based
                if confidence < 0.5:
                    priority = LearningPriority.HIGH
                
                item = ActiveLearningItem(
                    id=str(uuid.uuid4()),
                    job_id=job_id,
                    text_id=detail.get("text_id", f"item_{idx}"),
                    original_text=detail.get("content_preview", ""),
                    predicted_label=detail.get("assigned_label", ""),
                    confidence_score=confidence,
                    uncertainty_score=1.0 - confidence,
                    diversity_score=diversity_score,
                    priority=priority,
                    strategy_used=UncertaintyStrategy.DIVERSITY_BASED,
                    created_at=datetime.now().isoformat(),
                    metadata={
                        "cluster_id": int(cluster_labels[idx]),
                        "diversity_rank": i + 1
                    }
                )
                learning_items.append(item)
                
        except Exception as e:
            # Fallback to confidence-based if diversity analysis fails
            return self._confidence_based_selection(job_id, processing_details, max_items)
        
        return learning_items
    
    def _uncertainty_sampling_selection(self, job_id: str, processing_details: List[Dict], 
                                      max_items: int) -> List[ActiveLearningItem]:
        """Select items using uncertainty sampling (entropy-based)"""
        learning_items = []
        
        # Calculate entropy for each prediction (simulate multi-class uncertainty)
        scored_items = []
        for i, detail in enumerate(processing_details):
            confidence = detail.get("confidence_score", 0.0)
            
            # Simulate entropy calculation (in real scenario, would use actual model probabilities)
            if confidence > 0.9:
                entropy = 0.1
            elif confidence > 0.7:
                entropy = 0.5
            elif confidence > 0.5:
                entropy = 0.8
            else:
                entropy = 1.0
            
            scored_items.append((i, detail, entropy))
        
        # Sort by entropy (highest first)
        scored_items.sort(key=lambda x: x[2], reverse=True)
        
        for i, (idx, detail, entropy) in enumerate(scored_items[:max_items]):
            confidence = detail.get("confidence_score", 0.0)
            
            # Priority based on entropy
            if entropy > 0.8:
                priority = LearningPriority.CRITICAL
            elif entropy > 0.6:
                priority = LearningPriority.HIGH
            elif entropy > 0.4:
                priority = LearningPriority.MEDIUM
            else:
                priority = LearningPriority.LOW
            
            item = ActiveLearningItem(
                id=str(uuid.uuid4()),
                job_id=job_id,
                text_id=detail.get("text_id", f"item_{idx}"),
                original_text=detail.get("content_preview", ""),
                predicted_label=detail.get("assigned_label", ""),
                confidence_score=confidence,
                uncertainty_score=entropy,
                diversity_score=0.0,
                priority=priority,
                strategy_used=UncertaintyStrategy.UNCERTAINTY_SAMPLING,
                created_at=datetime.now().isoformat(),
                metadata={
                    "entropy_score": entropy,
                    "uncertainty_rank": i + 1
                }
            )
            learning_items.append(item)
        
        return learning_items
    
    def _query_by_committee_selection(self, job_id: str, processing_details: List[Dict], 
                                    max_items: int) -> List[ActiveLearningItem]:
        """Select items where multiple models would disagree (simulated)"""
        learning_items = []
        
        # Simulate committee disagreement
        for i, detail in enumerate(processing_details[:max_items]):
            confidence = detail.get("confidence_score", 0.0)
            
            # Simulate disagreement score (in real scenario, would use multiple models)
            if confidence < 0.4:
                disagreement = 0.9
            elif confidence < 0.6:
                disagreement = 0.7
            elif confidence < 0.8:
                disagreement = 0.5
            else:
                disagreement = 0.2
            
            # Priority based on disagreement
            if disagreement > 0.8:
                priority = LearningPriority.CRITICAL
            elif disagreement > 0.6:
                priority = LearningPriority.HIGH
            elif disagreement > 0.4:
                priority = LearningPriority.MEDIUM
            else:
                priority = LearningPriority.LOW
            
            item = ActiveLearningItem(
                id=str(uuid.uuid4()),
                job_id=job_id,
                text_id=detail.get("text_id", f"item_{i}"),
                original_text=detail.get("content_preview", ""),
                predicted_label=detail.get("assigned_label", ""),
                confidence_score=confidence,
                uncertainty_score=disagreement,
                diversity_score=0.0,
                priority=priority,
                strategy_used=UncertaintyStrategy.QUERY_BY_COMMITTEE,
                created_at=datetime.now().isoformat(),
                metadata={
                    "disagreement_score": disagreement,
                    "committee_rank": i + 1
                }
            )
            learning_items.append(item)
        
        return learning_items
    
    def _calculate_diversity_score(self, target_idx: int, selected_indices: List[int], 
                                 text_vectors) -> float:
        """Calculate how different this item is from other selected items"""
        if len(selected_indices) <= 1:
            return 1.0
        
        target_vector = text_vectors[target_idx]
        other_vectors = text_vectors[[i for i in selected_indices if i != target_idx]]
        
        # Calculate average similarity to other selected items
        similarities = cosine_similarity(target_vector, other_vectors).flatten()
        avg_similarity = np.mean(similarities)
        
        # Diversity score is inverse of similarity
        return 1.0 - avg_similarity
    
    def _store_learning_items(self, learning_items: List[ActiveLearningItem]):
        """Store active learning items in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for item in learning_items:
            cursor.execute("""
                INSERT INTO active_learning_items 
                (id, job_id, text_id, original_text, predicted_label, confidence_score,
                 uncertainty_score, diversity_score, priority, strategy_used, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (item.id, item.job_id, item.text_id, item.original_text,
                  item.predicted_label, item.confidence_score, item.uncertainty_score,
                  item.diversity_score, item.priority.value, item.strategy_used.value,
                  item.created_at, json.dumps(item.metadata or {})))
        
        conn.commit()
        conn.close()
    
    def get_learning_queue(self, job_id: Optional[str] = None, 
                          priority: Optional[LearningPriority] = None,
                          strategy: Optional[UncertaintyStrategy] = None,
                          limit: int = 50) -> List[ActiveLearningItem]:
        """Get pending active learning items"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM active_learning_items WHERE reviewed = FALSE"
        params = []
        
        if job_id:
            query += " AND job_id = ?"
            params.append(job_id)
        
        if priority:
            query += " AND priority = ?"
            params.append(priority.value)
        
        if strategy:
            query += " AND strategy_used = ?"
            params.append(strategy.value)
        
        query += " ORDER BY uncertainty_score DESC, created_at ASC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_learning_item(row) for row in rows]
    
    def submit_learning_review(self, item_id: str, human_label: str, 
                             human_confidence: float, review_notes: str = "") -> Dict[str, Any]:
        """Submit human review for an active learning item"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Update the learning item
        cursor.execute("""
            UPDATE active_learning_items 
            SET reviewed = TRUE, human_label = ?, human_confidence = ?, 
                review_time = ?, metadata = json_set(COALESCE(metadata, '{}'), '$.review_notes', ?)
            WHERE id = ?
        """, (human_label, human_confidence, datetime.now().isoformat(), review_notes, item_id))
        
        if cursor.rowcount == 0:
            conn.close()
            raise ValueError(f"Learning item {item_id} not found")
        
        # Get the updated item for analysis
        cursor.execute("SELECT * FROM active_learning_items WHERE id = ?", (item_id,))
        row = cursor.fetchone()
        item = self._row_to_learning_item(row)
        
        conn.commit()
        conn.close()
        
        # Analyze review quality
        agreement = 1.0 if item.predicted_label == human_label else 0.0
        confidence_diff = abs(item.confidence_score - human_confidence)
        
        # Update strategy performance
        self._update_strategy_performance(item, agreement)
        
        return {
            "item_id": item_id,
            "review_submitted": True,
            "ai_human_agreement": agreement,
            "confidence_difference": confidence_diff,
            "learning_impact": self._calculate_learning_impact(item, human_label, human_confidence)
        }
    
    def _generate_learning_insights(self, job_id: str, 
                                  learning_items: List[ActiveLearningItem]) -> LearningInsights:
        """Generate insights about active learning opportunities"""
        if not learning_items:
            return LearningInsights(
                total_uncertain_items=0,
                avg_uncertainty_score=0.0,
                priority_distribution={},
                strategy_effectiveness={},
                improvement_recommendations=[],
                confidence_gaps=[]
            )
        
        # Calculate statistics
        total_items = len(learning_items)
        avg_uncertainty = np.mean([item.uncertainty_score for item in learning_items])
        
        # Priority distribution
        priority_dist = {}
        for priority in LearningPriority:
            count = len([item for item in learning_items if item.priority == priority])
            priority_dist[priority.value] = count
        
        # Strategy effectiveness (would be calculated from historical data)
        strategy_effectiveness = {
            UncertaintyStrategy.CONFIDENCE_BASED.value: 0.75,
            UncertaintyStrategy.DIVERSITY_BASED.value: 0.68,
            UncertaintyStrategy.UNCERTAINTY_SAMPLING.value: 0.82,
            UncertaintyStrategy.QUERY_BY_COMMITTEE.value: 0.79
        }
        
        # Generate recommendations
        recommendations = []
        if avg_uncertainty > 0.7:
            recommendations.append("High uncertainty detected - consider model retraining")
        if priority_dist.get("critical", 0) > total_items * 0.3:
            recommendations.append("Many critical items found - prioritize human review")
        if total_items > 100:
            recommendations.append("Large number of uncertain items - consider batch review strategies")
        
        # Identify confidence gaps
        confidence_gaps = []
        low_confidence_items = [item for item in learning_items if item.confidence_score < 0.5]
        if low_confidence_items:
            gap_analysis = {
                "threshold": 0.5,
                "affected_items": len(low_confidence_items),
                "avg_confidence": np.mean([item.confidence_score for item in low_confidence_items]),
                "labels_affected": list(set([item.predicted_label for item in low_confidence_items]))
            }
            confidence_gaps.append(gap_analysis)
        
        return LearningInsights(
            total_uncertain_items=total_items,
            avg_uncertainty_score=avg_uncertainty,
            priority_distribution=priority_dist,
            strategy_effectiveness=strategy_effectiveness,
            improvement_recommendations=recommendations,
            confidence_gaps=confidence_gaps
        )
    
    def _calculate_priority_breakdown(self, learning_items: List[ActiveLearningItem]) -> Dict[str, int]:
        """Calculate breakdown of items by priority"""
        breakdown = {priority.value: 0 for priority in LearningPriority}
        for item in learning_items:
            breakdown[item.priority.value] += 1
        return breakdown
    
    def _update_strategy_performance(self, item: ActiveLearningItem, agreement: float):
        """Update performance metrics for the strategy used"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # This would be more sophisticated in a real implementation
        cursor.execute("""
            INSERT INTO strategy_performance 
            (strategy_name, job_id, items_reviewed, accuracy_improvement, 
             avg_review_time, human_agreement_rate, effectiveness_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (item.strategy_used.value, item.job_id, 1, 0.05, 60.0, agreement, 
              agreement * 0.8, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    def _calculate_learning_impact(self, item: ActiveLearningItem, 
                                 human_label: str, human_confidence: float) -> Dict[str, Any]:
        """Calculate the potential impact of this learning example"""
        return {
            "label_correction": item.predicted_label != human_label,
            "confidence_improvement": human_confidence - item.confidence_score,
            "uncertainty_reduction": max(0, item.uncertainty_score - (1.0 - human_confidence)),
            "training_value": "high" if item.uncertainty_score > 0.7 else "medium"
        }
    
    def get_learning_analytics(self, job_id: Optional[str] = None, 
                             time_period: str = "7d") -> Dict[str, Any]:
        """Get comprehensive active learning analytics"""
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
        
        # Get statistics
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_items,
                COUNT(CASE WHEN reviewed = TRUE THEN 1 END) as reviewed_items,
                AVG(uncertainty_score) as avg_uncertainty,
                AVG(confidence_score) as avg_confidence
            FROM active_learning_items
            {where_clause}
        """, params)
        
        stats = cursor.fetchone()
        
        # Get strategy performance
        cursor.execute(f"""
            SELECT strategy_used, COUNT(*), AVG(uncertainty_score)
            FROM active_learning_items
            {where_clause}
            GROUP BY strategy_used
        """, params)
        
        strategy_stats = cursor.fetchall()
        
        # Get priority distribution
        cursor.execute(f"""
            SELECT priority, COUNT(*)
            FROM active_learning_items
            {where_clause}
            GROUP BY priority
        """, params)
        
        priority_stats = cursor.fetchall()
        
        conn.close()
        
        return {
            "summary": {
                "total_items": stats[0] if stats else 0,
                "reviewed_items": stats[1] if stats else 0,
                "review_rate": (stats[1] / stats[0] * 100) if stats and stats[0] > 0 else 0,
                "avg_uncertainty": stats[2] if stats else 0,
                "avg_confidence": stats[3] if stats else 0
            },
            "strategy_performance": [
                {
                    "strategy": row[0],
                    "items_count": row[1],
                    "avg_uncertainty": row[2]
                }
                for row in strategy_stats
            ],
            "priority_distribution": [
                {
                    "priority": row[0],
                    "count": row[1]
                }
                for row in priority_stats
            ]
        }
    
    def _row_to_learning_item(self, row) -> ActiveLearningItem:
        """Convert database row to ActiveLearningItem"""
        return ActiveLearningItem(
            id=row[0], job_id=row[1], text_id=row[2], original_text=row[3],
            predicted_label=row[4], confidence_score=row[5], uncertainty_score=row[6],
            diversity_score=row[7], priority=LearningPriority(row[8]),
            strategy_used=UncertaintyStrategy(row[9]), created_at=row[10],
            reviewed=bool(row[11]), human_label=row[12], human_confidence=row[13],
            review_time=row[14], metadata=json.loads(row[15] or "{}")
        )
    
    def _learning_item_to_dict(self, item: ActiveLearningItem) -> Dict[str, Any]:
        """Convert ActiveLearningItem to dictionary"""
        return {
            "id": item.id,
            "job_id": item.job_id,
            "text_id": item.text_id,
            "original_text": item.original_text,
            "predicted_label": item.predicted_label,
            "confidence_score": item.confidence_score,
            "uncertainty_score": item.uncertainty_score,
            "diversity_score": item.diversity_score,
            "priority": item.priority.value,
            "strategy_used": item.strategy_used.value,
            "created_at": item.created_at,
            "reviewed": item.reviewed,
            "human_label": item.human_label,
            "human_confidence": item.human_confidence,
            "review_time": item.review_time,
            "metadata": item.metadata
        }

# Global active learning system instance
active_learning_system = ActiveLearningSystem()
