"""
Label Template Management System
Allows users to save, load, and manage label configurations for different domains
"""
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel

class LabelTemplate(BaseModel):
    id: str
    name: str
    description: str
    labels: List[str]
    instructions: str
    domain: str  # e.g., "product_reviews", "customer_support", "content_moderation"
    created_at: str
    updated_at: str
    usage_count: int = 0
    is_public: bool = False
    created_by: str = "system"

class LabelTemplateManager:
    """Manages label templates for reuse across jobs"""
    
    def __init__(self):
        self.templates_dir = Path("/Volumes/DATA/Projects/data_label_agent/data/templates")
        self.templates_dir.mkdir(parents=True, exist_ok=True)
        self.templates_file = self.templates_dir / "label_templates.json"
        self._ensure_default_templates()
    
    def _ensure_default_templates(self):
        """Create default templates if they don't exist"""
        if not self.templates_file.exists():
            default_templates = [
                {
                    "id": "product_reviews",
                    "name": "Product Reviews",
                    "description": "Standard labels for product review classification",
                    "labels": ["positive_review", "negative_review", "neutral_review", "question", "complaint"],
                    "instructions": "Classify product reviews based on sentiment and intent. Consider overall tone and specific feedback.",
                    "domain": "e-commerce",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "usage_count": 0,
                    "is_public": True,
                    "created_by": "system"
                },
                {
                    "id": "customer_support",
                    "name": "Customer Support Tickets",
                    "description": "Labels for customer support ticket categorization",
                    "labels": ["technical_issue", "billing_inquiry", "feature_request", "complaint", "general_question"],
                    "instructions": "Categorize customer support tickets by the primary issue type. Focus on the main concern raised.",
                    "domain": "customer_service",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "usage_count": 0,
                    "is_public": True,
                    "created_by": "system"
                },
                {
                    "id": "content_moderation",
                    "name": "Content Moderation",
                    "description": "Labels for content moderation and safety",
                    "labels": ["safe_content", "inappropriate_language", "spam", "harassment", "misinformation"],
                    "instructions": "Classify content for moderation purposes. Focus on safety, community guidelines, and appropriateness.",
                    "domain": "content_safety",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "usage_count": 0,
                    "is_public": True,
                    "created_by": "system"
                },
                {
                    "id": "news_articles",
                    "name": "News Article Categories",
                    "description": "Standard news article classification labels",
                    "labels": ["politics", "technology", "business", "sports", "entertainment", "health", "science"],
                    "instructions": "Classify news articles by their primary topic. Consider the main subject matter discussed.",
                    "domain": "journalism",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "usage_count": 0,
                    "is_public": True,
                    "created_by": "system"
                },
                {
                    "id": "social_media",
                    "name": "Social Media Posts",
                    "description": "Labels for social media content classification",
                    "labels": ["personal_update", "promotional", "educational", "entertainment", "news_sharing", "question"],
                    "instructions": "Classify social media posts by intent and content type. Consider the purpose and tone of the post.",
                    "domain": "social_media",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "usage_count": 0,
                    "is_public": True,
                    "created_by": "system"
                }
            ]
            
            with open(self.templates_file, 'w', encoding='utf-8') as f:
                json.dump(default_templates, f, indent=2, ensure_ascii=False)
    
    def get_all_templates(self) -> List[Dict[str, Any]]:
        """Get all available label templates"""
        try:
            with open(self.templates_file, 'r', encoding='utf-8') as f:
                templates = json.load(f)
            
            # Sort by usage count and name
            templates.sort(key=lambda x: (-x.get('usage_count', 0), x.get('name', '')))
            return templates
        except (FileNotFoundError, json.JSONDecodeError):
            return []
    
    def get_template_by_id(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific template by ID"""
        templates = self.get_all_templates()
        for template in templates:
            if template.get('id') == template_id:
                return template
        return None
    
    def get_templates_by_domain(self, domain: str) -> List[Dict[str, Any]]:
        """Get templates filtered by domain"""
        templates = self.get_all_templates()
        return [t for t in templates if t.get('domain', '').lower() == domain.lower()]
    
    def search_templates(self, query: str) -> List[Dict[str, Any]]:
        """Search templates by name, description, or labels"""
        templates = self.get_all_templates()
        query_lower = query.lower()
        
        matching_templates = []
        for template in templates:
            # Search in name, description, and labels
            if (query_lower in template.get('name', '').lower() or
                query_lower in template.get('description', '').lower() or
                any(query_lower in label.lower() for label in template.get('labels', []))):
                matching_templates.append(template)
        
        return matching_templates
    
    def create_template(self, template_data: Dict[str, Any]) -> str:
        """Create a new label template"""
        templates = self.get_all_templates()
        
        # Generate ID from name
        template_id = template_data.get('name', '').lower().replace(' ', '_').replace('-', '_')
        template_id = ''.join(c for c in template_id if c.isalnum() or c == '_')
        
        # Ensure unique ID
        existing_ids = [t.get('id') for t in templates]
        counter = 1
        original_id = template_id
        while template_id in existing_ids:
            template_id = f"{original_id}_{counter}"
            counter += 1
        
        # Create new template
        new_template = {
            "id": template_id,
            "name": template_data.get('name', ''),
            "description": template_data.get('description', ''),
            "labels": template_data.get('labels', []),
            "instructions": template_data.get('instructions', ''),
            "domain": template_data.get('domain', 'general'),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "usage_count": 0,
            "is_public": template_data.get('is_public', False),
            "created_by": template_data.get('created_by', 'user')
        }
        
        # Add to templates list
        templates.append(new_template)
        
        # Save to file
        with open(self.templates_file, 'w', encoding='utf-8') as f:
            json.dump(templates, f, indent=2, ensure_ascii=False)
        
        return template_id
    
    def update_template(self, template_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing template"""
        templates = self.get_all_templates()
        
        for i, template in enumerate(templates):
            if template.get('id') == template_id:
                # Update fields
                for key, value in updates.items():
                    if key not in ['id', 'created_at', 'usage_count']:  # Protect certain fields
                        template[key] = value
                
                template['updated_at'] = datetime.now().isoformat()
                templates[i] = template
                
                # Save to file
                with open(self.templates_file, 'w', encoding='utf-8') as f:
                    json.dump(templates, f, indent=2, ensure_ascii=False)
                
                return True
        
        return False
    
    def delete_template(self, template_id: str) -> bool:
        """Delete a template"""
        templates = self.get_all_templates()
        
        # Don't delete system templates
        template = self.get_template_by_id(template_id)
        if template and template.get('created_by') == 'system':
            return False
        
        # Remove template
        templates = [t for t in templates if t.get('id') != template_id]
        
        # Save to file
        with open(self.templates_file, 'w', encoding='utf-8') as f:
            json.dump(templates, f, indent=2, ensure_ascii=False)
        
        return True
    
    def increment_usage(self, template_id: str):
        """Increment usage count for a template"""
        templates = self.get_all_templates()
        
        for i, template in enumerate(templates):
            if template.get('id') == template_id:
                template['usage_count'] = template.get('usage_count', 0) + 1
                templates[i] = template
                
                # Save to file
                with open(self.templates_file, 'w', encoding='utf-8') as f:
                    json.dump(templates, f, indent=2, ensure_ascii=False)
                break
    
    def get_popular_templates(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most popular templates by usage"""
        templates = self.get_all_templates()
        templates.sort(key=lambda x: x.get('usage_count', 0), reverse=True)
        return templates[:limit]
    
    def get_template_analytics(self) -> Dict[str, Any]:
        """Get analytics about template usage"""
        templates = self.get_all_templates()
        
        total_templates = len(templates)
        total_usage = sum(t.get('usage_count', 0) for t in templates)
        
        # Domain distribution
        domains = {}
        for template in templates:
            domain = template.get('domain', 'unknown')
            domains[domain] = domains.get(domain, 0) + 1
        
        # Most popular
        popular = sorted(templates, key=lambda x: x.get('usage_count', 0), reverse=True)[:5]
        
        return {
            "total_templates": total_templates,
            "total_usage": total_usage,
            "average_usage": total_usage / total_templates if total_templates > 0 else 0,
            "domain_distribution": domains,
            "most_popular": [{"name": t.get('name'), "usage": t.get('usage_count', 0)} for t in popular],
            "public_templates": len([t for t in templates if t.get('is_public', False)]),
            "user_created_templates": len([t for t in templates if t.get('created_by') != 'system'])
        }

# Global template manager instance
template_manager = LabelTemplateManager()
