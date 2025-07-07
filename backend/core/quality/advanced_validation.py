"""
Advanced Validation Rules System
Provides custom business logic validation for data quality and label consistency
"""
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import re
import uuid

class ValidationSeverity(Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

class ValidationRuleType(Enum):
    TEXT_PATTERN = "text_pattern"
    LABEL_CONSISTENCY = "label_consistency"
    DATA_COMPLETENESS = "data_completeness"
    BUSINESS_LOGIC = "business_logic"
    CUSTOM_FUNCTION = "custom_function"
    STATISTICAL = "statistical"

class ValidationStatus(Enum):
    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class ValidationRule:
    id: str
    name: str
    description: str
    rule_type: ValidationRuleType
    severity: ValidationSeverity
    rule_definition: Dict[str, Any]
    is_active: bool = True
    created_at: str = ""
    created_by: str = "system"
    domain: Optional[str] = None
    tags: List[str] = field(default_factory=list)

@dataclass
class ValidationResult:
    id: str
    rule_id: str
    entity_id: str
    entity_type: str
    status: ValidationStatus
    message: str
    details: Dict[str, Any]
    severity: ValidationSeverity
    validated_at: str
    execution_time_ms: int = 0

@dataclass
class ValidationReport:
    id: str
    job_id: str
    total_items: int
    validated_items: int
    passed_count: int
    failed_count: int
    warning_count: int
    error_count: int
    validation_results: List[ValidationResult]
    created_at: str
    execution_time_ms: int

class AdvancedValidationSystem:
    """Advanced validation system with custom business logic and rules"""
    
    def __init__(self):
        self.data_dir = Path("/Volumes/DATA/Projects/data_label_agent/data/validation")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.data_dir / "validation.db"
        self._init_database()
        self._init_default_rules()
        
        # Custom validation functions registry
        self.custom_validators = {}
        
    def _init_database(self):
        """Initialize validation database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Validation rules table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation_rules (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                rule_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                rule_definition TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                domain TEXT,
                tags TEXT
            )
        """)
        
        # Validation results table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation_results (
                id TEXT PRIMARY KEY,
                rule_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT NOT NULL,
                details TEXT NOT NULL,
                severity TEXT NOT NULL,
                validated_at TEXT NOT NULL,
                execution_time_ms INTEGER DEFAULT 0,
                FOREIGN KEY (rule_id) REFERENCES validation_rules (id)
            )
        """)
        
        # Validation reports table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation_reports (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                total_items INTEGER NOT NULL,
                validated_items INTEGER NOT NULL,
                passed_count INTEGER NOT NULL,
                failed_count INTEGER NOT NULL,
                warning_count INTEGER NOT NULL,
                error_count INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                execution_time_ms INTEGER DEFAULT 0
            )
        """)
        
        # Validation metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id TEXT NOT NULL,
                date TEXT NOT NULL,
                execution_count INTEGER NOT NULL,
                success_rate REAL NOT NULL,
                avg_execution_time_ms REAL NOT NULL,
                FOREIGN KEY (rule_id) REFERENCES validation_rules (id)
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_entity ON validation_results(entity_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_rule ON validation_results(rule_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_results_validated_at ON validation_results(validated_at)")
        
        conn.commit()
        conn.close()
        
    def _init_default_rules(self):
        """Initialize default validation rules"""
        default_rules = [
            {
                "name": "Minimum Text Length",
                "description": "Ensures text items meet minimum length requirements",
                "rule_type": ValidationRuleType.TEXT_PATTERN,
                "severity": ValidationSeverity.WARNING,
                "rule_definition": {
                    "min_length": 10,
                    "field": "content"
                },
                "domain": "text_classification"
            },
            {
                "name": "No Empty Labels",
                "description": "Ensures all items have assigned labels",
                "rule_type": ValidationRuleType.DATA_COMPLETENESS,
                "severity": ValidationSeverity.ERROR,
                "rule_definition": {
                    "required_fields": ["ai_assigned_label"],
                    "allow_null": False
                }
            },
            {
                "name": "Label Format Validation",
                "description": "Validates label format and allowed values",
                "rule_type": ValidationRuleType.LABEL_CONSISTENCY,
                "severity": ValidationSeverity.ERROR,
                "rule_definition": {
                    "field": "ai_assigned_label",
                    "pattern": "^[a-z_]+$",
                    "allowed_values": []  # Will be populated per job
                }
            },
            {
                "name": "Confidence Score Range",
                "description": "Ensures confidence scores are within valid range",
                "rule_type": ValidationRuleType.STATISTICAL,
                "severity": ValidationSeverity.WARNING,
                "rule_definition": {
                    "field": "confidence_score",
                    "min_value": 0.0,
                    "max_value": 1.0
                }
            },
            {
                "name": "Suspicious Low Confidence",
                "description": "Flags items with suspiciously low confidence scores",
                "rule_type": ValidationRuleType.BUSINESS_LOGIC,
                "severity": ValidationSeverity.INFO,
                "rule_definition": {
                    "condition": "confidence_score < 0.3",
                    "threshold": 0.3
                }
            }
        ]
        
        for rule_def in default_rules:
            existing_rule = self.get_rule_by_name(rule_def["name"])
            if not existing_rule:
                self.create_validation_rule(**rule_def)
    
    def create_validation_rule(self, name: str, description: str, rule_type: ValidationRuleType,
                             severity: ValidationSeverity, rule_definition: Dict[str, Any],
                             domain: Optional[str] = None, tags: Optional[List[str]] = None,
                             created_by: str = "system") -> str:
        """Create a new validation rule"""
        
        rule = ValidationRule(
            id=str(uuid.uuid4()),
            name=name,
            description=description,
            rule_type=rule_type,
            severity=severity,
            rule_definition=rule_definition,
            created_at=datetime.now().isoformat(),
            created_by=created_by,
            domain=domain,
            tags=tags or []
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO validation_rules 
            (id, name, description, rule_type, severity, rule_definition,
             is_active, created_at, created_by, domain, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (rule.id, rule.name, rule.description, rule.rule_type.value,
              rule.severity.value, json.dumps(rule.rule_definition), rule.is_active,
              rule.created_at, rule.created_by, rule.domain, json.dumps(rule.tags)))
        
        conn.commit()
        conn.close()
        
        return rule.id
    
    def validate_job_results(self, job_id: str, 
                           domain: Optional[str] = None,
                           custom_rules: Optional[List[str]] = None) -> ValidationReport:
        """Validate all results from a completed job"""
        
        # Get job results
        from common.job_logger import job_logger
        job_log = job_logger.get_job_log(job_id)
        
        if not job_log:
            raise ValueError(f"Job {job_id} not found")
        
        processing_details = job_log.get("text_agent", {}).get("processing_details", [])
        if not processing_details:
            raise ValueError(f"No processing details found for job {job_id}")
        
        # Get applicable validation rules
        rules = self.get_applicable_rules(domain, custom_rules)
        
        # Update label validation rule with job's available labels
        available_labels = job_log.get("user_input", {}).get("available_labels", [])
        self._update_label_validation_rules(rules, available_labels)
        
        # Validate each item
        validation_results = []
        start_time = datetime.now()
        
        for detail in processing_details:
            for rule in rules:
                result = self._validate_item(detail, rule, job_id)
                if result:
                    validation_results.append(result)
        
        # Calculate report statistics
        execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
        
        passed_count = len([r for r in validation_results if r.status == ValidationStatus.PASSED])
        failed_count = len([r for r in validation_results if r.status == ValidationStatus.FAILED])
        warning_count = len([r for r in validation_results if r.severity == ValidationSeverity.WARNING])
        error_count = len([r for r in validation_results if r.severity == ValidationSeverity.ERROR])
        
        # Create validation report
        report = ValidationReport(
            id=str(uuid.uuid4()),
            job_id=job_id,
            total_items=len(processing_details),
            validated_items=len(processing_details),
            passed_count=passed_count,
            failed_count=failed_count,
            warning_count=warning_count,
            error_count=error_count,
            validation_results=validation_results,
            created_at=datetime.now().isoformat(),
            execution_time_ms=execution_time
        )
        
        # Store report
        self._store_validation_report(report)
        
        # Update metrics
        self._update_validation_metrics(rules, validation_results)
        
        return report
    
    def validate_single_item(self, item_data: Dict[str, Any], 
                           rule_ids: Optional[List[str]] = None) -> List[ValidationResult]:
        """Validate a single item against specified rules"""
        
        if rule_ids:
            rules = [self.get_rule_by_id(rule_id) for rule_id in rule_ids]
            rules = [r for r in rules if r]  # Remove None values
        else:
            rules = self.get_all_active_rules()
        
        results = []
        for rule in rules:
            result = self._validate_item(item_data, rule, "single_item_validation")
            if result:
                results.append(result)
        
        return results
    
    def get_applicable_rules(self, domain: Optional[str] = None, 
                           rule_ids: Optional[List[str]] = None) -> List[ValidationRule]:
        """Get applicable validation rules based on domain and rule IDs"""
        
        if rule_ids:
            return [self.get_rule_by_id(rule_id) for rule_id in rule_ids if self.get_rule_by_id(rule_id)]
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM validation_rules WHERE is_active = TRUE"
        params = []
        
        if domain:
            query += " AND (domain = ? OR domain IS NULL)"
            params.append(domain)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_rule(row) for row in rows]
    
    def _validate_item(self, item_data: Dict[str, Any], rule: ValidationRule, 
                      job_id: str) -> Optional[ValidationResult]:
        """Validate a single item against a rule"""
        
        start_time = datetime.now()
        entity_id = item_data.get("text_id", item_data.get("id", "unknown"))
        
        try:
            if rule.rule_type == ValidationRuleType.TEXT_PATTERN:
                status, message, details = self._validate_text_pattern(item_data, rule)
            elif rule.rule_type == ValidationRuleType.LABEL_CONSISTENCY:
                status, message, details = self._validate_label_consistency(item_data, rule)
            elif rule.rule_type == ValidationRuleType.DATA_COMPLETENESS:
                status, message, details = self._validate_data_completeness(item_data, rule)
            elif rule.rule_type == ValidationRuleType.BUSINESS_LOGIC:
                status, message, details = self._validate_business_logic(item_data, rule)
            elif rule.rule_type == ValidationRuleType.STATISTICAL:
                status, message, details = self._validate_statistical(item_data, rule)
            elif rule.rule_type == ValidationRuleType.CUSTOM_FUNCTION:
                status, message, details = self._validate_custom_function(item_data, rule)
            else:
                return None
            
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            
            result = ValidationResult(
                id=str(uuid.uuid4()),
                rule_id=rule.id,
                entity_id=entity_id,
                entity_type="text_item",
                status=status,
                message=message,
                details=details,
                severity=rule.severity,
                validated_at=datetime.now().isoformat(),
                execution_time_ms=execution_time
            )
            
            # Store result
            self._store_validation_result(result)
            
            return result
            
        except Exception as e:
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            
            result = ValidationResult(
                id=str(uuid.uuid4()),
                rule_id=rule.id,
                entity_id=entity_id,
                entity_type="text_item",
                status=ValidationStatus.FAILED,
                message=f"Validation error: {str(e)}",
                details={"error": str(e)},
                severity=ValidationSeverity.ERROR,
                validated_at=datetime.now().isoformat(),
                execution_time_ms=execution_time
            )
            
            self._store_validation_result(result)
            return result
    
    def _validate_text_pattern(self, item_data: Dict[str, Any], 
                             rule: ValidationRule) -> Tuple[ValidationStatus, str, Dict[str, Any]]:
        """Validate text patterns"""
        rule_def = rule.rule_definition
        field = rule_def.get("field", "content")
        text_content = item_data.get(field, "")
        
        # Check minimum length
        if "min_length" in rule_def:
            min_length = rule_def["min_length"]
            if len(text_content) < min_length:
                return (
                    ValidationStatus.FAILED,
                    f"Text length {len(text_content)} is below minimum {min_length}",
                    {"actual_length": len(text_content), "min_length": min_length}
                )
        
        # Check maximum length
        if "max_length" in rule_def:
            max_length = rule_def["max_length"]
            if len(text_content) > max_length:
                return (
                    ValidationStatus.FAILED,
                    f"Text length {len(text_content)} exceeds maximum {max_length}",
                    {"actual_length": len(text_content), "max_length": max_length}
                )
        
        # Check regex pattern
        if "pattern" in rule_def:
            pattern = rule_def["pattern"]
            if not re.search(pattern, text_content):
                return (
                    ValidationStatus.FAILED,
                    f"Text does not match required pattern: {pattern}",
                    {"pattern": pattern, "text_sample": text_content[:100]}
                )
        
        # Check forbidden patterns
        if "forbidden_patterns" in rule_def:
            for forbidden_pattern in rule_def["forbidden_patterns"]:
                if re.search(forbidden_pattern, text_content):
                    return (
                        ValidationStatus.FAILED,
                        f"Text contains forbidden pattern: {forbidden_pattern}",
                        {"forbidden_pattern": forbidden_pattern}
                    )
        
        return (
            ValidationStatus.PASSED,
            "Text pattern validation passed",
            {"validated_field": field}
        )
    
    def _validate_label_consistency(self, item_data: Dict[str, Any], 
                                  rule: ValidationRule) -> Tuple[ValidationStatus, str, Dict[str, Any]]:
        """Validate label consistency"""
        rule_def = rule.rule_definition
        field = rule_def.get("field", "ai_assigned_label")
        label_value = item_data.get(field, "")
        
        # Check allowed values
        if "allowed_values" in rule_def and rule_def["allowed_values"]:
            allowed_values = rule_def["allowed_values"]
            if label_value not in allowed_values:
                return (
                    ValidationStatus.FAILED,
                    f"Label '{label_value}' is not in allowed values: {allowed_values}",
                    {"actual_label": label_value, "allowed_values": allowed_values}
                )
        
        # Check label format pattern
        if "pattern" in rule_def:
            pattern = rule_def["pattern"]
            if not re.match(pattern, label_value):
                return (
                    ValidationStatus.FAILED,
                    f"Label '{label_value}' does not match required pattern: {pattern}",
                    {"actual_label": label_value, "pattern": pattern}
                )
        
        # Check case sensitivity
        if rule_def.get("case_sensitive", True) == False:
            # Normalize case for comparison
            pass
        
        return (
            ValidationStatus.PASSED,
            "Label consistency validation passed",
            {"validated_label": label_value}
        )
    
    def _validate_data_completeness(self, item_data: Dict[str, Any], 
                                  rule: ValidationRule) -> Tuple[ValidationStatus, str, Dict[str, Any]]:
        """Validate data completeness"""
        rule_def = rule.rule_definition
        required_fields = rule_def.get("required_fields", [])
        allow_null = rule_def.get("allow_null", False)
        
        missing_fields = []
        null_fields = []
        
        for field in required_fields:
            if field not in item_data:
                missing_fields.append(field)
            elif not allow_null and (item_data[field] is None or item_data[field] == ""):
                null_fields.append(field)
        
        if missing_fields or null_fields:
            message_parts = []
            if missing_fields:
                message_parts.append(f"Missing fields: {missing_fields}")
            if null_fields:
                message_parts.append(f"Null/empty fields: {null_fields}")
            
            return (
                ValidationStatus.FAILED,
                "; ".join(message_parts),
                {"missing_fields": missing_fields, "null_fields": null_fields}
            )
        
        return (
            ValidationStatus.PASSED,
            "Data completeness validation passed",
            {"validated_fields": required_fields}
        )
    
    def _validate_business_logic(self, item_data: Dict[str, Any], 
                               rule: ValidationRule) -> Tuple[ValidationStatus, str, Dict[str, Any]]:
        """Validate custom business logic"""
        rule_def = rule.rule_definition
        condition = rule_def.get("condition", "")
        
        # Simple condition evaluation (could be expanded with a proper expression parser)
        try:
            # Create a safe evaluation context with item data
            eval_context = {
                **item_data,
                "len": len,
                "abs": abs,
                "min": min,
                "max": max
            }
            
            # Evaluate the condition
            result = eval(condition, {"__builtins__": {}}, eval_context)
            
            if result:
                return (
                    ValidationStatus.FAILED,
                    f"Business logic condition failed: {condition}",
                    {"condition": condition, "evaluation_result": result}
                )
            else:
                return (
                    ValidationStatus.PASSED,
                    "Business logic validation passed",
                    {"condition": condition}
                )
                
        except Exception as e:
            return (
                ValidationStatus.FAILED,
                f"Business logic evaluation error: {str(e)}",
                {"condition": condition, "error": str(e)}
            )
    
    def _validate_statistical(self, item_data: Dict[str, Any], 
                            rule: ValidationRule) -> Tuple[ValidationStatus, str, Dict[str, Any]]:
        """Validate statistical constraints"""
        rule_def = rule.rule_definition
        field = rule_def.get("field", "")
        value = item_data.get(field)
        
        if value is None:
            return (
                ValidationStatus.FAILED,
                f"Field '{field}' is missing for statistical validation",
                {"field": field}
            )
        
        # Check numeric range
        if "min_value" in rule_def:
            min_value = rule_def["min_value"]
            if value < min_value:
                return (
                    ValidationStatus.FAILED,
                    f"Value {value} is below minimum {min_value}",
                    {"field": field, "value": value, "min_value": min_value}
                )
        
        if "max_value" in rule_def:
            max_value = rule_def["max_value"]
            if value > max_value:
                return (
                    ValidationStatus.FAILED,
                    f"Value {value} exceeds maximum {max_value}",
                    {"field": field, "value": value, "max_value": max_value}
                )
        
        # Check for outliers (simple z-score based)
        if "outlier_threshold" in rule_def:
            # This would require historical data to calculate mean and std
            # For now, just pass
            pass
        
        return (
            ValidationStatus.PASSED,
            "Statistical validation passed",
            {"field": field, "value": value}
        )
    
    def _validate_custom_function(self, item_data: Dict[str, Any], 
                                rule: ValidationRule) -> Tuple[ValidationStatus, str, Dict[str, Any]]:
        """Validate using custom function"""
        rule_def = rule.rule_definition
        function_name = rule_def.get("function_name", "")
        
        if function_name not in self.custom_validators:
            return (
                ValidationStatus.FAILED,
                f"Custom validation function '{function_name}' not found",
                {"function_name": function_name}
            )
        
        try:
            validator_func = self.custom_validators[function_name]
            result = validator_func(item_data, rule_def)
            
            if isinstance(result, bool):
                if result:
                    return (ValidationStatus.PASSED, "Custom validation passed", {})
                else:
                    return (ValidationStatus.FAILED, "Custom validation failed", {})
            elif isinstance(result, tuple):
                return result
            else:
                return (
                    ValidationStatus.FAILED,
                    "Invalid return type from custom validator",
                    {"function_name": function_name}
                )
                
        except Exception as e:
            return (
                ValidationStatus.FAILED,
                f"Custom validation error: {str(e)}",
                {"function_name": function_name, "error": str(e)}
            )
    
    def register_custom_validator(self, name: str, validator_func: Callable):
        """Register a custom validation function"""
        self.custom_validators[name] = validator_func
    
    def get_validation_report(self, report_id: str) -> Optional[ValidationReport]:
        """Get a validation report by ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM validation_reports WHERE id = ?", (report_id,))
        report_row = cursor.fetchone()
        
        if not report_row:
            conn.close()
            return None
        
        # Get validation results for this report
        cursor.execute("""
            SELECT * FROM validation_results 
            WHERE entity_id IN (
                SELECT DISTINCT entity_id FROM validation_results 
                WHERE validated_at >= ? AND validated_at <= ?
            )
            ORDER BY validated_at
        """, (report_row[8], report_row[8]))  # Using created_at as approximation
        
        result_rows = cursor.fetchall()
        conn.close()
        
        validation_results = [self._row_to_result(row) for row in result_rows]
        
        return ValidationReport(
            id=report_row[0],
            job_id=report_row[1],
            total_items=report_row[2],
            validated_items=report_row[3],
            passed_count=report_row[4],
            failed_count=report_row[5],
            warning_count=report_row[6],
            error_count=report_row[7],
            validation_results=validation_results,
            created_at=report_row[8],
            execution_time_ms=report_row[9]
        )
    
    def get_validation_analytics(self, time_period: str = "7d") -> Dict[str, Any]:
        """Get validation analytics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Time filter
        if time_period == "24h":
            start_time = datetime.now() - timedelta(hours=24)
        elif time_period == "7d":
            start_time = datetime.now() - timedelta(days=7)
        elif time_period == "30d":
            start_time = datetime.now() - timedelta(days=30)
        else:
            start_time = datetime.now() - timedelta(days=7)
        
        # Overall statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_validations,
                COUNT(CASE WHEN status = 'passed' THEN 1 END) as passed_count,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_count,
                AVG(execution_time_ms) as avg_execution_time
            FROM validation_results 
            WHERE validated_at >= ?
        """, (start_time.isoformat(),))
        
        stats = cursor.fetchone()
        
        # Rule performance
        cursor.execute("""
            SELECT 
                vr.rule_id,
                vru.name,
                COUNT(*) as execution_count,
                COUNT(CASE WHEN vr.status = 'passed' THEN 1 END) as success_count,
                AVG(vr.execution_time_ms) as avg_execution_time
            FROM validation_results vr
            JOIN validation_rules vru ON vr.rule_id = vru.id
            WHERE vr.validated_at >= ?
            GROUP BY vr.rule_id, vru.name
            ORDER BY execution_count DESC
        """, (start_time.isoformat(),))
        
        rule_stats = cursor.fetchall()
        
        # Severity distribution
        cursor.execute("""
            SELECT severity, COUNT(*) 
            FROM validation_results 
            WHERE validated_at >= ?
            GROUP BY severity
        """, (start_time.isoformat(),))
        
        severity_stats = cursor.fetchall()
        
        conn.close()
        
        return {
            "summary": {
                "total_validations": stats[0] if stats else 0,
                "passed_count": stats[1] if stats else 0,
                "failed_count": stats[2] if stats else 0,
                "success_rate": (stats[1] / stats[0] * 100) if stats and stats[0] > 0 else 0,
                "avg_execution_time_ms": stats[3] if stats else 0
            },
            "rule_performance": [
                {
                    "rule_id": row[0],
                    "rule_name": row[1],
                    "execution_count": row[2],
                    "success_count": row[3],
                    "success_rate": (row[3] / row[2] * 100) if row[2] > 0 else 0,
                    "avg_execution_time_ms": row[4]
                }
                for row in rule_stats
            ],
            "severity_distribution": [
                {
                    "severity": row[0],
                    "count": row[1]
                }
                for row in severity_stats
            ]
        }
    
    def get_rule_by_id(self, rule_id: str) -> Optional[ValidationRule]:
        """Get a validation rule by ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM validation_rules WHERE id = ?", (rule_id,))
        row = cursor.fetchone()
        conn.close()
        
        return self._row_to_rule(row) if row else None
    
    def get_rule_by_name(self, name: str) -> Optional[ValidationRule]:
        """Get a validation rule by name"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM validation_rules WHERE name = ?", (name,))
        row = cursor.fetchone()
        conn.close()
        
        return self._row_to_rule(row) if row else None
    
    def get_all_active_rules(self) -> List[ValidationRule]:
        """Get all active validation rules"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM validation_rules WHERE is_active = TRUE")
        rows = cursor.fetchall()
        conn.close()
        
        return [self._row_to_rule(row) for row in rows]

    def get_rules(self, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Get validation rules with optional filters (for API compatibility)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query with filters
        conditions = []
        params = []
        
        if filters:
            if 'rule_type' in filters:
                conditions.append("rule_type = ?")
                params.append(filters['rule_type'])
            
            if 'severity' in filters:
                conditions.append("severity = ?")
                params.append(filters['severity'])
            
            if 'is_active' in filters:
                conditions.append("is_active = ?")
                params.append(bool(filters['is_active']))
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        cursor.execute(f"SELECT * FROM validation_rules WHERE {where_clause} ORDER BY created_at DESC", params)
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "rule_type": row[3],
                "severity": row[4],
                "rule_definition": json.loads(row[5]),
                "is_active": bool(row[6]),
                "created_at": row[7],
                "created_by": row[8],
                "domain": row[9],
                "tags": json.loads(row[10] or "[]")
            }
            for row in rows
        ]

    def create_rule(self, name: str, description: str, rule_type: str, conditions: Dict[str, Any], 
                   severity: str = "medium", is_active: bool = True, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create a new validation rule (for API compatibility)"""
        try:
            from common.advanced_validation import ValidationRuleType, ValidationSeverity
            
            rule_id = self.create_validation_rule(
                name=name,
                description=description,
                rule_type=ValidationRuleType(rule_type),
                severity=ValidationSeverity(severity),
                rule_definition=conditions,
                domain=metadata.get('domain') if metadata else None,
                tags=metadata.get('tags', []) if metadata else []
            )
            
            return {"status": "success", "rule_id": rule_id}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def update_rule(self, rule_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update a validation rule (for API compatibility)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if rule exists
            cursor.execute("SELECT * FROM validation_rules WHERE id = ?", (rule_id,))
            if not cursor.fetchone():
                conn.close()
                return {"status": "error", "message": "Rule not found"}
            
            # Build update query
            update_fields = []
            params = []
            
            if 'name' in updates:
                update_fields.append("name = ?")
                params.append(updates['name'])
            
            if 'description' in updates:
                update_fields.append("description = ?")
                params.append(updates['description'])
            
            if 'is_active' in updates:
                update_fields.append("is_active = ?")
                params.append(bool(updates['is_active']))
            
            if 'severity' in updates:
                update_fields.append("severity = ?")
                params.append(updates['severity'])
            
            if update_fields:
                params.append(rule_id)
                cursor.execute(f"UPDATE validation_rules SET {', '.join(update_fields)} WHERE id = ?", params)
                conn.commit()
            
            conn.close()
            return {"status": "success"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def delete_rule(self, rule_id: str) -> Dict[str, Any]:
        """Delete a validation rule (for API compatibility)"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if rule exists
            cursor.execute("SELECT * FROM validation_rules WHERE id = ?", (rule_id,))
            if not cursor.fetchone():
                conn.close()
                return {"status": "error", "message": "Rule not found"}
            
            # Delete the rule
            cursor.execute("DELETE FROM validation_rules WHERE id = ?", (rule_id,))
            conn.commit()
            conn.close()
            
            return {"status": "success"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def validate_data(self, data: Any, rule_types: List[str] = None, job_id: str = None) -> List[Dict[str, Any]]:
        """Validate data against rules (for API compatibility)"""
        try:
            if isinstance(data, list):
                # Batch validation
                all_results = []
                for item in data:
                    results = self.validate_single_item(item)
                    all_results.extend(results)
                return [self._result_to_dict(r) for r in all_results]
            else:
                # Single item validation
                results = self.validate_single_item(data)
                return [self._result_to_dict(r) for r in results]
        except Exception as e:
            return [{"error": str(e)}]

    def _result_to_dict(self, result: ValidationResult) -> Dict[str, Any]:
        """Convert ValidationResult to dictionary"""
        return {
            "id": result.id,
            "rule_id": result.rule_id,
            "entity_id": result.entity_id,
            "entity_type": result.entity_type,
            "status": result.status.value,
            "message": result.message,
            "details": result.details,
            "severity": result.severity.value,
            "validated_at": result.validated_at,
            "execution_time_ms": result.execution_time_ms
        }
    
    def _update_label_validation_rules(self, rules: List[ValidationRule], 
                                     available_labels: List[str]):
        """Update label validation rules with job-specific allowed values"""
        for rule in rules:
            if rule.rule_type == ValidationRuleType.LABEL_CONSISTENCY:
                if "allowed_values" in rule.rule_definition:
                    rule.rule_definition["allowed_values"] = available_labels
    
    def _store_validation_result(self, result: ValidationResult):
        """Store validation result in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO validation_results 
            (id, rule_id, entity_id, entity_type, status, message, details,
             severity, validated_at, execution_time_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (result.id, result.rule_id, result.entity_id, result.entity_type,
              result.status.value, result.message, json.dumps(result.details),
              result.severity.value, result.validated_at, result.execution_time_ms))
        
        conn.commit()
        conn.close()
    
    def _store_validation_report(self, report: ValidationReport):
        """Store validation report in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO validation_reports 
            (id, job_id, total_items, validated_items, passed_count, failed_count,
             warning_count, error_count, created_at, execution_time_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (report.id, report.job_id, report.total_items, report.validated_items,
              report.passed_count, report.failed_count, report.warning_count,
              report.error_count, report.created_at, report.execution_time_ms))
        
        conn.commit()
        conn.close()
    
    def _update_validation_metrics(self, rules: List[ValidationRule], 
                                 results: List[ValidationResult]):
        """Update validation metrics"""
        # Group results by rule
        rule_results = {}
        for result in results:
            if result.rule_id not in rule_results:
                rule_results[result.rule_id] = []
            rule_results[result.rule_id].append(result)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().date().isoformat()
        
        for rule_id, rule_results_list in rule_results.items():
            execution_count = len(rule_results_list)
            success_count = len([r for r in rule_results_list if r.status == ValidationStatus.PASSED])
            success_rate = success_count / execution_count if execution_count > 0 else 0
            avg_execution_time = sum(r.execution_time_ms for r in rule_results_list) / execution_count
            
            # Update or insert metrics
            cursor.execute("""
                INSERT OR REPLACE INTO validation_metrics 
                (rule_id, date, execution_count, success_rate, avg_execution_time_ms)
                VALUES (?, ?, ?, ?, ?)
            """, (rule_id, today, execution_count, success_rate, avg_execution_time))
        
        conn.commit()
        conn.close()
    
    def _row_to_rule(self, row) -> ValidationRule:
        """Convert database row to ValidationRule"""
        return ValidationRule(
            id=row[0], name=row[1], description=row[2],
            rule_type=ValidationRuleType(row[3]), severity=ValidationSeverity(row[4]),
            rule_definition=json.loads(row[5]), is_active=bool(row[6]),
            created_at=row[7], created_by=row[8], domain=row[9],
            tags=json.loads(row[10] or "[]")
        )
    
    def _row_to_result(self, row) -> ValidationResult:
        """Convert database row to ValidationResult"""
        return ValidationResult(
            id=row[0], rule_id=row[1], entity_id=row[2], entity_type=row[3],
            status=ValidationStatus(row[4]), message=row[5], details=json.loads(row[6]),
            severity=ValidationSeverity(row[7]), validated_at=row[8], execution_time_ms=row[9]
        )

# Global validation system instance
validation_system = AdvancedValidationSystem()
