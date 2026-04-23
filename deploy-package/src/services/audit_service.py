"""
Audit Service
Handles audit logging with JSON-based change tracking for all entities.
Uses parent_record_types for polymorphic entity references.
"""

import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from flask import request as flask_request, has_request_context

logger = logging.getLogger(__name__)


class AuditService:
    """
    Service for creating and managing audit logs.
    
    Audit JSON format:
    {
        "field_name": {
            "old": <old_value>,  # For UPDATE/DELETE
            "new": <new_value>   # For INSERT/UPDATE
        }
    }
    """
    
    # Parent record type mappings (must match database)
    PARENT_RECORD_TYPES = {
        'request': 1,
        'email': 2,
        'document': 3,
        'request_field': 4,
        'job': 5,
        'template': 6,
        'template_field': 7,
        'user': 8,
        'annotation': 9,
        'analysis_run': 10,
    }
    
    def __init__(self, db_repository=None):
        self._db = db_repository
    
    @property
    def db(self):
        if self._db is None:
            from src.repositories import get_database_repository
            self._db = get_database_repository()
        return self._db
    
    def get_parent_record_type_id(self, entity_type: str) -> int:
        """Get the parent_record_type_id for an entity type"""
        return self.PARENT_RECORD_TYPES.get(entity_type.lower())
    
    def _get_request_metadata(self) -> Dict[str, Any]:
        """Extract request metadata for audit trail"""
        metadata = {
            'ip_address': None,
            'user_agent': None,
            'request_trace_id': None
        }
        
        if has_request_context():
            try:
                # Get IP address (handle proxies)
                metadata['ip_address'] = (
                    flask_request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or
                    flask_request.headers.get('X-Real-IP') or
                    flask_request.remote_addr
                )
                metadata['user_agent'] = flask_request.headers.get('User-Agent', '')[:500]
                metadata['request_trace_id'] = (
                    flask_request.headers.get('X-Request-ID') or
                    flask_request.headers.get('X-Trace-ID') or
                    flask_request.headers.get('X-Correlation-ID')
                )
            except Exception as e:
                logger.warning(f"Could not extract request metadata: {e}")
        
        return metadata
    
    def build_audit_json(self, action: str, old_values: Dict[str, Any] = None, 
                        new_values: Dict[str, Any] = None,
                        fields_to_track: List[str] = None) -> Dict[str, Any]:
        """
        Build the audit JSON showing old/new values for each changed field.
        
        Args:
            action: 'INSERT', 'UPDATE', or 'DELETE'
            old_values: Dict of field names to their old values
            new_values: Dict of field names to their new values
            fields_to_track: Optional list of specific fields to track
            
        Returns:
            Dict in format: { "field_name": { "old": value, "new": value } }
        """
        audit_json = {}
        old_values = old_values or {}
        new_values = new_values or {}
        
        # Determine which fields to track
        if fields_to_track:
            all_fields = set(fields_to_track)
        else:
            all_fields = set(old_values.keys()) | set(new_values.keys())
        
        for field in all_fields:
            old_val = old_values.get(field)
            new_val = new_values.get(field)
            
            if action == 'INSERT':
                # Only include new values for inserts
                if new_val is not None:
                    audit_json[field] = {'new': self._serialize_value(new_val)}
            elif action == 'DELETE':
                # Only include old values for deletes
                if old_val is not None:
                    audit_json[field] = {'old': self._serialize_value(old_val)}
            elif action == 'UPDATE':
                # Include both for updates, but only if changed
                if old_val != new_val:
                    entry = {}
                    if old_val is not None:
                        entry['old'] = self._serialize_value(old_val)
                    if new_val is not None:
                        entry['new'] = self._serialize_value(new_val)
                    if entry:
                        audit_json[field] = entry
        
        return audit_json
    
    def _serialize_value(self, value: Any) -> Any:
        """Serialize a value for JSON storage"""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, (dict, list)):
            return value
        # Convert to string for complex types
        try:
            json.dumps(value)
            return value
        except (TypeError, ValueError):
            return str(value)
    
    def create_audit_log(self, entity_type: str, entity_id: Any, action: str,
                        audit_json: Dict[str, Any], created_by: str,
                        reason: str = None) -> Optional[int]:
        """
        Create an audit log entry.
        
        Args:
            entity_type: Type of entity (e.g., 'request_field', 'request')
            entity_id: Primary key of the entity
            action: 'INSERT', 'UPDATE', or 'DELETE'
            audit_json: Dict of changes in format { "field": { "old": x, "new": y } }
            created_by: User ID who made the change
            reason: Optional reason for the change
            
        Returns:
            The audit log ID if created, None otherwise
        """
        parent_record_type_id = self.get_parent_record_type_id(entity_type)
        if not parent_record_type_id:
            logger.warning(f"Unknown entity type for audit: {entity_type}")
            return None
        
        # Get request metadata
        metadata = self._get_request_metadata()
        
        try:
            audit_log_id = self.db.create_audit_log_v2(
                parent_record_type_id=parent_record_type_id,
                entity_id=str(entity_id),
                action=action,
                audit_json=audit_json,
                created_by=created_by,
                reason=reason,
                ip_address=metadata.get('ip_address'),
                user_agent=metadata.get('user_agent'),
                request_trace_id=metadata.get('request_trace_id')
            )
            logger.debug(f"Created audit log {audit_log_id} for {entity_type}:{entity_id}")
            return audit_log_id
        except Exception as e:
            logger.error(f"Failed to create audit log: {e}")
            return None
    
    def log_insert(self, entity_type: str, entity_id: Any, 
                  new_values: Dict[str, Any], created_by: str,
                  reason: str = None, fields_to_track: List[str] = None) -> Optional[int]:
        """Log an INSERT operation"""
        audit_json = self.build_audit_json('INSERT', new_values=new_values, 
                                           fields_to_track=fields_to_track)
        return self.create_audit_log(entity_type, entity_id, 'INSERT', 
                                    audit_json, created_by, reason)
    
    def log_update(self, entity_type: str, entity_id: Any,
                  old_values: Dict[str, Any], new_values: Dict[str, Any],
                  created_by: str, reason: str = None,
                  fields_to_track: List[str] = None) -> Optional[int]:
        """Log an UPDATE operation"""
        audit_json = self.build_audit_json('UPDATE', old_values=old_values,
                                           new_values=new_values,
                                           fields_to_track=fields_to_track)
        if not audit_json:
            # No changes detected
            return None
        return self.create_audit_log(entity_type, entity_id, 'UPDATE',
                                    audit_json, created_by, reason)
    
    def log_delete(self, entity_type: str, entity_id: Any,
                  old_values: Dict[str, Any], created_by: str,
                  reason: str = None, fields_to_track: List[str] = None) -> Optional[int]:
        """Log a DELETE operation"""
        audit_json = self.build_audit_json('DELETE', old_values=old_values,
                                           fields_to_track=fields_to_track)
        return self.create_audit_log(entity_type, entity_id, 'DELETE',
                                    audit_json, created_by, reason)
    
    def log_request_field_changes(self, request_id: Any, version_id: Any,
                                  old_fields: List[Dict], new_fields: List[Dict],
                                  created_by: str, reason: str = None) -> List[int]:
        """
        Log changes to request fields (batch operation).
        Compares old and new field lists and creates audit logs for changes.
        
        Args:
            request_id: The request ID
            version_id: The version ID
            old_fields: List of field dicts before changes
            new_fields: List of field dicts after changes
            created_by: User who made the changes
            reason: Optional reason for changes
            
        Returns:
            List of created audit log IDs
        """
        audit_log_ids = []
        
        # Build lookup by field_id and field_name
        old_by_id = {f.get('id'): f for f in old_fields if f.get('id')}
        old_by_name = {f.get('field_name'): f for f in old_fields}
        new_by_id = {f.get('id'): f for f in new_fields if f.get('id')}
        new_by_name = {f.get('field_name'): f for f in new_fields}
        
        logger.info(f"Audit service - old_by_id keys: {list(old_by_id.keys())[:5]}")
        logger.info(f"Audit service - new_by_id keys: {list(new_by_id.keys())[:5]}")
        
        # Check for updates to existing fields
        for field_id, new_field in new_by_id.items():
            old_field = old_by_id.get(field_id)
            if old_field:
                # Get the actual field name for display
                field_name = new_field.get('field_name') or old_field.get('field_name') or f'Field {field_id}'
                
                logger.info(f"Comparing field {field_id} ({field_name}): old={old_field.get('field_value')}, new={new_field.get('field_value')}")
                
                # Check if value changed
                old_value = old_field.get('field_value')
                new_value = new_field.get('field_value')
                
                if old_value != new_value:
                    # Build audit JSON with actual field name as key
                    audit_json = {
                        field_name: {
                            'old': old_value,
                            'new': new_value
                        }
                    }
                    
                    log_id = self.create_audit_log(
                        'request_field', field_id, 'UPDATE',
                        audit_json, created_by, reason
                    )
                    if log_id:
                        audit_log_ids.append(log_id)
        
        # Check for new fields (in new but not in old - by name for new versions)
        for field_name, new_field in new_by_name.items():
            if field_name not in old_by_name and not new_field.get('id'):
                # Truly new field
                audit_json = {
                    field_name: {
                        'new': new_field.get('field_value')
                    }
                }
                # For new fields, we'll use a temporary ID or field_name as reference
                entity_ref = new_field.get('id') or f"{request_id}:{field_name}"
                log_id = self.create_audit_log(
                    'request_field', entity_ref, 'INSERT',
                    audit_json, created_by, reason
                )
                if log_id:
                    audit_log_ids.append(log_id)
        
        # Check for deleted fields
        for field_id, old_field in old_by_id.items():
            if field_id not in new_by_id:
                # Field was deleted
                field_name = old_field.get('field_name') or f'Field {field_id}'
                audit_json = {
                    field_name: {
                        'old': old_field.get('field_value')
                    }
                }
                log_id = self.create_audit_log(
                    'request_field', field_id, 'DELETE',
                    audit_json, created_by, reason
                )
                if log_id:
                    audit_log_ids.append(log_id)
        
        return audit_log_ids
    
    def get_audit_history(self, entity_type: str, entity_id: Any,
                         limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get audit history for an entity.
        
        Returns list of audit logs with parsed JSON fields.
        """
        parent_record_type_id = self.get_parent_record_type_id(entity_type)
        if not parent_record_type_id:
            return []
        
        return self.db.get_audit_logs_v2(parent_record_type_id, str(entity_id), limit)
    
    def get_field_change_summary(self, audit_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert audit_json to a user-friendly change summary.
        
        Returns:
            List of { field, old_value, new_value, display_label }
        """
        changes = []
        
        # Friendly display names for fields
        display_names = {
            'requestfield_field_name': 'Field Name',
            'requestfield_field_value': 'Value',
            'requestfield_extracted_value': 'Extracted Value',
            'requestfield_is_selected': 'Selected',
            'requestfield_is_active': 'Active',
            'requestfield_confidence': 'Confidence',
            'requestfield_is_manually_edited': 'Manually Edited'
        }
        
        for field, change in audit_json.items():
            changes.append({
                'field': field,
                'display_label': display_names.get(field, field.replace('requestfield_', '').replace('_', ' ').title()),
                'old_value': change.get('old'),
                'new_value': change.get('new')
            })
        
        return changes


# Singleton instance
_audit_service = None


def get_audit_service() -> AuditService:
    """Get the audit service singleton"""
    global _audit_service
    if _audit_service is None:
        _audit_service = AuditService()
    return _audit_service
