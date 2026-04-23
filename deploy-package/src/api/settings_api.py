"""
Settings API
Endpoints for organization-level settings management.
"""
import json
import logging
from flask import Blueprint, request, jsonify, g
from src.auth import require_auth, ensure_user_exists, require_role
from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

settings_bp = Blueprint('settings', __name__, url_prefix='/api/settings')


@settings_bp.route('/org', methods=['GET'])
@require_auth
def get_org_settings():
    """Get organization settings. Any authenticated user can read."""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db_tenant = g.tenant_db
    org = db.get_organization(user['organization_id'])
    if not org:
        return jsonify({'error': 'Organization not found'}), 404
    return jsonify({'settings': org.get('settings', {})})


@settings_bp.route('/org', methods=['PUT'])
@require_auth
@require_role('admin')
def update_org_settings():
    """Update organization settings. Admin only."""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    data = request.get_json() or {}

    org = db.get_organization(user['organization_id'])
    if not org:
        return jsonify({'error': 'Organization not found'}), 404

    current_settings = org.get('settings', {})

    # Only allow updating specific known keys
    if 'allow_reprocessing' in data:
        current_settings['allow_reprocessing'] = bool(data['allow_reprocessing'])

    db.update_organization_settings(user['organization_id'], current_settings)
    logger.info(f"Org settings updated by {user['email']}: {current_settings}")
    return jsonify({'settings': current_settings})