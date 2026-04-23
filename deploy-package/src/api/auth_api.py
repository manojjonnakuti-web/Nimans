"""
Auth API
Provides authentication endpoints for frontend
"""

from flask import Blueprint, request, jsonify, g
import logging

from src.auth import require_auth, ensure_user_exists
from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')


@auth_bp.route('/me', methods=['GET'])
@require_auth
def get_current_user():
    """
    Get current user info and sync to database
    Returns user profile information including subscription details
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    
    # Get organization info
    org = db.get_organization(user['organization_id'])
    
    # Get subscription info
    subscription = db.get_active_subscription(user['organization_id'])
    
    sub_info = None
    if subscription:
        sub_info = {
            'plan': subscription['plan'],
            'status': subscription['status'],
            'started_at': subscription.get('started_at'),
            'expires_at': subscription.get('expires_at'),
        }
    else:
        # No subscription record — default to free_trial for display
        sub_info = {
            'plan': 'free_trial',
            'status': 'active',
            'started_at': None,
            'expires_at': None,
        }
    
    return jsonify({
        'id': user['id'],
        'email': user['email'],
        'username': user['username'],
        'role': user['role'],
        'organization_id': user['organization_id'],
        'organization_name': org['name'] if org else None,
        'is_active': user['is_active'],
        'subscription': sub_info,
    })


@auth_bp.route('/subscription', methods=['GET'])
@require_auth
def get_subscription():
    """
    Get detailed subscription info for the current user's organization
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    
    org = db.get_organization(user['organization_id'])
    subscription = db.get_active_subscription(user['organization_id'])
    
    if subscription:
        return jsonify({
            'subscription_id': subscription['subscription_id'],
            'plan': subscription['plan'],
            'status': subscription['status'],
            'marketplace_id': subscription.get('marketplace_id'),
            'started_at': subscription.get('started_at'),
            'expires_at': subscription.get('expires_at'),
            'cancelled_at': subscription.get('cancelled_at'),
            'created_at': subscription.get('created_at'),
            'organization': {
                'id': org['organization_id'] if org else user['organization_id'],
                'name': org['name'] if org else None,
                'tier': org['tier'] if org else 'free_trial',
            }
        })
    
    return jsonify({
        'subscription_id': None,
        'plan': 'free_trial',
        'status': 'active',
        'marketplace_id': None,
        'started_at': None,
        'expires_at': None,
        'cancelled_at': None,
        'created_at': None,
        'organization': {
            'id': org['organization_id'] if org else user['organization_id'],
            'name': org['name'] if org else None,
            'tier': org['tier'] if org else 'free_trial',
        }
    })


@auth_bp.route('/logout', methods=['POST'])
@require_auth
def logout():
    """
    Server-side logout cleanup
    For Auth0, most logout is client-side, but this can be used for cleanup
    """
    # No server-side state to clear with JWT auth
    # Could add audit logging here if needed
    return jsonify({'success': True, 'message': 'Logged out successfully'})


@auth_bp.route('/profile', methods=['PUT'])
@require_auth
def update_profile():
    """Update user profile"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    
    data = request.get_json() or {}
    
    # Only allow updating certain fields
    allowed_fields = ['username']
    updates = {k: v for k, v in data.items() if k in allowed_fields}
    
    if updates:
        success = db.update_user(user['id'], **updates)
        if success:
            user = db.get_user(user['id'])
    
    return jsonify({
        'id': user['id'],
        'email': user['email'],
        'username': user['username'],
        'role': user['role'],
        'organization_id': user['organization_id']
    })
