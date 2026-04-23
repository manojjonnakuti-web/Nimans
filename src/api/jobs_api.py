"""
Jobs API
Handles async job management and status tracking
"""

from flask import Blueprint, request, jsonify, g
import logging

from src.auth import require_auth, ensure_user_exists, subscription_before_request
from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

jobs_bp = Blueprint('jobs', __name__, url_prefix='/api/jobs')
jobs_bp.before_request(subscription_before_request)


@jobs_bp.route('', methods=['GET'])
@require_auth
def list_jobs():
    """List all async jobs for the current user's organization"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 25, type=int)
    status = request.args.get('status')
    entity_type = request.args.get('entity_type')
    
    jobs, total = db.list_async_jobs(
        org_id=user['organization_id'],
        entity_type=entity_type,
        status=status,
        page=page,
        per_page=per_page
    )
    
    return jsonify({
        'jobs': jobs,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page if per_page > 0 else 0
        }
    })


@jobs_bp.route('/<job_id>', methods=['GET'])
@require_auth
def get_job(job_id: str):
    """Get a specific job"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    job = db.get_async_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    if job.get('organization_id') and job['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    return jsonify(job)


@jobs_bp.route('/<job_id>/status', methods=['GET'])
@require_auth
def get_job_status(job_id: str):
    """Get job status (lightweight endpoint)"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    job = db.get_async_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    if job.get('organization_id') and job['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    return jsonify({
        'id': job['id'],
        'status': job['status'],
        'progress_percent': job['progress_percent'],
        'progress_message': job.get('progress_message'),
        'error_message': job.get('error_message'),
        'result_data': job.get('result_data')
    })


@jobs_bp.route('/<job_id>/cancel', methods=['POST'])
@require_auth
def cancel_job(job_id: str):
    """Cancel a pending or running job"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    job = db.get_async_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    if job.get('organization_id') and job['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    if job['status'] not in ['pending', 'running']:
        return jsonify({'error': 'Job cannot be cancelled'}), 400
    
    success = db.cancel_async_job(job_id, user_id=user['id'])
    
    if success:
        logger.info(f"Job {job_id} cancelled by user {user['id']}")
        return jsonify({'message': 'Job cancelled'})
    else:
        return jsonify({'error': 'Failed to cancel job'}), 500


@jobs_bp.route('/<job_id>/retry', methods=['POST'])
@require_auth
def retry_job(job_id: str):
    """Retry a failed job"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    job = db.get_async_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    if job.get('organization_id') and job['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    if job['status'] != 'failed':
        return jsonify({'error': 'Only failed jobs can be retried'}), 400
    
    new_job_id = db.retry_async_job(job_id, user_id=user['id'])
    
    if new_job_id:
        logger.info(f"Job {job_id} retried as {new_job_id} by user {user['id']}")
        return jsonify({
            'message': 'Job retried',
            'new_job_id': new_job_id
        }), 201
    else:
        return jsonify({'error': 'Failed to retry job'}), 500


@jobs_bp.route('/pending', methods=['GET'])
@require_auth
def get_pending_jobs():
    """Get pending jobs for the current user's organization"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    limit = request.args.get('limit', 10, type=int)
    jobs = db.get_pending_jobs(limit=limit, org_id=user['organization_id'])
    
    return jsonify({
        'jobs': jobs
    })


@jobs_bp.route('/<job_id>/processing-log', methods=['GET'])
@require_auth
def get_processing_log(job_id: str):
    """Get real-time processing log for a job (polled by frontend during extraction)"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    job = db.get_async_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    if job.get('organization_id') and job['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    result_data = job.get('result_data') or {}
    processing_log = result_data.get('processing_log', [])
    
    return jsonify({
        'job_id': job['id'],
        'status': job['status'],
        'progress_percent': job.get('progress_percent', 0),
        'progress_message': job.get('progress_message', ''),
        'processing_log': processing_log,
        'total_elapsed_seconds': result_data.get('total_elapsed_seconds', 0),
        'error_message': job.get('error_message'),
    })


@jobs_bp.route('/stats', methods=['GET'])
@require_auth
def get_job_stats():
    """Get job statistics for the current user's organization"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    stats = db.get_job_stats(org_id=user['organization_id'])
    
    return jsonify(stats)
