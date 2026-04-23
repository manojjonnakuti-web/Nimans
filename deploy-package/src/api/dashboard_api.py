"""
Dashboard API
Provides metrics and statistics for the dashboard
Uses synchronous database calls for Flask compatibility
"""

from flask import Blueprint, request, jsonify, g, Response
import logging
import csv
import io
import os
from datetime import datetime

from src.auth import require_auth, ensure_user_exists, subscription_before_request
from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')
dashboard_bp.before_request(subscription_before_request)


@dashboard_bp.route('/stats', methods=['GET'])
@require_auth
def get_stats():
    """Get overall dashboard statistics, optionally filtered by date range"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    
    stats = db.get_dashboard_stats(user['organization_id'], date_from=date_from, date_to=date_to)
    
    return jsonify(stats)


@dashboard_bp.route('/recent-requests', methods=['GET'])
@require_auth
def get_recent_requests():
    """Get recently updated requests"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    limit = request.args.get('limit', 10, type=int)
    requests_list = db.get_recent_requests(user['organization_id'], limit=limit)
    
    return jsonify({
        'requests': requests_list
    })


@dashboard_bp.route('/recent-emails', methods=['GET'])
@require_auth
def get_recent_emails():
    """Get recently ingested emails"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    limit = request.args.get('limit', 10, type=int)
    emails = db.get_recent_emails(user['organization_id'], limit=limit)
    
    return jsonify({
        'emails': emails
    })


@dashboard_bp.route('/pending-review', methods=['GET'])
@require_auth
def get_pending_review():
    """Get requests pending review"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    limit = request.args.get('limit', 10, type=int)
    requests_list = db.get_pending_review_requests(user['organization_id'], limit=limit)
    
    return jsonify({
        'requests': requests_list
    })


@dashboard_bp.route('/processing', methods=['GET'])
@require_auth
def get_processing():
    """Get currently processing requests"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    limit = request.args.get('limit', 10, type=int)
    requests_list = db.get_processing_requests(user['organization_id'], limit=limit)
    
    return jsonify({
        'requests': requests_list
    })


@dashboard_bp.route('/activity', methods=['GET'])
@require_auth
def get_activity():
    """Get recent activity feed from real data (publishes, uploads, edits, extractions)"""
    try:
        db = get_database_repository()
        user = ensure_user_exists(db, g.current_user)
        db = g.tenant_db  # Route to tenant database
        
        limit = request.args.get('limit', 8, type=int)
        feed = db.get_activity_feed(user['organization_id'], limit=limit)
        
        return jsonify({
            'activity': feed
        })
    except Exception as e:
        logger.error(f"Error in get_activity: {str(e)}", exc_info=True)
        return jsonify({
            'error': 'Failed to fetch activity feed',
            'details': str(e)
        }), 500


@dashboard_bp.route('/activity/all', methods=['GET'])
@require_auth
def get_activity_paginated():
    """Get paginated, searchable activity feed"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 20, type=int)
    search = request.args.get('search', None, type=str)
    activity_type = request.args.get('type', None, type=str)

    result = db.get_activity_feed_paginated(
        user['organization_id'],
        page=page,
        page_size=page_size,
        search=search,
        activity_type=activity_type,
    )

    return jsonify(result)


@dashboard_bp.route('/requests-by-status', methods=['GET'])
@require_auth
def get_requests_by_status():
    """Get request counts by status for charts"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    stats = db.get_dashboard_stats(user['organization_id'])
    
    return jsonify({
        'data': [
            {'status': 'Pending', 'count': stats['requests']['pending']},
            {'status': 'Processing', 'count': stats['requests']['processing']},
            {'status': 'Reviewing', 'count': stats['requests']['reviewing']},
            {'status': 'Completed', 'count': stats['requests']['completed']}
        ]
    })


@dashboard_bp.route('/top-issuers', methods=['GET'])
@require_auth
def get_top_issuers():
    """Get top issuers for dashboard widget"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    limit = request.args.get('limit', 4, type=int)
    issuers = db.get_top_issuers(user['organization_id'], limit=limit)
    return jsonify({'issuers': issuers})


@dashboard_bp.route('/issuers/all', methods=['GET'])
@require_auth
def get_issuers_paginated():
    """Get paginated, searchable list of all issuers"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 20, type=int)
    search = request.args.get('search', None, type=str)
    result = db.get_issuers_paginated(
        user['organization_id'],
        page=page,
        page_size=page_size,
        search=search,
    )
    return jsonify(result)


@dashboard_bp.route('/system-health', methods=['GET'])
@require_auth
def get_system_health():
    """Get real-time system health metrics"""
    db = get_database_repository()
    ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    health = db.get_system_health()

    # Azure Storage check
    try:
        from src.services.storage_service import StorageService
        storage = StorageService()
        if storage.is_available():
            container = storage.blob_service_client.get_container_client(storage.container_name)
            container.get_container_properties()
            health['storage'] = {'status': 'connected'}
        else:
            health['storage'] = {'status': 'not_configured'}
    except Exception as e:
        logger.warning(f"Health: storage check failed: {e}")
        health['storage'] = {'status': 'disconnected'}

    # Azure AI check
    try:
        from src.services.azure_service import AzureContentUnderstandingClient
        ai = AzureContentUnderstandingClient()
        health['ai_service'] = {'status': 'configured' if ai.is_available() else 'not_configured'}
    except Exception as e:
        logger.warning(f"Health: AI check failed: {e}")
        health['ai_service'] = {'status': 'unknown'}

    # Background worker
    health['worker'] = {
        'enabled': os.getenv('ENABLE_BACKGROUND_WORKER', 'false').lower() == 'true'
    }

    return jsonify(health)


@dashboard_bp.route('/job-stats', methods=['GET'])
@require_auth
def get_job_stats():
    """Get job statistics for dashboard"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    stats = db.get_job_stats(org_id=user['organization_id'])
    
    return jsonify(stats)


@dashboard_bp.route('/export-report', methods=['GET'])
@require_auth
def export_report():
    """
    Export a monthly report as CSV with all statuses.
    Returns rows grouped by month with counts for each status.
    Optional query params: date_from, date_to (YYYY-MM-DD)
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    org_id = user['organization_id']

    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')

    # Fetch ALL requests (no pagination) for this org, optionally filtered by date
    all_requests, _ = db.list_requests(
        org_id=org_id,
        date_from=date_from,
        date_to=date_to,
        page=1,
        per_page=100000  # effectively no limit
    )

    # Group by month and status
    monthly_data = {}
    all_statuses = set()

    for req in all_requests:
        created = req.get('created_at')
        status = req.get('status', 'unknown')
        all_statuses.add(status)

        if created:
            try:
                dt = datetime.fromisoformat(created)
                month_key = dt.strftime('%Y-%m')
            except (ValueError, TypeError):
                month_key = 'Unknown'
        else:
            month_key = 'Unknown'

        if month_key not in monthly_data:
            monthly_data[month_key] = {}
        monthly_data[month_key][status] = monthly_data[month_key].get(status, 0) + 1

    # Sort statuses and months
    status_list = sorted(all_statuses)
    sorted_months = sorted(monthly_data.keys())

    fmt = request.args.get('format', 'csv')

    if fmt == 'json':
        # Return JSON for frontend table rendering
        rows = []
        for month in sorted_months:
            row = {'month': month}
            total = 0
            for s in status_list:
                count = monthly_data[month].get(s, 0)
                row[s] = count
                total += count
            row['total'] = total
            rows.append(row)
        return jsonify({'months': rows, 'statuses': status_list})

    # Default: CSV download
    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    header = ['Month'] + [s.capitalize() for s in status_list] + ['Total']
    writer.writerow(header)

    grand_totals = {s: 0 for s in status_list}
    grand_total_all = 0

    for month in sorted_months:
        row = [month]
        month_total = 0
        for s in status_list:
            count = monthly_data[month].get(s, 0)
            row.append(count)
            month_total += count
            grand_totals[s] += count
        row.append(month_total)
        grand_total_all += month_total
        writer.writerow(row)

    # Grand total row
    totals_row = ['Grand Total'] + [grand_totals[s] for s in status_list] + [grand_total_all]
    writer.writerow(totals_row)

    csv_content = output.getvalue()
    output.close()

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f'monthly_report_{timestamp}.csv'

    return Response(
        csv_content,
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )
