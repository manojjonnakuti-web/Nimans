"""
Metering API
Endpoints for viewing and reporting metered usage to Azure Marketplace.

Endpoints:
  GET  /api/metering/usage           — View usage summary (current org)
  POST /api/metering/report          — Trigger reporting of unreported usage to Marketplace
  GET  /api/metering/unreported      — View unreported usage awaiting submission
  GET  /api/metering/status          — Check metering service configuration status
"""

import json
import logging
from datetime import datetime

from flask import Blueprint, request, jsonify, g

from src.auth import require_auth, ensure_user_exists, subscription_before_request
from src.repositories import get_database_repository
from src.services.marketplace_metering_service import get_marketplace_metering_service

logger = logging.getLogger(__name__)

metering_bp = Blueprint('metering', __name__, url_prefix='/api/metering')
metering_bp.before_request(subscription_before_request)


@metering_bp.route('/status', methods=['GET'])
@require_auth
def metering_status():
    """Check metering service configuration and readiness."""
    service = get_marketplace_metering_service()
    return jsonify({
        'configured': service.is_configured(),
        'enabled': service.is_enabled(),
        'dimensions': ['pages_processed', 'fields_normalised'],
        'message': (
            'Metering is active — usage will be reported to Azure Marketplace.'
            if service.is_enabled()
            else 'Metering is in dry-run mode. Set MARKETPLACE_METERING_ENABLED=true to activate.'
        ),
    })


@metering_bp.route('/usage', methods=['GET'])
@require_auth
def get_usage():
    """Get metered usage summary for the current user's organization.
    
    Query params:
        period_start — Optional ISO date to filter from (e.g. 2026-01-01)
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    org_id = user['organization_id']

    period_start = None
    if request.args.get('period_start'):
        try:
            period_start = datetime.fromisoformat(request.args['period_start'])
        except ValueError:
            return jsonify({'error': 'Invalid period_start format. Use ISO 8601.'}), 400

    summary = db.get_usage_summary(organization_id=org_id, period_start=period_start)

    return jsonify({
        'organization_id': org_id,
        'usage': summary,
    })


@metering_bp.route('/unreported', methods=['GET'])
@require_auth
def get_unreported():
    """Get unreported usage awaiting submission to Marketplace."""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    org_id = user['organization_id']

    unreported = db.get_unreported_usage(organization_id=org_id)

    return jsonify({
        'organization_id': org_id,
        'unreported': unreported,
    })


@metering_bp.route('/report', methods=['POST'])
@require_auth
def report_usage():
    """Trigger reporting of all unreported usage to Azure Marketplace.
    
    This aggregates unreported usage per dimension and submits it to the
    Marketplace Metering API. Each successful submission marks the
    underlying records as reported.

    In dry-run mode (default), this shows what would be reported without
    calling the real API.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    org_id = user['organization_id']
    service = get_marketplace_metering_service()

    unreported = db.get_unreported_usage(organization_id=org_id)
    if not unreported:
        return jsonify({
            'message': 'No unreported usage to submit.',
            'reported': [],
        })

    results = []
    for usage_row in unreported:
        # Resolve the subscription's marketplace_id and plan
        sub = db.get_active_subscription_for_org(usage_row['organization_id'])
        if not sub:
            results.append({
                'dimension': usage_row['dimension'],
                'quantity': usage_row['total_quantity'],
                'status': 'skipped',
                'reason': 'No active subscription found',
            })
            continue

        marketplace_id = sub.get('marketplace_id')
        plan_id = sub.get('plan', 'enterprise')

        if not marketplace_id:
            results.append({
                'dimension': usage_row['dimension'],
                'quantity': usage_row['total_quantity'],
                'status': 'skipped',
                'reason': 'Subscription has no marketplace_id — not a Marketplace subscription',
            })
            continue

        try:
            api_result = service.report_usage(
                subscription_marketplace_id=marketplace_id,
                plan_id=plan_id,
                dimension=usage_row['dimension'],
                quantity=usage_row['total_quantity'],
            )

            # Mark as reported in DB
            period_start = datetime.fromisoformat(usage_row['period_start'])
            period_end = datetime.fromisoformat(usage_row['period_end'])
            marked = db.mark_usage_reported(
                organization_id=usage_row['organization_id'],
                subscription_id=usage_row['subscription_id'],
                dimension=usage_row['dimension'],
                period_start=period_start,
                period_end=period_end,
                marketplace_response=json.dumps(api_result),
            )

            results.append({
                'dimension': usage_row['dimension'],
                'quantity': usage_row['total_quantity'],
                'period_start': usage_row['period_start'],
                'period_end': usage_row['period_end'],
                'status': api_result.get('status', 'reported'),
                'records_marked': marked,
                'api_response': api_result,
            })

        except Exception as e:
            logger.error(f"Failed to report usage for {usage_row['dimension']}: {e}")
            results.append({
                'dimension': usage_row['dimension'],
                'quantity': usage_row['total_quantity'],
                'status': 'error',
                'error': str(e),
            })

    return jsonify({
        'organization_id': org_id,
        'metering_enabled': service.is_enabled(),
        'reported': results,
    })
