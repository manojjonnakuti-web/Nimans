"""
Marketplace API
Handles Azure Marketplace SaaS Fulfillment API v2 integration:
- Landing page redirect (POST /api/marketplace/landing)
- Webhook for subscription lifecycle events (POST /api/marketplace/webhook)
"""

import os
import uuid
import logging
import requests as http_requests
from datetime import datetime

from flask import Blueprint, request, jsonify, redirect
import jwt
from jwt import PyJWKClient

from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

marketplace_bp = Blueprint('marketplace', __name__, url_prefix='/api/marketplace')

# Marketplace configuration
MARKETPLACE_CLIENT_ID = os.getenv('MARKETPLACE_CLIENT_ID', '')
MARKETPLACE_CLIENT_SECRET = os.getenv('MARKETPLACE_CLIENT_SECRET', '')
MARKETPLACE_TENANT_ID = os.getenv('MARKETPLACE_TENANT_ID', '')
FRONTEND_URL = os.getenv('FRONTEND_URL', 'http://localhost:5173')

# Azure Marketplace API base URL
MARKETPLACE_API_BASE = 'https://marketplaceapi.microsoft.com/api/saas'
MARKETPLACE_API_VERSION = '2018-08-31'


def _get_marketplace_token() -> str:
    """
    Get an access token for the Azure Marketplace API
    Uses client credentials flow with the marketplace app registration
    """
    token_url = f'https://login.microsoftonline.com/{MARKETPLACE_TENANT_ID}/oauth2/v2.0/token'
    
    response = http_requests.post(token_url, data={
        'client_id': MARKETPLACE_CLIENT_ID,
        'client_secret': MARKETPLACE_CLIENT_SECRET,
        'grant_type': 'client_credentials',
        'scope': '20e940b3-4c77-4b0b-9a53-9e16a1b010a7/.default'  # Marketplace API resource ID
    })
    
    if response.status_code != 200:
        logger.error(f"Failed to get marketplace token: {response.status_code} {response.text}")
        raise Exception("Failed to authenticate with marketplace API")
    
    return response.json()['access_token']


def _resolve_marketplace_token(marketplace_token: str) -> dict:
    """
    Resolve a marketplace purchase token to get subscription details
    POST https://marketplaceapi.microsoft.com/api/saas/subscriptions/resolve
    """
    access_token = _get_marketplace_token()
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'x-ms-marketplace-token': marketplace_token,
        'Content-Type': 'application/json',
        'api-version': MARKETPLACE_API_VERSION
    }
    
    response = http_requests.post(
        f'{MARKETPLACE_API_BASE}/subscriptions/resolve?api-version={MARKETPLACE_API_VERSION}',
        headers=headers
    )
    
    if response.status_code != 200:
        logger.error(f"Failed to resolve marketplace token: {response.status_code} {response.text}")
        return None
    
    return response.json()


def _activate_subscription(subscription_id: str, plan_id: str) -> bool:
    """
    Activate a marketplace subscription
    POST https://marketplaceapi.microsoft.com/api/saas/subscriptions/{id}/activate
    """
    access_token = _get_marketplace_token()
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    response = http_requests.post(
        f'{MARKETPLACE_API_BASE}/subscriptions/{subscription_id}/activate?api-version={MARKETPLACE_API_VERSION}',
        headers=headers,
        json={'planId': plan_id}
    )
    
    if response.status_code in [200, 202]:
        logger.info(f"Activated marketplace subscription {subscription_id} with plan {plan_id}")
        return True
    
    logger.error(f"Failed to activate subscription {subscription_id}: {response.status_code} {response.text}")
    return False


def _validate_webhook_jwt(auth_header: str) -> bool:
    """
    Validate the JWT token sent by Microsoft in webhook calls.
    The 'aud' claim should match our marketplace client ID.
    The 'tid' claim should match our marketplace tenant ID.
    """
    if not auth_header or not auth_header.startswith('Bearer '):
        return False
    
    token = auth_header[7:]
    
    try:
        # Microsoft's common JWKS endpoint
        jwks_url = 'https://login.microsoftonline.com/common/discovery/v2.0/keys'
        jwks_client = PyJWKClient(jwks_url)
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        
        decoded = jwt.decode(
            token,
            signing_key.key,
            algorithms=['RS256'],
            audience=MARKETPLACE_CLIENT_ID,
            options={'verify_exp': True}
        )
        
        logger.info(f"Webhook JWT validated. Caller tid={decoded.get('tid')}, appid={decoded.get('appid') or decoded.get('azp')}")
        return True
        
    except Exception as e:
        logger.error(f"Webhook JWT validation failed: {e}")
        return False


# ==============================================
# LANDING PAGE ENDPOINT
# ==============================================

@marketplace_bp.route('/landing', methods=['GET'])
def marketplace_landing():
    """
    Landing page endpoint - Azure Marketplace redirects here after purchase.
    
    Flow:
    1. User buys on Azure Marketplace
    2. Marketplace redirects to: {landing_url}?token={purchase_token}
    3. We resolve the token to get subscription details
    4. We create/update org + subscription in our DB
    5. We activate the subscription with Microsoft
    6. We redirect user to our frontend app
    """
    marketplace_token = request.args.get('token')
    
    if not marketplace_token:
        logger.error("Landing page called without marketplace token")
        return redirect(f'{FRONTEND_URL}/marketplace/landing?error=missing_token')
    
    try:
        # Resolve the purchase token with Marketplace API
        subscription_data = _resolve_marketplace_token(marketplace_token)
        
        if not subscription_data:
            logger.error("Failed to resolve marketplace token")
            return redirect(f'{FRONTEND_URL}/marketplace/landing?error=invalid_token')
        
        logger.info(f"Resolved marketplace subscription: {subscription_data.get('id')}")
        
        # Extract key fields
        marketplace_subscription_id = subscription_data.get('id')
        plan_id = subscription_data.get('planId', 'free_trial')
        offer_id = subscription_data.get('offerId')
        beneficiary = subscription_data.get('subscription', {}).get('beneficiary', {})
        tenant_id = beneficiary.get('tenantId')
        purchaser_email = beneficiary.get('emailId', '')
        subscription_name = subscription_data.get('subscriptionName', '')
        
        if not tenant_id:
            logger.error("No tenant ID in marketplace subscription data")
            return redirect(f'{FRONTEND_URL}/marketplace/landing?error=no_tenant')
        
        db = get_database_repository()
        
        # ── Duplicate subscription check ──
        # If this tenant already has an active subscription, block the purchase.
        # This covers: same user buying again, or a different person in the same org.
        existing_org = db.get_organization_by_tenant_id(tenant_id)
        if existing_org:
            existing_sub = db.get_active_subscription(existing_org['organization_id'])
            if existing_sub:
                logger.warning(
                    f"Duplicate subscription attempt for tenant {tenant_id} "
                    f"(org={existing_org['organization_id']}, existing_sub={existing_sub.get('id')})"
                )
                from urllib.parse import urlencode
                dup_params = {
                    'error': 'duplicate_subscription',
                    'org_name': existing_org.get('name', existing_org.get('organization_name', '')),
                    'org_id': existing_org.get('organization_id', ''),
                    'purchaser_email': purchaser_email or '',
                }
                return redirect(f'{FRONTEND_URL}/marketplace/landing?{urlencode(dup_params)}')
        
        # Find or create the organization for this tenant
        org = existing_org  # Re-use if already fetched above
        
        if not org:
            # Create new organization
            org_id = f'org_{tenant_id[:8]}'
            org = db.create_organization(
                org_id=org_id,
                name=subscription_name or f'Organization ({tenant_id[:8]})',
                azure_tenant_id=tenant_id
            )
            logger.info(f"Created organization {org_id} for marketplace tenant {tenant_id}")
        
        # Create or update subscription record
        existing_sub = db.get_subscription_by_marketplace_id(marketplace_subscription_id)
        
        if existing_sub:
            # Update existing subscription
            db.update_subscription_status(
                existing_sub['subscription_id'],
                status='active',
                plan=plan_id
            )
            logger.info(f"Updated existing subscription {existing_sub['subscription_id']} to active")
        else:
            # Create new subscription
            sub_id = f"sub_{str(uuid.uuid4())[:8]}"
            db.create_subscription(
                sub_id=sub_id,
                org_id=org['organization_id'],
                plan=plan_id,
                marketplace_id=marketplace_subscription_id
            )
            logger.info(f"Created subscription {sub_id} for org {org['organization_id']} with plan {plan_id}")
        
        # Activate the subscription with Microsoft
        _activate_subscription(marketplace_subscription_id, plan_id)
        
        # ── Auto-provision dedicated resources for enterprise plans ──
        # Runs in a background thread so the customer isn't kept waiting.
        # Free/basic plans use shared infrastructure (instant, no provisioning).
        from src.services.tenant_provisioning_service import (
            should_provision_dedicated_resources,
            provision_tenant_async,
        )
        
        provisioning_status = 'shared'  # default: uses shared infra
        if should_provision_dedicated_resources(plan_id):
            org_name_for_provisioning = org.get('name', org.get('organization_name', f'org-{tenant_id[:8]}'))
            provision_tenant_async(
                org_id=org['organization_id'],
                org_name=org_name_for_provisioning,
                plan_id=plan_id,
            )
            provisioning_status = 'provisioning'
            logger.info(f"Started async resource provisioning for org {org['organization_id']} (plan={plan_id})")
        else:
            logger.info(f"Plan '{plan_id}' uses shared infrastructure — no provisioning needed")
        
        # Build redirect URL with all subscription details for the landing page
        from urllib.parse import urlencode
        
        org_name = org.get('name', org.get('organization_name', ''))
        org_id = org.get('organization_id', '')
        sub_record = existing_sub or db.get_subscription_by_marketplace_id(marketplace_subscription_id) or {}
        
        # ── Send welcome email to the buyer ──
        try:
            from src.services.email_service import send_welcome_email, is_email_configured
            if purchaser_email:
                buyer_display = subscription_name or org_name or purchaser_email.split('@')[0]
                send_welcome_email(
                    to_email=purchaser_email,
                    buyer_name=buyer_display,
                    plan_name=plan_id,
                    org_name=org_name,
                )
                if is_email_configured():
                    logger.info(f"Welcome email sent to {purchaser_email}")
                else:
                    logger.info(f"Welcome email dry-run for {purchaser_email} (EMAIL_ENABLED=false)")
            else:
                logger.warning("No purchaser email available — welcome email skipped")
        except Exception as email_err:
            logger.warning(f"Failed to send welcome email: {email_err}")

        # ── Notify Synapx team of new purchase ──
        try:
            from src.services.email_service import send_internal_notification
            send_internal_notification(
                event_type='new_purchase',
                org_name=org_name,
                plan_name=plan_id,
                tenant_id=tenant_id,
                purchaser_email=purchaser_email,
                subscription_id=marketplace_subscription_id or '',
            )
        except Exception as notify_err:
            logger.warning(f"Failed to send internal purchase notification: {notify_err}")
        
        redirect_params = {
            'marketplace': 'activated',
            'plan': plan_id,
            'offer': offer_id or '',
            'subscription_name': subscription_name or '',
            'marketplace_subscription_id': marketplace_subscription_id or '',
            'org_name': org_name,
            'org_id': org_id,
            'tenant_id': tenant_id or '',
            'purchaser_email': purchaser_email or '',
            'subscription_id': sub_record.get('subscription_id', sub_record.get('id', '')),
            'activated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'provisioning': provisioning_status,
        }
        
        # Redirect to frontend marketplace landing page with details
        return redirect(f'{FRONTEND_URL}/marketplace/landing?{urlencode(redirect_params)}')
        
    except Exception as e:
        logger.error(f"Error processing marketplace landing: {e}", exc_info=True)
        return redirect(f'{FRONTEND_URL}/marketplace/landing?error=processing_failed')


# ==============================================
# WEBHOOK ENDPOINT
# ==============================================

@marketplace_bp.route('/webhook', methods=['POST'])
def marketplace_webhook():
    """
    Webhook endpoint for Azure Marketplace subscription lifecycle events.
    
    Microsoft sends POST with JSON payload for:
    - ChangePlan
    - ChangeQuantity
    - Suspend
    - Reinstate
    - Unsubscribe
    - Renew
    
    Must return HTTP 200 quickly to acknowledge receipt.
    """
    # Validate the webhook JWT from Microsoft
    auth_header = request.headers.get('Authorization', '')
    
    # In production, validate the JWT. In dev, allow without validation.
    if os.getenv('FLASK_ENV') != 'development':
        if not _validate_webhook_jwt(auth_header):
            logger.warning("Webhook called with invalid authorization")
            return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.get_json(silent=True) or {}
    
    action = data.get('action', '')
    subscription_id = data.get('subscriptionId', '')
    plan_id = data.get('planId', '')
    status = data.get('status', '')
    subscription_data = data.get('subscription', {})
    beneficiary = subscription_data.get('beneficiary', {})
    tenant_id = beneficiary.get('tenantId', '')
    
    logger.info(f"Marketplace webhook: action={action}, subscriptionId={subscription_id}, "
                f"planId={plan_id}, status={status}, tenantId={tenant_id}")
    
    db = get_database_repository()
    
    try:
        # Find the subscription by marketplace ID
        sub = db.get_subscription_by_marketplace_id(subscription_id)
        
        if action == 'Suspend':
            if sub:
                db.update_subscription_status(sub['id'], status='suspended')
                # Also suspend tenant resources to stop routing
                try:
                    org_id = sub.get('organization_id')
                    if org_id:
                        db.upsert_tenant_config(org_id, {'status': 'suspended'})
                        logger.info(f"Suspended tenant config for org {org_id}")
                except Exception as tc_err:
                    logger.warning(f"Failed to suspend tenant config: {tc_err}")
                logger.info(f"Suspended subscription {sub['id']}")
            else:
                logger.warning(f"Suspend: subscription not found for marketplace ID {subscription_id}")
        
        elif action == 'Reinstate':
            if sub:
                db.update_subscription_status(sub['id'], status='active')
                # Reactivate tenant resources
                try:
                    org_id = sub.get('organization_id')
                    if org_id:
                        db.upsert_tenant_config(org_id, {'status': 'active'})
                        logger.info(f"Reactivated tenant config for org {org_id}")
                except Exception as tc_err:
                    logger.warning(f"Failed to reactivate tenant config: {tc_err}")
                logger.info(f"Reinstated subscription {sub['id']}")
            else:
                logger.warning(f"Reinstate: subscription not found for marketplace ID {subscription_id}")
        
        elif action == 'Unsubscribe':
            if sub:
                db.update_subscription_status(sub['id'], status='cancelled')
                # Decommission tenant resources (data preserved, routing stopped)
                try:
                    org_id = sub.get('organization_id')
                    if org_id:
                        db.upsert_tenant_config(org_id, {'status': 'decommissioned'})
                        logger.info(f"Decommissioned tenant config for org {org_id}")
                except Exception as tc_err:
                    logger.warning(f"Failed to decommission tenant config: {tc_err}")

                # ── Notify Synapx team of cancellation ──
                try:
                    from src.services.email_service import send_internal_notification
                    # Get org details for the notification
                    org = db.get_organization(sub.get('organization_id', '')) if sub.get('organization_id') else None
                    org_name = org.get('name', org.get('organization_name', '')) if org else ''
                    send_internal_notification(
                        event_type='cancellation',
                        org_name=org_name,
                        plan_name=sub.get('plan', ''),
                        tenant_id=tenant_id,
                        purchaser_email=beneficiary.get('emailId', ''),
                        subscription_id=subscription_id,
                    )
                except Exception as notify_err:
                    logger.warning(f"Failed to send internal cancellation notification: {notify_err}")

                # ── Send farewell + feedback email to the customer ──
                try:
                    from src.services.email_service import send_cancellation_email
                    customer_email = beneficiary.get('emailId', '')
                    if customer_email:
                        org = org if 'org' in dir() else None
                        if not org and sub.get('organization_id'):
                            org = db.get_organization(sub['organization_id'])
                        org_name_for_email = org.get('name', org.get('organization_name', '')) if org else ''
                        send_cancellation_email(
                            to_email=customer_email,
                            buyer_name=org_name_for_email or customer_email.split('@')[0],
                            org_name=org_name_for_email,
                            plan_name=sub.get('plan', ''),
                        )
                        logger.info(f"Cancellation email sent to {customer_email}")
                except Exception as cancel_email_err:
                    logger.warning(f"Failed to send cancellation email: {cancel_email_err}")

                logger.info(f"Cancelled subscription {sub['id']}")
            else:
                logger.warning(f"Unsubscribe: subscription not found for marketplace ID {subscription_id}")
        
        elif action == 'ChangePlan':
            if sub:
                db.update_subscription_status(sub['id'], status='active', plan=plan_id)
                logger.info(f"Changed plan for subscription {sub['id']} to {plan_id}")
            else:
                logger.warning(f"ChangePlan: subscription not found for marketplace ID {subscription_id}")
        
        elif action == 'Renew':
            if sub:
                logger.info(f"Subscription {sub['id']} renewed")
                # No status change needed — subscription stays active
            else:
                logger.warning(f"Renew: subscription not found for marketplace ID {subscription_id}")
        
        elif action == 'ChangeQuantity':
            # We use flat pricing, so quantity changes don't affect us
            logger.info(f"ChangeQuantity event for {subscription_id} - no action needed (flat pricing)")
        
        else:
            logger.warning(f"Unknown webhook action: {action}")
        
    except Exception as e:
        logger.error(f"Error processing webhook {action}: {e}", exc_info=True)
        # Still return 200 — we don't want Microsoft to retry endlessly
    
    # Always return 200 to acknowledge receipt
    return jsonify({'status': 'acknowledged'}), 200
