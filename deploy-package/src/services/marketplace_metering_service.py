"""
Azure Marketplace Metering Service
Reports metered usage to the Azure Marketplace Metering API.

Marketplace Metering API docs:
  https://learn.microsoft.com/en-us/partner-center/marketplace/marketplace-metering-service-apis

Authentication:
  Uses Azure AD client credentials to obtain a token scoped to
  the Marketplace API (resource: 20e940b3-4c77-4b0b-9a53-9e16a1b010a7).

Environment Variables Required:
  MARKETPLACE_TENANT_ID         — Azure AD tenant ID (publisher tenant)
  MARKETPLACE_CLIENT_ID         — App registration client ID
  MARKETPLACE_CLIENT_SECRET     — App registration client secret
  MARKETPLACE_METERING_ENABLED  — Set to 'true' to enable real API calls

Note: These are the SAME credentials used by the SaaS Fulfillment API
  in marketplace_api.py — no separate app registration needed.

When MARKETPLACE_METERING_ENABLED is not 'true', the service runs in
dry-run mode: it logs what it would report but does not call the API.
This is safe for dev/test environments.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

import requests as http_requests  # renamed to avoid conflict with app-level 'requests'

logger = logging.getLogger(__name__)

# Azure AD token endpoint and Marketplace resource
_AZURE_AD_TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/token"
_MARKETPLACE_RESOURCE = "20e940b3-4c77-4b0b-9a53-9e16a1b010a7"

# Marketplace Metering API
_METERING_API_URL = "https://marketplaceapi.microsoft.com/api/usageEvents"
_METERING_API_VERSION = "2018-08-31"
_METERING_BATCH_URL = "https://marketplaceapi.microsoft.com/api/batchUsageEvents"


class AzureMarketplaceMeteringService:
    """Reports metered usage to Azure Marketplace."""

    def __init__(self):
        # Use same env vars as the SaaS Fulfillment API (marketplace_api.py)
        self.tenant_id = os.getenv('MARKETPLACE_TENANT_ID', '')
        self.client_id = os.getenv('MARKETPLACE_CLIENT_ID', '')
        self.client_secret = os.getenv('MARKETPLACE_CLIENT_SECRET', '')
        self.enabled = os.getenv('MARKETPLACE_METERING_ENABLED', 'false').lower() == 'true'
        self._token = None
        self._token_expires_at = None

    def is_configured(self) -> bool:
        """Check if all required env vars are set."""
        return bool(self.tenant_id and self.client_id and self.client_secret)

    def is_enabled(self) -> bool:
        """Check if metering is enabled (env var + configured)."""
        return self.enabled and self.is_configured()

    def _get_access_token(self) -> str:
        """Obtain or refresh an Azure AD access token for the Marketplace API."""
        import time

        # Return cached token if still valid (with 5 min buffer)
        if self._token and self._token_expires_at and time.time() < (self._token_expires_at - 300):
            return self._token

        url = _AZURE_AD_TOKEN_URL.format(tenant_id=self.tenant_id)
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'resource': _MARKETPLACE_RESOURCE,
        }

        resp = http_requests.post(url, data=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        self._token = data['access_token']
        self._token_expires_at = time.time() + int(data.get('expires_in', 3600))
        logger.info("Marketplace Metering API token acquired")
        return self._token

    def report_usage(self, subscription_marketplace_id: str,
                     plan_id: str, dimension: str, quantity: float,
                     effective_start_time: datetime = None) -> Dict[str, Any]:
        """Report a single usage event to the Marketplace Metering API.

        Args:
            subscription_marketplace_id: The SaaS subscription resourceId from Marketplace
            plan_id: The plan ID (e.g. 'enterprise')
            dimension: The metered dimension (e.g. 'pages_processed')
            quantity: The quantity to report
            effective_start_time: When the usage occurred (UTC hour). Defaults to current hour.

        Returns:
            Dict with API response or dry-run info
        """
        if effective_start_time is None:
            effective_start_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

        payload = {
            "resourceUri": subscription_marketplace_id,
            "quantity": quantity,
            "dimension": dimension,
            "effectiveStartTime": effective_start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "planId": plan_id,
        }

        if not self.is_enabled():
            logger.info(
                f"[DRY-RUN] Marketplace metering — would report: "
                f"dimension={dimension}, quantity={quantity}, plan={plan_id}, "
                f"subscription={subscription_marketplace_id}"
            )
            return {
                'status': 'dry_run',
                'payload': payload,
                'message': 'Metering not enabled. Set MARKETPLACE_METERING_ENABLED=true to report.',
            }

        token = self._get_access_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        }
        params = {'api-version': _METERING_API_VERSION}

        logger.info(
            f"Reporting metered usage: dimension={dimension}, "
            f"quantity={quantity}, plan={plan_id}"
        )

        resp = http_requests.post(
            _METERING_API_URL,
            headers=headers,
            params=params,
            json=payload,
            timeout=30,
        )

        result = {
            'status_code': resp.status_code,
            'payload_sent': payload,
        }

        try:
            result['response'] = resp.json()
        except Exception:
            result['response_text'] = resp.text

        if resp.status_code in (200, 201):
            logger.info(f"Marketplace usage reported successfully: {dimension}={quantity}")
        elif resp.status_code == 409:
            # Duplicate — already reported for this hour, dimension is acceptable
            logger.warning(f"Marketplace duplicate usage report (409): {dimension}={quantity}")
            result['duplicate'] = True
        else:
            logger.error(
                f"Marketplace metering failed ({resp.status_code}): {resp.text}"
            )
            resp.raise_for_status()

        return result

    def report_batch_usage(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Report multiple usage events in a single batch call.

        Each event dict should contain:
            subscription_marketplace_id, plan_id, dimension, quantity, effective_start_time

        Returns:
            Dict with batch API response or dry-run info
        """
        batch_payload = []
        for evt in events:
            est = evt.get('effective_start_time') or datetime.utcnow().replace(
                minute=0, second=0, microsecond=0
            )
            batch_payload.append({
                "resourceUri": evt['subscription_marketplace_id'],
                "quantity": evt['quantity'],
                "dimension": evt['dimension'],
                "effectiveStartTime": est.strftime("%Y-%m-%dT%H:%M:%SZ") if isinstance(est, datetime) else est,
                "planId": evt['plan_id'],
            })

        if not self.is_enabled():
            logger.info(
                f"[DRY-RUN] Marketplace batch metering — would report {len(batch_payload)} events"
            )
            return {
                'status': 'dry_run',
                'event_count': len(batch_payload),
                'events': batch_payload,
                'message': 'Metering not enabled. Set MARKETPLACE_METERING_ENABLED=true to report.',
            }

        token = self._get_access_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        }
        params = {'api-version': _METERING_API_VERSION}

        logger.info(f"Reporting batch metered usage: {len(batch_payload)} events")

        resp = http_requests.post(
            _METERING_BATCH_URL,
            headers=headers,
            params=params,
            json=batch_payload,
            timeout=60,
        )

        result = {
            'status_code': resp.status_code,
            'event_count': len(batch_payload),
        }

        try:
            result['response'] = resp.json()
        except Exception:
            result['response_text'] = resp.text

        if resp.status_code in (200, 201):
            logger.info(f"Marketplace batch usage reported successfully: {len(batch_payload)} events")
        else:
            logger.error(f"Marketplace batch metering failed ({resp.status_code}): {resp.text}")

        return result


# Module-level singleton
_metering_service = None


def get_marketplace_metering_service() -> AzureMarketplaceMeteringService:
    """Get the marketplace metering service singleton."""
    global _metering_service
    if _metering_service is None:
        _metering_service = AzureMarketplaceMeteringService()
    return _metering_service
