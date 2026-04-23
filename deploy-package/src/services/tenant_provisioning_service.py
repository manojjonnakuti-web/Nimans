"""
Tenant Provisioning Service
============================
Automatically provisions dedicated Azure resources for enterprise customers
when they purchase via Azure Marketplace.

Flow:
    1. Customer buys on Marketplace → landing page webhook fires
    2. This service provisions:  SQL Database, Storage Account, CU instance
    3. Runs customer_schema.sql to set up tables + seed data
    4. Writes connection details to tenant_configs table
    5. Customer logs in → tenant routing sends their data to dedicated resources

All resources are created in YOUR Azure subscription (not the customer's).
The customer doesn't see or manage any of this — it's fully transparent.

Uses Azure SDK (azure-identity + azure-mgmt-*) for provisioning.
Falls back to Azure CLI if SDK is not available.
"""

import os
import re
import json
import time
import logging
import threading
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# These come from environment variables (set in your Container App / .env)
PROVISIONING_SUBSCRIPTION_ID = os.getenv('AZURE_PROVISIONING_SUBSCRIPTION_ID', os.getenv('AZURE_SUBSCRIPTION_ID', ''))
PROVISIONING_RESOURCE_GROUP = os.getenv('AZURE_PROVISIONING_RESOURCE_GROUP', '')
PROVISIONING_LOCATION = os.getenv('AZURE_PROVISIONING_LOCATION', 'uksouth')
PROVISIONING_SQL_SERVER = os.getenv('AZURE_PROVISIONING_SQL_SERVER', '')  # Shared SQL Server for tenant DBs
PROVISIONING_SQL_ADMIN_USER = os.getenv('AZURE_PROVISIONING_SQL_ADMIN_USER', 'sqladmin')
PROVISIONING_SQL_ADMIN_PASSWORD = os.getenv('AZURE_PROVISIONING_SQL_ADMIN_PASSWORD', '')

# Feature flag: set to 'true' to enable auto-provisioning on marketplace purchase
AUTO_PROVISION_ENABLED = os.getenv('AUTO_PROVISION_TENANT_RESOURCES', 'false').lower() == 'true'


def _slugify(name: str) -> str:
    """Convert org name to a lowercase slug safe for Azure resource names."""
    slug = re.sub(r'[^a-z0-9]', '', name.lower())
    return slug[:16]  # Keep short for Azure naming limits


# ---------------------------------------------------------------------------
# Provisioning implementation
# ---------------------------------------------------------------------------

def provision_tenant_resources(
    org_id: str,
    org_name: str,
    plan_id: str = 'standard',
) -> Dict[str, Any]:
    """
    Provision dedicated Azure resources for a new tenant.

    This creates:
    1. A dedicated SQL Database (on a shared SQL Server)
    2. A dedicated Storage Account
    3. Optionally a dedicated Content Understanding instance

    Then deploys the customer schema and registers the config.

    Args:
        org_id: Organization ID from central database
        org_name: Human-readable org name (used for resource naming)
        plan_id: Marketplace plan (determines resource tier)

    Returns:
        Dict with provisioning status and resource details
    """
    slug = _slugify(org_name or org_id)
    suffix = org_id.replace('org_', '')[:8]

    logger.info(f"🔧 Starting tenant provisioning for org={org_id}, name={org_name}, plan={plan_id}")

    result = {
        'org_id': org_id,
        'status': 'provisioning',
        'resources': {},
        'errors': [],
    }

    config = {
        'db_connection_string': '',
        'storage_connection_string': '',
        'storage_container': 'documents',
        'cu_endpoint': '',
        'cu_api_key': '',
        'openai_endpoint': '',
        'openai_api_key': '',
        'openai_deployment': '',
        'region': PROVISIONING_LOCATION,
        'status': 'provisioning',
    }

    try:
        # ── Step 1: Create dedicated SQL Database ──
        db_name = f"xtract-{slug}-{suffix}"
        logger.info(f"  1/4 Creating SQL Database: {db_name}")
        db_conn = _provision_sql_database(db_name, plan_id)
        if db_conn:
            config['db_connection_string'] = db_conn
            result['resources']['sql_database'] = db_name
            logger.info(f"  ✅ SQL Database created: {db_name}")
        else:
            result['errors'].append('Failed to create SQL Database')
            logger.error(f"  ❌ SQL Database creation failed")

        # ── Step 2: Deploy customer schema ──
        if db_conn:
            logger.info(f"  2/4 Deploying customer schema")
            schema_ok = _deploy_customer_schema(db_conn)
            if schema_ok:
                logger.info(f"  ✅ Customer schema deployed")
            else:
                result['errors'].append('Failed to deploy customer schema')
                logger.error(f"  ❌ Schema deployment failed")

        # ── Step 3: Create dedicated Storage Account ──
        storage_name = f"stxtract{slug}{suffix}"[:24]  # Max 24 chars
        logger.info(f"  3/4 Creating Storage Account: {storage_name}")
        storage_conn = _provision_storage_account(storage_name)
        if storage_conn:
            config['storage_connection_string'] = storage_conn
            result['resources']['storage_account'] = storage_name
            logger.info(f"  ✅ Storage Account created: {storage_name}")
        else:
            result['errors'].append('Failed to create Storage Account')
            logger.error(f"  ❌ Storage Account creation failed")

        # ── Step 4: Content Understanding ──
        # For now, CU is shared (expensive per-instance).
        # Enterprise customers can request dedicated CU later.
        logger.info(f"  4/4 Content Understanding: using shared instance (default)")
        # config['cu_endpoint'] and config['cu_api_key'] stay empty → fallback to shared

        # ── Register tenant config ──
        if not result['errors']:
            config['status'] = 'active'
            result['status'] = 'active'
        else:
            config['status'] = 'provisioning'  # Partial success
            result['status'] = 'partial'

        _register_tenant_config(org_id, config)
        logger.info(f"✅ Tenant provisioning complete for {org_id}: {result['status']}")

    except Exception as e:
        logger.error(f"❌ Tenant provisioning failed for {org_id}: {e}", exc_info=True)
        result['status'] = 'failed'
        result['errors'].append(str(e))

        # Still register config with 'provisioning' status so we can retry
        config['status'] = 'provisioning'
        try:
            _register_tenant_config(org_id, config)
        except Exception:
            pass

    return result


def provision_tenant_async(org_id: str, org_name: str, plan_id: str = 'standard'):
    """
    Start tenant provisioning in a background thread.
    The marketplace landing page doesn't need to wait for this.
    """
    thread = threading.Thread(
        target=provision_tenant_resources,
        args=(org_id, org_name, plan_id),
        name=f'provision-{org_id}',
        daemon=True,
    )
    thread.start()
    logger.info(f"Started async provisioning for {org_id} (thread: {thread.name})")
    return thread


# ---------------------------------------------------------------------------
# Internal provisioning functions
# ---------------------------------------------------------------------------

def _provision_sql_database(db_name: str, plan_id: str) -> Optional[str]:
    """
    Create a SQL Database on the shared SQL Server.
    Returns the connection string or None on failure.
    """
    if not PROVISIONING_SQL_SERVER or not PROVISIONING_SQL_ADMIN_PASSWORD:
        logger.warning("SQL provisioning not configured (missing AZURE_PROVISIONING_SQL_SERVER or password)")
        return None

    try:
        # Determine SKU based on plan
        sku = 'S0' if plan_id in ('free_trial', 'basic') else 'S1'

        from azure.identity import DefaultAzureCredential
        from azure.mgmt.sql import SqlManagementClient

        credential = DefaultAzureCredential()
        sql_client = SqlManagementClient(credential, PROVISIONING_SUBSCRIPTION_ID)

        # Create the database
        poller = sql_client.databases.begin_create_or_update(
            resource_group_name=PROVISIONING_RESOURCE_GROUP,
            server_name=PROVISIONING_SQL_SERVER,
            database_name=db_name,
            parameters={
                'location': PROVISIONING_LOCATION,
                'sku': {'name': sku, 'tier': 'Standard'},
                'properties': {
                    'collation': 'SQL_Latin1_General_CP1_CI_AS',
                    'max_size_bytes': 2 * 1024 * 1024 * 1024,  # 2 GB
                }
            }
        )

        # Wait for completion (typically 30-60 seconds)
        poller.result(timeout=300)

        # Build connection string
        server_fqdn = f"{PROVISIONING_SQL_SERVER}.database.windows.net"
        conn_str = (
            f"mssql+pyodbc://{PROVISIONING_SQL_ADMIN_USER}:{PROVISIONING_SQL_ADMIN_PASSWORD}"
            f"@{server_fqdn}/{db_name}"
            f"?driver=ODBC+Driver+18+for+SQL+Server"
        )
        return conn_str

    except ImportError:
        logger.error("azure-mgmt-sql not installed. Run: pip install azure-mgmt-sql azure-identity")
        return _provision_sql_database_cli(db_name, plan_id)
    except Exception as e:
        logger.error(f"SQL Database provisioning failed: {e}", exc_info=True)
        return None


def _provision_sql_database_cli(db_name: str, plan_id: str) -> Optional[str]:
    """Fallback: provision SQL DB using Azure CLI."""
    import subprocess

    sku = 'S0' if plan_id in ('free_trial', 'basic') else 'S1'

    try:
        cmd = (
            f"az sql db create --name {db_name} "
            f"--resource-group {PROVISIONING_RESOURCE_GROUP} "
            f"--server {PROVISIONING_SQL_SERVER} "
            f"--service-objective {sku} "
            f"--output json"
        )
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"az sql db create failed: {result.stderr}")
            return None

        server_fqdn = f"{PROVISIONING_SQL_SERVER}.database.windows.net"
        conn_str = (
            f"mssql+pyodbc://{PROVISIONING_SQL_ADMIN_USER}:{PROVISIONING_SQL_ADMIN_PASSWORD}"
            f"@{server_fqdn}/{db_name}"
            f"?driver=ODBC+Driver+18+for+SQL+Server"
        )
        return conn_str

    except Exception as e:
        logger.error(f"CLI SQL provisioning failed: {e}")
        return None


def _provision_storage_account(storage_name: str) -> Optional[str]:
    """
    Create a Storage Account + default container.
    Returns the connection string or None on failure.
    """
    try:
        from azure.identity import DefaultAzureCredential
        from azure.mgmt.storage import StorageManagementClient
        from azure.mgmt.storage.models import (
            StorageAccountCreateParameters,
            Sku,
            Kind,
        )

        credential = DefaultAzureCredential()
        storage_client = StorageManagementClient(credential, PROVISIONING_SUBSCRIPTION_ID)

        # Create storage account
        poller = storage_client.storage_accounts.begin_create(
            resource_group_name=PROVISIONING_RESOURCE_GROUP,
            account_name=storage_name,
            parameters=StorageAccountCreateParameters(
                sku=Sku(name='Standard_LRS'),
                kind=Kind.STORAGE_V2,
                location=PROVISIONING_LOCATION,
                enable_https_traffic_only=True,
                minimum_tls_version='TLS1_2',
            )
        )
        poller.result(timeout=300)

        # Get keys
        keys = storage_client.storage_accounts.list_keys(
            resource_group_name=PROVISIONING_RESOURCE_GROUP,
            account_name=storage_name,
        )
        key = keys.keys[0].value

        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage_name};"
            f"AccountKey={key};"
            f"EndpointSuffix=core.windows.net"
        )

        # Create default container
        try:
            from azure.storage.blob import BlobServiceClient
            blob_client = BlobServiceClient.from_connection_string(conn_str)
            blob_client.create_container('documents')
        except Exception as ce:
            logger.warning(f"Container creation warning: {ce}")

        return conn_str

    except ImportError:
        logger.error("azure-mgmt-storage not installed. Run: pip install azure-mgmt-storage azure-identity")
        return _provision_storage_account_cli(storage_name)
    except Exception as e:
        logger.error(f"Storage Account provisioning failed: {e}", exc_info=True)
        return None


def _provision_storage_account_cli(storage_name: str) -> Optional[str]:
    """Fallback: provision Storage Account using Azure CLI."""
    import subprocess

    try:
        # Create account
        cmd = (
            f"az storage account create --name {storage_name} "
            f"--resource-group {PROVISIONING_RESOURCE_GROUP} "
            f"--location {PROVISIONING_LOCATION} "
            f"--sku Standard_LRS --kind StorageV2 --output json"
        )
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"az storage account create failed: {result.stderr}")
            return None

        # Get keys
        cmd_keys = (
            f"az storage account keys list --account-name {storage_name} "
            f"--resource-group {PROVISIONING_RESOURCE_GROUP} --output json"
        )
        keys_result = subprocess.run(cmd_keys, shell=True, capture_output=True, text=True, timeout=60)
        if keys_result.returncode != 0:
            logger.error(f"Failed to get storage keys: {keys_result.stderr}")
            return None

        keys = json.loads(keys_result.stdout)
        key = keys[0]['value']

        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={storage_name};"
            f"AccountKey={key};"
            f"EndpointSuffix=core.windows.net"
        )

        # Create container
        subprocess.run(
            f"az storage container create --name documents "
            f"--account-name {storage_name} --account-key {key}",
            shell=True, capture_output=True, timeout=60
        )

        return conn_str

    except Exception as e:
        logger.error(f"CLI Storage provisioning failed: {e}")
        return None


def _deploy_customer_schema(connection_string: str) -> bool:
    """Deploy the customer database schema to a new tenant database."""
    try:
        import pyodbc
        from urllib.parse import urlparse, parse_qs, unquote

        # Parse SQLAlchemy connection string to ODBC
        if 'pyodbc://' in connection_string:
            parsed = urlparse(connection_string.replace('mssql+pyodbc://', 'http://'))
            server = parsed.hostname
            database = parsed.path.lstrip('/')
            username = unquote(parsed.username or '')
            password = unquote(parsed.password or '')
            driver = parse_qs(parsed.query).get('driver', ['ODBC Driver 18 for SQL Server'])[0]
            odbc_str = (
                f"Driver={{{driver}}};Server={server};Database={database};"
                f"Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;"
            )
        else:
            odbc_str = connection_string

        conn = pyodbc.connect(odbc_str)
        conn.autocommit = True
        cursor = conn.cursor()

        # Read customer schema SQL
        schema_path = os.path.join(
            os.path.dirname(__file__), '..', '..', '..', 'database', 'customer_schema.sql'
        )
        with open(schema_path, 'r') as f:
            sql = f.read()

        # Split on GO statements and execute each batch
        batches = re.split(r'^\s*GO\s*$', sql, flags=re.MULTILINE | re.IGNORECASE)
        for batch in batches:
            batch = batch.strip()
            if batch:
                cursor.execute(batch)

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Schema deployment failed: {e}", exc_info=True)
        return False


def _register_tenant_config(org_id: str, config: dict):
    """Write tenant config to the central database."""
    try:
        from src.repositories import get_database_repository
        db = get_database_repository()
        db.upsert_tenant_config(org_id, config)
        logger.info(f"Tenant config registered for {org_id} (status={config.get('status')})")
    except Exception as e:
        logger.error(f"Failed to register tenant config: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Plan-based provisioning decision
# ---------------------------------------------------------------------------

def should_provision_dedicated_resources(plan_id: str) -> bool:
    """
    Determine whether a marketplace plan requires dedicated resources.

    Current logic:
      - free_trial / basic → shared infrastructure (no provisioning)
      - standard / professional → dedicated DB + storage
      - enterprise → dedicated everything including CU

    This can be customized per your marketplace offer plans.
    """
    if not AUTO_PROVISION_ENABLED:
        return False

    dedicated_plans = {'standard', 'professional', 'enterprise', 'premium'}
    return plan_id.lower() in dedicated_plans
