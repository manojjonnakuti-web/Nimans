"""
Branding API
Per-tenant white-labelling / customer branding endpoints
"""

import os
import uuid
import logging
import re
from datetime import datetime
from flask import Blueprint, request, jsonify, g, Response

from src.auth import require_auth, ensure_user_exists
from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

branding_bp = Blueprint('branding', __name__, url_prefix='/api/branding')


# ── Defaults (used for reset and when no branding row exists) ─────────────

BRANDING_DEFAULTS = {
    'app_name': 'Xtract',
    'subtitle': 'Synapx AI',
    'logo_url': None,
    'favicon_url': None,
    'primary_color': '#1e2a3b',
    'accent_color': '#2563eb',
    'login_tagline': None,
    'apply_to_plugin': False,
    'plugin_body_text': None,
    'plugin_footer_text': None,
}


@branding_bp.route('', methods=['GET'])
@require_auth
def get_branding():
    """Return the branding config for the caller's organization.
    Falls back to defaults when no custom branding exists."""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    org_id = user['organization_id']

    try:
        branding = db.get_organization_branding(org_id)
    except Exception as e:
        err_msg = str(e).lower()
        if 'organization_branding' in err_msg and ('invalid object' in err_msg or 'does not exist' in err_msg):
            # Table genuinely doesn't exist yet (migration pending)
            logger.warning(f"Branding table not available, returning defaults: {e}")
            branding = None
        else:
            # Real error — don't hide it
            logger.exception(f"Failed to read branding for org {org_id}")
            return jsonify({'error': f'Failed to load branding: {str(e)}'}), 500

    if branding is None:
        branding = {**BRANDING_DEFAULTS, 'organization_id': org_id, 'is_default': True}
    else:
        branding['is_default'] = False

    return jsonify(branding), 200


@branding_bp.route('', methods=['PUT'])
@require_auth
def update_branding():
    """Create or update the branding config for the caller's organization."""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    org_id = user['organization_id']

    data = request.get_json(silent=True) or {}

    # Only allow known fields
    allowed = {'app_name', 'subtitle', 'primary_color', 'accent_color',
               'login_tagline', 'logo_url', 'favicon_url',
               'apply_to_plugin', 'plugin_body_text', 'plugin_footer_text'}
    updates = {k: v for k, v in data.items() if k in allowed}

    if not updates:
        return jsonify({'error': 'No valid fields provided'}), 400

    # Basic validation
    for color_field in ('primary_color', 'accent_color'):
        if color_field in updates:
            val = updates[color_field]
            if not (isinstance(val, str) and len(val) == 7 and val.startswith('#')):
                return jsonify({'error': f'Invalid {color_field}: must be a 7-char hex like #1e2a3b'}), 400

    try:
        result = db.upsert_organization_branding(org_id, updates)
        return jsonify(result), 200
    except Exception as e:
        err_msg = str(e).lower()
        if 'organization_branding' in err_msg and ('invalid object' in err_msg or 'does not exist' in err_msg):
            # Table genuinely doesn't exist yet — tell the frontend
            logger.warning(f"Branding table not available: {e}")
            return jsonify({'error': 'Branding table has not been created yet. Please run the database migration.'}), 503
        # Real error — don't swallow it
        logger.exception(f"Failed to save branding for org {org_id}")
        return jsonify({'error': f'Failed to save branding: {str(e)}'}), 500


@branding_bp.route('/reset', methods=['POST'])
@require_auth
def reset_branding():
    """Reset branding to Xtract defaults."""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    org_id = user['organization_id']

    try:
        result = db.upsert_organization_branding(org_id, BRANDING_DEFAULTS)
        result['is_default'] = True
        return jsonify(result), 200
    except Exception as e:
        err_msg = str(e).lower()
        if 'organization_branding' in err_msg and ('invalid object' in err_msg or 'does not exist' in err_msg):
            logger.warning(f"Branding table not available: {e}")
            return jsonify({'error': 'Branding table has not been created yet. Please run the database migration.'}), 503
        logger.exception(f"Failed to reset branding for org {org_id}")
        return jsonify({'error': f'Failed to reset branding: {str(e)}'}), 500


@branding_bp.route('/logo', methods=['POST'])
@require_auth
def upload_logo():
    """Upload a logo or favicon image. Stored in Azure Blob Storage."""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    org_id = user['organization_id']

    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'Empty filename'}), 400

    # Determine type: logo or favicon
    image_type = request.form.get('type', 'logo')  # 'logo' or 'favicon'
    if image_type not in ('logo', 'favicon'):
        return jsonify({'error': 'type must be "logo" or "favicon"'}), 400

    # Validate file type
    allowed_types = {'image/png', 'image/jpeg', 'image/svg+xml', 'image/webp', 'image/gif'}
    if file.content_type not in allowed_types:
        return jsonify({'error': f'Unsupported file type: {file.content_type}'}), 400

    # Upload to blob storage
    try:
        from src.services.storage_service import StorageService
        storage = StorageService()
        if not storage.is_available():
            return jsonify({'error': 'Storage service not available'}), 503

        # Generate blob path
        ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else 'png'
        blob_name = f"branding/{org_id}/{image_type}.{ext}"

        # Upload
        blob_client = storage.blob_service_client.get_blob_client(
            container=storage.container_name,
            blob=blob_name
        )
        
        from azure.storage.blob import ContentSettings
        blob_client.upload_blob(
            file.read(),
            overwrite=True,
            content_settings=ContentSettings(content_type=file.content_type)
        )

        # Build a long-lived SAS URL (1 year) so the image is accessible
        from azure.storage.blob import generate_blob_sas, BlobSasPermissions
        from datetime import datetime, timedelta
        account_name = account_key = None
        for part in storage.connection_string.split(';'):
            if part.startswith('AccountName='):
                account_name = part.split('=', 1)[1]
            elif part.startswith('AccountKey='):
                account_key = part.split('=', 1)[1]
        
        if account_name and account_key:
            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=storage.container_name,
                blob_name=blob_name,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(days=365)
            )
            blob_url = f"{blob_client.url}?{sas_token}"
        else:
            blob_url = blob_client.url

        # Return the SAS URL — it will be saved to the branding record
        # only when the user clicks Save (via PUT /api/branding)
        return jsonify({'url': blob_url, 'type': image_type}), 200

    except Exception as e:
        logger.exception(f"Failed to upload branding {image_type} for org {org_id}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500


# ── Manifest download ────────────────────────────────────────────────────

PLUGIN_BASE_URL = os.getenv(
    'PLUGIN_URL',
    'https://xtractai-dev-plugin-tuo03u.delightfulgrass-29f46ee0.uksouth.azurecontainerapps.io'
).rstrip('/')

MANIFEST_TEMPLATE = '''<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<OfficeApp xmlns="http://schemas.microsoft.com/office/appforoffice/1.1"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xmlns:bt="http://schemas.microsoft.com/office/officeappbasictypes/1.0"
           xmlns:mailappor="http://schemas.microsoft.com/office/mailappversionoverrides/1.0"
           xsi:type="MailApp">
  <Id>%(plugin_id)s</Id>
  <Version>%(version)s</Version>
  <ProviderName>%(display_name)s</ProviderName>
  <DefaultLocale>en-US</DefaultLocale>
  <DisplayName DefaultValue="%(display_name)s" />
  <Description DefaultValue="%(display_name)s turns every incoming attachment into structured, ready-to-use data." />
  <IconUrl DefaultValue="%(icon_url)s" />
  <HighResolutionIconUrl DefaultValue="%(icon_url)s" />
  <SupportUrl DefaultValue="https://www.xtract.ai/support" />
  <AppDomains><AppDomain>%(plugin_base_url)s</AppDomain></AppDomains>
  <Hosts><Host Name="Mailbox" /></Hosts>
  <Requirements><Sets><Set Name="Mailbox" MinVersion="1.8" /></Sets></Requirements>
  <FormSettings>
    <Form xsi:type="ItemRead">
      <DesktopSettings>
        <SourceLocation DefaultValue="%(taskpane_url)s" />
        <RequestedHeight>250</RequestedHeight>
      </DesktopSettings>
    </Form>
  </FormSettings>
  <Permissions>ReadWriteItem</Permissions>
  <Rule xsi:type="RuleCollection" Mode="Or">
    <Rule xsi:type="ItemIs" ItemType="Message" FormType="Read" />
  </Rule>
  <DisableEntityHighlighting>false</DisableEntityHighlighting>
  <VersionOverrides xmlns="http://schemas.microsoft.com/office/mailappversionoverrides" xsi:type="VersionOverridesV1_0">
    <Requirements><bt:Sets DefaultMinVersion="1.8"><bt:Set Name="Mailbox" /></bt:Sets></Requirements>
    <Hosts>
      <Host xsi:type="MailHost">
        <DesktopFormFactor>
          <SupportsSharedFolders>true</SupportsSharedFolders>
          <ExtensionPoint xsi:type="MessageReadCommandSurface">
            <OfficeTab id="TabDefault">
              <Group id="msgReadCmdGroup">
                <Label resid="CommandsGroup.Label" />
                <Control xsi:type="Button" id="msgReadInsertGist">
                  <Label resid="TaskpaneButton.Label" />
                  <Supertip>
                    <Title resid="TaskpaneButton.SupertipTitle" />
                    <Description resid="TaskpaneButton.SupertipText" />
                  </Supertip>
                  <Icon>
                    <bt:Image size="16" resid="Icon.16x16" />
                    <bt:Image size="32" resid="Icon.32x32" />
                    <bt:Image size="80" resid="Icon.80x80" />
                  </Icon>
                  <Action xsi:type="ShowTaskpane"><SourceLocation resid="Taskpane.Url" /></Action>
                </Control>
              </Group>
            </OfficeTab>
          </ExtensionPoint>
        </DesktopFormFactor>
      </Host>
    </Hosts>
    <Resources>
      <bt:Images>
        <bt:Image id="Icon.16x16" DefaultValue="%(icon_url)s" />
        <bt:Image id="Icon.32x32" DefaultValue="%(icon_url)s" />
        <bt:Image id="Icon.80x80" DefaultValue="%(icon_url)s" />
      </bt:Images>
      <bt:Urls><bt:Url id="Taskpane.Url" DefaultValue="%(taskpane_url)s" /></bt:Urls>
      <bt:ShortStrings>
        <bt:String id="CommandsGroup.Label" DefaultValue="%(display_name)s" />
        <bt:String id="TaskpaneButton.Label" DefaultValue="Retrieve Mail Attachment" />
        <bt:String id="TaskpaneButton.SupertipTitle" DefaultValue="Retrieve and manage email attachments" />
      </bt:ShortStrings>
      <bt:LongStrings>
        <bt:String id="TaskpaneButton.SupertipText" DefaultValue="Open the taskpane to view and assign attachments" />
      </bt:LongStrings>
    </Resources>
    <VersionOverrides xmlns="http://schemas.microsoft.com/office/mailappversionoverrides/1.1" xsi:type="VersionOverridesV1_1">
      <Requirements><bt:Sets DefaultMinVersion="1.8"><bt:Set Name="Mailbox" /></bt:Sets></Requirements>
      <Hosts>
        <Host xsi:type="MailHost">
          <DesktopFormFactor>
            <SupportsSharedFolders>true</SupportsSharedFolders>
            <ExtensionPoint xsi:type="MessageReadCommandSurface">
              <OfficeTab id="TabDefault">
                <Group id="msgReadCmdGroup">
                  <Label resid="CommandsGroup.Label" />
                  <Control xsi:type="Button" id="msgReadInsertGist">
                    <Label resid="TaskpaneButton.Label" />
                    <Supertip>
                      <Title resid="TaskpaneButton.SupertipTitle" />
                      <Description resid="TaskpaneButton.SupertipText" />
                    </Supertip>
                    <Icon>
                      <bt:Image size="16" resid="Icon.16x16" />
                      <bt:Image size="32" resid="Icon.32x32" />
                      <bt:Image size="80" resid="Icon.80x80" />
                    </Icon>
                    <Action xsi:type="ShowTaskpane"><SourceLocation resid="Taskpane.Url" /></Action>
                  </Control>
                </Group>
              </OfficeTab>
            </ExtensionPoint>
          </DesktopFormFactor>
        </Host>
      </Hosts>
      <Resources>
        <bt:Images>
          <bt:Image id="Icon.16x16" DefaultValue="%(icon_url)s" />
          <bt:Image id="Icon.32x32" DefaultValue="%(icon_url)s" />
          <bt:Image id="Icon.80x80" DefaultValue="%(icon_url)s" />
        </bt:Images>
        <bt:Urls><bt:Url id="Taskpane.Url" DefaultValue="%(taskpane_url)s" /></bt:Urls>
        <bt:ShortStrings>
          <bt:String id="CommandsGroup.Label" DefaultValue="%(display_name)s" />
          <bt:String id="TaskpaneButton.Label" DefaultValue="Retrieve Mail Attachment" />
          <bt:String id="TaskpaneButton.SupertipTitle" DefaultValue="Retrieve and manage email attachments" />
        </bt:ShortStrings>
        <bt:LongStrings>
          <bt:String id="TaskpaneButton.SupertipText" DefaultValue="Open the taskpane to view and assign attachments" />
        </bt:LongStrings>
      </Resources>
      <WebApplicationInfo>
        <Id>%(sso_client_id)s</Id>
        <Resource>api://%(plugin_host)s/%(sso_client_id)s</Resource>
        <Scopes><Scope>openid</Scope><Scope>profile</Scope></Scopes>
      </WebApplicationInfo>
    </VersionOverrides>
  </VersionOverrides>
</OfficeApp>'''


@branding_bp.route('/manifest', methods=['GET'])
@require_auth
def download_manifest():
    """Generate a branded manifest.xml for the caller's tenant.
    Uses the tenant's branding (app_name, logo) — tenant-scoped, secure.
    Optional query params to override: ?app_name=...&logo_url=..."""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    org_id = user['organization_id']

    try:
        branding = db.get_organization_branding(org_id)
    except Exception:
        branding = None

    if branding is None:
        branding = {**BRANDING_DEFAULTS}

    # Allow query param overrides (for custom plugin branding separate from webapp)
    display_name = request.args.get('app_name') or branding.get('app_name') or BRANDING_DEFAULTS['app_name']
    logo_override = request.args.get('logo_url')
    logo_url = logo_override or branding.get('logo_url')
    plugin_base = PLUGIN_BASE_URL
    default_icon = f"{plugin_base}/PlugIn/CompanyLogoSynapx.png"
    icon_url = logo_url if logo_url else default_icon
    sso_client_id = os.getenv('AZURE_CLIENT_ID', '87fa33aa-e2b6-4429-bca3-7b0fa7c4c803')
    plugin_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"xtract-plugin-{org_id}"))
    version = f"1.0.0.{datetime.utcnow().strftime('%Y%m%d')}"
    taskpane_url = f"{plugin_base}/PlugIn/taskpane.html?v={datetime.utcnow().strftime('%Y%m%d')}"
    plugin_host = plugin_base.replace('https://', '').replace('http://', '')

    xml = MANIFEST_TEMPLATE % dict(
        plugin_id=plugin_id,
        version=version,
        display_name=display_name,
        icon_url=icon_url,
        plugin_base_url=plugin_base,
        taskpane_url=taskpane_url,
        sso_client_id=sso_client_id,
        plugin_host=plugin_host,
    )

    return Response(
        xml,
        mimetype='application/xml',
        headers={'Content-Disposition': 'attachment; filename=manifest.xml'}
    )
