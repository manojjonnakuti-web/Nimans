"""
Authentication Module
Microsoft Entra ID (Azure AD) JWT token validation for API endpoints
"""

import os
import re
import json
import logging
from functools import wraps
from typing import Optional, Dict, Any, Callable
from urllib.request import urlopen

from flask import request, g, jsonify
from jose import jwt, JWTError

logger = logging.getLogger(__name__)


class AuthConfig:
    """Microsoft Entra ID configuration"""
    
    def __init__(self):
        self.tenant_id = os.getenv('AZURE_TENANT_ID', '')  # Home tenant (for reference)
        self.client_id = os.getenv('AZURE_CLIENT_ID', '')
        self.audience = os.getenv('AZURE_API_AUDIENCE', self.client_id)
        
        # Multi-tenant: use the common JWKS endpoint so tokens from
        # ANY Azure AD tenant can be signature-verified.
        if self.client_id:
            self.jwks_uri = 'https://login.microsoftonline.com/common/discovery/v2.0/keys'
        else:
            self.jwks_uri = ''
        
        self.algorithms = ['RS256']
        self._jwks = None
        
        logger.info(
            f"AuthConfig initialized (multi-tenant): "
            f"client_id={self.client_id[:8] if self.client_id else 'None'}..., "
            f"jwks_uri={self.jwks_uri}"
        )
    
    @property
    def is_configured(self) -> bool:
        """Check if Microsoft Entra ID is configured (client_id is sufficient for multi-tenant)"""
        return bool(self.client_id)
    
    @staticmethod
    def is_valid_azure_ad_issuer(issuer: str) -> bool:
        """Validate that an issuer matches known Azure AD multi-tenant patterns.
        
        Azure AD tokens have issuers in these formats:
        - v2: https://login.microsoftonline.com/{tenant-id}/v2.0
        - v1: https://sts.windows.net/{tenant-id}/
        """
        guid = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
        return bool(
            re.match(rf'^https://login\.microsoftonline\.com/{guid}/v2\.0$', issuer)
            or re.match(rf'^https://sts\.windows\.net/{guid}/$', issuer)
        )
    
    def get_jwks(self) -> Dict[str, Any]:
        """Get JSON Web Key Set from Microsoft Entra ID"""
        if self._jwks is None:
            with urlopen(self.jwks_uri) as response:
                self._jwks = json.loads(response.read())
        return self._jwks


# Global config instance
auth_config = AuthConfig()


class AuthError(Exception):
    """Authentication error with status code and details"""
    
    def __init__(self, error: str, status_code: int = 401, details: str = None):
        self.error = error
        self.status_code = status_code
        self.details = details
        super().__init__(error)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'error': self.error,
            'details': self.details
        }


def get_token_from_header() -> Optional[str]:
    """Extract the token from the Authorization header"""
    auth_header = request.headers.get('Authorization', '')
    
    if not auth_header:
        return None
    
    parts = auth_header.split()
    
    if len(parts) != 2 or parts[0].lower() != 'bearer':
        return None
    
    return parts[1]


def validate_token(token: str) -> Dict[str, Any]:
    """
    Validate the JWT token and return the decoded payload
    Raises AuthError if validation fails
    """
    if not auth_config.is_configured:
        raise AuthError("Microsoft Entra ID not configured", 500)
    
    try:
        jwks = auth_config.get_jwks()
    except Exception as e:
        logger.error(f"Error fetching JWKS: {e}")
        raise AuthError("Unable to fetch authentication keys", 500)
    
    try:
        unverified_header = jwt.get_unverified_header(token)
    except JWTError:
        raise AuthError("Invalid token header", 401)
    
    # Find the key with matching kid
    rsa_key = None
    for key in jwks.get('keys', []):
        if key.get('kid') == unverified_header.get('kid'):
            rsa_key = {
                'kty': key['kty'],
                'kid': key['kid'],
                'use': key['use'],
                'n': key['n'],
                'e': key['e']
            }
            break
    
    if not rsa_key:
        raise AuthError("Unable to find appropriate key", 401)
    
    try:
        # Microsoft Entra ID tokens use 'aud' which can be the client_id or api://<client_id>
        # First, decode without verification to check the token claims
        unverified_payload = jwt.decode(
            token,
            rsa_key,
            algorithms=auth_config.algorithms,
            options={
                'verify_aud': False,
                'verify_iss': False,
                'verify_exp': False,
            }
        )
        
        # Get the audience and issuer from the token
        token_aud = unverified_payload.get('aud', '')
        token_iss = unverified_payload.get('iss', '')
        
        logger.debug(f"Token claims - aud: {token_aud}, iss: {token_iss}")
        
        # Valid audiences we accept
        valid_audiences = [
            auth_config.audience,
            f"api://{auth_config.client_id}",
            auth_config.client_id
        ]
        
        # Check if token audience matches any valid audience
        if token_aud not in valid_audiences:
            logger.error(f"Invalid audience: {token_aud}, expected one of: {valid_audiences}")
            raise AuthError("Invalid audience", 401)
        
        # Multi-tenant: validate issuer matches Azure AD pattern (any tenant)
        if not auth_config.is_valid_azure_ad_issuer(token_iss):
            logger.error(f"Invalid issuer format: {token_iss}")
            raise AuthError("Invalid issuer", 401)
        
        # Now verify the token properly with the matching audience and issuer
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=auth_config.algorithms,
            audience=token_aud,  # Use the actual audience from the token
            issuer=token_iss,    # Use the actual issuer from the token (already validated)
            options={
                'verify_aud': True,
                'verify_iss': True,
                'verify_exp': True,
            }
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise AuthError("Token has expired", 401)
    except jwt.JWTClaimsError as e:
        logger.error(f"JWT claims error: {e}")
        raise AuthError("Invalid claims", 401)
    except JWTError as e:
        logger.error(f"JWT validation error: {e}")
        raise AuthError("Invalid token", 401)


def get_current_user_info(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extract user information from the Microsoft Entra ID token payload.
    
    Handles both:
    - User tokens (delegated flow): contain 'oid', 'preferred_username', etc.
    - App-only tokens (client credentials): contain 'oid' (service principal) but NO user claims
    
    Key difference: App tokens have oid but NO preferred_username/email/upn
    They may have 'idtyp' = 'app' (v2) or 'roles' without 'scp' (scope)
    """
    oid = payload.get('oid')
    appid = payload.get('appid') or payload.get('azp')
    
    # User claims - only present in user/delegated tokens
    preferred_username = payload.get('preferred_username')
    email_claim = payload.get('email')
    upn = payload.get('upn')
    name = payload.get('name')
    
    # Check if this is an app-only token (client credentials flow)
    # App tokens have:
    # - 'idtyp' = 'app' (v2 tokens)
    # - 'oid' (service principal object ID) but NO preferred_username/email/upn
    # - 'roles' but typically no 'scp' (scope)
    idtyp = payload.get('idtyp', '')
    has_user_claims = bool(preferred_username or email_claim or upn)
    
    is_app_token = (idtyp == 'app') or (appid and not has_user_claims)
    
    logger.debug(f"Token analysis: oid={oid}, appid={appid}, idtyp={idtyp}, "
                 f"has_user_claims={has_user_claims}, is_app_token={is_app_token}")
    
    if is_app_token:
        # App-only token - use appid as identity (more stable than service principal oid)
        identity_id = appid or oid
        email = f"{appid or oid}@app.entra"  # Synthetic email for app identity
        display_name = payload.get('app_displayname') or f"Service Principal ({(appid or oid)[:8]}...)"
        logger.info(f"Processing app-only token: appid={appid}, oid={oid}")
    else:
        # User token - extract user claims
        identity_id = oid or payload.get('sub')
        email = preferred_username or email_claim or upn
        display_name = name
        logger.debug(f"Processing user token: oid={oid}, email={email}")
    
    return {
        'entra_id': identity_id,
        'email': email,
        'name': display_name,
        'tenant_id': payload.get('tid'),
        'roles': payload.get('roles', []),
        'scope': payload.get('scp', '').split() if payload.get('scp') else [],
        # Legacy compatibility
        'auth0_id': identity_id,
        'permissions': payload.get('roles', []),
        # Flag to identify token type
        'is_app_token': is_app_token,
    }


def require_auth(f: Callable) -> Callable:
    """
    Decorator that requires a valid JWT token
    Sets g.current_user and g.token_payload on success
    In development mode without Microsoft Entra ID, allows unauthenticated access with a mock user
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        # Check if Microsoft Entra ID is configured
        if not auth_config.is_configured:
            # Development mode - use mock user
            g.token_payload = {'sub': 'dev|local-user', 'oid': 'dev|local-user'}
            g.current_user = {
                'entra_id': 'dev|local-user',
                'auth0_id': 'dev|local-user',  # Legacy compatibility
                'email': 'dev@localhost',
                'name': 'Development User',
                'tenant_id': 'local',
                'roles': ['admin'],
                'permissions': ['admin'],
                'scope': ['openid', 'profile', 'email']
            }
            logger.debug("Microsoft Entra ID not configured - using mock user for development")
            return f(*args, **kwargs)
        
        token = get_token_from_header()
        
        if not token:
            return jsonify({'error': 'Authorization header required'}), 401
        
        try:
            payload = validate_token(token)
            g.token_payload = payload
            g.current_user = get_current_user_info(payload)
            return f(*args, **kwargs)
        except AuthError as e:
            return jsonify(e.to_dict()), e.status_code
    
    return decorated


def optional_auth(f: Callable) -> Callable:
    """
    Decorator that optionally validates the JWT token
    Sets g.current_user to None if no token is present
    Does not reject the request if auth fails - just logs it
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = get_token_from_header()
        
        if token:
            try:
                payload = validate_token(token)
                g.token_payload = payload
                g.current_user = get_current_user_info(payload)
            except AuthError as e:
                logger.warning(f"Optional auth failed: {e.error}")
                g.token_payload = None
                g.current_user = None
        else:
            g.token_payload = None
            g.current_user = None
        
        return f(*args, **kwargs)
    
    return decorated


def require_role(role: str) -> Callable:
    """
    Decorator factory that requires a specific role
    Must be used after require_auth
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated(*args, **kwargs):
            if not hasattr(g, 'current_user') or not g.current_user:
                return jsonify({'error': 'Authentication required'}), 401
            
            user_roles = g.current_user.get('roles', [])
            if role not in user_roles:
                return jsonify({
                    'error': 'Insufficient permissions',
                    'required': role
                }), 403
            
            return f(*args, **kwargs)
        return decorated
    return decorator


# Alias for backward compatibility
def require_permission(permission: str) -> Callable:
    """
    Decorator factory that requires a specific permission/role
    Must be used after require_auth
    """
    return require_role(permission)


def require_scope(scope: str) -> Callable:
    """
    Decorator factory that requires a specific scope
    Must be used after require_auth
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated(*args, **kwargs):
            if not hasattr(g, 'current_user') or not g.current_user:
                return jsonify({'error': 'Authentication required'}), 401
            
            user_scopes = g.current_user.get('scope', [])
            if scope not in user_scopes:
                return jsonify({
                    'error': 'Insufficient scope',
                    'required': scope
                }), 403
            
            return f(*args, **kwargs)
        return decorated
    return decorator


def require_active_subscription(f: Callable) -> Callable:
    """
    Decorator that checks the user's organization has an active subscription.
    Must be used AFTER require_auth and ensure_user_exists has been called in the route.
    
    This is a lightweight check — it only blocks if subscription is explicitly
    suspended/cancelled/expired. If no subscription record exists yet (new org),
    access is allowed (they get provisioned via marketplace webhook later).
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        result = _check_subscription_status()
        if result is not None:
            return result
        return f(*args, **kwargs)
    
    return decorated


def _check_subscription_status():
    """
    Shared subscription check logic used by both the decorator and before_request hook.
    Returns None if access is allowed, or a (response, status_code) tuple to block.
    """
    from src.repositories import get_database_repository
    
    if not hasattr(g, 'current_user') or not g.current_user:
        return None  # Let require_auth handle this
    
    # Dev mode bypass
    tenant_id = g.current_user.get('tenant_id')
    if not tenant_id or tenant_id == 'local':
        return None
    
    db = get_database_repository()
    org_id = getattr(g, '_subscription_org_id', None)
    
    if not org_id:
        org = db.get_organization_by_tenant_id(tenant_id)
        if org:
            org_id = org['organization_id']
    
    if org_id:
        sub = db.get_active_subscription(org_id)
        if sub is None:
            org = db.get_organization(org_id)
            if org and org.get('tier') == 'enterprise':
                return None
            
            logger.info(f"No subscription found for org {org_id}, allowing access (pending provisioning)")
            return None
        
        if sub['status'] != 'active':
            logger.warning(f"Blocked request: org {org_id} subscription status is {sub['status']}")
            return jsonify({
                'error': 'Subscription inactive',
                'message': f"Your organization's subscription is {sub['status']}. Please contact your administrator.",
                'subscription_status': sub['status']
            }), 403
    
    return None


def subscription_before_request():
    """
    Blueprint before_request hook to enforce subscription checks.
    Register on any blueprint that should gate access behind an active subscription:
    
        from src.auth import subscription_before_request
        my_bp.before_request(subscription_before_request)
    """
    result = _check_subscription_status()
    if result is not None:
        return result


def get_user_from_db(db_repo, entra_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a user from the database based on Entra ID (or legacy Auth0 ID)
    Returns the user dict or None
    """
    # Try by Entra ID first (stored in auth0_id field for compatibility)
    user = db_repo.get_user_by_auth0_id(entra_id)
    return user


def _resolve_organization(db_repo, tenant_id: str) -> Dict[str, Any]:
    """
    Resolve the organization for a given Azure AD tenant ID.
    If no organization exists for this tenant, auto-create one.
    Returns the organization dict.
    """
    import uuid
    from sqlalchemy.exc import IntegrityError
    
    # Look up org by Azure AD tenant ID
    org = db_repo.get_organization_by_tenant_id(tenant_id)
    if org:
        return org
    
    # Auto-create a new organization for this tenant
    org_id = f"org_{tenant_id.replace('-', '')[:12]}"
    org_name = f"Organization {tenant_id[:8]}"
    
    logger.info(f"Auto-creating organization for new Azure AD tenant: tenant_id={tenant_id}, org_id={org_id}")
    
    try:
        org = db_repo.create_organization(
            org_id=org_id,
            name=org_name,
            azure_tenant_id=tenant_id,
            tier='free_trial'
        )
        logger.info(f"Created organization: {org_id} for tenant {tenant_id}")
        
        # Auto-create a free_trial subscription for the new org
        sub_id = f"sub_{org_id}_trial"
        try:
            db_repo.create_subscription(sub_id=sub_id, org_id=org_id, plan='free_trial')
            logger.info(f"Created free_trial subscription: {sub_id} for org {org_id}")
        except Exception as sub_err:
            logger.warning(f"Failed to create subscription for new org {org_id}: {sub_err}")
        
        return org
    except IntegrityError:
        # Race condition — another request created it first
        logger.warning(f"Org creation race condition for tenant {tenant_id}, fetching existing")
        org = db_repo.get_organization_by_tenant_id(tenant_id)
        if org:
            return org
        raise ValueError(f"Failed to create or find organization for tenant: {tenant_id}")


def ensure_user_exists(db_repo, user_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure a user exists in the database.
    Derives the organization from the Azure AD tenant_id in the token.
    Auto-creates the organization if this is a new tenant.
    Creates the user if they don't exist.
    Returns the user dict.
    
    Handles both user tokens and app-only tokens (service principals).
    """
    import uuid
    from sqlalchemy.exc import IntegrityError
    
    logger.debug(f"ensure_user_exists called with user_info: {user_info}")
    
    entra_id = user_info.get('entra_id') or user_info.get('auth0_id')
    if not entra_id:
        logger.error(f"No entra_id found in user_info: {user_info}")
        raise ValueError(f"entra_id is required, got user_info: {user_info}")
    
    email = user_info.get('email', '')
    tenant_id = user_info.get('tenant_id')
    is_app_token = user_info.get('is_app_token', False)
    
    # Resolve the organization from the Azure AD tenant ID
    if tenant_id and tenant_id != 'local':
        org = _resolve_organization(db_repo, tenant_id)
        organization_id = org['organization_id']
    else:
        # Dev mode fallback
        organization_id = 'org_xtractai'
    
    logger.debug(f"Looking up user: entra_id={entra_id}, email={email}, org={organization_id}, is_app_token={is_app_token}")
    
    # Try to find existing user by Entra ID (stored in auth0_id field)
    user = db_repo.get_user_by_auth0_id(entra_id)
    
    if not user:
        # Try by email with organization
        if email:
            user = db_repo.get_user_by_email(email, org_id=organization_id)
        
        if user:
            # Found user by email, update with Entra ID if not set
            logger.info(f"Found existing user by email: {email}")
            # Update last login
            db_repo.update_user_last_login(user['id'])
        else:
            # Create new user - handle potential race condition
            try:
                user_id = str(uuid.uuid4())
                
                # Determine username based on token type
                if is_app_token:
                    username = user_info.get('name', f'Service Principal ({entra_id[:8]}...)')
                else:
                    username = user_info.get('name', user_info.get('email', 'Unknown User'))
                
                # Ensure we have an email
                user_email = email or f"{entra_id}@entra.local"
                
                # Note: DB constraint allows only 'user', 'admin', 'analyst'
                # Service principals are created as 'user' role
                user_role = 'user'
                
                logger.info(f"Creating new user: email={user_email}, username={username}, entra_id={entra_id}, org={organization_id}, role={user_role}, is_app={is_app_token}")
                
                user = db_repo.create_user(
                    user_id=user_id,
                    org_id=organization_id,
                    email=user_email,
                    username=username,
                    auth0_id=entra_id,  # Using auth0_id field for Entra ID
                    role=user_role
                )
                
                if user:
                    logger.info(f"Created new user: {user['email']} ({user['id']})")
                else:
                    logger.error(f"create_user returned None for entra_id={entra_id}")
                    raise ValueError(f"create_user returned None for email={user_email}, entra_id={entra_id}")
                    
            except IntegrityError as e:
                # User was created by another request, try to fetch again
                logger.warning(f"User creation race condition, fetching existing user: {e}")
                if email:
                    user = db_repo.get_user_by_email(email, org_id=organization_id)
                if not user:
                    user = db_repo.get_user_by_auth0_id(entra_id)
                if not user:
                    logger.error(f"Failed to find user after IntegrityError: email={email}, entra_id={entra_id}")
                    raise ValueError(f"Failed to create or find user: email={email}, entra_id={entra_id}")
    else:
        logger.debug(f"Found existing user by entra_id: {user['email']} ({user['id']})")
        # Update last login
        db_repo.update_user_last_login(user['id'])
    
    if not user:
        logger.error(f"User is None after all attempts: entra_id={entra_id}, email={email}")
        raise ValueError(f"Failed to create or find user: entra_id={entra_id}, email={email}")
    
    # ── Set up tenant-aware services on Flask's g object ──
    # Every endpoint that calls ensure_user_exists() automatically gets
    # tenant-routed DB, storage, CU, and OpenAI via g.tenant_db etc.
    # Falls back to shared services when no tenant config exists.
    #
    # g.central_db always points to the central/shared database — use it
    # for job-queue operations (async_jobs), user/org lookups, billing, etc.
    g.central_db = db_repo
    try:
        from src.tenant import (
            get_tenant_database_repository,
            get_tenant_storage_service,
            get_tenant_cu_client,
            get_tenant_openai_service,
        )
        org_id = user['organization_id']
        g.tenant_db = get_tenant_database_repository(org_id)
        g.tenant_storage = get_tenant_storage_service(org_id)
        g.tenant_cu = get_tenant_cu_client(org_id)
        g.tenant_openai = get_tenant_openai_service(org_id)
        g.org_id = org_id
    except Exception as e:
        # If tenant resolution fails, fall back to shared services
        logger.warning(f"Tenant service resolution failed for org {user.get('organization_id')}: {e}")
        g.tenant_db = db_repo
        from src.services import get_storage_service, get_azure_client, get_openai_service
        g.tenant_storage = get_storage_service()
        g.tenant_cu = get_azure_client()
        g.tenant_openai = get_openai_service()
        g.org_id = user.get('organization_id', '')
    
    return user
