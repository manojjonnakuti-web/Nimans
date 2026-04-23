"""
Auth Package
"""

from .auth import (
    require_auth,
    optional_auth,
    require_permission,
    require_scope,
    require_active_subscription,
    subscription_before_request,
    get_token_from_header,
    validate_token,
    get_current_user_info,
    get_user_from_db,
    ensure_user_exists,
    AuthError,
    AuthConfig,
    auth_config
)

__all__ = [
    'require_auth',
    'optional_auth',
    'require_permission',
    'require_scope',
    'require_active_subscription',
    'subscription_before_request',
    'get_token_from_header',
    'validate_token',
    'get_current_user_info',
    'get_user_from_db',
    'ensure_user_exists',
    'AuthError',
    'AuthConfig',
    'auth_config',
]
