"""
API Package
Registers all API blueprints
"""

from flask import Flask


from .requests_api import requests_bp
from .emails_api import emails_bp
from .documents_api import documents_bp
from .jobs_api import jobs_bp
from .dashboard_api import dashboard_bp
from .auth_api import auth_bp
from .templates_api import templates_bp
from .marketplace_api import marketplace_bp
from .metering_api import metering_bp
from .analyze_api import analyze_bp
from .plugin_api import plugin_bp
from .branding_api import branding_bp


def register_blueprints(app: Flask):
    """Register all API blueprints with the Flask app"""
    app.register_blueprint(requests_bp)
    app.register_blueprint(emails_bp)
    app.register_blueprint(documents_bp)
    app.register_blueprint(jobs_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(templates_bp)
    app.register_blueprint(marketplace_bp)
    app.register_blueprint(metering_bp)
    app.register_blueprint(analyze_bp)
    app.register_blueprint(plugin_bp)
    app.register_blueprint(branding_bp)


__all__ = [
    'register_blueprints',
    'requests_bp',
    'emails_bp',
    'documents_bp',
    'templates_bp',
    'jobs_bp',
    'dashboard_bp',
    'auth_bp',
    'analyze_bp',
    'plugin_bp',
]
