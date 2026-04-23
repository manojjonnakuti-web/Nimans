"""
Request-Driven Extraction System
Main Flask Application

This is the main entry point for the backend API.
It provides a request-centric document extraction system with:
- Email ingestion with automatic request creation
- Document management and storage
- Azure Content Understanding integration for document analysis
- Field extraction and versioning
- Async job processing
- Dashboard and metrics
"""

import os
import logging
from datetime import datetime
from threading import Thread
import time

from flask import Flask, jsonify, g, request as flask_request
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_app(config_name: str = None) -> Flask:
    """Application factory"""
    app = Flask(__name__)
    
    # Load configuration
    app.config.from_mapping(
        SECRET_KEY=os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production'),
        SQLALCHEMY_DATABASE_URI=os.getenv('MSSQL_URI'),
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        MAX_CONTENT_LENGTH=50 * 1024 * 1024,  # 50MB max upload
    )
    
    # Enable CORS with full configuration for Azure AD auth
    # Note: When using credentials, wildcard (*) origins don't work - must be explicit
    cors_origins_str = os.getenv('CORS_ORIGINS', '')
    if cors_origins_str and cors_origins_str != '*':
        cors_origins = [o.strip() for o in cors_origins_str.split(',') if o.strip()]
    else:
        # Default: allow all origins but without credentials for those not explicitly listed
        cors_origins = "*"
    
    CORS(app, 
         resources={r"/api/*": {"origins": cors_origins}},
         supports_credentials=True if cors_origins != "*" else False,
         allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
         methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
         expose_headers=["Content-Type", "Authorization", "Content-Length", "Content-Disposition"]
    )
    
    # Initialize database
    from src.repositories import init_database, get_database_repository
    init_database(app.config['SQLALCHEMY_DATABASE_URI'])
    
    # Register API blueprints
    from src.api import register_blueprints
    from src.auth import require_auth
    register_blueprints(app)
    
    # Register error handlers
    register_error_handlers(app)
    
    # Health check endpoint - ALWAYS returns 200 so container probes don't restart on DB lag
    @app.route('/health')
    def health():
        """Health check endpoint (liveness). Always 200 so container isn't killed."""
        db = get_database_repository()
        db_healthy = db.health_check()
        
        # Also check storage connectivity
        storage_status = {'ok': False, 'error': 'not checked'}
        try:
            from src.services import get_storage_service
            storage = get_storage_service()
            storage_status = storage.check_connectivity()
        except Exception as e:
            storage_status = {'ok': False, 'error': str(e)}
        
        return jsonify({
            'status': 'healthy' if (db_healthy and storage_status.get('ok')) else 'degraded',
            'timestamp': datetime.utcnow().isoformat(),
            'database': 'connected' if db_healthy else 'disconnected',
            'storage': storage_status,
            'version': '2.0.0'
        }), 200   # <-- Always 200: the container is alive even if DB is slow
    
    @app.route('/health/ready')
    def health_ready():
        """Readiness check - returns 503 only when DB is truly unreachable."""
        db = get_database_repository()
        db_healthy = db.health_check()
        return jsonify({
            'ready': db_healthy,
            'timestamp': datetime.utcnow().isoformat(),
        }), 200 if db_healthy else 503
    
    # ── Lightweight diagnostic (no auth) — shows blob_url and blob existence
    @app.route('/debug/doc/<int:doc_id>')
    def debug_doc(doc_id):
        """No-auth diagnostic: shows document blob_url and whether blob exists.
        Remove this endpoint before production use."""
        import time
        result = {'document_id': doc_id}
        
        try:
            db = get_database_repository()
            doc = db.get_document(doc_id)
            if not doc:
                result['error'] = 'Document not found in DB'
                return jsonify(result), 404
            
            result['filename'] = doc.get('filename')
            result['blob_url'] = doc.get('blob_url')
            result['content_type'] = doc.get('content_type')
            result['file_size_bytes'] = doc.get('file_size_bytes')
            result['organization_id'] = doc.get('organization_id')
            result['request_id'] = doc.get('request_id')
            
            blob_path = doc.get('blob_url', '')
            result['blob_path_repr'] = repr(blob_path)  # Show exact bytes
            result['blob_path_is_pending'] = (blob_path == 'PENDING')
            
            # Check blob existence
            from src.services import get_storage_service
            storage = get_storage_service()
            if storage.is_available() and blob_path and blob_path != 'PENDING':
                t0 = time.perf_counter()
                meta = storage.get_document_metadata(blob_path)
                elapsed_ms = round((time.perf_counter() - t0) * 1000)
                if meta:
                    result['blob_exists'] = True
                    result['blob_size'] = meta.get('size')
                    result['blob_content_type'] = meta.get('content_type')
                    result['blob_check_ms'] = elapsed_ms
                else:
                    result['blob_exists'] = False
                    result['blob_check_ms'] = elapsed_ms
                    # Try sanitised path as fallback
                    sanitized = storage.sanitize_blob_filename(blob_path)
                    if sanitized != blob_path:
                        result['sanitized_path'] = sanitized
                        meta2 = storage.get_document_metadata(sanitized)
                        result['sanitized_blob_exists'] = meta2 is not None
                        if meta2:
                            result['sanitized_blob_size'] = meta2.get('size')
                            result['note'] = 'DB path has non-ASCII chars; sanitised path matches actual blob'
            elif blob_path == 'PENDING':
                result['blob_exists'] = False
                result['note'] = 'document_file_path is still PENDING — finalize step may have failed'
            else:
                result['blob_exists'] = None
                result['note'] = 'Storage not available or no blob_url'
        except Exception as e:
            result['error'] = str(e)
        
        return jsonify(result)
    
    @app.route('/debug/recent-docs')
    def debug_recent_docs():
        """No-auth diagnostic: list recent documents with blob_url and existence check.
        Remove this endpoint before production use."""
        from src.services import get_storage_service
        
        limit = flask_request.args.get('limit', 20, type=int)
        
        try:
            db = get_database_repository()
            storage = get_storage_service()
            
            # Get recent documents using raw SQL for simplicity
            from src.models import Document
            with db.get_session() as session:
                docs = session.query(Document)\
                    .order_by(Document.document_id.desc())\
                    .limit(min(limit, 50))\
                    .all()
                
                results = []
                for doc in docs:
                    blob_path = doc.document_file_path or ''
                    entry = {
                        'id': doc.document_id,
                        'request_id': doc.document_request_id,
                        'filename': doc.document_file_name,
                        'blob_url': blob_path,
                        'blob_url_repr': repr(blob_path),
                        'is_pending': blob_path == 'PENDING',
                    }
                    # Quick blob existence check
                    if storage.is_available() and blob_path and blob_path != 'PENDING':
                        try:
                            meta = storage.get_document_metadata(blob_path)
                            entry['blob_exists'] = meta is not None
                            entry['blob_size'] = meta.get('size') if meta else None
                        except Exception as e:
                            entry['blob_exists'] = False
                            entry['blob_error'] = str(e)
                    else:
                        entry['blob_exists'] = None
                    
                    results.append(entry)
            
            return jsonify({'count': len(results), 'documents': results})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/debug/cost/<int:request_id>')
    def debug_cost(request_id):
        """No-auth diagnostic: returns the cost tracking data saved in the
        most recent completed async job for this request.
        Visit /debug/cost/123 in a browser to see the full cost breakdown.
        Remove this endpoint before production use."""
        try:
            db = get_database_repository()
            
            # Find the most recent completed job for this request
            from src.models import AsyncJob
            with db.get_session() as session:
                job = session.query(AsyncJob)\
                    .filter_by(async_job_entity_id=str(request_id), async_job_entity_type='request')\
                    .order_by(AsyncJob.async_job_created_at.desc())\
                    .first()
                
                if not job:
                    return jsonify({'error': f'No async job found for request {request_id}'}), 404
                
                import json as _json
                result_data = None
                if job.async_job_result_data:
                    try:
                        result_data = _json.loads(job.async_job_result_data)
                    except Exception:
                        result_data = {'raw': job.async_job_result_data}
                
                status_name = db._get_status_value_from_type(session, job.async_job_status_type_id) or 'unknown'
                
                return jsonify({
                    'request_id': request_id,
                    'job_id': job.async_job_id,
                    'job_status': status_name,
                    'started_at': job.async_job_started_at.isoformat() if job.async_job_started_at else None,
                    'completed_at': job.async_job_completed_at.isoformat() if job.async_job_completed_at else None,
                    'cost_data': result_data,
                })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/debug/cost')
    def debug_cost_list():
        """No-auth diagnostic: list recent jobs with cost data.
        Remove this endpoint before production use."""
        try:
            db = get_database_repository()
            from src.models import AsyncJob
            import json as _json
            
            limit = flask_request.args.get('limit', 10, type=int)
            
            with db.get_session() as session:
                jobs = session.query(AsyncJob)\
                    .filter(AsyncJob.async_job_result_data.isnot(None))\
                    .order_by(AsyncJob.async_job_created_at.desc())\
                    .limit(min(limit, 50))\
                    .all()
                
                results = []
                for job in jobs:
                    try:
                        cost = _json.loads(job.async_job_result_data) if job.async_job_result_data else None
                    except Exception:
                        cost = None
                    
                    results.append({
                        'job_id': job.async_job_id,
                        'request_id': job.async_job_entity_id,
                        'started_at': job.async_job_started_at.isoformat() if job.async_job_started_at else None,
                        'completed_at': job.async_job_completed_at.isoformat() if job.async_job_completed_at else None,
                        'cost_summary': {
                            'total_pages': cost.get('total_pages', 0) if cost else 0,
                            'grand_total_usd': cost.get('grand_total_estimated_usd', 0) if cost else 0,
                            'processing_seconds': cost.get('processing_time_seconds', 0) if cost else 0,
                        } if cost else None,
                    })
                
                return jsonify({'count': len(results), 'jobs': results})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    # Root endpoint
    @app.route('/')
    def root():
        """Serve frontend or API info"""
        _dist = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'dist')
        if os.path.isdir(_dist):
            from flask import send_from_directory
            return send_from_directory(_dist, 'index.html')
        return jsonify({
            'name': 'Request-Driven Extraction System',
            'version': '2.0.0',
            'description': 'Document extraction API with request-centric workflow',
            'endpoints': {
                'health': '/health',
                'requests': '/api/requests',
                'emails': '/api/emails',
                'documents': '/api/documents',
                'jobs': '/api/jobs',
                'dashboard': '/api/dashboard'
            }
        })
    
    # Process pending jobs endpoint (for manual trigger or webhooks)
    @app.route('/api/process-jobs', methods=['POST'])
    @require_auth
    def process_jobs():
        """Trigger job processing (for development/testing)"""
        from src.jobs import process_pending_jobs
        
        # In production, this should be protected or disabled
        if os.getenv('FLASK_ENV') == 'production':
            return jsonify({'error': 'Not available in production'}), 403
        
        processed = process_pending_jobs(max_jobs=5)
        return jsonify({
            'message': f'Processed {processed} jobs'
        })
    
    # PDF Proxy endpoint for secure document viewing
    @app.route('/api/proxy-pdf', methods=['GET'])
    def proxy_pdf():
        """
        Proxy PDF content from blob storage
        This allows the frontend to view PDFs without exposing storage URLs
        """
        from flask import request, Response
        import requests as http_requests
        
        url = request.args.get('url')
        if not url:
            return jsonify({'error': 'URL parameter required'}), 400
        
        try:
            # Fetch the PDF from the URL
            response = http_requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Return the PDF content with appropriate headers
            return Response(
                response.content,
                mimetype='application/pdf',
                headers={
                    'Content-Disposition': 'inline',
                    'Cache-Control': 'private, max-age=3600'
                }
            )
        except Exception as e:
            logger.error(f"Error proxying PDF: {e}")
            return jsonify({'error': 'Failed to fetch document'}), 500
    
    logger.info("Application initialized successfully")
    
    # ── Tenant resource cleanup on shutdown ──
    import atexit
    from src.tenant import shutdown_all, cleanup_idle_resources
    atexit.register(shutdown_all)
    
    return app


def register_error_handlers(app: Flask):
    """Register error handlers"""
    
    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({
            'error': 'Bad Request',
            'message': str(error.description) if hasattr(error, 'description') else 'Invalid request'
        }), 400
    
    @app.errorhandler(401)
    def unauthorized(error):
        return jsonify({
            'error': 'Unauthorized',
            'message': 'Authentication required'
        }), 401
    
    @app.errorhandler(403)
    def forbidden(error):
        return jsonify({
            'error': 'Forbidden',
            'message': 'Access denied'
        }), 403
    
    @app.errorhandler(404)
    def not_found(error):
        # If this is an API request, return JSON
        if flask_request.path.startswith('/api/'):
            return jsonify({
                'error': 'Not Found',
                'message': 'Resource not found'
            }), 404
        # Otherwise serve frontend index.html (SPA client-side routing)
        _dist = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'dist')
        if os.path.isdir(_dist):
            from flask import send_from_directory
            return send_from_directory(_dist, 'index.html')
        return jsonify({
            'error': 'Not Found',
            'message': 'Resource not found'
        }), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"Internal server error: {error}")
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'An unexpected error occurred'
        }), 500


def start_background_worker():
    """
    Start background worker for job processing
    This runs in a separate thread and processes pending jobs
    """
    from src.jobs import process_pending_jobs
    
    def worker():
        while True:
            try:
                process_pending_jobs(max_jobs=10)
            except Exception as e:
                logger.error(f"Background worker error: {e}")
            time.sleep(10)  # Check every 10 seconds
    
    thread = Thread(target=worker, daemon=True)
    thread.start()
    logger.info("Background worker started")


# Create the application instance
app = create_app()

# ---------- Serve frontend static build (local dev only) ----------
_frontend_dist = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'dist')
if os.path.isdir(_frontend_dist):
    from flask import send_from_directory

    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def serve_frontend(path):
        # Don't intercept API routes
        if path.startswith('api/'):
            from flask import abort
            abort(404)
        file_path = os.path.join(_frontend_dist, path)
        if path and os.path.isfile(file_path):
            return send_from_directory(_frontend_dist, path)
        return send_from_directory(_frontend_dist, 'index.html')

    logger.info(f"Serving frontend from {os.path.abspath(_frontend_dist)}")

# Start background worker if enabled
if os.getenv('ENABLE_BACKGROUND_WORKER', 'false').lower() == 'true':
    start_background_worker()


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    
    logger.info(f"Starting server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug, use_reloader=False, threaded=True)
