"""
Documents API
Handles document management and file operations
"""

from flask import Blueprint, request, jsonify, g, send_file, Response
from datetime import datetime
import uuid
import logging
import io

from src.auth import require_auth, ensure_user_exists, subscription_before_request
from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

documents_bp = Blueprint('documents', __name__, url_prefix='/api/documents')
documents_bp.before_request(subscription_before_request)


@documents_bp.route('', methods=['GET'])
@require_auth
def list_documents():
    """List all documents for the current user's organization"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 25, type=int)
    search = request.args.get('search')
    
    documents, total = db.list_documents(
        org_id=user['organization_id'],
        search=search,
        page=page,
        per_page=per_page
    )
    
    return jsonify({
        'documents': documents,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page if per_page > 0 else 0
        }
    })


@documents_bp.route('/<document_id>', methods=['GET'])
@require_auth
def get_document(document_id: str):
    """Get document metadata"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    return jsonify(doc)


@documents_bp.route('/upload', methods=['POST'])
@require_auth
def upload_document():
    """
    Upload a new document
    Supports multipart/form-data with file upload
    Optional request_id parameter to upload to requests/{request_id}/ folder
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    # Check for file in request
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'No filename provided'}), 400
    
    # Get optional request_id for folder structure and direct linking
    request_id = request.form.get('request_id')
    
    # Read file content
    content = file.read()
    if not content:
        return jsonify({'error': 'Empty file'}), 400
    
    # Create document record
    doc_id = str(uuid.uuid4())
    
    # Set blob_url based on request_id - always include org prefix for tenant isolation
    if request_id:
        blob_url = f"{user['organization_id']}/requests/{request_id}/{file.filename}"
    else:
        blob_url = f"{user['organization_id']}/{doc_id}/{file.filename}"
    
    # Create document with direct request_id link if provided
    created_doc = db.create_document(
        doc_id=doc_id,
        org_id=user['organization_id'],
        filename=file.filename,
        blob_url=blob_url,
        content_type=file.content_type or 'application/octet-stream',
        file_size_bytes=len(content),
        uploaded_by=user['id'],
        document_type='manual_upload',
        request_id=int(request_id) if request_id and str(request_id).isdigit() else None  # Direct link
    )
    
    # Upload to storage if available
    try:
        from src.services import get_storage_service
        storage = get_storage_service()
        
        if storage.is_available():
            actual_blob_url = storage.upload_document(
                file_content=content,
                filename=file.filename,
                request_id=request_id,
                organization_id=user['organization_id'],
                document_id=doc_id,
                content_type=file.content_type
            )
            db.update_document(doc_id, blob_url=actual_blob_url)
            created_doc['blob_url'] = actual_blob_url
            
            # Auto-link document to request if request_id provided
            if request_id:
                db.link_request_document(request_id, doc_id, source_type='upload')
                
    except Exception as e:
        logger.warning(f"Storage upload failed: {e}")
    
    logger.info(f"Uploaded document {doc_id}: {file.filename}" + (f" to request {request_id}" if request_id else ""))
    
    return jsonify(created_doc), 201


@documents_bp.route('/<document_id>/download', methods=['GET'])
@require_auth
def download_document(document_id: str):
    """Download document content — uses streaming to avoid loading entire file into memory"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    if not doc.get('blob_url') or doc['blob_url'] == 'PENDING':
        return jsonify({'error': 'Document is still being processed (PENDING)'}), 404
    
    try:
        from src.services import get_storage_service
        storage = get_storage_service()
        
        if not storage.is_available():
            logger.error(f"Storage service not available when downloading document {document_id}")
            return jsonify({'error': 'Storage service not available'}), 503
        
        blob_path = doc['blob_url']
        
        # Resolve the actual blob path — handles plugin filename sanitisation mismatches
        resolved_path, was_corrected = storage.resolve_blob_path(blob_path)
        if was_corrected:
            logger.info(f"Auto-correcting blob_url for document {document_id}: '{blob_path}' → '{resolved_path}'")
            try:
                db.update_document(document_id, blob_url=resolved_path)
            except Exception as upd_err:
                logger.warning(f"Failed to update corrected blob_url in DB: {upd_err}")
            blob_path = resolved_path
        
        logger.info(f"Streaming download for document {document_id} from blob: {blob_path}")
        
        try:
            chunks, properties = storage.download_document_stream(blob_path)
        except Exception as blob_err:
            err_msg = str(blob_err)
            if 'BlobNotFound' in err_msg or '404' in err_msg:
                logger.error(f"Blob not found for document {document_id}. DB blob_url='{blob_path}'")
                return jsonify({
                    'error': 'Document file not found in storage',
                    'detail': f'Blob path: {blob_path}'
                }), 404
            raise
        
        # Check if caller wants to view inline (for PDF viewer) vs download
        inline = request.args.get('inline', 'false').lower() == 'true'
        content_type = properties.content_settings.content_type if properties.content_settings else doc.get('content_type', 'application/octet-stream')
        
        response = Response(
            chunks,
            mimetype=content_type,
            headers={
                'Content-Disposition': f'{"inline" if inline else "attachment"}; filename="{doc["filename"]}"',
                'Content-Length': str(properties.size),
            }
        )
        return response
    except Exception as e:
        logger.error(f"Error downloading document {document_id}: {e}", exc_info=True)
        return jsonify({'error': f'Download failed: {str(e)}'}), 500


@documents_bp.route('/<document_id>/pdf', methods=['GET'])
@require_auth
def get_document_pdf(document_id: str):
    """
    Get PDF content directly — streams from blob storage to avoid loading
    the entire file into memory (prevents OOM / worker crash on large PDFs).
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        logger.warning(f"PDF request for non-existent document: {document_id}")
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        logger.warning(f"PDF access denied for document {document_id}, user org {user['organization_id']}")
        return jsonify({'error': 'Access denied'}), 403
    
    if not doc.get('blob_url') or doc['blob_url'] == 'PENDING':
        logger.warning(f"PDF request for document without blob_url: {document_id} (blob_url={doc.get('blob_url')!r})")
        return jsonify({'error': 'Document is still being processed (PENDING)'}), 404
    
    try:
        from src.services import get_storage_service
        storage = get_storage_service()
        
        if not storage.is_available():
            logger.error(f"Storage service not available when fetching PDF {document_id}")
            return jsonify({'error': 'Storage service not available'}), 503
        
        blob_path = doc['blob_url']
        
        # Resolve the actual blob path — handles plugin filename sanitisation mismatches
        resolved_path, was_corrected = storage.resolve_blob_path(blob_path)
        if was_corrected:
            # Self-heal: update DB so future requests don't need resolution
            logger.info(f"Auto-correcting blob_url for document {document_id}: '{blob_path}' → '{resolved_path}'")
            try:
                db.update_document(document_id, blob_url=resolved_path)
            except Exception as upd_err:
                logger.warning(f"Failed to update corrected blob_url in DB: {upd_err}")
            blob_path = resolved_path
        
        logger.info(f"Streaming PDF for document {document_id} from blob: {blob_path}")
        
        try:
            chunks, properties = storage.download_document_stream(blob_path)
        except Exception as blob_err:
            err_msg = str(blob_err)
            if 'BlobNotFound' in err_msg or '404' in err_msg:
                logger.error(f"PDF blob not found for document {document_id}. DB blob_url='{blob_path}'")
                return jsonify({
                    'error': 'PDF file not found in storage',
                    'detail': f'Blob path: {blob_path}'
                }), 404
            raise
        
        content_type = 'application/pdf'
        if properties.content_settings and properties.content_settings.content_type:
            content_type = properties.content_settings.content_type
        
        logger.info(f"Streaming PDF, size: {properties.size} bytes, content-type: {content_type}")
        
        response = Response(
            chunks,
            mimetype=content_type,
            headers={
                'Content-Disposition': f'inline; filename="{doc["filename"]}"',
                'Content-Length': str(properties.size),
                'Cache-Control': 'private, max-age=3600',
                'Accept-Ranges': 'bytes',
            }
        )
        return response
        
    except Exception as e:
        logger.error(f"Error fetching PDF for document {document_id}: {e}", exc_info=True)
        return jsonify({'error': f'Failed to fetch PDF: {str(e)}'}), 500


@documents_bp.route('/<document_id>/diag', methods=['GET'])
@require_auth
def diagnose_document(document_id: str):
    """
    Diagnostic endpoint — checks auth, DB lookup, subscription, blob existence
    WITHOUT downloading the full blob. Helps pinpoint 503 root cause.
    """
    import time
    steps = {}
    
    try:
        # Step 1: Auth (already passed @require_auth)
        steps['auth'] = {'ok': True, 'user': g.current_user.get('email')}
        
        # Step 2: DB lookup
        t0 = time.perf_counter()
        db = get_database_repository()
        user = ensure_user_exists(db, g.current_user)
        db = g.tenant_db  # Route to tenant database
        steps['ensure_user'] = {'ok': True, 'user_id': user.get('id'), 'org_id': user.get('organization_id'), 'ms': round((time.perf_counter() - t0) * 1000)}
        
        # Step 3: Get document
        t0 = time.perf_counter()
        doc = db.get_document(document_id)
        steps['get_document'] = {
            'ok': doc is not None,
            'ms': round((time.perf_counter() - t0) * 1000),
            'blob_url': doc.get('blob_url') if doc else None,
            'filename': doc.get('filename') if doc else None,
            'content_type': doc.get('content_type') if doc else None,
            'file_size_bytes': doc.get('file_size_bytes') if doc else None,
        }
        
        if not doc:
            steps['overall'] = 'document_not_found'
            return jsonify(steps), 404
        
        if doc['organization_id'] != user['organization_id']:
            steps['overall'] = 'access_denied'
            return jsonify(steps), 403
        
        # Step 4: Check blob exists (metadata only, no download)
        t0 = time.perf_counter()
        from src.services import get_storage_service
        storage = get_storage_service()
        steps['storage_available'] = storage.is_available()
        
        if storage.is_available() and doc.get('blob_url'):
            try:
                meta = storage.get_document_metadata(doc['blob_url'])
                steps['blob_check'] = {
                    'ok': meta is not None,
                    'ms': round((time.perf_counter() - t0) * 1000),
                    'size': meta.get('size') if meta else None,
                    'content_type': meta.get('content_type') if meta else None,
                    'blob_path_used': doc['blob_url'],
                }
            except Exception as blob_err:
                steps['blob_check'] = {
                    'ok': False,
                    'ms': round((time.perf_counter() - t0) * 1000),
                    'error': str(blob_err),
                    'blob_path_used': doc['blob_url'],
                }
        
        steps['overall'] = 'all_checks_passed' if steps.get('blob_check', {}).get('ok') else 'blob_issue'
        return jsonify(steps), 200
        
    except Exception as e:
        steps['error'] = str(e)
        steps['overall'] = 'exception'
        logger.error(f"Diagnostic error for document {document_id}: {e}", exc_info=True)
        return jsonify(steps), 500


@documents_bp.route('/<document_id>/sas-url', methods=['GET'])
@require_auth
def get_document_sas_url(document_id: str):
    """Get a SAS URL for direct document access"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    if not doc.get('blob_url') or doc['blob_url'] == 'PENDING':
        return jsonify({'error': 'Document is still being processed (PENDING)'}), 404
    
    try:
        from src.services import get_storage_service
        storage = get_storage_service()
        
        if not storage.is_available():
            return jsonify({'error': 'Storage service not available'}), 503
        
        blob_path = doc['blob_url']
        
        # Resolve the actual blob path — handles plugin filename sanitisation mismatches
        resolved_path, was_corrected = storage.resolve_blob_path(blob_path)
        if was_corrected:
            logger.info(f"Auto-correcting blob_url for document {document_id}: '{blob_path}' → '{resolved_path}'")
            try:
                db.update_document(document_id, blob_url=resolved_path)
            except Exception as upd_err:
                logger.warning(f"Failed to update corrected blob_url in DB: {upd_err}")
            blob_path = resolved_path
        
        expiry_hours = request.args.get('expiry_hours', 24, type=int)
        sas_url = storage.generate_sas_url(blob_path, expiry_hours=expiry_hours)
        
        return jsonify({
            'url': sas_url,
            'expires_in_hours': expiry_hours
        })
    except Exception as e:
        logger.error(f"Error generating SAS URL for document {document_id}: {e}")
        return jsonify({'error': 'Failed to generate access URL'}), 500


@documents_bp.route('/<document_id>/analysis', methods=['GET'])
@require_auth
def get_document_analysis(document_id: str):
    """Get analysis results for a document"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Get latest analysis run for this document
    analysis = db.get_document_analysis(document_id)
    
    if not analysis:
        return jsonify({
            'analysis': None,
            'message': 'No analysis available'
        })
    
    return jsonify({
        'analysis_id': analysis['id'],
        'status': analysis['status'],
        'started_at': analysis.get('started_at'),
        'completed_at': analysis.get('completed_at'),
        'result': analysis.get('analysis_payload')
    })


@documents_bp.route('/<document_id>/analyze', methods=['POST'])
@require_auth
def trigger_document_analysis(document_id: str):
    """Trigger analysis for a specific document"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Create async job for analysis
    job_id = str(uuid.uuid4())
    job = db.create_async_job(
        job_id=job_id,
        job_type='document_analysis',
        entity_id=document_id,
        entity_type='document',
        created_by=user['id'],
        org_id=user['organization_id']
    )
    
    # Update document status to processing
    db.update_document(document_id, status_name='processing')
    
    logger.info(f"Triggered analysis for document {document_id}, job {job_id}")
    
    return jsonify({
        'message': 'Analysis started',
        'job_id': job_id
    }), 202


@documents_bp.route('/<document_id>/requests', methods=['GET'])
@require_auth
def get_document_requests(document_id: str):
    """Get requests that include this document"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    linked_requests = db.get_requests_for_document(
        document_id=document_id,
        org_id=user['organization_id']
    )
    
    return jsonify({
        'requests': linked_requests
    })


@documents_bp.route('/<document_id>', methods=['PUT'])
@require_auth
def update_document(document_id: str):
    """Update document metadata"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json() or {}
    
    # Currently no updatable fields exposed
    # This endpoint is here for future extensibility
    
    return jsonify(doc)


@documents_bp.route('/<document_id>/reprocess', methods=['POST'])
@require_auth
def reprocess_document(document_id: str):
    """
    Reprocess a document - triggers new analysis
    Useful when analysis failed or needs to be re-run
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Reset document status
    db.update_document(document_id, status_name='uploaded', user_id=user['id'])
    
    # Create async job for reprocessing
    job_id = str(uuid.uuid4())
    job = db.create_async_job(
        job_id=job_id,
        job_type='document_analysis',
        entity_id=document_id,
        entity_type='document',
        created_by=user['id'],
        org_id=user['organization_id']
    )
    
    logger.info(f"Reprocessing document {document_id}, job {job_id}")
    
    return jsonify({
        'success': True,
        'job_id': job_id,
        'message': 'Document reprocessing started'
    })


@documents_bp.route('/<document_id>', methods=['DELETE'])
@require_auth
def delete_document(document_id: str):
    """Delete a document"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Delete from storage if available
    if doc.get('blob_url'):
        try:
            from src.services import get_storage_service
            storage = get_storage_service()
            
            if storage.is_available():
                storage.delete_document(doc['blob_url'])
        except Exception as e:
            logger.warning(f"Failed to delete document from storage: {e}")
    
    db.delete_document(document_id, user_id=user['id'])
    
    logger.info(f"Deleted document {document_id} by user {user['id']}")
    
    return '', 204
