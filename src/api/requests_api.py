"""
Requests API
Primary API for request management (the aggregate root)
"""

from flask import Blueprint, request, jsonify, g
from datetime import datetime
import uuid
import logging

from src.auth import require_auth, ensure_user_exists, subscription_before_request
from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

requests_bp = Blueprint('requests', __name__, url_prefix='/api/requests')
requests_bp.before_request(subscription_before_request)


@requests_bp.route('', methods=['GET'])
@require_auth
def list_requests():
    """
    List all requests for the current user's organization
    Supports filtering and pagination
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    # Parse query parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 25, type=int)
    status = request.args.get('status')
    search = request.args.get('search')
    issuer = request.args.get('issuer')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    email_id = request.args.get('email_id', type=int)
    
    # Get requests with filters
    requests_list, total = db.list_requests(
        org_id=user['organization_id'],
        status_name=status,
        search=search,
        issuer=issuer,
        date_from=date_from,
        date_to=date_to,
        email_id=email_id,
        page=page,
        per_page=per_page
    )
    
    return jsonify({
        'requests': requests_list,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page if per_page > 0 else 0
        }
    })


@requests_bp.route('/<request_id>', methods=['GET'])
@require_auth
def get_request(request_id: str):
    """Get a single request with all related data"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    return jsonify(req)


@requests_bp.route('', methods=['POST'])
@require_auth
def create_request():
    """
    Create a new request
    This is the primary way to start the extraction workflow
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    data = request.get_json() or {}
    
    # Validate required fields
    if not data.get('title'):
        return jsonify({'error': 'title is required'}), 400
    
    # Create the request
    request_id = str(uuid.uuid4())
    created = db.create_request(
        request_id=request_id,
        org_id=user['organization_id'],
        title=data['title'],
        email_id=data.get('email_id'),
        template_id=data.get('template_id'),
        description=data.get('description'),
        created_by=user['id'],
        extraction_prompt=data.get('extraction_prompt')
    )
    
    # Link documents if provided
    document_ids = data.get('document_ids', [])
    for doc_id in document_ids:
        db.link_request_document(request_id, doc_id)
    
    logger.info(f"Created request {request_id} by user {user['id']}")
    
    return jsonify(created), 201


@requests_bp.route('/<request_id>', methods=['PUT'])
@require_auth
def update_request(request_id: str):
    """Update a request"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json() or {}
    
    # Update allowed fields
    db.update_request(
        request_id=request_id,
        title=data.get('title'),
        description=data.get('description'),
        issuer=data.get('issuer'),
        user_id=user['id']
    )
    
    updated = db.get_request(request_id)
    return jsonify(updated)


@requests_bp.route('/<request_id>/status', methods=['PUT'])
@require_auth
def update_request_status(request_id: str):
    """Update the status of a request"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json() or {}
    new_status = data.get('status')
    
    if not new_status:
        return jsonify({'error': 'status is required'}), 400
    
    success = db.update_request_status(
        request_id=request_id,
        status_name=new_status,
        user_id=user['id'],
        reason=data.get('reason')
    )
    
    if not success:
        return jsonify({'error': f'Invalid status: {new_status}'}), 400
    
    logger.info(f"Request {request_id} status changed to {new_status} by user {user['id']}")
    
    updated = db.get_request(request_id)
    return jsonify(updated)


@requests_bp.route('/<request_id>/documents', methods=['GET'])
@require_auth
def get_request_documents(request_id: str):
    """Get all documents linked to a request"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    documents = db.get_request_documents(request_id)
    
    return jsonify({
        'documents': documents
    })


@requests_bp.route('/<request_id>/upload', methods=['POST'])
@require_auth
def upload_document_to_request(request_id: str):
    """
    Upload a document directly to a request
    Documents are stored in: {organization_id}/requests/{request_id}/{filename}
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Check for file in request
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'No filename provided'}), 400
    
    # Read file content
    content = file.read()
    if not content:
        return jsonify({'error': 'Empty file'}), 400
    
    # Create document record with direct request_id link
    doc_id = str(uuid.uuid4())
    blob_url = f"{user['organization_id']}/requests/{request_id}/{file.filename}"
    
    created_doc = db.create_document(
        doc_id=doc_id,
        org_id=user['organization_id'],
        filename=file.filename,
        blob_url=blob_url,
        content_type=file.content_type or 'application/octet-stream',
        file_size_bytes=len(content),
        uploaded_by=user['id'],
        document_type='manual_upload',
        request_id=int(request_id) if str(request_id).isdigit() else request_id  # Direct link
    )
    
    # Use the actual auto-generated document ID from the database
    actual_doc_id = created_doc['id']
    
    # Upload to storage
    try:
        from src.services import get_storage_service
        storage = get_storage_service()
        
        if storage.is_available():
            actual_blob_url = storage.upload_document(
                file_content=content,
                filename=file.filename,
                organization_id=user['organization_id'],
                request_id=request_id,
                content_type=file.content_type
            )
            db.update_document(actual_doc_id, blob_url=actual_blob_url)
            created_doc['blob_url'] = actual_blob_url
    except Exception as e:
        logger.warning(f"Storage upload failed: {e}")
    
    # Link document to request using the real database ID
    db.link_request_document(request_id, actual_doc_id, source_type='upload')
    
    logger.info(f"Uploaded document {actual_doc_id} to request {request_id}: {file.filename}")
    
    return jsonify(created_doc), 201


@requests_bp.route('/<request_id>/analyze', methods=['POST'])
@require_auth
def analyze_request(request_id: str):
    """
    Run AI analysis on a request using ONLY the request_id.
    
    This endpoint:
    1. Gets all documents linked to the request (via document_request_id)
    2. Sends documents to AI for extraction
    3. Stores extracted fields in request_fields
    4. Updates request status
    
    No other parameters required - everything is retrieved from request_id.
    
    Returns:
        - job_id for async tracking, or
        - extracted fields if run synchronously
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Get documents directly linked to this request
    documents = req.get('documents', [])
    if not documents:
        # Also check via get_request_documents for backward compatibility
        documents = db.get_request_documents(request_id)
    
    if not documents:
        return jsonify({'error': 'No documents found for this request'}), 400
    
    # Check if async processing is requested
    run_async = request.args.get('async', 'true').lower() == 'true'
    
    if run_async:
        # Create async job - only need request_id
        job_id = str(uuid.uuid4())
        db.create_async_job(
            job_id=job_id,
            job_type='document_analysis',
            entity_type='request',
            entity_id=request_id,
            created_by=user['id'],
            org_id=user['organization_id']
        )
        
        db.update_request_status(request_id, 'processing')
        
        logger.info(f"Created analysis job {job_id} for request {request_id} with {len(documents)} documents")
        
        return jsonify({
            'message': 'Analysis started',
            'job_id': job_id,
            'request_id': request_id,
            'document_count': len(documents)
        }), 202
    else:
        # Run synchronously
        db.update_request_status(request_id, 'processing')
        
        try:
            from src.jobs.request_processor import RequestProcessor
            processor = RequestProcessor(db)
            
            # Create job for tracking
            job_id = str(uuid.uuid4())
            db.create_async_job(
                job_id=job_id,
                job_type='document_analysis',
                entity_type='request',
                entity_id=request_id,
                created_by=user['id'],
                org_id=user['organization_id']
            )
            
            # Process synchronously
            success = processor.process_job(job_id)
            
            if success:
                updated_request = db.get_request(request_id)
                return jsonify({
                    'message': 'Analysis completed successfully',
                    'request_id': request_id,
                    'fields': updated_request.get('fields', []),
                    'status': updated_request.get('status')
                })
            else:
                job = db.get_async_job(job_id)
                return jsonify({
                    'error': 'Analysis failed',
                    'details': job.get('error_message') if job else None
                }), 500
                
        except Exception as e:
            logger.error(f"Error analyzing request {request_id}: {e}")
            import traceback
            traceback.print_exc()
            db.update_request_status(request_id, 'failed')
            return jsonify({'error': str(e)}), 500


@requests_bp.route('/<request_id>/process', methods=['POST'])
@require_auth
def process_request(request_id: str):
    """
    Manually trigger document processing for a request (legacy endpoint)
    Use /analyze endpoint for new implementations
    This is useful for local development where background workers are disabled
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Check if request has documents
    documents = db.get_request_documents(request_id)
    if not documents:
        return jsonify({'error': 'No documents attached to request'}), 400
    
    # Update status to processing
    db.update_request_status(request_id, 'processing')
    
    try:
        from src.jobs.request_processor import RequestProcessor
        processor = RequestProcessor(db)
        
        # Create a mock job for processing
        job = {
            'id': str(uuid.uuid4()),
            'job_type': 'document_analysis',
            'entity_type': 'request',
            'entity_id': request_id,
            'status': 'running'
        }
        
        # Store the job
        db.create_async_job(
            job_id=job['id'],
            job_type=job['job_type'],
            entity_type=job['entity_type'],
            entity_id=job['entity_id'],
            created_by=user['id'],
            org_id=user['organization_id']
        )
        
        # Process synchronously (blocking)
        success = processor.process_job(job['id'])
        
        if success:
            updated = db.get_request(request_id)
            return jsonify({
                'message': 'Processing completed successfully',
                'request': updated
            })
        else:
            # Get the job for error details
            updated_job = db.get_async_job(job['id'])
            return jsonify({
                'error': 'Processing failed',
                'details': updated_job.get('error_message') if updated_job else None
            }), 500
            
    except Exception as e:
        logger.error(f"Error processing request {request_id}: {e}")
        import traceback
        traceback.print_exc()
        db.update_request_status(request_id, 'failed')
        return jsonify({'error': str(e)}), 500


@requests_bp.route('/<request_id>/documents', methods=['POST'])
@require_auth
def add_document_to_request(request_id: str):
    """Link a document to a request"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json() or {}
    document_id = data.get('document_id')
    
    if not document_id:
        return jsonify({'error': 'document_id is required'}), 400
    
    # Check document exists and belongs to org
    doc = db.get_document(document_id)
    if not doc:
        return jsonify({'error': 'Document not found'}), 404
    
    if doc['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Document access denied'}), 403
    
    success = db.link_request_document(
        request_id=request_id,
        doc_id=document_id,
        source_type=data.get('source_type', 'manual_upload')
    )
    
    if success:
        return jsonify({'message': 'Document linked successfully'}), 201
    else:
        return jsonify({'error': 'Failed to link document'}), 500


@requests_bp.route('/<request_id>/documents/<document_id>', methods=['DELETE'])
@require_auth
def remove_document_from_request(request_id: str, document_id: str):
    """Unlink a document from a request"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    success = db.unlink_request_document(request_id, document_id, user_id=user['id'])
    
    if not success:
        return jsonify({'error': 'Document not linked to request'}), 404
    
    return '', 204


@requests_bp.route('/<request_id>/versions', methods=['GET'])
@require_auth
def get_request_versions(request_id: str):
    """Get all versions of a request's extracted fields"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    versions = db.get_request_versions(request_id)
    
    return jsonify({
        'versions': versions
    })


@requests_bp.route('/<request_id>/versions', methods=['POST'])
@require_auth
def create_request_version(request_id: str):
    """Create a new version with updated field values"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json() or {}
    fields = data.get('fields', [])
    
    # Create new version
    version = db.create_request_version(
        request_id=request_id,
        version_label=data.get('version_label'),
        fields=fields,
        user_id=user['id']
    )
    
    # Set as current version if requested
    if data.get('set_current', True):
        db.set_current_version(request_id, version['id'], user_id=user['id'])
    
    return jsonify(version), 201


@requests_bp.route('/<request_id>/fields', methods=['GET'])
@require_auth
def get_request_fields(request_id: str):
    """Get the current version's fields for a request"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    fields = db.get_request_fields(request_id)
    
    return jsonify({
        'fields': fields
    })


@requests_bp.route('/<request_id>/fields', methods=['PUT'])
@require_auth
def update_request_fields(request_id: str):
    """
    Update fields in place (without creating a new version) and logs audit trail.
    Called when user clicks Save Changes or Publish button.
    This maintains field IDs and preserves audit log linkages.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json() or {}
    fields = data.get('fields', [])
    reason = data.get('reason')  # Optional reason for the change
    
    if not fields:
        return jsonify({'error': 'fields are required'}), 400
    
    # Get current fields BEFORE the update (for audit comparison)
    current_fields = db.get_request_fields(
        request_id, 
        version_id=req.get('current_version_id'),
        include_inactive=True
    )
    
    # Update fields in place (maintains field IDs)
    try:
        update_result = db.update_request_fields_in_place(
            request_id=request_id,
            fields=fields,
            user_id=user['id']
        )
        logger.info(f"Updated {update_result['updated_count']} fields, created {update_result['created_count']} new fields for request {request_id}")
    except Exception as e:
        logger.error(f"Failed to update request fields: {e}")
        return jsonify({'error': f'Failed to update fields: {str(e)}'}), 500
    
    # Get updated fields AFTER the change
    updated_fields = db.get_request_fields(request_id, include_inactive=True)
    
    # ===== AUDIT LOGGING =====
    # Log changes for each field
    try:
        from src.services import get_audit_service
        audit_service = get_audit_service()
        
        # Debug: Log what we're comparing
        logger.info(f"Audit comparison - old_fields count: {len(current_fields)}, new_fields count: {len(fields)}")
        if current_fields:
            logger.info(f"Sample old field keys: {list(current_fields[0].keys()) if current_fields else 'none'}")
        if fields:
            logger.info(f"Sample new field keys: {list(fields[0].keys()) if fields else 'none'}")
        
        # Log the field changes
        audit_log_ids = audit_service.log_request_field_changes(
            request_id=request_id,
            version_id=update_result['current_version_id'],
            old_fields=current_fields,
            new_fields=updated_fields,  # Use the actual updated fields from DB
            created_by=user['id'],
            reason=reason or 'User updated request fields'
        )
        
        logger.info(f"Created {len(audit_log_ids)} audit logs for request {request_id} field updates by user {user['id']}")
        
        # Also create a request-level audit log entry with the save reason
        # This links the reason to the request record for easy retrieval
        old_by_id = {f.get('id'): f for f in current_fields if f.get('id')}
        new_by_id = {f.get('id'): f for f in updated_fields if f.get('id')}
        changed_fields_summary = []
        for field_id, new_field in new_by_id.items():
            old_field = old_by_id.get(field_id)
            if old_field and old_field.get('field_value') != new_field.get('field_value'):
                changed_fields_summary.append(new_field.get('field_name', f'Field {field_id}'))
        
        if changed_fields_summary:
            request_audit_json = {
                'action': {
                    'new': 'fields_updated'
                },
                'fields_changed': {
                    'new': changed_fields_summary
                },
                'save_reason': {
                    'new': reason or 'No reason provided'
                }
            }
            
            audit_service.create_audit_log(
                entity_type='request',
                entity_id=request_id,
                action='UPDATE',
                audit_json=request_audit_json,
                created_by=str(user['id']),
                reason=reason or 'User updated request fields'
            )
            logger.info(f"Created request-level save audit log for request {request_id}")
    except Exception as e:
        # Don't fail the update if audit logging fails
        logger.error(f"Failed to create audit logs for request {request_id}: {e}")
    
    return jsonify({
        'version_id': update_result['current_version_id'],
        'fields': updated_fields,
        'updated_count': update_result['updated_count'],
        'created_count': update_result['created_count']
    })


# NOTE: The analyze endpoint is defined above as analyze_request()
# Do NOT add a duplicate route here.


@requests_bp.route('/<request_id>/normalise', methods=['POST'])
@require_auth
def normalise_request_fields(request_id: str):
    """
    AI-powered field normalisation.
    
    Builds a JSON payload from the request's extracted fields + their template
    normalisation instructions, sends it to an AI agent, and updates the
    normalised_value on each request field.
    
    Request body (optional):
        {
            "field_ids": [1, 2, 3]       // Normalise only these fields (default: all)
        }
    
    Response:
        {
            "normalised_fields": [ ... ],   // Full result array
            "normalised_count": 5,
            "skipped_count": 2
        }
    """
    from src.services.ai_normalisation_service import get_ai_normalisation_service

    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404

    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403

    # Get the request's extracted fields (ALL fields including alternatives)
    fields = db.get_request_fields(request_id, include_inactive=True)
    if not fields:
        return jsonify({'error': 'No fields found for this request'}), 404

    data = request.get_json() or {}
    filter_ids = data.get('field_ids')  # optional subset

    # Build the normalisation payload — one entry per field (keyed by field id)
    from src.services.ai_normalisation_service import build_datatype_instruction

    normalisation_payload = []

    for f in fields:
        if filter_ids and f['id'] not in filter_ids:
            continue

        # Get the normalisation instruction from the template field
        instruction = None
        data_type = None
        field_values = None
        if f.get('template_field'):
            tf = f['template_field']
            instruction = tf.get('normalisation_instruction')
            data_type = tf.get('data_type') or tf.get('field_type')
            field_values = tf.get('field_values')

        datatype_rule = build_datatype_instruction(data_type, field_values)

        normalisation_payload.append({
            'request_field_id': f['id'],
            'template_field_id': f.get('template_field_id'),
            'field_name': f.get('field_name'),
            'extracted_value': f.get('extracted_value') or f.get('field_value') or '',
            'data_type': data_type or 'text',
            'normalisation_instruction': instruction or '',
            'datatype_format_rule': datatype_rule,
            'normalised_value': ''
        })

    if not normalisation_payload:
        return jsonify({'error': 'No fields to normalise'}), 400

    # Call the AI normalisation service
    ai_service = get_ai_normalisation_service()
    normalised_fields = ai_service.normalise_fields(normalisation_payload)

    # Persist the normalised values back to the database (match by request_field_id)
    normalised_count = 0
    skipped_count = 0
    for nf in normalised_fields:
        normalised_value = nf.get('normalised_value', '')
        if not normalised_value:
            skipped_count += 1
            continue

        field_id = nf.get('request_field_id')
        if field_id:
            db.update_request_field_normalised_value(
                field_id=field_id,
                normalised_value=normalised_value
            )
            normalised_count += 1
        else:
            skipped_count += 1

    all_field_ids = [f['id'] for f in normalisation_payload if 'request_field_id' in f]
    active_count = sum(1 for f in fields if f.get('is_active') and f['id'] in all_field_ids)
    alt_count = len(normalisation_payload) - active_count
    logger.info(f"Normalised {normalised_count} fields ({active_count} active, {alt_count} alternatives) for request {request_id}")

    return jsonify({
        'normalised_fields': normalised_fields,
        'normalised_count': normalised_count,
        'skipped_count': skipped_count
    })


@requests_bp.route('/<request_id>/approve', methods=['POST'])
@require_auth
def approve_request(request_id: str):
    """Approve a request (mark as completed)"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Update status to approved (published)
    success = db.update_request_status(request_id, 'approved', user_id=user['id'])
    
    if not success:
        return jsonify({'error': 'Failed to complete request'}), 500
    
    logger.info(f"Request {request_id} approved/published by user {user['id']}")
    
    updated = db.get_request(request_id)
    return jsonify(updated)


@requests_bp.route('/<request_id>/cancel', methods=['POST'])
@require_auth
def cancel_request(request_id: str):
    """Cancel a request"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Get cancellation reason from request body
    data = request.get_json(silent=True) or {}
    reason = data.get('reason', '').strip() or None
    
    # Capture old status for audit logging
    old_status = req.get('status', 'unknown')
    
    # Update status to cancelled
    success = db.update_request_status(request_id, 'cancelled', user_id=user['id'], reason=reason)
    
    if not success:
        return jsonify({'error': 'Failed to cancel request'}), 500
    
    logger.info(f"Request {request_id} cancelled by user {user['id']}, reason: {reason}")
    
    # ===== AUDIT LOGGING =====
    try:
        from src.services import get_audit_service
        audit_service = get_audit_service()
        
        audit_json = {
            'status': {
                'old': old_status,
                'new': 'cancelled'
            },
            'cancellation_reason': {
                'new': reason or 'No reason provided'
            }
        }
        
        audit_service.create_audit_log(
            entity_type='request',
            entity_id=request_id,
            action='UPDATE',
            audit_json=audit_json,
            created_by=str(user['id']),
            reason=reason or 'Request cancelled'
        )
        logger.info(f"Created cancellation audit log for request {request_id}")
    except Exception as e:
        # Don't fail the cancellation if audit logging fails
        logger.error(f"Failed to create cancellation audit log for request {request_id}: {e}")
    
    updated = db.get_request(request_id)
    return jsonify(updated)


@requests_bp.route('/<request_id>', methods=['DELETE'])
@require_auth
def delete_request(request_id: str):
    """Delete a request"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    db.delete_request(request_id, user_id=user['id'])
    
    logger.info(f"Deleted request {request_id} by user {user['id']}")
    
    return '', 204


# ==========================================
# FIELD ALTERNATIVE VALUE ENDPOINTS
# ==========================================

@requests_bp.route('/<request_id>/fields/<field_name>/alternatives', methods=['GET'])
@require_auth
def get_field_alternatives(request_id: str, field_name: str):
    """
    Get all alternative (inactive) values for a specific field.
    Returns the active value and all inactive alternatives.
    Uses lightweight request lookup for fast auth check.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    # Use lightweight header-only query instead of full get_request
    req = db.get_request_header(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    version_id = request.args.get('version_id') or req.get('current_version_id')
    if not version_id:
        return jsonify({'error': 'No version found'}), 404
    
    # Single combined query for active + alternatives (instead of 2 separate calls)
    result = db.get_field_alternatives_with_active(request_id, version_id, field_name)
    
    return jsonify({
        'field_name': field_name,
        'active': result['active'],
        'alternatives': result['alternatives'],
        'total_alternatives': len(result['alternatives'])
    })


@requests_bp.route('/<request_id>/fields/<field_id>/set-active', methods=['POST'])
@require_auth
def set_field_active(request_id: str, field_id: str):
    """
    Set a specific field value as active.
    Used when user selects an alternative value for a field.
    The current active value becomes inactive, and the selected value becomes active.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request_header(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    result = db.set_field_as_active(field_id, user_id=user['id'])
    
    if not result:
        return jsonify({'error': 'Failed to set field as active'}), 400
    
    logger.info(f"Field {field_id} set as active by user {user['id']}")
    
    # Create audit log for alternative value selection
    try:
        from src.services import get_audit_service
        audit_service = get_audit_service()
        
        field_name = result.get('field_name', 'field')
        old_value = result.get('old_value')
        new_value = result.get('new_value')
        
        # Log on the old field: made inactive
        if result.get('old_field_id'):
            audit_service.create_audit_log(
                'request_field', result['old_field_id'], 'UPDATE',
                {field_name: {'old': old_value, 'new': 'Made inactive'}},
                str(user['id']),
                reason=f'Replaced by alternative value for {field_name}'
            )
        
        # Log on the new field: selected as alternative
        audit_service.create_audit_log(
            'request_field', field_id, 'UPDATE',
            {field_name: {'old': old_value, 'new': new_value}},
            str(user['id']),
            reason=f'Selected as alternative for {field_name}'
        )
    except Exception as e:
        logger.error(f"Failed to create audit log for alternative selection: {e}")
    
    return jsonify({'message': 'Field set as active', 'field_id': field_id})


# ==========================================
# AUDIT LOG ENDPOINTS
# ==========================================

@requests_bp.route('/<request_id>/audit-logs', methods=['GET'])
@require_auth
def get_request_audit_logs(request_id: str):
    """
    Get audit logs for a request and its fields.
    
    Query params:
        - entity_type: 'request' or 'request_field' (default: all)
        - field_id: Specific field ID to filter by
        - limit: Max number of records (default: 50)
    
    Returns:
        List of audit logs with change details
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    entity_type = request.args.get('entity_type')
    field_id = request.args.get('field_id')
    limit = request.args.get('limit', 50, type=int)
    
    try:
        from src.services import get_audit_service
        audit_service = get_audit_service()
        
        audit_logs = []
        
        if field_id:
            # Get logs for specific field
            logs = audit_service.get_audit_history('request_field', field_id, limit)
            audit_logs.extend(logs)
        elif entity_type == 'request':
            # Get logs for request only
            logs = audit_service.get_audit_history('request', request_id, limit)
            audit_logs.extend(logs)
        elif entity_type == 'request_field':
            # Get logs for all fields in this request (across all versions)
            audit_logs = db.get_audit_logs_for_request_fields(request_id, limit)
        else:
            # Get all audit logs (request + fields from all versions)
            request_logs = audit_service.get_audit_history('request', request_id, limit // 2)
            audit_logs.extend(request_logs)
            
            # Get audit logs for all fields across all versions
            field_logs = db.get_audit_logs_for_request_fields(request_id, limit)
            audit_logs.extend(field_logs)
            
            # Sort by created_at desc and limit
            audit_logs.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            audit_logs = audit_logs[:limit]
        
        # Enrich with change summaries for frontend display
        for log in audit_logs:
            log['changes'] = audit_service.get_field_change_summary(log.get('audit_json', {}))
        
        return jsonify({
            'audit_logs': audit_logs,
            'total': len(audit_logs)
        })
        
    except Exception as e:
        logger.error(f"Failed to get audit logs for request {request_id}: {e}")
        return jsonify({'error': 'Failed to retrieve audit logs'}), 500


@requests_bp.route('/<request_id>/fields/<field_id>/audit-logs', methods=['GET'])
@require_auth
def get_field_audit_logs(request_id: str, field_id: str):
    """
    Get audit logs for a specific request field.
    
    Returns:
        List of audit logs with change details for the field
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request_header(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    limit = request.args.get('limit', 50, type=int)
    
    try:
        from src.services import get_audit_service
        audit_service = get_audit_service()
        
        # Get audit logs for this field
        audit_logs = audit_service.get_audit_history('request_field', field_id, limit)
        
        # Enrich with change summaries
        for log in audit_logs:
            log['changes'] = audit_service.get_field_change_summary(log.get('audit_json', {}))
        
        return jsonify({
            'field_id': field_id,
            'audit_logs': audit_logs,
            'total': len(audit_logs)
        })
        
    except Exception as e:
        logger.error(f"Failed to get audit logs for field {field_id}: {e}")
        return jsonify({'error': 'Failed to retrieve audit logs'}), 500


@requests_bp.route('/<request_id>/fields-audit-summary', methods=['GET'])
@require_auth
def get_fields_audit_summary(request_id: str):
    """
    Batch check which fields have audit history.
    Returns a map of field_id -> true for fields that have been edited.
    Single query instead of N individual API calls per field.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    req = db.get_request_header(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    try:
        audit_map = db.get_fields_with_audit_history(request_id)
        return jsonify({
            'fields_with_history': audit_map
        })
    except Exception as e:
        logger.error(f"Failed to get fields audit summary for request {request_id}: {e}")
        return jsonify({'error': 'Failed to retrieve audit summary'}), 500


@requests_bp.route('/<request_id>/cost-details', methods=['GET'])
@require_auth
def get_request_cost_details(request_id: str):
    """
    Get AI processing cost breakdown for a request.
    Reads from the async_job result_data where cost_tracker is stored.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db

    req = db.get_request_header(request_id)
    if not req:
        return jsonify({'error': 'Request not found'}), 404
    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403

    # Find the most recent completed job for this request
    try:
        import json as _json
        from sqlalchemy import text as _text
        with db.get_session() as session:
            row = session.execute(
                _text("""
                    SELECT TOP 1
                        async_job_id,
                        async_job_result_data,
                        async_job_created_at,
                        async_job_modified_at
                    FROM async_jobs
                    WHERE async_job_entity_id = :req_id
                      AND async_job_entity_type = 'request'
                      AND async_job_result_data IS NOT NULL
                    ORDER BY async_job_created_at DESC
                """),
                {'req_id': request_id}
            ).first()

            if not row:
                return jsonify({'available': False, 'message': 'No processing data found for this request'}), 200

            result_data = row[1]
            if isinstance(result_data, str):
                result_data = _json.loads(result_data)

            # Build a clean cost summary for the frontend
            cost = {
                'available': True,
                'job_id': row[0],
                'processed_at': str(row[3] or row[2]),
                'total_pages': result_data.get('total_pages', 0),
                'total_documents': result_data.get('total_documents', 0),
                'processing_time_seconds': result_data.get('processing_time_seconds', 0),
                'grand_total_usd': result_data.get('grand_total_estimated_usd', 0),
                'operations': [],
                'per_document': result_data.get('per_document', []),
            }

            # Operation 1: Azure CU
            cu_cost = result_data.get('azure_cu_cost', 0)
            pages = result_data.get('total_pages', 0)
            if cu_cost > 0 or pages > 0:
                cost['operations'].append({
                    'name': 'Azure Content Understanding',
                    'description': 'Document content extraction + layout analysis + field extraction',
                    'service': 'Azure CU',
                    'details': {
                        'pages_processed': pages,
                        'non_text_files_cost': round(pages * 0.005, 4),
                        'layout_cost': round(pages * 0.005, 4),
                        'total_cost': cu_cost,
                    },
                    'cost_usd': cu_cost,
                })

            # Operation 2: OpenAI Email Analysis
            email_t = result_data.get('openai_email_tokens', {})
            if email_t.get('cost', 0) > 0 or email_t.get('input', 0) > 0:
                cost['operations'].append({
                    'name': 'Email Body Analysis',
                    'description': 'GPT-4.1 extracts fields directly from email text',
                    'service': 'Azure OpenAI GPT-4.1',
                    'details': {
                        'input_tokens': email_t.get('input', 0),
                        'output_tokens': email_t.get('output', 0),
                    },
                    'cost_usd': email_t.get('cost', 0),
                })

            # Operation 3: LLM Fallback
            llm_t = result_data.get('llm_fallback_tokens', {})
            if llm_t.get('cost', 0) > 0 or llm_t.get('input', 0) > 0:
                cost['operations'].append({
                    'name': 'LLM Fallback Extraction',
                    'description': 'GPT-4.1 fills fields that CU could not extract',
                    'service': 'Azure OpenAI GPT-4.1',
                    'details': {
                        'input_tokens': llm_t.get('input', 0),
                        'output_tokens': llm_t.get('output', 0),
                        'pending_fields': llm_t.get('pending_fields', 0),
                    },
                    'cost_usd': llm_t.get('cost', 0),
                })

            # Operation 4: AI Normalisation
            norm_t = result_data.get('azure_openai_normalisation_tokens', {})
            if norm_t.get('cost', 0) > 0 or norm_t.get('input', 0) > 0:
                cost['operations'].append({
                    'name': 'AI Field Normalisation',
                    'description': 'GPT-4.1 standardises date/currency/number formats',
                    'service': 'Azure OpenAI GPT-4.1',
                    'details': {
                        'input_tokens': norm_t.get('input', 0),
                        'output_tokens': norm_t.get('output', 0),
                        'fields_sent': norm_t.get('fields_sent', 0),
                    },
                    'cost_usd': norm_t.get('cost', 0),
                })

            # Total token summary
            total_input = (
                email_t.get('input', 0)
                + llm_t.get('input', 0)
                + norm_t.get('input', 0)
            )
            total_output = (
                email_t.get('output', 0)
                + llm_t.get('output', 0)
                + norm_t.get('output', 0)
            )
            cost['token_summary'] = {
                'total_input_tokens': total_input,
                'total_output_tokens': total_output,
                'total_tokens': total_input + total_output,
            }

            return jsonify(cost)

    except Exception as e:
        logger.error(f"Failed to get cost details for request {request_id}: {e}")
        return jsonify({'available': False, 'error': str(e)}), 500
