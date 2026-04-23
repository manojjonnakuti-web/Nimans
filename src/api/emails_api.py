"""
Emails API
Handles email ingestion and management
Maps to the old email ingest workflow but creates requests automatically
"""

from flask import Blueprint, request, jsonify, g
from datetime import datetime, timedelta
import uuid
import base64
import logging

from src.auth import require_auth, ensure_user_exists, subscription_before_request
from src.repositories import get_database_repository


def _parse_time_range(time_range: str):
    """Convert a time_range label to a datetime cutoff"""
    now = datetime.utcnow()
    if time_range == 'today':
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif time_range == 'week':
        return now - timedelta(days=7)
    elif time_range == 'month':
        return now - timedelta(days=30)
    return None

logger = logging.getLogger(__name__)

emails_bp = Blueprint('emails', __name__, url_prefix='/api/emails')
emails_bp.before_request(subscription_before_request)


@emails_bp.route('', methods=['GET'])
@require_auth
def list_emails():
    """List all emails for the current user's organization"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 25, type=int)
    status = request.args.get('status')
    search = request.args.get('search')
    time_range = request.args.get('time_range')
    date_from = _parse_time_range(time_range) if time_range else None
    
    emails, total = db.list_emails(
        org_id=user['organization_id'],
        status_name=status,
        search=search,
        date_from=date_from,
        page=page,
        per_page=per_page
    )
    
    return jsonify({
        'emails': emails,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page if per_page > 0 else 0
        }
    })


@emails_bp.route('/stats', methods=['GET'])
@require_auth
def email_stats():
    """Get email counts grouped by status"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    search = request.args.get('search')
    time_range = request.args.get('time_range')
    date_from = _parse_time_range(time_range) if time_range else None

    stats = db.get_email_stats(
        org_id=user['organization_id'],
        search=search,
        date_from=date_from
    )
    return jsonify(stats)


@emails_bp.route('/<email_id>', methods=['GET'])
@require_auth
def get_email(email_id: str):
    """Get a single email with details"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    email = db.get_email(email_id)
    if not email:
        return jsonify({'error': 'Email not found'}), 404
    
    if email['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    return jsonify(email)


@emails_bp.route('/ingest', methods=['POST'])
@require_auth
def ingest_email():
    """
    Ingest an email and create a request
    This is the primary entry point for email-based document extraction
    
    NEW FLOW ORDER:
    1. Create email record
    2. Create request record (with email_id link via emailrequests junction)
    3. Create documents with direct request_id link
    4. Upload to blob storage
    5. Update document blob_url
    6. Start async job
    
    Request body:
    {
        "subject": "Email subject",
        "sender": "sender@example.com",
        "body": "Email body text",
        "attachments": [
            {
                "filename": "document.pdf",
                "content_type": "application/pdf",
                "content_base64": "base64-encoded-content"
            }
        ],
        "auto_analyze": true
    }
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    data = request.get_json() or {}
    
    # Validate required fields
    if not data.get('subject'):
        return jsonify({'error': 'subject is required'}), 400
    if not data.get('sender'):
        return jsonify({'error': 'sender is required'}), 400
    
    # =====================================================
    # STEP 1: Create the email record FIRST
    # =====================================================
    created_email = db.create_email(
        email_id=None,  # Let the database generate the ID
        org_id=user['organization_id'],
        subject=data['subject'],
        body=data.get('body', ''),
        sender=data['sender'],
        ingested_by=user['id'],
        mailbox_email=data.get('mailbox_email'),
        is_shared_mailbox=bool(data.get('is_shared_mailbox', False))
    )
    email_id = created_email['id']
    logger.info(f"Step 1: Created email record {email_id}")
    
    # =====================================================
    # STEP 2: Create the request record (links via emailrequests junction)
    # =====================================================
    created_request = db.create_request(
        request_id=None,  # Let the database generate the ID
        org_id=user['organization_id'],
        email_id=email_id,  # This creates the emailrequests junction entry
        title=data['subject'],
        description=f"Request from email: {data['sender']}",
        created_by=user['id'],
        template_id=data.get('template_id')
    )
    request_id = created_request['id']
    logger.info(f"Step 2: Created request record {request_id} linked to email {email_id}")
    
    # =====================================================
    # STEP 3 & 4: Create documents with request_id, upload to blob
    # =====================================================
    documents = []
    attachments = data.get('attachments', [])
    
    # Try to get storage service
    storage = None
    try:
        from src.services import get_storage_service
        storage = get_storage_service()
    except Exception as e:
        logger.warning(f"Storage service not available: {e}")
    
    # Try to get PDF service for email body conversion
    pdf_service = None
    try:
        from src.services import get_pdf_service
        pdf_service = get_pdf_service()
    except Exception as e:
        logger.warning(f"PDF service not available: {e}")
    
    # Convert email body to PDF and add as first document
    email_body_doc_id = None
    email_body = (data.get('body') or '').strip()
    logger.info(f"PDF service available: {pdf_service is not None and pdf_service.is_available() if pdf_service else False}, body length: {len(email_body)}")
    if pdf_service and pdf_service.is_available() and email_body:
        try:
            # Parse received_at if provided
            received_at = None
            if data.get('received_at'):
                try:
                    received_at = datetime.fromisoformat(data['received_at'].replace('Z', '+00:00'))
                except:
                    pass
            
            # Create PDF from email content
            # Recipients may come as a list from the frontend - convert to string
            recipients_raw = data.get('recipients', '')
            if isinstance(recipients_raw, list):
                recipients_str = ', '.join(str(r) for r in recipients_raw)
            else:
                recipients_str = str(recipients_raw) if recipients_raw else ''
            
            pdf_content = pdf_service.create_email_pdf(
                subject=data['subject'],
                sender=data['sender'],
                body=data.get('body', ''),
                recipients=recipients_str,
                received_at=received_at
            )
            
            logger.info(f"PDF generation result: {'success, size=' + str(len(pdf_content)) + ' bytes' if pdf_content else 'FAILED - returned None'}")
            
            if pdf_content:
                # Use request_id for folder structure: requests/{request_id}/{filename}
                email_date = received_at if received_at else datetime.utcnow()
                email_body_filename = f"Email_{email_date.strftime('%Y-%m-%d')}.pdf"
                blob_url = f"requests/{request_id}/{email_body_filename}"
                
                # Create document record with direct request_id link
                email_body_doc = db.create_document(
                    doc_id=None,
                    org_id=user['organization_id'],
                    filename=email_body_filename,
                    blob_url=blob_url,
                    content_type='application/pdf',
                    file_size_bytes=len(pdf_content),
                    uploaded_by=user['id'],
                    document_type='email_body',
                    email_id=email_id,
                    request_id=request_id  # Direct link to request
                )
                email_body_doc_id = email_body_doc['id']
                
                # Upload to blob storage using request_id folder structure
                if storage and storage.is_available():
                    try:
                        actual_blob_url = storage.upload_document(
                            file_content=pdf_content,
                            filename=email_body_filename,
                            organization_id=user['organization_id'],
                            request_id=str(request_id),  # Use request_id for folder
                            content_type='application/pdf'
                        )
                        db.update_document(email_body_doc_id, blob_url=actual_blob_url)
                        email_body_doc['blob_url'] = actual_blob_url
                    except Exception as e:
                        logger.error(f"Error uploading email body PDF: {e}")
                
                documents.append(email_body_doc)
                logger.info(f"Step 3: Created email body document {email_body_doc_id} with request_id={request_id}")
        except Exception as e:
            logger.error(f"Error creating email body PDF: {e}", exc_info=True)
    else:
        logger.warning(f"Skipping email body PDF: pdf_service={'available' if pdf_service and pdf_service.is_available() else 'unavailable'}, body_length={len(email_body)}")
    
    # Process attachments
    for i, attachment in enumerate(attachments):
        filename = attachment.get('filename', 'unknown')
        content_type = attachment.get('content_type', 'application/octet-stream')
        
        # Decode base64 content
        content_base64 = attachment.get('content_base64', '')
        try:
            content = base64.b64decode(content_base64) if content_base64 else b''
        except Exception as e:
            logger.error(f"Error decoding attachment {filename}: {e}")
            continue
        
        if not content:
            continue
        
        # Use request_id for folder structure with org prefix for tenant isolation
        blob_url = f"{user['organization_id']}/requests/{request_id}/{filename}"
        
        # Create document record with direct request_id link
        created_doc = db.create_document(
            doc_id=None,
            org_id=user['organization_id'],
            filename=filename,
            blob_url=blob_url,
            content_type=content_type,
            file_size_bytes=len(content),
            uploaded_by=user['id'],
            document_type='email_attachment',
            email_id=email_id,
            request_id=request_id  # Direct link to request
        )
        doc_id = created_doc['id']
        
        # Upload to blob storage using request_id folder structure
        if storage and storage.is_available():
            try:
                actual_blob_url = storage.upload_document(
                    file_content=content,
                    filename=filename,
                    organization_id=user['organization_id'],
                    request_id=str(request_id),  # Use request_id for folder
                    content_type=content_type
                )
                db.update_document(doc_id, blob_url=actual_blob_url)
                created_doc['blob_url'] = actual_blob_url
            except Exception as e:
                logger.error(f"Error uploading document {filename}: {e}")
        
        documents.append(created_doc)
        logger.info(f"Step 3: Created attachment document {doc_id} with request_id={request_id}")
    
    # =====================================================
    # STEP 5: Start async analysis if requested
    # =====================================================
    job_id = None
    if data.get('auto_analyze', True) and documents:
        job_id = str(uuid.uuid4())
        db.create_async_job(
            job_id=job_id,
            job_type='document_analysis',
            entity_id=request_id,
            entity_type='request',
            created_by=user['id'],
            org_id=user['organization_id']
        )
        
        # Update statuses to processing
        db.update_email_status(email_id, 'processing', user_id=user['id'])
        db.update_request_status(request_id, 'processing', user_id=user['id'])
        logger.info(f"Step 5: Created async job {job_id} for request {request_id}")
    
    logger.info(f"Email ingestion complete: email={email_id}, request={request_id}, documents={len(documents)}")
    
    return jsonify({
        'email_id': email_id,
        'request_id': request_id,
        'documents': [
            {
                'id': d['id'],
                'filename': d['filename'],
                'file_size_bytes': d.get('file_size_bytes')
            } for d in documents
        ],
        'job_id': job_id,
        'message': 'Email ingested successfully'
    }), 201


@emails_bp.route('/<email_id>/body-analysis', methods=['POST'])
@require_auth
def analyze_email_body(email_id: str):
    """
    Analyze the email body using OpenAI
    Used when the email body contains relevant data
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    email = db.get_email(email_id)
    if not email:
        return jsonify({'error': 'Email not found'}), 404
    
    if email['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    try:
        from src.services import get_openai_service
        openai = get_openai_service()
        
        if not openai.is_available():
            return jsonify({'error': 'OpenAI service not configured'}), 503
        
        # Analyze email body
        result = openai.analyze_email_body(email['subject'], email['body'])
        
        if result:
            return jsonify({
                'analysis': result,
                'message': 'Email body analyzed successfully'
            })
        else:
            return jsonify({'error': 'Analysis failed'}), 500
    except Exception as e:
        logger.error(f"Error analyzing email body: {e}")
        return jsonify({'error': 'Analysis service unavailable'}), 503


@emails_bp.route('/<email_id>/documents', methods=['GET'])
@require_auth
def get_email_documents(email_id: str):
    """Get documents attached to an email"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    email = db.get_email(email_id)
    if not email:
        return jsonify({'error': 'Email not found'}), 404
    
    if email['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Documents are already included in the email response
    return jsonify({
        'documents': email.get('documents', [])
    })


@emails_bp.route('/<email_id>/requests', methods=['GET'])
@require_auth
def get_email_requests(email_id: str):
    """Get requests created from an email"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    email = db.get_email(email_id)
    if not email:
        return jsonify({'error': 'Email not found'}), 404
    
    if email['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Get requests linked to this email
    requests_list, _ = db.list_requests(
        org_id=user['organization_id'],
        page=1,
        per_page=100
    )
    
    # Filter to only those linked to this email
    linked_requests = [r for r in requests_list if r.get('email_id') == email_id]
    
    return jsonify({
        'requests': linked_requests
    })


@emails_bp.route('/<email_id>', methods=['DELETE'])
@require_auth
def delete_email(email_id: str):
    """Delete an email"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    email = db.get_email(email_id)
    if not email:
        return jsonify({'error': 'Email not found'}), 404
    
    if email['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    db.delete_email(email_id, user_id=user['id'])
    
    logger.info(f"Deleted email {email_id} by user {user['id']}")
    
    return '', 204
