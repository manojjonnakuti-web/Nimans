"""
Templates API
API for managing templates and template fields
"""

from flask import Blueprint, request, jsonify, g
from datetime import datetime
import uuid
import logging
import json

from src.auth import require_auth, ensure_user_exists, subscription_before_request
from src.repositories import get_database_repository
from src.services.azure_service import get_azure_client

logger = logging.getLogger(__name__)

templates_bp = Blueprint('templates', __name__, url_prefix='/api/templates')
templates_bp.before_request(subscription_before_request)


@templates_bp.route('', methods=['GET'])
@require_auth
def list_templates():
    """
    List all templates for the current user's organization
    Only returns active templates by default
    Query params:
        - include_inactive: Include inactive templates (default: false)
        - has_analyzer: Only return templates with an analyzer configured (default: false)
        - include_fields: Include template fields in response (default: false)
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    # Parse query parameters
    include_inactive = request.args.get('include_inactive', 'false').lower() == 'true'
    has_analyzer = request.args.get('has_analyzer', 'false').lower() == 'true'
    include_fields = request.args.get('include_fields', 'false').lower() == 'true'
    
    templates = db.list_templates(
        org_id=user['organization_id'],
        include_inactive=include_inactive,
        include_fields=include_fields
    )
    
    # Filter to only templates with analyzers if requested
    if has_analyzer:
        templates = [t for t in templates if t.get('analyzer_id')]
    
    return jsonify({
        'templates': templates
    })


@templates_bp.route('/categories', methods=['GET'])
@require_auth
def get_field_categories():
    """Get available template field categories"""
    db = get_database_repository()
    ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    categories = db.get_field_categories()
    return jsonify({'categories': categories})


@templates_bp.route('/<template_id>', methods=['GET'])
@require_auth
def get_template(template_id: str):
    """Get a single template with all its fields"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404
    
    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    return jsonify(template)


@templates_bp.route('/<template_id>/fields', methods=['GET'])
@require_auth
def get_template_fields(template_id: str):
    """
    Get all active fields for a template
    Optionally filter by category
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404
    
    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Parse query parameters
    category = request.args.get('category')
    include_inactive = request.args.get('include_inactive', 'false').lower() == 'true'
    
    fields = db.get_template_fields(
        template_id=template_id,
        category_name=category,
        include_inactive=include_inactive
    )
    
    return jsonify({
        'template_id': template_id,
        'fields': fields,
        'total': len(fields)
    })


@templates_bp.route('', methods=['POST'])
@require_auth
def create_template():
    """Create a new template"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    data = request.get_json() or {}
    
    if not data.get('name'):
        return jsonify({'error': 'name is required'}), 400
    
    template_id = str(uuid.uuid4())
    created = db.create_template(
        template_id=template_id,
        org_id=user['organization_id'],
        name=data['name'],
        description=data.get('description'),
        user_id=user['id']
    )
    
    # Set the creation method metadata (column may not exist if migration hasn't run)
    creation_method = data.get('creation_method', 'manual')
    try:
        db.update_template(
            template_id=template_id,
            creation_method=creation_method,
        )
    except Exception:
        pass
    
    logger.info(f"Created template {template_id} by user {user['id']} (method={creation_method})")
    
    return jsonify(created), 201


@templates_bp.route('/<template_id>', methods=['PUT'])
@require_auth
def update_template(template_id: str):
    """Update a template"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404
    
    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json() or {}
    
    db.update_template(
        template_id=template_id,
        name=data.get('name'),
        description=data.get('description'),
        is_active=data.get('is_active'),
        user_id=user['id'],
        source_documents=data.get('source_documents'),
        creation_prompt=data.get('creation_prompt'),
        creation_method=data.get('creation_method'),
        allow_reprocessing=data.get('allow_reprocessing'),
    )
    
    updated = db.get_template(template_id)
    return jsonify(updated)


@templates_bp.route('/<template_id>', methods=['DELETE'])
@require_auth
def delete_template(template_id: str):
    """
    Delete a template: soft-deletes the template, all its fields,
    and unlinks/deactivates the associated analyzer (if any).
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404

    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403

    errors = []
    fields_deleted = 0
    analyzer_unlinked = False

    # 1) Soft-delete all fields
    try:
        fields = db.get_template_fields(template_id=template_id, include_inactive=False)
        for field in fields:
            db.update_template_field(field_id=field['id'], is_active=False)
            fields_deleted += 1
    except Exception as e:
        logger.error(f"Error deactivating fields for template {template_id}: {e}", exc_info=True)
        errors.append(f"field deactivation: {str(e)}")

    # 2) Deactivate linked analyzer (if any) and delete from Azure CU
    analyzer_id = template.get('analyzer_id')
    if analyzer_id:
        try:
            # Delete from Azure CU studio first
            analyzer_record = db.get_analyzer(analyzer_id)
            if analyzer_record:
                azure_id = analyzer_record.get('azure_analyzer_id')
                if azure_id:
                    try:
                        azure_client = get_azure_client()
                        if azure_client.is_available():
                            azure_client.delete_custom_analyzer(azure_id)
                            logger.info(f"Deleted CU analyzer '{azure_id}' during template deletion")
                    except Exception as cu_e:
                        logger.warning(f"Could not delete CU analyzer '{azure_id}': {cu_e}")
            # Soft-delete DB record
            db.delete_analyzer(analyzer_id)
            analyzer_unlinked = True
        except Exception as e:
            logger.error(f"Error deactivating analyzer {analyzer_id} for template {template_id}: {e}", exc_info=True)
            errors.append(f"analyzer deactivation: {str(e)}")

    # 3) Soft-delete the template itself
    try:
        db.update_template(template_id=template_id, is_active=False)
    except Exception as e:
        logger.error(f"Error deactivating template {template_id}: {e}", exc_info=True)
        errors.append(f"template deactivation: {str(e)}")
        # This is the critical step — if this fails, return 500
        return jsonify({'error': f'Failed to delete template: {str(e)}'}), 500

    logger.info(f"Deleted template {template_id} ({fields_deleted} fields) by user {user['id']}")
    result = {
        'message': 'Template deleted successfully',
        'fields_deleted': fields_deleted,
        'analyzer_unlinked': analyzer_unlinked
    }
    if errors:
        result['warnings'] = errors
    return jsonify(result)


@templates_bp.route('/<template_id>/build-analyzer', methods=['POST'])
@require_auth
def build_template_analyzer(template_id: str):
    """
    Build a custom Azure Content Understanding analyzer from template fields,
    then link it to the template.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404

    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403

    fields = db.get_template_fields(template_id=template_id, include_inactive=False)
    if not fields:
        return jsonify({'error': 'Template has no active fields. Add fields before building an analyzer.'}), 400

    azure_client = get_azure_client()
    if not azure_client.is_available():
        return jsonify({'error': 'Azure Content Understanding is not configured'}), 503

    data = request.get_json() or {}

    # ── Clean up the old analyzer before creating a new one ──
    old_analyzer_id = template.get('analyzer_id')
    if old_analyzer_id:
        old_analyzer = db.get_analyzer(old_analyzer_id)
        if old_analyzer:
            old_azure_id = old_analyzer.get('azure_analyzer_id')
            # Delete from Azure CU studio
            if old_azure_id:
                try:
                    azure_client.delete_custom_analyzer(old_azure_id)
                    logger.info(f"Deleted old CU analyzer '{old_azure_id}' for template {template_id}")
                except Exception as e:
                    logger.warning(f"Could not delete old CU analyzer '{old_azure_id}': {e}")
            # Soft-delete old DB record and unlink template
            try:
                db.delete_analyzer(old_analyzer_id)
                logger.info(f"Soft-deleted old analyzer record '{old_analyzer_id}'")
            except Exception as e:
                logger.warning(f"Could not soft-delete old analyzer '{old_analyzer_id}': {e}")

    # Generate stable but unique IDs
    analyzer_suffix = str(uuid.uuid4())[:8]
    analyzer_record_id = f"anl_{analyzer_suffix}"
    azure_analyzer_id = f"tmpl_{template_id}_{analyzer_suffix}"

    # Build CU field schema input from template fields
    # IMPORTANT: Always send field_type='text' to Azure CU — it extracts far more
    # reliably when all fields are typed as 'string'. Our normalization layer
    # handles date/number/percentage conversion post-extraction.
    cu_fields = []
    for f in fields:
        cu_fields.append({
            'field_name': f.get('field_name'),
            'display_name': f.get('display_name'),
            'field_type': 'text',  # Always text — Azure CU works best with string type
            'description': f.get('description') or f.get('display_name') or f.get('field_name'),
            'method': data.get('default_method', 'extract')
        })

    analyzer_name = data.get('name') or f"{template.get('name')} Analyzer"
    analyzer_description = data.get('description') or (
        f"Custom analyzer for template '{template.get('name')}' in organization {template.get('organization_id')}"
    )

    created_analyzer = None
    try:
        # 1) Create analyzer record in DB (inactive until Azure confirms)
        created_analyzer = db.create_analyzer(
            analyzer_id=analyzer_record_id,
            org_id=user['organization_id'],
            name=analyzer_name,
            description=analyzer_description,
            analyzer_type='azure_cu',
            azure_analyzer_id=azure_analyzer_id,
            configuration=json.dumps({
                'template_id': template_id,
                'field_count': len(cu_fields),
                'created_by': user['id'],
                'created_via': 'template_build',
            })
        )

        # 2) Create analyzer in Azure CU
        create_result = azure_client.create_custom_analyzer(
            analyzer_id=azure_analyzer_id,
            fields=cu_fields,
            description=analyzer_description,
            config={
                'returnDetails': True,
                'estimateFieldSourceAndConfidence': True,
                'enableOcr': True,
                'enableLayout': True,
            }
        )

        # 3) Poll operation until complete
        operation_location = create_result.get('operation_location')
        operation_payload = None
        if operation_location:
            operation_payload = azure_client.poll_operation(operation_location=operation_location, timeout_seconds=180)

        # 4) Mark analyzer active and link to template
        db.update_analyzer(
            analyzer_id=analyzer_record_id,
            is_active=True,
            configuration=json.dumps({
                'template_id': template_id,
                'field_count': len(cu_fields),
                'created_by': user['id'],
                'created_via': 'template_build',
                'operation_id': create_result.get('operation_id'),
                'operation_location': operation_location,
                'operation_status': (operation_payload or {}).get('status', 'succeeded')
            })
        )
        db.link_template_to_analyzer(template_id=template_id, analyzer_id=analyzer_record_id)

        updated_template = db.get_template(template_id)

        return jsonify({
            'message': 'Analyzer built successfully',
            'template': updated_template,
            'analyzer': db.get_analyzer(analyzer_record_id),
            'azure': {
                'azure_analyzer_id': azure_analyzer_id,
                'operation_id': create_result.get('operation_id'),
                'status': (operation_payload or {}).get('status', 'succeeded')
            }
        }), 201

    except Exception as e:
        logger.error(f"Failed to build analyzer for template {template_id}: {e}", exc_info=True)

        # Best effort: keep analyzer record but mark inactive and store error context
        if created_analyzer:
            db.update_analyzer(
                analyzer_id=analyzer_record_id,
                is_active=False,
                configuration=json.dumps({
                    'template_id': template_id,
                    'field_count': len(cu_fields),
                    'created_by': user['id'],
                    'created_via': 'template_build',
                    'error': str(e)
                })
            )

        return jsonify({
            'error': 'Failed to build analyzer',
            'details': str(e)
        }), 500


@templates_bp.route('/<template_id>/fields', methods=['POST'])
@require_auth
def add_template_field(template_id: str):
    """Add a field to a template"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404
    
    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json() or {}
    
    if not data.get('field_name'):
        return jsonify({'error': 'field_name is required'}), 400
    if not data.get('display_name'):
        return jsonify({'error': 'display_name is required'}), 400
    
    # Sanitize category_id: empty string → None
    category_id = data.get('category_id') or data.get('category') or None
    if category_id == '':
        category_id = None
    
    # Check ALL fields (including soft-deleted) for duplicate field_name
    all_fields = db.get_template_fields(template_id=template_id, include_inactive=True)
    existing_active = None
    existing_inactive = None
    for ef in all_fields:
        if ef.get('field_name') == data['field_name']:
            if ef.get('is_active'):
                existing_active = ef
            else:
                existing_inactive = ef
    
    # If an active field with this name exists, return 409
    if existing_active:
        return jsonify({'error': f"Field '{data['field_name']}' already exists in this template"}), 409
    
    # If a soft-deleted field with this name exists, reactivate it with new data
    if existing_inactive:
        try:
            db.update_template_field(
                field_id=existing_inactive['id'],
                display_name=data['display_name'],
                field_type=data.get('field_type', 'text'),
                category_id=category_id,
                is_required=False,
                extraction_is_required=False,
                sort_order=data.get('sort_order', 0),
                description=data.get('description'),
                validation_rules=data.get('validation_rules'),
                normalisation_instruction=data.get('normalisation_instruction'),
                is_active=True
            )
            # Fetch the updated field to return
            updated = db.get_template_field(existing_inactive['id'])
            logger.info(f"Reactivated soft-deleted field {existing_inactive['id']} ('{data['field_name']}') in template {template_id}")
            return jsonify(updated), 201
        except Exception as e:
            logger.error(f"Failed to reactivate field '{data['field_name']}' in template {template_id}: {e}", exc_info=True)
            return jsonify({'error': f"Failed to create field: {str(e)}"}), 500
    
    # No existing field — create new
    try:
        field_id = str(uuid.uuid4())
        created = db.create_template_field(
            field_id=field_id,
            template_id=template_id,
            field_name=data['field_name'],
            display_name=data['display_name'],
            field_type=data.get('field_type', 'text'),
            category_id=category_id,
            is_required=False,
            extraction_is_required=False,
            sort_order=data.get('sort_order', 0),
            description=data.get('description'),
            validation_rules=data.get('validation_rules'),
            normalisation_instruction=data.get('normalisation_instruction')
        )
        
        logger.info(f"Added field {field_id} to template {template_id}")
        return jsonify(created), 201
    except Exception as e:
        error_str = str(e)
        # Handle duplicate key constraint violation gracefully
        if 'UNIQUE' in error_str.upper() or 'duplicate key' in error_str.lower() or '2627' in error_str:
            logger.warning(f"Duplicate field '{data['field_name']}' in template {template_id} — already exists")
            return jsonify({'error': f"Field '{data['field_name']}' already exists in this template"}), 409
        logger.error(f"Failed to create field '{data['field_name']}' in template {template_id}: {e}", exc_info=True)
        return jsonify({'error': f"Failed to create field: {str(e)}"}), 500


@templates_bp.route('/<template_id>/fields', methods=['DELETE'])
@require_auth
def delete_all_template_fields(template_id: str):
    """Delete (deactivate) ALL fields in a template"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404
    
    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    fields = db.get_template_fields(template_id=template_id, include_inactive=False)
    count = 0
    for field in fields:
        db.update_template_field(field_id=field['id'], is_active=False)
        count += 1

    logger.info(f"Deleted all {count} fields from template {template_id}")
    return jsonify({'message': f'Deleted {count} fields', 'count': count})


@templates_bp.route('/<template_id>/import-fields', methods=['POST'])
@require_auth
def import_template_fields(template_id: str):
    """
    Bulk-import fields from a JSON file (e.g. catbond_analyzer_improved.json).
    Accepts JSON body with a 'fields' dict (field_name → { type, description, method })
    matching the Azure CU fieldSchema format.
    Optionally accepts 'clear_existing': true to delete all current fields first.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404

    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403

    data = request.get_json() or {}
    fields_dict = data.get('fields', {})

    if not fields_dict or not isinstance(fields_dict, dict):
        return jsonify({'error': 'Request body must contain a "fields" dictionary'}), 400

    # Optionally clear existing fields first
    if data.get('clear_existing'):
        existing = db.get_template_fields(template_id=template_id, include_inactive=False)
        for ef in existing:
            db.update_template_field(field_id=ef['id'], is_active=False)
        logger.info(f"Cleared {len(existing)} existing fields from template {template_id}")

    # Get existing field names (including inactive) for duplicate/reactivation handling
    all_fields = db.get_template_fields(template_id=template_id, include_inactive=True)
    existing_map = {}
    for ef in all_fields:
        existing_map[ef['field_name']] = ef

    added = 0
    skipped = 0
    reactivated = 0

    for field_name, field_def in fields_dict.items():
        if not field_name:
            continue

        # Auto-generate display name from PascalCase/camelCase field_name
        display_name = ''.join(
            (' ' + c if c.isupper() and i > 0 and not field_name[i-1].isupper() else c)
            for i, c in enumerate(field_name)
        ).strip()

        cu_type = field_def.get('type', 'string') if isinstance(field_def, dict) else 'text'
        description = field_def.get('description', '') if isinstance(field_def, dict) else ''

        # Map CU types to our field types
        type_map = {'string': 'text', 'number': 'number', 'integer': 'number',
                     'date': 'date', 'boolean': 'boolean', 'array': 'text'}
        field_type = type_map.get(cu_type, 'text')

        existing = existing_map.get(field_name)

        if existing and existing.get('is_active'):
            skipped += 1
            continue

        if existing and not existing.get('is_active'):
            # Reactivate with new data
            try:
                db.update_template_field(
                    field_id=existing['id'],
                    display_name=display_name,
                    field_type=field_type,
                    description=description,
                    is_active=True
                )
                reactivated += 1
            except Exception as e:
                logger.error(f"Failed to reactivate field '{field_name}': {e}")
                skipped += 1
            continue

        # Create new field
        try:
            field_id = str(uuid.uuid4())
            db.create_template_field(
                field_id=field_id,
                template_id=template_id,
                field_name=field_name,
                display_name=display_name,
                field_type=field_type,
                description=description,
                is_required=False,
                extraction_is_required=False,
                sort_order=added,
            )
            added += 1
        except Exception as e:
            logger.error(f"Failed to import field '{field_name}': {e}")
            skipped += 1

    logger.info(f"Imported fields to template {template_id}: {added} added, {reactivated} reactivated, {skipped} skipped")
    return jsonify({
        'message': f'Imported {added + reactivated} fields',
        'added': added,
        'reactivated': reactivated,
        'skipped': skipped,
        'total': added + reactivated + skipped,
    })
    
    logger.info(f"Deleted all {count} fields from template {template_id}")
    return jsonify({'message': f'Deleted {count} fields', 'count': count})


@templates_bp.route('/<template_id>/fields/<field_id>', methods=['PUT'])
@require_auth
def update_template_field(template_id: str, field_id: str):
    """Update a template field"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404
    
    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    data = request.get_json() or {}
    
    db.update_template_field(
        field_id=field_id,
        display_name=data.get('display_name'),
        field_type=data.get('field_type'),
        category_id=data.get('category_id') or data.get('category'),
        is_required=False,
        extraction_is_required=False,
        is_active=data.get('is_active'),
        sort_order=data.get('sort_order'),
        description=data.get('description'),
        validation_rules=data.get('validation_rules'),
        normalisation_instruction=data.get('normalisation_instruction')
    )
    
    updated = db.get_template_field(field_id)
    return jsonify(updated)


@templates_bp.route('/<template_id>/fields/<field_id>', methods=['DELETE'])
@require_auth
def delete_template_field(template_id: str, field_id: str):
    """Delete (deactivate) a template field"""
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    template = db.get_template(template_id)
    if not template:
        return jsonify({'error': 'Template not found'}), 404
    
    if template['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403
    
    # Soft delete by setting is_active to False
    db.update_template_field(field_id=field_id, is_active=False)
    
    return jsonify({'message': 'Field deleted successfully'})


@templates_bp.route('/suggest-fields', methods=['POST'])
@require_auth
def suggest_fields():
    """
    AI-powered field suggestion: upload up to 5 sample PDFs and get back
    suggested template fields with names, types, and descriptions.
    
    Accepts multipart/form-data with:
        - files: One or more PDF files (required, max 5)
        - prompt: Optional user hint about what fields to look for
        - max_pages: Max pages to extract text from per file (default 30)
    """
    from src.services.openai_service import get_openai_service
    
    db = get_database_repository()
    ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database
    
    # Accept both 'files' (multi) and 'file' (legacy single) keys
    uploaded_files = request.files.getlist('files')
    if not uploaded_files:
        # Fallback for legacy single-file upload
        single = request.files.get('file')
        if single:
            uploaded_files = [single]
    
    if not uploaded_files:
        return jsonify({'error': 'No files uploaded. Send PDFs as multipart/form-data with key "files".'}), 400
    
    if len(uploaded_files) > 5:
        return jsonify({'error': 'Maximum 5 files allowed per request.'}), 400
    
    # Validate all files are PDFs
    for f in uploaded_files:
        if not f.filename:
            return jsonify({'error': 'One of the uploaded files has no filename'}), 400
        if not f.filename.lower().endswith('.pdf'):
            return jsonify({'error': f'Only PDF files are supported. "{f.filename}" is not a PDF.'}), 400
    
    user_prompt = request.form.get('prompt', '').strip()
    max_pages = int(request.form.get('max_pages', '50'))
    
    # Process each file: extract text and concatenate
    all_text_parts = []
    total_pages_all = 0
    pages_read_all = 0
    extraction_method = 'pypdf'
    source_filenames = []
    
    for file_idx, file in enumerate(uploaded_files):
        pdf_bytes = file.read()
        source_filenames.append(file.filename)
        
        if len(pdf_bytes) < 100:
            return jsonify({'error': f'File "{file.filename}" is too small — appears invalid'}), 400
        if len(pdf_bytes) > 50 * 1024 * 1024:
            return jsonify({'error': f'File "{file.filename}" is too large (max 50MB per file)'}), 400
        
        document_text = None
        total_pages = 0
        pages_to_read = 0

        # --- Try Azure CU prebuilt-layout for richer structured extraction ---
        try:
            from src.services.azure_service import get_azure_client
            azure_client = get_azure_client()
            if azure_client.is_available():
                logger.info(f"Running prebuilt-layout analysis on '{file.filename}' ({file_idx+1}/{len(uploaded_files)})...")
                cu_result = azure_client.analyze_pdf_with_prebuilt_layout(
                    pdf_bytes, timeout_seconds=120
                )
                structured_text = azure_client.format_prebuilt_layout_as_text(cu_result)
                if structured_text and len(structured_text.strip()) > 100:
                    document_text = structured_text
                    extraction_method = 'prebuilt-layout'
                    contents = cu_result.get('result', {}).get('contents', [])
                    total_pages = len(contents) or 1
                    pages_to_read = total_pages
                    logger.info(
                        f"Prebuilt-layout extracted {len(document_text)} chars "
                        f"from {total_pages} pages of '{file.filename}'"
                    )
        except Exception as cu_err:
            logger.warning(f"Prebuilt-layout failed for '{file.filename}', falling back to pypdf: {cu_err}")

        # --- Fallback: extract text using pypdf if CU didn't work ---
        if not document_text:
            try:
                from pypdf import PdfReader
                import io
                
                reader = PdfReader(io.BytesIO(pdf_bytes))
                total_pages = len(reader.pages)
                pages_to_read = min(total_pages, max_pages)
                
                text_parts = []
                for i in range(pages_to_read):
                    page_text = reader.pages[i].extract_text() or ''
                    if page_text.strip():
                        text_parts.append(f"--- Page {i+1} ---\n{page_text}")
                
                document_text = '\n\n'.join(text_parts)
                
                if len(document_text.strip()) < 50:
                    return jsonify({
                        'error': f'Could not extract readable text from "{file.filename}". '
                                 'It may be a scanned/image-based document.'
                    }), 400
                
                logger.info(
                    f"pypdf extracted {len(document_text)} chars from {pages_to_read}/{total_pages} pages "
                    f"of '{file.filename}'"
                )
            except Exception as e:
                logger.error(f"PDF text extraction failed for '{file.filename}': {e}", exc_info=True)
                return jsonify({'error': f'Failed to read PDF "{file.filename}": {str(e)}'}), 400
        
        # Add separator between documents
        if len(uploaded_files) > 1:
            all_text_parts.append(f"\n\n{'='*60}\n=== DOCUMENT {file_idx+1}: {file.filename} ===\n{'='*60}\n\n{document_text}")
        else:
            all_text_parts.append(document_text)
        
        total_pages_all += total_pages
        pages_read_all += pages_to_read
    
    # Concatenate all document texts
    combined_text = '\n'.join(all_text_parts)
    
    # Call OpenAI to suggest fields
    openai_service = get_openai_service()
    if not openai_service.is_available():
        return jsonify({'error': 'Azure OpenAI service not configured. Set AZURE_AI_AGENT_API_KEY in environment.'}), 503
    
    result = openai_service.suggest_fields_from_text(
        document_text=combined_text,
        user_prompt=user_prompt,
        page_count=total_pages_all
    )
    
    if not result.get('success'):
        error_msg = result.get('error', 'Unknown error')
        # Return 429 for rate limit errors so frontend can show a retry message
        if '429' in str(error_msg) or 'Too Many Requests' in str(error_msg):
            return jsonify({
                'error': 'Azure OpenAI is temporarily rate-limited. Please wait 30-60 seconds and try again.',
                'details': error_msg,
                'retryable': True
            }), 429
        return jsonify({
            'error': 'AI field suggestion failed',
            'details': error_msg
        }), 500
    
    return jsonify({
        'document_type': result.get('document_type', 'Unknown'),
        'document_summary': result.get('document_summary', ''),
        'fields': result.get('fields', []),
        'total_fields': result.get('total_fields', 0),
        'notes': result.get('notes', ''),
        'source_file': source_filenames[0] if len(source_filenames) == 1 else None,
        'source_files': source_filenames,
        'file_count': len(source_filenames),
        'pages_analyzed': pages_read_all,
        'total_pages': total_pages_all,
        'extraction_method': extraction_method,
        'cost': result.get('cost', {})
    })


@templates_bp.route('/restore-default-analyzer', methods=['POST'])
@require_auth
def restore_default_analyzer():
    """
    Re-register the default catbond analyzer in Azure CU from the
    catbond_analyzer_improved.json definition file.
    PUT is idempotent — this overwrites whatever is currently registered.
    """
    import os as _os
    
    db = get_database_repository()
    ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    azure_client = get_azure_client()
    if not azure_client.is_available():
        return jsonify({'error': 'Azure Content Understanding is not configured'}), 503

    # Find the JSON definition file (backend/catbond_analyzer_improved.json or repo root)
    possible_paths = [
        _os.path.join(_os.path.dirname(__file__), '..', '..', 'catbond_analyzer_improved.json'),  # backend/
        _os.path.join(_os.path.dirname(__file__), '..', '..', '..', 'catbond_analyzer_improved.json'),  # repo root
        '/app/catbond_analyzer_improved.json',  # Docker container
    ]
    json_path = None
    for p in possible_paths:
        if _os.path.isfile(p):
            json_path = p
            break
    if not json_path:
        return jsonify({'error': 'catbond_analyzer_improved.json not found'}), 404

    try:
        analyzer_def = json.load(open(json_path, 'r'))
        analyzer_id = analyzer_def['analyzerId']

        fields = []
        for name, field_def in analyzer_def.get('fieldSchema', {}).get('fields', {}).items():
            fields.append({
                'field_name': name,
                'field_type': field_def.get('type', 'string'),
                'description': field_def.get('description', name),
                'method': field_def.get('method', 'extract'),
            })

        create_result = azure_client.create_custom_analyzer(
            analyzer_id=analyzer_id,
            fields=fields,
            description=analyzer_def.get('description', ''),
            config=analyzer_def.get('config', {}),
        )

        # Poll for completion
        operation_location = create_result.get('operation_location')
        operation_payload = None
        if operation_location:
            operation_payload = azure_client.poll_operation(
                operation_location=operation_location, timeout_seconds=180
            )

        return jsonify({
            'message': f'Analyzer {analyzer_id} restored successfully',
            'analyzer_id': analyzer_id,
            'fields_count': len(fields),
            'status': (operation_payload or {}).get('status', 'succeeded'),
        })

    except Exception as e:
        logger.error(f"Failed to restore default analyzer: {e}", exc_info=True)
        return jsonify({'error': f'Failed to restore analyzer: {str(e)}'}), 500
