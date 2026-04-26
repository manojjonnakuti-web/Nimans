"""
Plugin API
Endpoints for the Outlook plugin integration.

The plugin calls these endpoints via service-principal auth to:
1. Classify a document and pick the best template/analyzer
2. Trigger extraction with the matched template
3. List available templates for the Advanced Settings dropdown

Flow:
  Plugin uploads files → Plugin calls POST /api/plugin/smart-analyze →
  Backend classifies doc → picks best template → triggers extraction →
  Returns classification result + job_id + webapp URL
"""

import logging
import os

from flask import Blueprint, request, jsonify, g

from src.auth import require_auth, ensure_user_exists, subscription_before_request
from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

plugin_bp = Blueprint('plugin', __name__, url_prefix='/api/plugin')
plugin_bp.before_request(subscription_before_request)

FRONTEND_URL = os.getenv('FRONTEND_URL', 'http://localhost:5173')

# ── Classification cache ──
# Key: (conversation_id, org_id) → classification result.
# Avoids re-classifying the same email conversation on retry.
# TTL: entries older than 10 minutes are evicted.
import time as _time
_classification_cache = {}  # { (conv_id, org_id): { 'result': {...}, 'ts': float } }
_CACHE_TTL_SECONDS = 600  # 10 minutes


def _get_cached_classification(conversation_id, org_id, template_count=None):
    """Return cached classification result or None."""
    if not conversation_id:
        return None
    key = (conversation_id, org_id)
    entry = _classification_cache.get(key)
    # Invalidate if template count changed (user added/deleted a template)
    if entry and template_count is not None and entry.get('template_count') != template_count:
        logger.info(f"🗂️ Classification cache INVALIDATED — template count changed ({entry.get('template_count')} → {template_count})")
        del _classification_cache[key]
        return None
    if entry and (_time.time() - entry['ts']) < _CACHE_TTL_SECONDS:
        logger.info(f"🗂️ Classification cache HIT for conversation {conversation_id[:30]}...")
        return entry['result']
    if entry:
        del _classification_cache[key]  # expired
    return None


def _set_cached_classification(conversation_id, org_id, result, template_count=None):
    """Cache a classification result."""
    if not conversation_id:
        return
    key = (conversation_id, org_id)
    _classification_cache[key] = {'result': result, 'ts': _time.time(), 'template_count': template_count}
    # Evict old entries if cache grows too large
    if len(_classification_cache) > 200:
        cutoff = _time.time() - _CACHE_TTL_SECONDS
        expired = [k for k, v in _classification_cache.items() if v['ts'] < cutoff]
        for k in expired:
            del _classification_cache[k]


@plugin_bp.route('/smart-analyze', methods=['POST'])
@require_auth
def smart_analyze():
    """
    Smart analyze endpoint for the Outlook plugin.

    Called AFTER the plugin has already created the request and uploaded
    documents (via the existing CreateRequest + UploadDocument flow).
    Replaces the old FinalizeRequest → /api/requests/{id}/analyze call.

    This endpoint:
    1. Downloads the first PDF from the request's documents
    2. Classifies it against the org's templates (GPT-4.1)
    3. If a match is found: updates the request's template_id
    4. If no match: keeps existing template (or falls back to default)
    5. Saves extraction_prompt if provided
    6. Triggers async analysis job
    7. Returns classification result + job_id + webapp link

    Accepts JSON:
    {
        "request_id": 123,                  // required
        "prompt": "Extract all ...",        // optional extraction prompt
        "template_id": 456,                 // optional override (skips classify)
        "force_new_analyzer": false          // optional: delete + rebuild analyzer
    }

    Returns:
    {
        "request_id": 123,
        "job_id": "uuid",
        "classification": {
            "document_type": "CAT Bond Offering Circular",
            "matched_template_id": 1,
            "matched_template_name": "CAT Bond Template",
            "confidence": 0.95,
            "recommendation": "use_template",
            "all_scores": [...]
        },
        "template_used": { "id": 1, "name": "...", "field_count": 74 },
        "webapp_url": "https://..."
    }
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    data = request.get_json() or {}
    request_id = data.get('request_id')
    if not request_id:
        return jsonify({'error': 'request_id is required'}), 400

    # --- 1. Validate request ---
    req = db.get_request(request_id)
    if not req:
        return jsonify({'error': f'Request {request_id} not found'}), 404

    if req['organization_id'] != user['organization_id']:
        return jsonify({'error': 'Access denied'}), 403

    # Get documents for this request
    documents = db.get_request_documents(request_id)
    if not documents:
        return jsonify({'error': 'No documents found for this request'}), 400

    extraction_prompt = data.get('prompt', '').strip() or None
    template_id_override = data.get('template_id')
    force_new_analyzer = data.get('force_new_analyzer', False)
    max_fields = data.get('max_fields')  # None = auto (40-60), or int 10-100
    if max_fields is not None:
        try:
            max_fields = max(10, min(100, int(max_fields)))
        except (ValueError, TypeError):
            max_fields = None

    # --- 2. Save extraction prompt if provided ---
    if extraction_prompt:
        try:
            from src.models import Request as RequestModel
            with db.get_session() as session:
                req_obj = session.query(RequestModel).filter_by(
                    request_id=int(request_id)
                ).first()
                if req_obj:
                    req_obj.request_extraction_prompt = extraction_prompt
                    logger.info(f"Saved extraction prompt for request {request_id}")
        except Exception as e:
            logger.warning(f"Failed to save extraction prompt: {e}")

    classify_only = data.get('classify_only', False)

    logger.info(
        f"🔍 SMART-ANALYZE INPUTS: request_id={request_id}, "
        f"prompt={'YES('+str(len(extraction_prompt))+' chars)' if extraction_prompt else 'NO'}, "
        f"template_id_override={template_id_override}, "
        f"classify_only={classify_only}, "
        f"force_new_analyzer={force_new_analyzer}, "
        f"doc_count={len(documents)}"
    )

    # --- 3. Determine template ---
    classification_result = None
    template_id = None

    if template_id_override:
        # User explicitly selected a template — skip classification
        template_id = int(template_id_override)
        logger.info(f"🔀 BRANCH: template_id_override → using template {template_id} for request {request_id}")
        classification_result = {
            'document_type': 'User-selected template',
            'matched_template_id': template_id,
            'confidence': 1.0,
            'recommendation': 'use_template',
            'skipped_reason': 'template_id provided by user'
        }

    elif force_new_analyzer and not template_id_override:
        # Force rebuild requested → skip classification entirely to save time.
        # Extract doc text, auto-create a brand-new template, and proceed.
        logger.info(f"🔀 BRANCH: force_new_analyzer → skipping classification, creating fresh template for request {request_id}")
        document_text = _extract_document_text(db, documents)
        if not document_text:
            return jsonify({'error': 'Could not extract text from document'}), 400

        # Quick doc-type detection (cheap — just first line of GPT response)
        doc_type_label = 'Document'
        try:
            from src.services.openai_service import get_openai_service
            openai_svc = get_openai_service()
            if openai_svc and openai_svc.is_available():
                quick_resp = openai_svc.client.chat.completions.create(
                    model=openai_svc.deployment,
                    messages=[
                        {"role": "system", "content": "Reply with ONLY the document type in 3-8 words. No explanation."},
                        {"role": "user", "content": f"What type of document is this?\n\n{document_text[:3000]}"}
                    ],
                    temperature=0, max_tokens=30,
                )
                doc_type_label = quick_resp.choices[0].message.content.strip().strip('"').strip("'")
                logger.info(f"Quick doc-type detection: '{doc_type_label}'")
        except Exception as e:
            logger.warning(f"Quick doc-type detection failed: {e}")

        auto_template = _auto_create_template(
            db, user['organization_id'], user['id'],
            doc_type_label, document_text, extraction_prompt,
            max_fields=max_fields
        )
        if auto_template:
            template_id = auto_template['id']
            classification_result = {
                'document_type': doc_type_label,
                'matched_template_id': template_id,
                'matched_template_name': auto_template['name'],
                'confidence': 1.0,
                'recommendation': 'force_created',
                'force_created': True,
                'all_scores': [],
            }
            logger.info(
                f"Force-created template '{auto_template['name']}' (id={template_id}) "
                f"with {auto_template['field_count']} fields"
            )
        else:
            classification_result = {
                'document_type': doc_type_label,
                'matched_template_id': None,
                'confidence': 0,
                'recommendation': 'creation_failed',
                'all_scores': [],
            }

    elif extraction_prompt and not classify_only:
        # Prompt given → create a brand-new template from the prompt + doc text.
        # Skip classification — the user wants specific output.
        logger.info(f"🔀 BRANCH: prompt-based creation for request {request_id} (prompt={extraction_prompt[:80]}...)")
        document_text = _extract_document_text(db, documents)
        if not document_text:
            return jsonify({'error': 'Could not extract text from document'}), 400

        auto_template = _auto_create_template(
            db, user['organization_id'], user['id'],
            'Custom Extraction', document_text, extraction_prompt,
            max_fields=max_fields
        )
        if auto_template:
            template_id = auto_template['id']
            classification_result = {
                'document_type': 'Custom extraction (from prompt)',
                'matched_template_id': template_id,
                'matched_template_name': auto_template['name'],
                'confidence': 1.0,
                'recommendation': 'prompt_created',
                'prompt_created': True,
                'all_scores': [],
            }
            logger.info(
                f"Prompt-created template '{auto_template['name']}' (id={template_id}) "
                f"with {auto_template['field_count']} fields"
            )
        else:
            classification_result = {
                'document_type': 'Unknown',
                'matched_template_id': None,
                'confidence': 0,
                'recommendation': 'creation_failed',
                'all_scores': [],
            }

    elif classify_only:
        # Classify-only mode: classify first, check how many templates match.
        logger.info(f"🔀 BRANCH: classify_only for request {request_id}")

        # Check classification cache first (skip cache if force rebuild requested)
        conv_id = req.get('ref')  # conversationId from Outlook
        tpl_count = len(db.list_templates(user['organization_id']) or [])
        cached = None if force_new_analyzer else _get_cached_classification(conv_id, user['organization_id'], template_count=tpl_count)
        if cached:
            classification_result = cached
        else:
            classification_result = _classify_request_document(
                db, request_id, documents, user['organization_id']
            )
            # Cache for repeat attempts
            cache_copy = {k: v for k, v in classification_result.items() if k != '_document_text'}
            _set_cached_classification(conv_id, user['organization_id'], cache_copy, template_count=tpl_count)

        # Count how many templates scored >= 0.5 (show selection only for strong matches)
        all_scores = classification_result.get('all_scores', [])
        good_matches = [s for s in all_scores if s.get('score', 0) >= 0.5]

        logger.info(f"🔢 Classify-only: {len(all_scores)} total scores, {len(good_matches)} good matches (>=0.5)")
        for s in all_scores:
            logger.info(f"   📊 {s.get('template_name','?')}: score={s.get('score',0):.2f}, id={s.get('template_id','?')}")

        if len(good_matches) <= 1:
            # 0 or 1 match: no need for user selection — proceed normally.
            template_id = classification_result.get('matched_template_id')
            logger.info(
                f"🔀 Classify-only: {len(good_matches)} match(es) → auto-proceeding (template_id={template_id})"
            )

            # If no match → auto-create template
            if not template_id and classification_result.get('recommendation') == 'no_match':
                doc_text = classification_result.get('_document_text')
                if not doc_text:
                    # Classification couldn't extract text (e.g. scanned PDF,
                    # recently-decrypted password-protected file) — retry with
                    # Azure CU which has OCR capabilities.
                    logger.info(f"No _document_text from classification — retrying with Azure CU")
                    doc_text = _extract_document_text(db, documents, use_azure_cu=True)
                if doc_text:
                    auto_template = _auto_create_template(
                        db, user['organization_id'], user['id'],
                        classification_result.get('document_type', 'Unknown'),
                        doc_text, extraction_prompt,
                        max_fields=max_fields
                    )
                    if auto_template:
                        template_id = auto_template['id']
                        classification_result['matched_template_id'] = template_id
                        classification_result['matched_template_name'] = auto_template['name']
                        classification_result['recommendation'] = 'auto_created'
                        classification_result['auto_created'] = True

            # Fall through to the normal extraction flow below
        else:
            # Multiple matches: return scores >= 15% to plugin for user selection.
            logger.info(f"🔀 Classify-only: {len(good_matches)} matches → returning for user selection")
            classification_result.pop('_document_text', None)
            # Filter out very low scores (< 15%) to reduce noise in the selection list
            filtered_scores = [s for s in all_scores if s.get('score', 0) >= 0.15]
            classification_result['all_scores'] = filtered_scores
            return jsonify({
                'request_id': int(request_id),
                'classification': classification_result,
                'classify_only': True,
            })

    else:
        # Full auto: classify + pick best + auto-create if no match
        logger.info(f"🔀 BRANCH: full-auto classify for request {request_id}")

        # Check classification cache first (skip cache if force rebuild requested)
        conv_id = req.get('ref')
        tpl_count = len(db.list_templates(user['organization_id']) or [])
        cached = None if force_new_analyzer else _get_cached_classification(conv_id, user['organization_id'], template_count=tpl_count)
        if cached:
            classification_result = cached
        else:
            classification_result = _classify_request_document(
                db, request_id, documents, user['organization_id']
            )
            cache_copy = {k: v for k, v in classification_result.items() if k != '_document_text'}
            _set_cached_classification(conv_id, user['organization_id'], cache_copy, template_count=tpl_count)
        template_id = classification_result.get('matched_template_id')

        # Auto-create template when no match found
        if (
            classification_result.get('recommendation') == 'no_match'
            and not template_id
        ):
            doc_text = classification_result.get('_document_text')
            if not doc_text:
                # Classification couldn't extract text (e.g. scanned PDF,
                # recently-decrypted password-protected file) — retry with
                # Azure CU which has OCR capabilities.
                logger.info(f"No _document_text from classification — retrying with Azure CU")
                doc_text = _extract_document_text(db, documents, use_azure_cu=True)
            if doc_text:
                auto_template = _auto_create_template(
                    db, user['organization_id'], user['id'],
                    classification_result.get('document_type', 'Unknown'),
                    doc_text, extraction_prompt,
                    max_fields=max_fields
                )
                if auto_template:
                    template_id = auto_template['id']
                    classification_result['matched_template_id'] = template_id
                    classification_result['matched_template_name'] = auto_template['name']
                    classification_result['recommendation'] = 'auto_created'
                    classification_result['auto_created'] = True
                    logger.info(
                        f"Auto-created template '{auto_template['name']}' (id={template_id}) "
                        f"with {auto_template['field_count']} fields for request {request_id}"
                    )

    # Strip internal fields before returning to client
    if classification_result:
        # ===== METERED BILLING: classification tokens → page-equivalents =====
        # GPT-4.1: $2/1M input, $8/1M output.  Page rate: $0.05/page.
        # page_equiv = token_cost_usd / 0.05
        cls_tokens = classification_result.pop('_classification_tokens', None)
        if cls_tokens and cls_tokens.get('total_tokens', 0) > 0:
            try:
                inp = cls_tokens.get('input_tokens', 0)
                out = cls_tokens.get('output_tokens', 0)
                cls_cost_usd = (inp * 2.0 + out * 8.0) / 1_000_000
                PAGE_RATE = 0.05
                page_equiv = round(cls_cost_usd / PAGE_RATE, 2)
                if page_equiv > 0:
                    from src.repositories import get_database_repository as _get_central_db
                    central_db = _get_central_db()
                    org_id = user['organization_id']
                    sub = central_db.get_active_subscription_for_org(org_id)
                    if sub:
                        central_db.record_metered_usage(
                            organization_id=org_id,
                            subscription_id=sub['id'],
                            dimension='pages_processed',
                            quantity=page_equiv,
                            request_id=request_id,
                        )
                        logger.info(
                            f"Classification cost ${cls_cost_usd:.4f} → {page_equiv} page-equiv "
                            f"(pages_processed) for org {org_id}, request {request_id}"
                        )
            except Exception as _mu_err:
                logger.warning(f"Failed to record classification metered usage: {_mu_err}")
        # ===== END METERED BILLING =====
        classification_result.pop('_document_text', None)

    # --- 3b. Force rebuild analyzer if requested ---
    if force_new_analyzer and template_id:
        try:
            t = db.get_template(template_id)
            if t and t.get('analyzer'):
                old_analyzer_id = t['analyzer'].get('id')
                if old_analyzer_id:
                    db.delete_analyzer(old_analyzer_id)
                    logger.info(
                        f"🔨 Force rebuild: deleted old analyzer '{old_analyzer_id}' "
                        f"for template {template_id} — job processor will rebuild"
                    )
        except Exception as e:
            logger.warning(f"Failed to delete old analyzer for force rebuild: {e}")

    # --- 4. Update request template if classification found a better match ---
    # Also handle the case where classification explicitly found NO match:
    # the C# plugin sets a default template_id (lowest active template) at
    # request creation time, but we must NOT use an unrelated template for
    # extraction when the document doesn't match any known type.
    classification_no_match = (
        classification_result
        and classification_result.get('recommendation') in ('no_match', 'creation_failed')
        and not template_id
    )

    if template_id and template_id != req.get('template_id'):
        # Classification/auto-create found a matching template — update the request
        try:
            from src.models import Request as RequestModel
            with db.get_session() as session:
                req_obj = session.query(RequestModel).filter_by(
                    request_id=int(request_id)
                ).first()
                if req_obj:
                    req_obj.request_template_id = int(template_id)
                    logger.info(
                        f"Plugin: updated request {request_id} template "
                        f"{req.get('template_id')} → {template_id}"
                    )
        except Exception as e:
            logger.warning(f"Failed to update request template: {e}")
    elif classification_no_match and req.get('template_id'):
        # Classification ran and found NO match — clear the default template
        # so extraction uses generic prebuilt-layout + LLM field discovery
        # instead of an unrelated template (e.g. catBond for a non-catBond doc).
        try:
            from src.models import Request as RequestModel
            with db.get_session() as session:
                req_obj = session.query(RequestModel).filter_by(
                    request_id=int(request_id)
                ).first()
                if req_obj:
                    old_tpl = req_obj.request_template_id
                    req_obj.request_template_id = None
                    logger.info(
                        f"Plugin: cleared default template {old_tpl} for request {request_id} "
                        f"(classification: {classification_result.get('recommendation')}) — "
                        f"extraction will use generic prebuilt-layout + LLM"
                    )
        except Exception as e:
            logger.warning(f"Failed to clear default template: {e}")

    # --- 5. Get template details for response ---
    template_used = None
    # Only fall back to the request's default template when classification
    # didn't explicitly reject all templates.  This prevents unrelated
    # default templates (set by the C# plugin at request creation) from
    # being forced onto documents that don't match any known type.
    if classification_no_match:
        final_template_id = None  # generic extraction via prebuilt-layout + LLM
    else:
        final_template_id = template_id or req.get('template_id')
    
    # Check if this is an email-only request (only email body docs, no real attachments)
    email_only_request = False
    if documents:
        non_email_docs = [d for d in documents if d.get('source_type') != 'email_body'
                          and not (d.get('document', {}).get('filename', '').lower().endswith('.eml')
                                   or d.get('document', {}).get('filename', '').lower().endswith('.msg'))]
        email_only_request = len(non_email_docs) == 0
    
    if email_only_request:
        # Email-only requests use OpenAI LLM dynamic extraction — don't show
        # the auto-assigned catbond template, it's irrelevant for email content.
        template_used = {
            'id': None,
            'name': 'OpenAI Email Extraction',
            'description': 'Dynamic field discovery from email content',
            'field_count': None,
            'has_analyzer': False,
        }
        logger.info(f"Email-only request {request_id} — template_used set to OpenAI Email Extraction")
    elif classification_no_match:
        # Classification found no matching template — extraction will use
        # prebuilt-layout OCR + LLM dynamic field discovery.
        template_used = {
            'id': None,
            'name': 'Dynamic Extraction',
            'description': 'No matching template found — using AI-powered dynamic field discovery',
            'field_count': None,
            'has_analyzer': False,
        }
        logger.info(
            f"No-match request {request_id} — template_used set to Dynamic Extraction "
            f"(prebuilt-layout + LLM)"
        )
    elif final_template_id:
        t = db.get_template(final_template_id)
        if t:
            template_used = {
                'id': t['id'],
                'name': t['name'],
                'description': t.get('description', ''),
                'field_count': len(t.get('fields') or []),
                'has_analyzer': bool(t.get('analyzer')),
            }

    # --- 6. Trigger async analysis ---
    # NOTE: create_async_job ignores the job_id param and uses auto-increment.
    # We capture the real DB integer ID from the returned dict.
    job_dict = db.create_async_job(
        job_id='plugin',
        job_type='document_analysis',
        entity_type='request',
        entity_id=str(request_id),
        created_by=user['id'],
        org_id=user['organization_id']
    )
    job_id = job_dict['id']  # real integer ID from DB
    db.update_request_status(str(request_id), 'processing')

    logger.info(
        f"Plugin smart-analyze: request={request_id}, template={final_template_id}, "
        f"job={job_id}, docs={len(documents)}, "
        f"classify_confidence={classification_result.get('confidence', 'N/A')}"
    )

    # --- 7. Build response ---
    webapp_url = f"{FRONTEND_URL}/requests/{request_id}"

    return jsonify({
        'request_id': int(request_id),
        'job_id': job_id,
        'classification': classification_result,
        'template_used': template_used,
        'document_count': len(documents),
        'extraction_prompt': extraction_prompt,
        'webapp_url': webapp_url,
    }), 202


@plugin_bp.route('/templates', methods=['GET'])
@require_auth
def list_templates_for_plugin():
    """
    List available templates for the plugin's Advanced Settings dropdown.

    Returns a lightweight list with id, name, description, field_count,
    and whether the template has a built analyzer.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    templates = db.list_templates(
        user['organization_id'],
        include_fields=True
    )

    result = []
    for t in templates:
        desc = t.get('description', '') or ''
        # Extract prompt from description if present
        prompt_used = None
        if 'Prompt: ' in desc:
            prompt_used = desc.split('Prompt: ', 1)[1]

        allow_reprocessing = bool(t.get('allow_reprocessing', True))

        result.append({
            'id': t['id'],
            'name': t['name'],
            'description': desc,
            'field_count': t.get('field_count', 0),
            'has_analyzer': bool(t.get('analyzer')),
            'prompt_used': prompt_used,
            # Duplicate handling policy for plugin UI:
            # allow_reprocessing=False => duplicates should be blocked for this template.
            'allow_reprocessing': allow_reprocessing,
            'block_duplicates': not allow_reprocessing,
            'prevent_reprocessing': not allow_reprocessing,
        })

    return jsonify({'templates': result})


@plugin_bp.route('/job-status/<job_id>', methods=['GET'])
@require_auth
def get_job_status(job_id):
    """
    Poll job status for the plugin progress indicator.

    Returns the current job state, progress percentage, and processing log
    so the plugin can show real-time extraction progress.
    """
    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    job = db.get_async_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404

    # result_data is already parsed by _job_to_dict (dict or None)
    result_data = job.get('result_data') or {}

    return jsonify({
        'job_id': job_id,
        'status': job.get('status', 'unknown'),
        'progress_percent': job.get('progress_percent', 0),
        'progress_message': job.get('progress_message', ''),
        'error_message': job.get('error_message'),
        'processing_log': result_data.get('processing_log', []),
        'cost': {
            'grand_total_estimated_usd': result_data.get('grand_total_estimated_usd', 0),
            'total_pages': result_data.get('total_pages', 0),
        }
    })


# =========================================================================
# Internal helpers
# =========================================================================

def _extract_text_fast_pypdf(pdf_bytes, max_pages=15):
    """
    Fast local text extraction using pypdf. No API calls.
    Good enough for classification — doesn't need perfect OCR layout.
    Returns extracted text or empty string.
    """
    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for i in range(min(len(reader.pages), max_pages)):
            text = reader.pages[i].extract_text() or ''
            if text.strip():
                parts.append(text)
        return '\n\n'.join(parts)
    except Exception as e:
        logger.warning(f"pypdf fast extraction failed: {e}")
        return ''


def _extract_document_text(db, documents, use_azure_cu=True):
    """
    Download the first PDF from the request's documents and extract its text.
    
    If use_azure_cu=True: Uses Azure CU prebuilt-layout first, falls back to pypdf.
    If use_azure_cu=False: Uses pypdf only (fast, local — for classification).

    Returns the extracted text string, or None on failure.
    """
    try:
        # 1. Find the first non-email PDF document
        pdf_doc = None
        for req_doc in documents:
            doc_id = req_doc.get('document_id') or req_doc.get('document', {}).get('id')
            doc = db.get_document(doc_id)
            if not doc:
                continue
            source_type = req_doc.get('source_type', '')
            filename = (doc.get('filename') or '').lower()
            if source_type == 'email_body' or filename.endswith(('.eml', '.msg')):
                continue
            if doc.get('blob_url') or doc.get('file_path'):
                pdf_doc = doc
                break

        if not pdf_doc:
            logger.info("No PDF document found for text extraction")
            return None

        # 2. Download PDF from blob
        blob_path = pdf_doc.get('file_path') or pdf_doc.get('blob_url', '')
        if not blob_path:
            return None

        from src.services import get_storage_service
        storage = get_storage_service()
        if not storage:
            logger.warning("Storage service not available")
            return None

        resolved_path, _ = storage.resolve_blob_path(blob_path)
        pdf_bytes = storage.download_document(resolved_path)
        if not pdf_bytes or len(pdf_bytes) < 100:
            return None

        logger.info(
            f"Downloaded {len(pdf_bytes)} bytes of '{pdf_doc.get('filename')}' "
            f"for text extraction (azure_cu={'ON' if use_azure_cu else 'OFF'})"
        )

        # 3. Extract text
        document_text = None

        # Fast path: pypdf only (for classification — no API call needed)
        if not use_azure_cu:
            document_text = _extract_text_fast_pypdf(pdf_bytes, max_pages=15)
            if document_text and len(document_text.strip()) >= 50:
                logger.info(f"pypdf extracted {len(document_text)} chars (fast mode for classification)")
                return document_text
            # Fall through to Azure CU if pypdf got nothing
            logger.info("pypdf got insufficient text, falling back to Azure CU")

        # Full path: Azure CU prebuilt-layout → pypdf fallback
        try:
            from src.services.azure_service import get_azure_client
            azure_client = get_azure_client()
            if azure_client and azure_client.is_available():
                cu_result = azure_client.analyze_pdf_with_prebuilt_layout(
                    pdf_bytes, timeout_seconds=60
                )
                document_text = azure_client.format_prebuilt_layout_as_text(cu_result)
        except Exception as e:
            logger.warning(f"Prebuilt-layout text extraction failed: {e}")

        if not document_text or len(document_text.strip()) < 50:
            try:
                from pypdf import PdfReader
                import io
                reader = PdfReader(io.BytesIO(pdf_bytes))
                parts = []
                for i in range(min(len(reader.pages), 10)):
                    text = reader.pages[i].extract_text() or ''
                    if text.strip():
                        parts.append(text)
                document_text = '\n\n'.join(parts)
            except Exception as e:
                logger.error(f"pypdf text extraction failed: {e}")
                return None

        if not document_text or len(document_text.strip()) < 50:
            logger.info("Not enough text extracted from document")
            return None

        return document_text

    except Exception as e:
        logger.error(f"Document text extraction failed: {e}", exc_info=True)
        return None


def _classify_request_document(db, request_id, documents, org_id):
    """
    Download the first PDF from a request's documents and classify it
    against the organization's templates using GPT-4.1.

    Returns a classification dict with matched_template_id, confidence, etc.
    Also includes '_document_text' internally for reuse by auto-create.
    """
    from src.services.openai_service import get_openai_service

    default_result = {
        'document_type': 'Unknown',
        'matched_template_id': None,
        'matched_template_name': None,
        'confidence': 0,
        'recommendation': 'no_match',
        'all_scores': [],
    }

    try:
        # 1. Extract text from document (pypdf — fast, no API call)
        import time as _time
        t0 = _time.time()
        document_text = _extract_document_text(db, documents, use_azure_cu=False)
        extract_ms = int((_time.time() - t0) * 1000)
        if not document_text:
            logger.info(f"No usable text for classification (request {request_id})")
            return default_result
        logger.info(f"Classification text extracted in {extract_ms}ms ({len(document_text)} chars) via pypdf")

        # 2. Get org templates
        templates = db.list_templates(org_id, include_fields=True)
        if not templates:
            logger.info(f"No templates for org {org_id} — classification skipped")
            return default_result

        # 3. Call GPT-4.1 classification
        openai_service = get_openai_service()
        if not openai_service or not openai_service.is_available():
            logger.warning("OpenAI not available — classification skipped")
            return default_result

        import json as _json

        template_summaries = []
        for t in templates:
            field_names = [f['field_name'] for f in (t.get('fields') or [])]
            # Send first 20 field names (enough for classification) + total count
            template_summaries.append({
                'id': t['id'],
                'name': t['name'],
                'description': (t.get('description', '') or '')[:200],  # trim long descriptions
                'field_count': len(field_names),
                'sample_fields': field_names[:20],
            })

        template_info = _json.dumps(template_summaries, indent=2)
        logger.info(f"Classification: {len(templates)} templates, {len(template_info)} chars of template JSON")
        # Safety cap — but 25k is enough for 20+ templates
        template_info = template_info[:25000]
        doc_excerpt = document_text[:15000]

        prompt = f"""You are a document classification expert. Your job is to determine which extraction template (if any) is the BEST match for this document.

CLASSIFICATION METHOD:
1. First identify what type of document this is (e.g., bond prospectus, employment contract, insurance policy, invoice, etc.)
2. For each template, check:
   a) Does the template name / description suggest it's for this document type?
   b) Do the template's field names appear as actual data points in the document?
   c) What percentage of the template's fields would have extractable values in this document?
3. Score based on field overlap — a template where 70%+ of fields match the document content scores 0.8+

SCORING RULES:
- 0.9-1.0: Template was clearly designed for this exact document type, nearly all fields present
- 0.7-0.89: Strong match, most fields present, same document category
- 0.5-0.69: Partial match, some fields overlap but not ideal
- 0.1-0.49: Weak match, few fields relevant
- 0.0: Completely unrelated

IMPORTANT: Be generous with scoring for templates that genuinely match. Don't under-score a good match.
If a template's fields like "IssuerName", "MaturityDate", "CouponRate" appear in a bond document, that's 0.85+.

TEMPLATES:
{template_info}

DOCUMENT EXCERPT:
---
{doc_excerpt}
---

Return JSON:
{{
    "best_match_template_id": <id or null if none match well>,
    "best_match_confidence": <0.0 to 1.0>,
    "document_type": "Brief description of what this document is",
    "scores": [
        {{
            "template_id": <id>,
            "template_name": "name",
            "score": 0.85,
            "reasoning": "Brief explanation of why this score — mention which fields match"
        }}
    ],
    "recommendation": "use_template" or "no_match"
}}"""

        import time
        try:
            response = openai_service.client.chat.completions.create(
                model=openai_service.deployment,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a document classification expert. Match documents "
                            "to extraction templates. Return valid JSON only."
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=2000,
            )
        except Exception as api_err:
            logger.error(f"Classification GPT call failed: {api_err}")
            return default_result

        content = response.choices[0].message.content
        result = _json.loads(content)

        # Track classification token usage
        classification_tokens = {
            'input_tokens': getattr(response.usage, 'prompt_tokens', 0) if response.usage else 0,
            'output_tokens': getattr(response.usage, 'completion_tokens', 0) if response.usage else 0,
        }
        classification_tokens['total_tokens'] = classification_tokens['input_tokens'] + classification_tokens['output_tokens']

        # 4. Build classification response
        best_id = result.get('best_match_template_id')
        best_conf = result.get('best_match_confidence', 0)
        matched_name = None

        if best_id and best_conf >= 0.7:
            for t in templates:
                if t['id'] == best_id:
                    matched_name = t['name']
                    break
        else:
            best_id = None

        logger.info(
            f"Classification: doc_type='{result.get('document_type')}', "
            f"match={matched_name} (id={best_id}, conf={best_conf:.2f})"
        )

        # Enrich scores with prompt_used from template descriptions
        scores = result.get('scores', [])
        template_lookup = {t['id']: t for t in templates}
        for s in scores:
            tid = s.get('template_id')
            t = template_lookup.get(tid)
            if t:
                desc = t.get('description', '') or ''
                if 'Prompt: ' in desc:
                    s['prompt_used'] = desc.split('Prompt: ', 1)[1]
                s['field_count'] = len(t.get('fields') or [])

        return {
            'document_type': result.get('document_type', 'Unknown'),
            'matched_template_id': best_id,
            'matched_template_name': matched_name,
            'confidence': best_conf,
            'recommendation': result.get('recommendation', 'no_match'),
            'all_scores': scores,
            '_document_text': document_text,  # internal: reused by auto-create
            '_classification_tokens': classification_tokens,  # internal: for metered billing
        }

    except Exception as e:
        logger.error(f"Classification failed for request {request_id}: {e}", exc_info=True)
        return default_result


def _auto_create_template(db, org_id, user_id, document_type, document_text, extraction_prompt=None, max_fields=None):
    """
    Auto-create a new template from the document when no existing template matches.

    Uses the two-pass AI suggest-fields flow to enumerate all data points,
    then creates a template + fields in the database.

    Returns a dict with { id, name, field_count } or None on failure.
    """
    from src.services.openai_service import get_openai_service

    try:
        openai_service = get_openai_service()
        if not openai_service or not openai_service.is_available():
            logger.warning("OpenAI not available — cannot auto-create template")
            return None

        # Clean up document_type for use as template name
        doc_type_clean = (document_type or 'Document').strip()
        if len(doc_type_clean) > 80:
            doc_type_clean = doc_type_clean[:80].rsplit(' ', 1)[0]

        template_name = f"{doc_type_clean} (Auto)"

        logger.info(
            f"Auto-creating template '{template_name}' from "
            f"{len(document_text)} chars of text"
        )

        # Use the existing two-pass suggest-fields pipeline
        suggest_result = openai_service.suggest_fields_from_text(
            document_text=document_text,
            user_prompt=extraction_prompt,
            max_fields=max_fields,
        )

        if not suggest_result.get('success') or not suggest_result.get('fields'):
            logger.warning(
                f"suggest_fields_from_text returned no fields: "
                f"{suggest_result.get('error', 'empty result')}"
            )
            return None

        fields = suggest_result['fields']
        logger.info(f"AI suggested {len(fields)} fields for auto-create template")

        # Build description including the prompt (if any) so users can
        # distinguish templates in the dropdown and selection UI.
        desc_parts = [f"Auto-created from {doc_type_clean} document."]
        desc_parts.append(f"{len(fields)} fields suggested by AI.")
        if extraction_prompt:
            # Keep first 200 chars of the prompt in the description
            prompt_preview = extraction_prompt[:200]
            if len(extraction_prompt) > 200:
                prompt_preview += '...'
            desc_parts.append(f"Prompt: {prompt_preview}")

        # Create the template
        template_dict = db.create_template(
            template_id=None,
            org_id=org_id,
            name=template_name,
            description=' '.join(desc_parts),
            user_id=user_id
        )

        template_id = template_dict['id']
        logger.info(f"Created template id={template_id}")

        # Get or create field categories
        category_map = _ensure_field_categories(db, template_id, fields)

        # Create template fields
        created_count = 0
        for i, f in enumerate(fields):
            try:
                category_name = f.get('category', 'Other Fields')
                category_id = category_map.get(category_name)

                db.create_template_field(
                    field_id=None,
                    template_id=template_id,
                    field_name=f.get('field_name', f'Field_{i+1}'),
                    display_name=f.get('display_name', f.get('field_name', f'Field {i+1}')),
                    field_type=f.get('field_type', 'text'),
                    category_id=category_id,
                    is_required=False,  # Never auto-mark required — user decides
                    extraction_is_required=False,
                    sort_order=i,
                    description=f.get('description', ''),
                )
                created_count += 1
            except Exception as field_err:
                logger.warning(f"Failed to create field '{f.get('field_name')}': {field_err}")

        # Save creation method and prompt as metadata
        try:
            import json as _json
            db.update_template(
                template_id=template_id,
                creation_method='plugin_auto',
                creation_prompt=extraction_prompt,
                source_documents=_json.dumps([doc_type_clean]),
            )
        except Exception:
            pass

        logger.info(
            f"Auto-created template '{template_name}' (id={template_id}) "
            f"with {created_count}/{len(fields)} fields"
        )

        # NOTE: Analyzer build is NOT done here. The job processor will build
        # the analyzer synchronously before extraction starts, so the custom
        # analyzer is guaranteed to be ready when extraction runs.

        return {
            'id': template_id,
            'name': template_name,
            'field_count': created_count,
            'has_analyzer': False,  # job processor will build it
            'analyzer_building': False,
        }

    except Exception as e:
        logger.error(f"Auto-create template failed: {e}", exc_info=True)
        return None


def _auto_build_analyzer(db, org_id, user_id, template_id, template_name, fields):
    """
    Build an Azure CU custom analyzer for an auto-created template.

    Mirrors the templates_api build-analyzer endpoint but runs inline
    during smart-analyze so the analyzer is ready when extraction starts.

    Returns { analyzer_id, azure_analyzer_id } on success, None on failure.
    """
    import uuid
    import json

    try:
        from src.services.azure_service import get_azure_client
        azure_client = get_azure_client()
        if not azure_client or not azure_client.is_available():
            logger.warning("Azure CU not available — skipping analyzer build")
            return None

        # Generate unique IDs
        suffix = str(uuid.uuid4())[:8]
        analyzer_record_id = f"anl_{suffix}"
        azure_analyzer_id = f"tmpl_{template_id}_{suffix}"

        # Build CU field schema — always 'text' type for best extraction
        cu_fields = []
        for f in fields:
            fname = f.get('field_name')
            if not fname:
                continue
            cu_fields.append({
                'field_name': fname,
                'display_name': f.get('display_name') or fname,
                'field_type': 'text',
                'description': f.get('description') or f.get('display_name') or fname,
                'method': 'extract',
            })

        if not cu_fields:
            logger.warning("No valid fields for analyzer build")
            return None

        logger.info(
            f"🔨 Building analyzer '{azure_analyzer_id}' for auto-created "
            f"template '{template_name}' ({len(cu_fields)} fields)"
        )

        # 1) Create analyzer record in DB (inactive until Azure confirms)
        db.create_analyzer(
            analyzer_id=analyzer_record_id,
            org_id=org_id,
            name=f"{template_name} Analyzer",
            description=f"Auto-built analyzer for template '{template_name}'",
            analyzer_type='azure_cu',
            azure_analyzer_id=azure_analyzer_id,
            configuration=json.dumps({
                'template_id': template_id,
                'field_count': len(cu_fields),
                'created_by': user_id,
                'created_via': 'plugin_auto_create',
            })
        )

        # 2) Create analyzer in Azure CU
        create_result = azure_client.create_custom_analyzer(
            analyzer_id=azure_analyzer_id,
            fields=cu_fields,
            description=f"Auto-built analyzer for template '{template_name}'",
            config={
                'returnDetails': True,
                'estimateFieldSourceAndConfidence': True,
                'enableOcr': True,
                'enableLayout': True,
            }
        )

        # 3) Poll until complete (up to 180s)
        operation_location = create_result.get('operation_location')
        operation_payload = None
        if operation_location:
            operation_payload = azure_client.poll_operation(
                operation_location=operation_location,
                timeout_seconds=180
            )

        # 4) Mark analyzer active and link to template
        db.update_analyzer(
            analyzer_id=analyzer_record_id,
            is_active=True,
            configuration=json.dumps({
                'template_id': template_id,
                'field_count': len(cu_fields),
                'created_by': user_id,
                'created_via': 'plugin_auto_create',
                'operation_id': create_result.get('operation_id'),
                'operation_status': (operation_payload or {}).get('status', 'succeeded'),
            })
        )
        db.link_template_to_analyzer(
            template_id=template_id,
            analyzer_id=analyzer_record_id
        )

        logger.info(
            f"✅ Analyzer '{azure_analyzer_id}' built and linked to "
            f"template {template_id} ({len(cu_fields)} fields)"
        )

        return {
            'analyzer_id': analyzer_record_id,
            'azure_analyzer_id': azure_analyzer_id,
        }

    except Exception as e:
        logger.error(f"Auto-build analyzer failed for template {template_id}: {e}", exc_info=True)
        # Template still works — extraction will use prebuilt-layout + LLM fallback
        return None


def _ensure_field_categories(db, template_id, fields):
    """
    Look up field categories for the AI-suggested fields.
    Categories are global (not per-template). The AI uses display names like
    "Instrument Details", "Financial Details", etc. We map those to existing
    category IDs, and create any missing ones.

    Returns a dict mapping category_display_name → category_id.
    """
    from src.models import TemplateFieldCategory
    import uuid

    # Collect unique category display names from AI output
    cat_names = set()
    for f in fields:
        cat = f.get('category', 'Other Fields')
        if cat:
            cat_names.add(cat)

    category_map = {}
    try:
        with db.get_session() as session:
            # Load all existing categories
            existing = session.query(TemplateFieldCategory).filter_by(
                template_field_category_is_active=True
            ).all()

            # Build a lookup by display_name (case-insensitive)
            display_lookup = {}
            for cat in existing:
                dn = (cat.template_field_category_display_name or '').strip().lower()
                display_lookup[dn] = cat.template_field_category_id
                # Also map by name for flexibility
                n = (cat.template_field_category_name or '').strip().lower()
                display_lookup[n] = cat.template_field_category_id

            for cat_display in cat_names:
                key = cat_display.strip().lower()
                if key in display_lookup:
                    category_map[cat_display] = display_lookup[key]
                else:
                    # Create a new global category
                    cat_id = f"cat_{uuid.uuid4().hex[:8]}"
                    cat_name = cat_display.lower().replace(' ', '_')[:50]
                    new_cat = TemplateFieldCategory(
                        template_field_category_id=cat_id,
                        template_field_category_name=cat_name,
                        template_field_category_display_name=cat_display,
                        template_field_category_description=f"Auto-created for {cat_display}",
                        template_field_category_is_active=True,
                    )
                    session.add(new_cat)
                    session.flush()
                    category_map[cat_display] = cat_id
                    logger.info(f"Created new field category '{cat_display}' (id={cat_id})")
    except Exception as e:
        logger.warning(f"Failed to resolve field categories: {e}")

    return category_map
