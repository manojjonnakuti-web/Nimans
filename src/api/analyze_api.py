"""
Analyze API
Provides document classification and prebuilt-layout analysis endpoints.
These are used by the plugin and the webapp to:
1. Classify a document against existing templates
2. Run prebuilt-layout extraction for documents without a custom analyzer
"""

from flask import Blueprint, request, jsonify, g
import logging

from src.auth import require_auth, ensure_user_exists, subscription_before_request
from src.repositories import get_database_repository

logger = logging.getLogger(__name__)

analyze_bp = Blueprint('analyze', __name__, url_prefix='/api/analyze')
analyze_bp.before_request(subscription_before_request)


@analyze_bp.route('/classify', methods=['POST'])
@require_auth
def classify_document():
    """
    Classify a document against the organization's templates.

    Accepts multipart/form-data with:
        - file: PDF file (required)
    OR JSON body with:
        - document_text: pre-extracted text (optional, if no file)

    Returns:
        - matched_template: best-matching template (or null)
        - confidence: 0-1 classification confidence
        - all_scores: list of all templates with match scores
        - recommendation: 'use_template' | 'no_match'
    """
    from src.services.openai_service import get_openai_service

    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    # ---- Get document text ----
    document_text = None
    filename = None

    if 'file' in request.files:
        file = request.files['file']
        if not file.filename:
            return jsonify({'error': 'No file selected'}), 400
        filename = file.filename
        pdf_bytes = file.read()

        # Try prebuilt-layout first, fallback to pypdf
        try:
            from src.services.azure_service import get_azure_client
            azure_client = get_azure_client()
            if azure_client.is_available():
                cu_result = azure_client.analyze_pdf_with_prebuilt_layout(
                    pdf_bytes, timeout_seconds=300
                )
                document_text = azure_client.format_prebuilt_layout_as_text(cu_result)
        except Exception as e:
            logger.warning(f"Prebuilt-layout classify failed, falling back to pypdf: {e}")

        if not document_text:
            try:
                from pypdf import PdfReader
                import io
                reader = PdfReader(io.BytesIO(pdf_bytes))
                text_parts = []
                for i in range(min(len(reader.pages), 10)):
                    page_text = reader.pages[i].extract_text() or ''
                    if page_text.strip():
                        text_parts.append(page_text)
                document_text = '\n\n'.join(text_parts)
            except Exception as e:
                return jsonify({'error': f'Failed to read PDF: {e}'}), 400
    else:
        data = request.get_json() or {}
        document_text = data.get('document_text', '')

    if not document_text or len(document_text.strip()) < 50:
        return jsonify({'error': 'Not enough text to classify'}), 400

    # ---- Get all templates with fields ----
    templates = db.list_templates(user['organization_id'], include_fields=True)
    if not templates:
        return jsonify({
            'matched_template': None,
            'confidence': 0,
            'recommendation': 'no_match',
            'reason': 'No templates exist for this organization',
            'all_scores': []
        })

    # ---- Build template summaries for GPT ----
    template_summaries = []
    for t in templates:
        field_names = [f['field_name'] for f in (t.get('fields') or [])]
        template_summaries.append({
            'id': t['id'],
            'name': t['name'],
            'description': t.get('description', ''),
            'field_count': len(field_names),
            'fields': field_names[:50],  # Cap at 50 field names
        })

    # ---- Call GPT-4.1 to classify ----
    openai_service = get_openai_service()
    if not openai_service.is_available():
        return jsonify({'error': 'OpenAI service not available'}), 503

    import json as _json

    template_info = _json.dumps(template_summaries, indent=2)[:8000]
    # Only send first 15K chars of doc text for classification (we just need doc type)
    doc_excerpt = document_text[:15000]

    prompt = f"""You are a document classification expert. Given a document excerpt and a list of extraction templates, determine which template (if any) best matches this document.

TEMPLATES:
{template_info}

DOCUMENT EXCERPT:
---
{doc_excerpt}
---

For each template, score how well this document matches (0.0 to 1.0).
A score of 0.8+ means high confidence match.
A score below 0.5 means the template is not relevant to this document.

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
            "reasoning": "Brief explanation"
        }}
    ],
    "recommendation": "use_template" or "no_match"
}}"""

    try:
        import time
        max_retries = 2
        base_delay = 10
        response = None

        for attempt in range(max_retries + 1):
            try:
                response = openai_service.client.chat.completions.create(
                    model=openai_service.deployment,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are a document classification expert. You match documents "
                                "to extraction templates based on document type and content. "
                                "Return valid JSON only."
                            )
                        },
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.1,
                    max_tokens=2000,
                )
                break
            except Exception as api_err:
                error_str = str(api_err)
                if ('429' in error_str or 'rate' in error_str.lower()) and attempt < max_retries:
                    time.sleep(base_delay * (2 ** attempt))
                else:
                    raise

        content = response.choices[0].message.content
        result = _json.loads(content)

        # ===== METERED BILLING: classification tokens → page-equivalents =====
        # GPT-4.1: $2/1M input, $8/1M output.  Page rate: $0.05/page.
        cls_input = getattr(response.usage, 'prompt_tokens', 0) if response.usage else 0
        cls_output = getattr(response.usage, 'completion_tokens', 0) if response.usage else 0
        cls_cost_usd = (cls_input * 2.0 + cls_output * 8.0) / 1_000_000
        PAGE_RATE = 0.05
        page_equiv = round(cls_cost_usd / PAGE_RATE, 2)
        if page_equiv > 0:
            try:
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
                    )
                    logger.info(f"Classification cost ${cls_cost_usd:.4f} → {page_equiv} page-equiv (pages_processed) for org {org_id}")
            except Exception as _mu_err:
                logger.warning(f"Failed to record classification metered usage: {_mu_err}")
        # ===== END METERED BILLING =====

        # Resolve template details for the best match
        matched_template = None
        best_id = result.get('best_match_template_id')
        if best_id:
            for t in templates:
                if t['id'] == best_id:
                    matched_template = {
                        'id': t['id'],
                        'name': t['name'],
                        'description': t.get('description', ''),
                        'field_count': len(t.get('fields') or []),
                    }
                    break

        return jsonify({
            'matched_template': matched_template,
            'confidence': result.get('best_match_confidence', 0),
            'document_type': result.get('document_type', 'Unknown'),
            'recommendation': result.get('recommendation', 'no_match'),
            'all_scores': result.get('scores', []),
            'source_file': filename,
        })

    except Exception as e:
        logger.error(f"Document classification failed: {e}", exc_info=True)
        error_msg = str(e)
        if '429' in error_msg or 'rate' in error_msg.lower():
            return jsonify({
                'error': 'Rate limited. Please wait 30-60 seconds and try again.',
                'retryable': True
            }), 429
        return jsonify({'error': f'Classification failed: {error_msg}'}), 500


@analyze_bp.route('/extract', methods=['POST'])
@require_auth
def extract_with_prebuilt_layout():
    """
    Extract ALL fields from a document using prebuilt-layout + LLM.
    This is the "no custom analyzer" path — uses Azure CU prebuilt-layout
    to get structured data, then GPT-4.1 to identify and extract every field.

    Accepts multipart/form-data with:
        - file: PDF file (required)
        - prompt: Optional extraction guidance
        - template_id: Optional — if provided, prioritizes template fields

    Returns:
        - fields: list of extracted field/value pairs
        - document_type: detected document type
        - extraction_method: 'prebuilt-layout+llm'
    """
    from src.services.openai_service import get_openai_service
    from src.services.azure_service import get_azure_client

    db = get_database_repository()
    user = ensure_user_exists(db, g.current_user)
    db = g.tenant_db  # Route to tenant database

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    if not file.filename or not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Only PDF files are supported'}), 400

    pdf_bytes = file.read()
    if len(pdf_bytes) < 100:
        return jsonify({'error': 'File too small'}), 400

    user_prompt = request.form.get('prompt', '').strip()
    template_id = request.form.get('template_id', '').strip()

    # ---- Run prebuilt-layout ----
    azure_client = get_azure_client()
    if not azure_client.is_available():
        return jsonify({'error': 'Azure Content Understanding not configured'}), 503

    try:
        logger.info(f"Prebuilt-layout extraction for '{file.filename}'...")
        cu_result = azure_client.analyze_pdf_with_prebuilt_layout(
            pdf_bytes, timeout_seconds=600
        )
        document_text = azure_client.format_prebuilt_layout_as_text(cu_result)
    except Exception as e:
        logger.error(f"Prebuilt-layout extraction failed: {e}", exc_info=True)
        return jsonify({'error': f'Document analysis failed: {str(e)}'}), 500

    if not document_text or len(document_text.strip()) < 50:
        return jsonify({'error': 'Could not extract text from this document'}), 400

    # ---- Get template fields if template_id provided ----
    template_field_names = []
    if template_id:
        template_fields = db.get_template_fields(template_id)
        template_field_names = [tf['field_name'] for tf in template_fields]

    # ---- Call LLM to extract all fields ----
    openai_service = get_openai_service()
    if not openai_service.is_available():
        return jsonify({'error': 'OpenAI service not available'}), 503

    result = openai_service.extract_all_fields(
        document_text=document_text,
        already_extracted_fields=[],  # Nothing extracted yet — get everything
        user_prompt=user_prompt or None,
    )

    if not result.get('success'):
        return jsonify({
            'error': 'Extraction failed',
            'details': result.get('error', 'Unknown')
        }), 500

    fields = result.get('fields', [])

    # If template was provided, split into template-matched and additional
    template_matched = []
    additional = []
    template_names_lower = {n.lower() for n in template_field_names}

    for f in fields:
        if f.get('field_name', '').lower() in template_names_lower:
            template_matched.append(f)
        else:
            additional.append(f)

    return jsonify({
        'fields': fields,
        'template_matched_fields': template_matched,
        'additional_fields': additional,
        'total_fields': len(fields),
        'extraction_method': 'prebuilt-layout+llm',
        'source_file': file.filename,
        'text_length': len(document_text),
        'cost': result.get('cost', {}),
    })
