"""
Request Processor
Background job processing for document analysis and field extraction
"""

import copy
import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Any, Optional, List

from src.repositories import get_database_repository
from src.services import get_field_normalizer

logger = logging.getLogger(__name__)


class RequestProcessor:
    """
    Processes requests by analyzing documents and extracting fields
    This is the core processing logic that runs in the background
    """
    
    def __init__(self, db=None, org_id=None):
        self.org_id = org_id
        
        # Central DB — always used for async_jobs table (job queue lives centrally)
        self._central_db = get_database_repository()
        
        # Data DB — routes to tenant DB when org has its own database,
        # otherwise falls back to central DB (same object as _central_db).
        if db is not None:
            self.db = db
        elif org_id:
            from src.tenant import get_tenant_database_repository
            self.db = get_tenant_database_repository(org_id)
        else:
            self.db = self._central_db
        
        self._azure_client = None
        self._storage = None
        self._openai = None
        # Text extracted by Azure CU (OCR) — used by LLM fallback when pypdf
        # returns nothing (scanned/image-only PDFs).
        self._last_cu_document_text = ''
    
    @property
    def azure_client(self):
        if self._azure_client is None:
            try:
                if self.org_id:
                    from src.tenant import get_tenant_cu_client
                    self._azure_client = get_tenant_cu_client(self.org_id)
                else:
                    from src.services import get_azure_client
                    self._azure_client = get_azure_client()
            except Exception as e:
                logger.warning(f"Azure client not available: {e}")
        return self._azure_client
    
    @property
    def storage(self):
        if self._storage is None:
            try:
                if self.org_id:
                    from src.tenant import get_tenant_storage_service
                    self._storage = get_tenant_storage_service(self.org_id)
                else:
                    from src.services import get_storage_service
                    self._storage = get_storage_service()
            except Exception as e:
                logger.warning(f"Storage service not available: {e}")
        return self._storage
    
    @property
    def openai(self):
        if self._openai is None:
            try:
                if self.org_id:
                    from src.tenant import get_tenant_openai_service
                    self._openai = get_tenant_openai_service(self.org_id)
                else:
                    from src.services import get_openai_service
                    self._openai = get_openai_service()
            except Exception as e:
                logger.warning(f"OpenAI service not available: {e}")
        return self._openai
    
    def process_job(self, job_id: str) -> bool:
        """
        Process a single async job
        Returns True if successful, False otherwise
        """
        job = self._central_db.get_async_job(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return False
        
        # ── CRITICAL: Clear per-request mutable state ──
        # _last_cu_document_text persists on the singleton RequestProcessor
        # instance between jobs.  Without this reset, document text from a
        # PREVIOUS request leaks into the LLM fallback of the CURRENT
        # request, causing cross-request data contamination.
        self._last_cu_document_text = ''
        
        # Set current job ID so _log_step() works in all nested methods
        self._current_job_id = job_id
        
        # Update job status to running
        self._central_db.update_async_job(job_id, status='running', progress_percent=0)
        self._append_step_log(job_id, f"Job started (type={job['job_type']}, entity={job['entity_type']})")
        
        try:
            if job['job_type'] == 'document_analysis':
                if job['entity_type'] == 'request':
                    self._process_request_analysis(job)
                elif job['entity_type'] == 'document':
                    self._process_document_analysis(job)
                else:
                    raise ValueError(f"Unsupported entity type: {job['entity_type']}")
            else:
                raise ValueError(f"Unsupported job type: {job['job_type']}")
            
            # Mark job as completed
            self._central_db.update_async_job(
                job_id,
                status='completed',
                progress_percent=100,
                progress_message="Completed successfully"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing job {job_id}: {e}")
            logger.error(traceback.format_exc())
            
            # Mark job as failed
            self._central_db.update_async_job(
                job_id,
                status='failed',
                error_message=str(e)
            )
            
            # Update parent record status
            self._update_parent_status(job, 'failed')
            
            return False
    
    def _process_request_analysis(self, job: Dict[str, Any]):
        """
        Process analysis for all documents in a request — INCREMENTAL approach.
        
        Instead of waiting for all documents to finish before saving, we:
        1. Create a version with empty placeholders after the FIRST document extraction
        2. Save extracted fields and update status to 'reviewing' immediately
        3. For each subsequent document, merge new/better fields into the same version
        4. The frontend can show partial results while extraction continues
        
        The confidence threshold check for skipping remaining docs still applies.
        """
        request_id = job['entity_id']
        self._current_job_id = job['id']  # Store for sub-method step logging
        request = self.db.get_request(request_id)
        if not request:
            raise ValueError(f"Request {request_id} not found")
        
        # Get all documents for this request
        request_docs = self.db.get_request_documents(request_id)
        
        # If no documents, check if we have an email body to analyze directly
        if not request_docs:
            if request.get('email_id'):
                email = self.db.get_email(request['email_id'])
                if email and email.get('body'):
                    logger.info(f"No documents found, but found email body for request {request_id}")
                    return self._process_email_body_analysis(job, request, email)
            raise ValueError(f"No documents found for request {request_id}")
        
        self._update_job_progress(job['id'], 5, "Starting document analysis")
        self._append_step_log(job['id'], f"Request {request_id}: found {len(request_docs)} document(s)")
        
        # ── Ensure the template's Azure CU analyzer is ready ──
        # If the template was just auto-created (e.g. from a custom prompt),
        # it won't have an analyzer yet. Build it now BEFORE extraction starts
        # so the custom analyzer handles the document properly.
        if request.get('template_id'):
            self._ensure_analyzer_ready(request, job['id'])
        
        # Default confidence threshold for saving fields
        DEFAULT_CONFIDENCE_THRESHOLD = 0.60
        
        # Get template fields upfront to identify thresholds
        template_fields = []
        template_field_map = {}
        field_thresholds = {}
        if request.get('template_id'):
            template_fields = self.db.get_template_fields(request['template_id'])
            template_field_map = {tf['field_name']: tf for tf in template_fields}
            for tf in template_fields:
                threshold = tf.get('precision_threshold')
                if threshold is not None:
                    field_thresholds[tf['field_name']] = threshold
                else:
                    field_thresholds[tf['field_name']] = DEFAULT_CONFIDENCE_THRESHOLD
            # Required-field concept removed — all fields are optional
            logger.info(f"Request has template with {len(template_fields)} fields")
            self._append_step_log(job['id'], f"Template loaded: {len(template_fields)} fields defined")
            self._append_step_log(job['id'], f"Loaded template: {len(template_fields)} fields defined")
        
        # Separate email body from other documents
        email_docs = []
        other_docs = []
        for req_doc in request_docs:
            doc_id = req_doc.get('document_id') or req_doc.get('document', {}).get('id')
            doc = self.db.get_document(doc_id)
            if not doc:
                logger.warning(f"Document {doc_id} not found")
                continue
            
            source_type = req_doc.get('source_type', '')
            filename = doc.get('filename', '').lower()
            if source_type == 'email_body' or filename.endswith('.eml') or filename.endswith('.msg'):
                email_docs.append(doc)
            else:
                other_docs.append(doc)
        
        # Process attachments FIRST (real PDFs with authoritative data),
        # then email docs (supplementary — fills in any gaps the attachments missed).
        # This ensures the initial version is created from the most reliable source.
        all_docs = other_docs + email_docs
        total_docs = len(all_docs)
        email_only = len(other_docs) == 0  # True when request has ONLY email body PDFs, no real attachments
        self._append_step_log(job['id'], f"Documents: {len(email_docs)} email, {len(other_docs)} attachments = {total_docs} total")
        self._append_step_log(job['id'], f"Processing order: attachments first, then emails")
        
        # Get email information if request has email_id
        email = None
        if request.get('email_id'):
            email = self.db.get_email(request['email_id'])
            if email:
                logger.info(f"Found parent email: {email.get('subject', 'No subject')}")
        
        # Track the version we create — will be reused across all documents
        version_id = None
        # Track cumulative field extractions for confidence checking
        cumulative_extractions: Dict[str, List[Dict[str, Any]]] = {}
        first_document_saved = False
        
        self._append_step_log(job['id'], f"Found {total_docs} document(s) to analyze ({len(other_docs)} attachments first, then {len(email_docs)} email)")
        
        # ===== COST TRACKER: accumulate costs across all operations =====
        import time as _time
        _request_start_time = _time.time()
        cost_tracker = {
            'total_pages': 0,
            'total_documents': total_docs,
            'azure_cu_cost': 0.0,
            'openai_email_tokens': {'input': 0, 'output': 0, 'cost': 0.0},
            'azure_openai_normalisation_tokens': {'input': 0, 'output': 0, 'cost': 0.0, 'fields_sent': 0},
            'per_document': [],
            'processing_time_seconds': 0,
        }
        
        for doc_idx, doc in enumerate(all_docs):
            is_email_doc = doc in email_docs
            doc_num = doc_idx + 1
            progress = 5 + int((doc_idx / max(total_docs, 1)) * 80)
            
            self._update_job_progress(
                job['id'], progress, 
                f"Analyzing document {doc_num}/{total_docs}: {doc.get('filename', 'unknown')}"
            )
            
            # Analyze the document
            _doc_start = _time.time()
            if is_email_doc:
                # Email body PDFs ALWAYS use dynamic field discovery.
                # Even in mixed requests (email + attachment), the email content
                # is fundamentally different from the document — forcing it to
                # match template fields (e.g. catbond ISIN/CUSIP) loses most
                # of the email's useful data.
                self._append_step_log(job['id'], f"📧 Email doc → routing to OpenAI LLM extraction (not Azure CU)")
                result = self._analyze_single_document(doc, request, email=email, is_email_doc=True, template_fields=None, job_id=job['id'])
            else:
                result = self._analyze_single_document(doc, request)
            _doc_elapsed = round(_time.time() - _doc_start, 2)
            
            # ===== COST TRACKING: capture per-doc costs from result =====
            if result:
                # Capture OpenAI email analysis costs if present
                email_cost = result.get('_cost_tracking')
                if email_cost:
                    cost_tracker['openai_email_tokens']['input'] += email_cost.get('input_tokens', 0)
                    cost_tracker['openai_email_tokens']['output'] += email_cost.get('output_tokens', 0)
                    cost_tracker['openai_email_tokens']['cost'] += email_cost.get('estimated_cost_usd', 0)
                
                # Capture page count from result metadata (set by _analyze_single_blob / _analyze_chunked)
                doc_pages = result.get('_pages_analyzed', 0)
                cost_tracker['total_pages'] += doc_pages
                # CU cost = Non Text Files ($0.005/page) + Layout ($0.005/page) = $0.01/page
                # For custom analyzers, add Output: (fields/100) * (pages/1000) * $22
                cost_tracker['azure_cu_cost'] = round(cost_tracker['total_pages'] * 0.01, 4)

                cost_tracker['per_document'].append({
                    'filename': doc.get('filename', 'unknown'),
                    'document_id': doc.get('id'),
                    'pages': doc_pages,
                    'is_email_doc': is_email_doc,
                    'processing_time_seconds': _doc_elapsed,
                    'azure_cu_cost': round(doc_pages * 0.01, 4),
                })
            # ===== END PER-DOC COST TRACKING =====
            
            if not result:
                logger.warning(f"No result for document {doc.get('id')} ({doc.get('filename')})")
                self._append_step_log(job['id'], f"⚠️ No result for document {doc.get('filename', 'unknown')} (took {_doc_elapsed}s)")
                continue
            
            # Extract fields from this document's analysis
            self._append_step_log(job['id'], f"Extracting fields from analysis result...")
            fields = self._extract_fields_from_analysis(result, doc['id'], template_field_map)
            if not fields:
                logger.info(f"No fields extracted from document {doc.get('id')}")
                self._append_step_log(job['id'], f"⚠️ No fields found in document {doc.get('filename', 'unknown')}")
                continue
            
            self._append_step_log(job['id'], f"Found {len(fields)} fields from {doc.get('filename', 'unknown')} ({_doc_elapsed}s)")
            
            # Add template_field_id to each extracted field
            for field in fields:
                fname = field.get('field_name')
                if fname in template_field_map:
                    field['template_field_id'] = template_field_map[fname]['id']
                field['is_extracted'] = True
                # Mark email-extracted fields so the frontend can show them separately
                if is_email_doc:
                    field['source_type'] = 'email_body'

            # Update cumulative extractions for confidence checking
            for field in fields:
                fname = field['field_name']
                if fname not in cumulative_extractions:
                    cumulative_extractions[fname] = []
                cumulative_extractions[fname].append(field)
            
            if not first_document_saved:
                # === FIRST DOCUMENT: Create version, save fields, update status ===
                
                # Build the initial field list: extracted + empty placeholders
                initial_fields = []
                extracted_field_names = set()
                
                for field_name, extractions in cumulative_extractions.items():
                    confident = [
                        e for e in extractions
                        if (e.get('confidence', 0) or 0) >= field_thresholds.get(field_name, DEFAULT_CONFIDENCE_THRESHOLD)
                    ]
                    if not confident:
                        continue
                    
                    sorted_ext = sorted(confident, key=lambda x: x.get('confidence', 0) or 0, reverse=True)
                    for idx, f in enumerate(sorted_ext):
                        f['is_active'] = (idx == 0)
                        initial_fields.append(f)
                    extracted_field_names.add(field_name)
                
                # Add empty placeholders for template fields not yet extracted.
                # Skip for email-only requests — the template was auto-assigned by the
                # plugin and is irrelevant; the dynamically discovered email fields ARE
                # the field set.
                if template_fields and not email_only:
                    for tf in template_fields:
                        if tf['field_name'] not in extracted_field_names:
                            initial_fields.append({
                                'field_name': tf['field_name'],
                                'field_value': None,
                                'confidence': None,
                                'source_type': 'pending',
                                'source_id': None,
                                'is_active': True,
                                'is_extracted': False,
                                'template_field_id': tf['id']
                            })
                
                if initial_fields:
                    version = self.db.create_request_version(
                        request_id=request_id,
                        version_label='Extraction',
                        fields=initial_fields
                    )
                    version_id = version['id']
                    self.db.set_current_version(request_id, version_id)
                    
                    logger.info(
                        f"[Doc {doc_num}/{total_docs}] Created initial version with "
                        f"{len(initial_fields)} fields ({len(extracted_field_names)} extracted, "
                        f"{len(initial_fields) - len(extracted_field_names)} pending)"
                    )
                    
                    # Update status to 'reviewing' immediately so frontend can show partial results
                    status_updated = self.db.update_request_status(request_id, 'reviewing')
                    if not status_updated:
                        status_updated = self.db.update_request_status(request_id, 'extracted')
                    if status_updated:
                        logger.info(f"Request {request_id} status updated to reviewing after first document")
                        self._append_step_log(job['id'], f"✅ Initial fields saved — {len(extracted_field_names)} extracted, {len(initial_fields) - len(extracted_field_names)} pending")
                    
                    first_document_saved = True

            else:
                # === SUBSEQUENT DOCUMENTS: Merge fields into existing version ===
                if version_id:
                    merge_result = self.db.merge_fields_into_version(
                        request_id=request_id,
                        version_id=version_id,
                        new_fields=fields,
                        field_thresholds=field_thresholds,
                        default_threshold=DEFAULT_CONFIDENCE_THRESHOLD
                    )
                    logger.info(
                        f"[Doc {doc_num}/{total_docs}] Merged fields into version: "
                        f"{merge_result['updated_count']} updated, "
                        f"{merge_result['inserted_count']} added as alternatives, "
                        f"{merge_result['skipped_count']} skipped (below threshold)"
                    )
        
        # If no fields were saved at all (all documents failed), still create an empty version
        if not first_document_saved:
            if template_fields and not email_only:
                empty_fields = [{
                    'field_name': tf['field_name'],
                    'field_value': None,
                    'confidence': None,
                    'source_type': 'pending',
                    'source_id': None,
                    'is_active': True,
                    'is_extracted': False,
                    'template_field_id': tf['id']
                } for tf in template_fields]
                
                version = self.db.create_request_version(
                    request_id=request_id,
                    version_label='Extraction',
                    fields=empty_fields
                )
                self.db.set_current_version(request_id, version['id'])
                logger.info(f"Created empty version — no documents yielded fields")
            
            status_updated = self.db.update_request_status(request_id, 'reviewing')
            if not status_updated:
                self.db.update_request_status(request_id, 'extracted')
        
        _extraction_elapsed = round(_time.time() - _request_start_time, 2)
        extracted_count = sum(1 for exts in cumulative_extractions.values() if exts)
        self._update_job_progress(job['id'], 85, f"Extraction complete ({_extraction_elapsed}s)")
        self._append_step_log(job['id'], f"All documents processed — {extracted_count} fields extracted in {_extraction_elapsed}s")
        
        # Update email status if linked
        if request.get('email_id'):
            self.db.update_email_status(request['email_id'], 'processed')
        logger.info(
            f"Incremental extraction completed for request {request_id}: "
            f"{extracted_count} fields extracted from {total_docs} documents"
        )
        
        # --- LLM fallback for pending fields ---
        # If Azure CU left some fields empty, use GPT-4.1 to try to extract them
        # from the document text that CU already parsed.
        if version_id and template_fields:
            self._update_job_progress(job['id'], 88, "Running LLM fallback for empty fields")
            _llm_start = _time.time()
            self._run_llm_fallback_extraction(
                request_id=request_id,
                version_id=version_id,
                request=request,
                template_fields=template_fields,
                template_field_map=template_field_map,
                field_thresholds=field_thresholds,
                default_threshold=DEFAULT_CONFIDENCE_THRESHOLD,
                cost_tracker=cost_tracker,
            )
            _llm_elapsed = round(_time.time() - _llm_start, 2)
            if _llm_elapsed > 1:
                self._append_step_log(job['id'], f"LLM fallback completed ({_llm_elapsed}s)")
        
        # --- Auto-normalise extracted fields ---
        self._update_job_progress(job['id'], 95, "Running AI normalisation")
        self._append_step_log(job['id'], "Starting AI field normalisation...")
        _norm_start = _time.time()
        self._run_ai_normalisation(request_id, cost_tracker=cost_tracker)
        _norm_elapsed = round(_time.time() - _norm_start, 2)
        self._append_step_log(job['id'], f"✅ AI normalisation complete ({_norm_elapsed}s)")
        
        # ===== FINAL COST SUMMARY — uses accumulated cost_tracker (no re-downloads) =====
        import json as _json
        _total_elapsed = round(_time.time() - _request_start_time, 2)
        cost_tracker['processing_time_seconds'] = _total_elapsed
        
        _grand_total = (
            cost_tracker['azure_cu_cost']
            + cost_tracker['openai_email_tokens']['cost']
            + cost_tracker['azure_openai_normalisation_tokens']['cost']
            + cost_tracker.get('llm_fallback_tokens', {}).get('cost', 0.0)
        )
        cost_tracker['grand_total_estimated_usd'] = round(_grand_total, 6)
        
        _llm_fb = cost_tracker.get('llm_fallback_tokens', {})
        logger.info(
            f"\n{'#'*60}\n"
            f"💰💰💰 TOTAL COST SUMMARY — Request {request_id} 💰💰💰\n"
            f"{'#'*60}\n"
            f"  Processing time:       {_total_elapsed:.1f}s\n"
            f"  Documents processed:   {total_docs}\n"
            f"  Total pages (CU):      {cost_tracker['total_pages']}\n"
            f"  Azure CU cost:         ${cost_tracker['azure_cu_cost']:.4f}\n"
            f"  OpenAI email tokens:   in={cost_tracker['openai_email_tokens']['input']}, "
            f"out={cost_tracker['openai_email_tokens']['output']}, "
            f"cost=${cost_tracker['openai_email_tokens']['cost']:.4f}\n"
            f"  LLM fallback tokens:   in={_llm_fb.get('input', 0)}, "
            f"out={_llm_fb.get('output', 0)}, "
            f"cost=${_llm_fb.get('cost', 0.0):.4f}\n"
            f"  Azure OpenAI norm:     in={cost_tracker['azure_openai_normalisation_tokens']['input']}, "
            f"out={cost_tracker['azure_openai_normalisation_tokens']['output']}, "
            f"cost=${cost_tracker['azure_openai_normalisation_tokens']['cost']:.4f}\n"
            f"  GRAND TOTAL:           ${_grand_total:.6f}\n"
            f"{'#'*60}"
        )
        
        # Persist cost data to the async job's result_data column
        # Include the processing log we've been building
        try:
            # Merge processing log into cost tracker
            if hasattr(self, '_processing_logs') and job['id'] in self._processing_logs:
                cost_tracker['processing_log'] = self._processing_logs[job['id']]['steps']
            self._append_step_log(job['id'], f"✅ Processing complete! Total time: {_total_elapsed}s, Cost: ${_grand_total:.4f}")
            if hasattr(self, '_processing_logs') and job['id'] in self._processing_logs:
                cost_tracker['processing_log'] = self._processing_logs[job['id']]['steps']
            self._central_db.update_async_job(
                job['id'],
                result_data=_json.dumps(cost_tracker)
            )
            logger.info(f"Cost data saved to async_job result_data for job {job['id']}")
        except Exception as _e:
            logger.warning(f"Failed to save cost data to result_data: {_e}")
        
        # ===== METERED BILLING: record usage for marketplace reporting =====
        try:
            org_id = request.get('organization_id') or job.get('organization_id')
            if org_id:
                sub = self._central_db.get_active_subscription_for_org(org_id)
                if sub:
                    sub_id = sub['id']
                    
                    # Dimension 1: pages_processed
                    pages = cost_tracker.get('total_pages', 0)
                    if pages > 0:
                        self._central_db.record_metered_usage(
                            organization_id=org_id,
                            subscription_id=sub_id,
                            dimension='pages_processed',
                            quantity=pages,
                            request_id=request_id,
                            job_id=job['id'],
                        )
                        logger.info(f"Metered usage recorded: {pages} pages_processed for org {org_id}")
                    
                    # Dimension 2: fields_normalised
                    norm = cost_tracker.get('azure_openai_normalisation_tokens', {})
                    fields_sent = norm.get('fields_sent', 0)
                    if fields_sent > 0:
                        self._central_db.record_metered_usage(
                            organization_id=org_id,
                            subscription_id=sub_id,
                            dimension='fields_normalised',
                            quantity=fields_sent,
                            request_id=request_id,
                            job_id=job['id'],
                        )
                        logger.info(f"Metered usage recorded: {fields_sent} fields_normalised for org {org_id}")
                    
                    # AI token costs → page-equivalents (pages_processed @ $0.05/page)
                    # Since Marketplace dimensions are locked, we convert token USD cost
                    # into equivalent page units: page_equiv = cost_usd / 0.05
                    PAGE_RATE = 0.05  # $0.05 per page_processed
                    ai_page_equiv = 0.0
                    
                    # Email analysis token cost
                    email_cost_usd = cost_tracker.get('openai_email_tokens', {}).get('cost', 0.0)
                    if email_cost_usd > 0:
                        ai_page_equiv += email_cost_usd / PAGE_RATE
                        logger.info(f"Email analysis cost ${email_cost_usd:.4f} → {email_cost_usd/PAGE_RATE:.2f} page-equivalents")
                    
                    # LLM fallback extraction token cost
                    llm_cost_usd = cost_tracker.get('llm_fallback_tokens', {}).get('cost', 0.0)
                    if llm_cost_usd > 0:
                        ai_page_equiv += llm_cost_usd / PAGE_RATE
                        logger.info(f"LLM fallback cost ${llm_cost_usd:.4f} → {llm_cost_usd/PAGE_RATE:.2f} page-equivalents")
                    
                    # Record combined AI page-equivalents as pages_processed
                    if ai_page_equiv > 0:
                        # Round up to 2 decimal places (Marketplace accepts decimals)
                        ai_page_equiv = round(ai_page_equiv, 2)
                        self._central_db.record_metered_usage(
                            organization_id=org_id,
                            subscription_id=sub_id,
                            dimension='pages_processed',
                            quantity=ai_page_equiv,
                            request_id=request_id,
                            job_id=job['id'],
                        )
                        logger.info(f"Metered usage recorded: {ai_page_equiv} AI page-equivalents (pages_processed) for org {org_id}")
                else:
                    logger.warning(f"No active subscription for org {org_id} — metered usage NOT recorded")
            else:
                logger.warning(f"No organization_id on request/job — metered usage NOT recorded")
        except Exception as _mu_err:
            logger.warning(f"Failed to record metered usage: {_mu_err}")
        # ===== END METERED BILLING =====
        # ===== END FINAL COST SUMMARY =====
    
    # ------------------------------------------------------------------
    # LLM Fallback Extraction — fills in fields Azure CU left empty
    # ------------------------------------------------------------------

    def _run_llm_fallback_extraction(
        self,
        request_id: str,
        version_id: str,
        request: dict,
        template_fields: list,
        template_field_map: dict,
        field_thresholds: dict,
        default_threshold: float,
        cost_tracker: dict = None,
    ):
        """
        After Azure CU extraction, some fields may remain 'pending' (empty).
        This method:
        1. Reads the current request fields to find which are still pending
        2. Gets the document text from the analysis run result (Azure CU markdown)
        3. Sends the pending field descriptions + document text to GPT-4.1
        4. Updates the pending fields in the DB with LLM-extracted values

        Failures are logged but never fail the overall extraction job.
        """
        try:
            if not self.openai or not self.openai.is_available():
                logger.info("OpenAI not available — skipping LLM fallback extraction")
                return

            # 1. Find pending fields
            all_fields = self.db.get_request_fields(request_id, include_inactive=False)
            pending = [
                f for f in all_fields
                if f.get('source_type') == 'pending'
                and not f.get('is_extracted')
            ]

            if not pending:
                logger.info(f"No pending fields — LLM fallback not needed (request {request_id})")
                return

            pending_names = {f['field_name'] for f in pending}
            logger.info(
                f"LLM fallback: {len(pending)} pending fields: "
                f"{', '.join(sorted(pending_names))}"
            )
            self._log_step(
                f"🤖 {len(pending)} fields still empty after Azure CU — "
                f"running LLM fallback extraction..."
            )

            # 2. Get document text from the most recent analysis run
            document_text = self._get_document_text_for_request(request_id)
            if not document_text or len(document_text) < 100:
                logger.warning(
                    f"Could not retrieve document text for LLM fallback "
                    f"(got {len(document_text) if document_text else 0} chars)"
                )
                self._log_step("⚠️ No document text available for LLM fallback — skipping")
                return

            logger.info(f"LLM fallback: got {len(document_text)} chars of document text")

            # 3. Build the pending field specs for the LLM
            #    Include field_type, category, and field_values (enum options)
            #    so the LLM knows WHAT it's looking for, not just the name.
            pending_field_specs = []
            for pf in pending:
                field_name = pf['field_name']
                tf = template_field_map.get(field_name, {})
                spec = {
                    'field_name': field_name,
                    'display_name': tf.get('display_name', field_name),
                    'description': tf.get('description', ''),
                    'field_type': tf.get('field_type', 'text'),
                    'category': tf.get('category', ''),
                }
                # Include enum/dropdown options if defined
                field_values = tf.get('field_values')
                if field_values:
                    spec['field_values'] = field_values
                pending_field_specs.append(spec)

            # 4. Build enriched context for the LLM
            #    Include already-extracted field names so the LLM understands
            #    what kind of document it's working with.
            extracted_fields = [
                f for f in all_fields
                if f.get('source_type') != 'pending' and f.get('is_extracted')
            ]
            extracted_summary = {}
            for ef in extracted_fields[:20]:  # Cap at 20 to avoid token bloat
                val = ef.get('field_value', '')
                if val and len(str(val)) > 60:
                    val = str(val)[:60] + '...'
                extracted_summary[ef['field_name']] = val

            # Get template name for context
            template_name = ''
            if request.get('template_id'):
                tmpl = self.db.get_template(request['template_id'])
                template_name = tmpl.get('name', '') if tmpl else ''

            llm_context = {
                'template_name': template_name,
                'total_fields_in_template': len(template_fields),
                'fields_already_extracted': len(extracted_fields),
                'fields_pending': len(pending),
                'extracted_fields_sample': extracted_summary,
            }

            # 5. Call OpenAI LLM to extract the pending fields
            import time as _t
            _llm_start = _t.time()
            result = self.openai.extract_pending_fields(
                document_text=document_text,
                pending_fields=pending_field_specs,
                context=llm_context,
            )
            _llm_elapsed = round(_t.time() - _llm_start, 2)

            if not result.get('success'):
                logger.warning(f"LLM fallback failed: {result.get('error', 'unknown')}")
                self._log_step(f"⚠️ LLM fallback failed: {result.get('error', 'unknown')[:80]}")
                return

            # Track cost
            if cost_tracker and result.get('cost'):
                llm_cost = result['cost']
                cost_tracker.setdefault('llm_fallback_tokens', {
                    'input': 0, 'output': 0, 'cost': 0.0, 'fields_sent': 0
                })
                cost_tracker['llm_fallback_tokens']['input'] += llm_cost.get('input_tokens', 0)
                cost_tracker['llm_fallback_tokens']['output'] += llm_cost.get('output_tokens', 0)
                cost_tracker['llm_fallback_tokens']['cost'] += llm_cost.get('estimated_cost_usd', 0)
                cost_tracker['llm_fallback_tokens']['fields_sent'] += len(pending_field_specs)

            # 5. Build field data for merge — with hallucination validation
            llm_fields = []
            hallucinated_count = 0
            for llm_field in result.get('fields', []):
                field_name = llm_field.get('field_name')
                value = llm_field.get('value')
                confidence = llm_field.get('confidence', 0)

                if not field_name or not value or not str(value).strip():
                    continue

                # LLM fallback uses a lenient confidence floor (0.30)
                # because this is already a second-pass rescue attempt.
                # The main threshold was already applied during CU extraction.
                LLM_CONFIDENCE_FLOOR = 0.30
                if confidence < LLM_CONFIDENCE_FLOOR:
                    logger.info(
                        f"LLM field '{field_name}' below floor "
                        f"({confidence:.2f} < {LLM_CONFIDENCE_FLOOR})"
                    )
                    continue

                # ── HALLUCINATION GUARD ──
                # Verify that the extracted value (or a significant fragment)
                # actually appears in the source document text. If not, the
                # LLM likely fabricated it from its training data.
                value_str = str(value).strip()
                is_verified = False
                if document_text:
                    doc_lower = document_text.lower()
                    val_lower = value_str.lower()
                    if val_lower in doc_lower:
                        is_verified = True
                    else:
                        # For multi-word values (names, addresses), check if
                        # ALL significant words appear in the document.
                        words = [w for w in val_lower.split() if len(w) > 2]
                        if words and all(w in doc_lower for w in words):
                            is_verified = True

                if not is_verified:
                    hallucinated_count += 1
                    logger.warning(
                        f"HALLUCINATION BLOCKED: LLM extracted '{field_name}' = "
                        f"'{value_str[:80]}' but value not found in document text. "
                        f"Discarding this field."
                    )
                    self._log_step(
                        f"⚠️ Blocked hallucinated value for '{field_name}' — "
                        f"'{value_str[:40]}' not found in document"
                    )
                    continue

                # Cap LLM confidence at 0.85 — LLM-extracted values are
                # inherently less reliable than Azure CU extraction.
                LLM_CONFIDENCE_CAP = 0.85
                capped_confidence = min(confidence, LLM_CONFIDENCE_CAP)
                if capped_confidence != confidence:
                    logger.info(
                        f"LLM field '{field_name}' confidence capped: "
                        f"{confidence:.2f} → {capped_confidence:.2f}"
                    )

                tf = template_field_map.get(field_name, {})
                llm_fields.append({
                    'field_name': field_name,
                    'field_value': str(value),
                    'extracted_value': str(value),
                    'confidence': capped_confidence,
                    'source_type': 'llm_fallback',
                    'source_id': None,
                    'is_active': True,
                    'is_extracted': True,
                    'template_field_id': tf.get('id'),
                    'page_number': None,
                    'bounding_box': None,
                })

            if not llm_fields:
                logger.info("LLM fallback: no additional fields extracted")
                self._log_step(
                    f"LLM fallback: 0 additional fields found "
                    f"({hallucinated_count} blocked as hallucinated, {_llm_elapsed}s)"
                )
                return

            # 6. Merge LLM-extracted fields into the existing version
            merge_result = self.db.merge_fields_into_version(
                request_id=request_id,
                version_id=version_id,
                new_fields=llm_fields,
                field_thresholds=field_thresholds,
                default_threshold=default_threshold,
            )

            logger.info(
                f"LLM fallback: merged {merge_result['updated_count']} fields, "
                f"{merge_result['skipped_count']} skipped, "
                f"{hallucinated_count} hallucinations blocked ({_llm_elapsed}s)"
            )
            self._log_step(
                f"✅ LLM fallback: {merge_result['updated_count']} additional fields "
                f"extracted, {hallucinated_count} blocked ({_llm_elapsed}s)"
            )

        except Exception as e:
            logger.error(
                f"LLM fallback extraction failed for request {request_id}: {e}",
                exc_info=True,
            )
            self._log_step(f"⚠️ LLM fallback error: {str(e)[:80]}")

    def _get_document_text_for_request(self, request_id: str) -> str:
        """
        Get the full document text for a request.

        Priority:
        1. Azure CU text captured during analysis (works for scanned/OCR docs)
        2. pypdf extraction (works for text-selectable PDFs)
        """
        # 1. Use Azure CU text if available (captured during analysis —
        #    this is the ONLY source for scanned/image PDFs)
        if self._last_cu_document_text and len(self._last_cu_document_text) >= 100:
            logger.info(
                f"Using Azure CU text for LLM fallback "
                f"({len(self._last_cu_document_text)} chars)"
            )
            return self._last_cu_document_text

        # 2. Fall back to pypdf (fast, local, but fails on scanned docs)
        pypdf_text = self._extract_text_from_pdf_directly(request_id)
        if pypdf_text and len(pypdf_text) >= 100:
            return pypdf_text

        # 3. Last resort: return whatever we have
        return pypdf_text or self._last_cu_document_text or ''

    def _extract_text_from_pdf_directly(self, request_id: str) -> str:
        """
        Fallback: Download the PDF and extract text using pypdf.
        This is less accurate than Azure CU's OCR+layout but works
        when the CU markdown is not stored.
        """
        try:
            docs = self.db.get_request_documents(request_id)
            if not docs:
                return ''

            # Get the first non-email document
            for req_doc in docs:
                doc_id = req_doc.get('document_id') or req_doc.get('document', {}).get('id')
                doc = self.db.get_document(doc_id)
                if not doc or not doc.get('blob_url'):
                    continue
                source_type = req_doc.get('source_type', '')
                if source_type == 'email_body':
                    continue

                # Download and extract text
                if self.storage and self.storage.is_available():
                    pdf_bytes = self.storage.download_document(doc['blob_url'])
                    if pdf_bytes:
                        return self._extract_text_from_bytes(pdf_bytes)

            return ''
        except Exception as e:
            logger.warning(f"Failed to extract text from PDF: {e}")
            return ''

    @staticmethod
    def _extract_text_from_cu_result(result: Dict[str, Any]) -> str:
        """
        Extract the OCR text/markdown from an Azure CU analysis result.
        Azure CU returns text in contents[].markdown or contents[].content,
        which includes OCR-extracted text from scanned documents.
        """
        try:
            contents = result.get('result', {}).get('contents', [])
            if not contents:
                return ''

            parts = []
            for page_idx, content in enumerate(contents):
                page_num = content.get('startPageNumber', page_idx + 1)
                # Azure CU stores OCR text in 'markdown' or 'content'
                page_text = content.get('markdown') or content.get('content') or ''
                if page_text.strip():
                    parts.append(f"--- Page {page_num} ---\n{page_text.strip()}")

            full_text = '\n\n'.join(parts)
            if full_text:
                logger.info(
                    f"Extracted {len(full_text)} chars from Azure CU result "
                    f"({len(contents)} pages)"
                )
            return full_text
        except Exception as e:
            logger.warning(f"Failed to extract text from CU result: {e}")
            return ''

    @staticmethod
    def _extract_text_from_bytes(pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pypdf."""
        try:
            import io
            from pypdf import PdfReader

            reader = PdfReader(io.BytesIO(pdf_bytes))
            text_parts = []
            # Limit to first 100 pages (smart page limit)
            max_pages = min(len(reader.pages), 100)
            for i in range(max_pages):
                page_text = reader.pages[i].extract_text() or ''
                if page_text.strip():
                    text_parts.append(f"--- Page {i+1} ---\n{page_text}")

            full_text = '\n\n'.join(text_parts)
            logger.info(f"Extracted {len(full_text)} chars from {max_pages} PDF pages via pypdf")
            return full_text
        except Exception as e:
            logger.warning(f"pypdf text extraction failed: {e}")
            return ''

    def _run_ai_normalisation(self, request_id: str, cost_tracker: dict = None):
        """
        Run AI normalisation on ALL extracted fields of a request,
        including inactive/alternative fields so that every value has
        a normalised counterpart regardless of which alternative is selected.
        Called automatically after extraction completes.
        Failures are logged but do not fail the overall extraction job.
        """
        try:
            from src.services.ai_normalisation_service import get_ai_normalisation_service

            ai_service = get_ai_normalisation_service()
            if not ai_service.is_available():
                logger.warning(f"AI normalisation service not available — skipping for request {request_id}")
                return

            # Fetch ALL fields including inactive alternatives
            fields = self.db.get_request_fields(request_id, include_inactive=True)
            if not fields:
                logger.info(f"No fields to normalise for request {request_id}")
                return

            # Build normalisation payload — one entry per field (keyed by field id)
            # Include fields with empty values too: normalisation instructions may
            # derive a value from other fields (e.g. "use same as IssuanceDate")
            from src.services.ai_normalisation_service import build_datatype_instruction

            normalisation_payload = []

            for idx, f in enumerate(fields):
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

            normalised_fields = ai_service.normalise_fields(normalisation_payload)

            # ===== COST TRACKING: capture normalisation token costs =====
            if cost_tracker and hasattr(ai_service, '_last_usage'):
                usage = ai_service._last_usage
                if usage:
                    cost_tracker['azure_openai_normalisation_tokens'] = {
                        'input': usage.get('input', 0),
                        'output': usage.get('output', 0),
                        'cost': usage.get('cost', 0.0),
                        'fields_sent': len(normalisation_payload)
                    }
            # ===== END COST TRACKING =====

            # Persist normalised values — match by request_field_id (unique per field)
            normalised_count = 0
            for nf in normalised_fields:
                normalised_value = nf.get('normalised_value', '')
                if not normalised_value:
                    continue
                field_id = nf.get('request_field_id')
                if field_id:
                    self.db.update_request_field_normalised_value(
                        field_id=field_id,
                        normalised_value=normalised_value
                    )
                    normalised_count += 1

            active_count = sum(1 for f in fields if f.get('is_active'))
            alt_count = len(fields) - active_count
            logger.info(
                f"Auto-normalised {normalised_count}/{len(normalisation_payload)} fields "
                f"({active_count} active, {alt_count} alternatives) for request {request_id}"
            )

        except Exception as e:
            # Never fail the extraction job because of normalisation errors
            logger.error(f"AI normalisation failed for request {request_id}: {e}", exc_info=True)

    def _process_email_body_analysis(self, job: Dict[str, Any], request: Dict[str, Any], email: Dict[str, Any]):
        """
        Process analysis directly from email body when no documents are linked.
        Uses OpenAI to extract fields from the email text content.
        """
        request_id = request['id']
        
        self._update_job_progress(job['id'], 10, "Analyzing email body with AI")
        
        # Default confidence threshold for saving fields
        DEFAULT_CONFIDENCE_THRESHOLD = 0.60
        
        # Get template fields for this request's template
        template_fields = []
        template_field_map = {}
        field_thresholds = {}
        if request.get('template_id'):
            template_fields = self.db.get_template_fields(request['template_id'])
            template_field_map = {tf['field_name']: tf for tf in template_fields}
            for tf in template_fields:
                threshold = tf.get('precision_threshold')
                if threshold is not None:
                    field_thresholds[tf['field_name']] = threshold
                else:
                    field_thresholds[tf['field_name']] = DEFAULT_CONFIDENCE_THRESHOLD
            logger.info(f"Found {len(template_fields)} template fields for extraction")
        
        # Use OpenAI to extract fields from email body
        field_extractions = {}
        
        if self.openai:
            try:
                self._update_job_progress(job['id'], 30, "Extracting fields with OpenAI")
                
                # Build extraction prompt based on template fields
                field_names = [tf['field_name'] for tf in template_fields] if template_fields else None
                
                extraction_result = self.openai.extract_fields_from_text(
                    text=email.get('body', ''),
                    field_names=field_names,
                    context={
                        'subject': email.get('subject', ''),
                        'sender': email.get('sender', ''),
                        'document_type': 'CAT Bond Email'
                    }
                )
                
                if extraction_result and extraction_result.get('fields'):
                    for field in extraction_result['fields']:
                        field_name = field.get('field_name')
                        if not field_name:
                            continue
                        
                        confidence = field.get('confidence', 0.8)
                        threshold = field_thresholds.get(field_name, DEFAULT_CONFIDENCE_THRESHOLD)
                        if confidence < threshold:
                            logger.info(f"Field '{field_name}' below threshold ({threshold:.2%}): {confidence:.2%}")
                            continue
                        
                        field_data = {
                            'field_name': field_name,
                            'field_value': field.get('value'),
                            'confidence': confidence,
                            'source_type': 'email_body',
                            'source_id': str(email['id']),
                            'is_active': True,
                            'is_extracted': True
                        }
                        
                        if field_name not in field_extractions:
                            field_extractions[field_name] = []
                        field_extractions[field_name].append(field_data)
                        
                logger.info(f"Extracted {len(field_extractions)} fields from email body")
                
            except Exception as e:
                logger.error(f"Error extracting fields with OpenAI: {e}")
        else:
            logger.warning("OpenAI service not available, using mock extraction")
            # Mock extraction for testing - extract some sample fields
            mock_fields = self._mock_extract_from_email(email, template_fields)
            for field in mock_fields:
                field_name = field['field_name']
                if field_name not in field_extractions:
                    field_extractions[field_name] = []
                field_extractions[field_name].append(field)
        
        self._update_job_progress(job['id'], 70, "Creating request version")
        
        # Build final fields list
        all_fields = []
        extracted_field_names = set()
        
        for field_name, extractions in field_extractions.items():
            sorted_extractions = sorted(
                extractions, 
                key=lambda x: x.get('confidence', 0) or 0, 
                reverse=True
            )
            
            for idx, field in enumerate(sorted_extractions):
                field['is_active'] = (idx == 0)
                if field_name in template_field_map:
                    field['template_field_id'] = template_field_map[field_name]['id']
                all_fields.append(field)
            
            extracted_field_names.add(field_name)
        
        # Add empty records for non-extracted template fields
        if template_fields:
            for tf in template_fields:
                if tf['field_name'] not in extracted_field_names:
                    empty_field = {
                        'field_name': tf['field_name'],
                        'field_value': None,
                        'confidence': None,
                        'source_type': 'pending',
                        'source_id': None,
                        'is_active': True,
                        'is_extracted': False,
                        'template_field_id': tf['id']
                    }
                    all_fields.append(empty_field)
        
        # Create version
        if all_fields:
            version = self.db.create_request_version(
                request_id=request_id,
                version_label='Email Body Extraction',
                fields=all_fields
            )
            self.db.set_current_version(request_id, version['id'])
            logger.info(f"Created version with {len(all_fields)} fields ({len(extracted_field_names)} extracted)")
        
        self._update_job_progress(job['id'], 90, "Updating request status")
        
        # Update status
        status_updated = self.db.update_request_status(request_id, 'reviewing')
        if not status_updated:
            status_updated = self.db.update_request_status(request_id, 'extracted')
        
        # Update email status
        self.db.update_email_status(email['id'], 'processed')
        
        # --- Auto-normalise extracted fields ---
        self._update_job_progress(job['id'], 95, "Running AI normalisation")
        self._run_ai_normalisation(request_id)
        
        logger.info(f"Email body analysis completed for request {request_id}")
    
    def _mock_extract_from_email(self, email: Dict[str, Any], template_fields: list) -> list:
        """Mock extraction for testing when OpenAI is not available"""
        import re
        
        body = email.get('body', '')
        extracted = []
        
        # Simple regex patterns for CAT bond fields
        patterns = {
            'SPV': r'ISSUER:\s*([^\n]+)',
            'TotalIssueSize': r'\$\[?(\d+[\d,]*(?:,\d{3})*)\]?\s*Class',
            'AttachmentPoint': r'ATTACHMENT\s*POINT:\s*\$?([\d,]+)',
            'ExhaustionPoint': r'EXHAUSTION\s*POINT:\s*\$?([\d,]+)',
            'ExpectedLoss': r'EXPECTED\s*LOSS:\s*([\d.]+%)',
            'RiskPeriodStart': r'FIRST\s*ANNUAL\s*RISK\s*PERIOD:.*?on\s+(\w+\s+\d+,\s*\d{4})',
            'RiskPeriodEnd': r'on\s+(\w+\s+\d+,\s*\d{4})[^\n]*SCHEDULED',
            'CoveredArea': r'COVERED\s*AREA:\s*([^\n]+)',
            'CoveredPerils': r'COVERED\s*EVENT:\s*([^\n]+)',
            'TriggerType': r'TRIGGER\s*TYPE:\s*([^\n;]+)',
            'BondSeries': r'Series\s+(\d{4}-\d+)',
            'Bookrunner': r'BOOKRUNNER:\s*([^\n]+)',
            'CedingInsurer': r'CEDING\s*INSURER:\s*([^\n(]+)',
            'ModelingFirm': r'MODELING\s*FIRM.*?:\s*([^\n]+)',
        }
        
        for field_name, pattern in patterns.items():
            match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
            if match:
                value = match.group(1).strip()
                if value:
                    extracted.append({
                        'field_name': field_name,
                        'field_value': value,
                        'confidence': 0.85,
                        'source_type': 'email_body',
                        'source_id': str(email['id']),
                        'is_active': True,
                        'is_extracted': True
                    })
        
        return extracted
    
    def _process_document_analysis(self, job: Dict[str, Any]):
        """Process analysis for a single document"""
        document_id = job['entity_id']
        doc = self.db.get_document(document_id)
        if not doc:
            raise ValueError(f"Document {document_id} not found")
        
        self._update_job_progress(job['id'], 10, "Starting document analysis")
        
        result = self._analyze_single_document(doc)
        
        self._update_job_progress(job['id'], 90, "Analysis complete")
        
        # Update document status
        if result:
            self.db.update_document(document_id, status_name='processed')
        else:
            self.db.update_document(document_id, status_name='failed')
    
    def _analyze_single_document(self, doc: Dict[str, Any], request: Dict[str, Any] = None, email: Dict[str, Any] = None, is_email_doc: bool = False, template_fields: List[Dict[str, Any]] = None, job_id: str = None) -> Optional[Dict[str, Any]]:
        """
        Analyze a single document.
        
        Email body PDFs → OpenAI LLM extraction (no Azure analyzer).
        Regular PDFs     → Azure Content Understanding analyzer.
        """
        if is_email_doc:
            # Email body PDFs ALWAYS use OpenAI LLM — never Azure CU.
            # The CU analyzer is trained on real document structures, not email text.
            logger.info(f"Email doc '{doc.get('filename')}' → routing to OpenAI LLM (not Azure CU)")
            return self._analyze_email_body(email, doc, request, template_fields=template_fields, job_id=job_id)

        # Regular documents → Azure Content Understanding
        return self._analyze_document_with_azure(doc, request)
    
    def _mock_analysis(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Generate mock analysis for testing when Azure is not available"""
        return {
            'status': 'succeeded',
            'document_id': doc['id'],
            'pages': 1,
            'fields': [
                {
                    'name': 'sample_field',
                    'value': 'Sample extracted value',
                    'confidence': 0.95,
                    'page': 1
                }
            ],
            'mock': True
        }
    
    def _analyze_email_body(self, email: Optional[Dict[str, Any]], doc: Dict[str, Any], request: Dict[str, Any] = None, template_fields: List[Dict[str, Any]] = None, job_id: str = None) -> Optional[Dict[str, Any]]:
        """
        Analyze email body using OpenAI LLM extraction.
        
        Text source priority:
        1. Email body text from DB (if email record exists)
        2. Text extracted from the email body PDF using pypdf
        
        This method NEVER falls back to Azure CU — email body PDFs are always
        processed by OpenAI LLM since CU analyzers are trained on real document
        structures, not email conversation text.
        """
        def _step(msg):
            if job_id:
                self._append_step_log(job_id, msg)
        
        # --- 1. Get the email text (from DB or from PDF) ---
        subject = ''
        body = ''
        email_id = None
        
        if email:
            subject = email.get('subject', '')
            body = email.get('body', '')
            email_id = email.get('id')
            _step(f"Email record found (id={email_id}, subject='{subject[:60]}...')")
            if body:
                _step(f"Email body text from DB: {len(body)} chars")
            else:
                _step(f"⚠️ Email body text is empty in DB")
        else:
            _step(f"⚠️ No email record in DB — will extract text from PDF")
        
        # If no body text from DB, extract it from the PDF itself
        if not body:
            _step(f"Downloading PDF for text extraction: {doc.get('filename')}")
            body = self._extract_text_from_email_pdf(doc)
            if body:
                _step(f"Extracted {len(body)} chars from PDF")
            else:
                _step(f"❌ Failed to extract text from PDF")
            if not subject:
                subject = doc.get('filename', 'Email Body PDF')
        
        if not body:
            _step(f"❌ No text content available — cannot analyze")
            logger.warning(f"Could not extract any text from email doc '{doc.get('filename')}'")
            return None
        
        # --- 2. Send to OpenAI LLM ---
        if not self.openai or not self.openai.is_available():
            _step(f"❌ OpenAI service not available — cannot analyze email")
            logger.error("OpenAI service not available")
            return None
        
        try:
            mode = 'template-guided' if template_fields else 'dynamic discovery'
            _step(f"Sending to OpenAI LLM ({mode}, {len(body)} chars)...")
            logger.info(
                f"Analyzing email body with OpenAI LLM: '{subject[:50]}...' "
                f"({len(body)} chars, mode={mode})"
            )
            result = self.openai.analyze_email_body(subject, body, template_fields=template_fields)
            
            if result and result.get('success'):
                # Count extracted fields
                fields = result.get('result', {}).get('contents', [{}])[0].get('fields', {})
                field_count = len(fields)
                _step(f"✅ OpenAI extracted {field_count} fields")
                
                # Create analysis run record (audit only — failure here must not
                # lose the extraction result)
                try:
                    run_id = self.db.create_analysis_run(
                        run_id=None,
                        source_type='email',
                        source_id=email_id or doc.get('id'),
                        analyzer_id='openai_email_analyzer',
                        triggered_by=None
                    )
                    
                    fields_payload = self._extract_fields_only_payload(result)
                    self.db.update_analysis_run(
                        run_id=run_id,
                        status='succeeded',
                        analysis_payload=fields_payload
                    )
                except Exception as audit_err:
                    logger.warning(f"Failed to create analysis_run audit record (non-fatal): {audit_err}")
                    _step(f"⚠️ Audit record skipped (non-fatal DB error)")
                
                logger.info(f"OpenAI email analysis succeeded — {field_count} fields from '{doc.get('filename')}'")
                return result
            else:
                error_msg = result.get('error', 'Unknown error') if result else 'No result returned'
                _step(f"❌ OpenAI returned error: {error_msg[:100]}")
                logger.warning(f"OpenAI email analysis failed: {error_msg}")
                return None
                
        except Exception as e:
            _step(f"❌ OpenAI call failed: {str(e)[:100]}")
            logger.error(f"OpenAI email analysis failed for '{doc.get('filename')}': {e}", exc_info=True)
            return None
    
    def _extract_text_from_email_pdf(self, doc: Dict[str, Any]) -> str:
        """
        Download an email body PDF and extract its text using pypdf.
        Used when the email body text is not stored in the DB (e.g. plugin upload).
        """
        try:
            blob_url = doc.get('blob_url')
            if not blob_url:
                logger.warning(f"Email doc {doc.get('id')} has no blob_url — cannot extract text")
                return ''
            
            if not self.storage or not self.storage.is_available():
                logger.warning("Storage service not available — cannot download email PDF")
                return ''
            
            logger.info(f"Downloading email PDF for text extraction: {doc.get('filename')}")
            pdf_bytes = self.storage.download_document(blob_url)
            if not pdf_bytes:
                logger.warning(f"Failed to download email PDF: {doc.get('filename')}")
                return ''
            
            text = self._extract_text_from_bytes(pdf_bytes)
            logger.info(f"Extracted {len(text)} chars from email PDF '{doc.get('filename')}'")
            return text
        except Exception as e:
            logger.error(f"Failed to extract text from email PDF: {e}")
            return ''
    
    def _analyze_document_with_azure(self, doc: Dict[str, Any], request: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        Analyze a document using Azure Content Understanding.
        
        If the PDF exceeds PDF_CHUNK_MAX_PAGES (default 40), the document is
        split into chunks which are uploaded to blob storage and analyzed in
        parallel.  The per-chunk results are then merged into a single result
        by keeping the highest-confidence value for every field.
        """
        import time as _t
        _method_start = _t.time()
        
        blob_url = doc.get('blob_url')
        if not blob_url:
            logger.warning(f"Document {doc['id']} has no blob_url")
            return self._mock_analysis(doc)
        
        # Check if Azure client is available
        if not self.azure_client or not self.azure_client.is_available():
            logger.warning("Azure Content Understanding not available, using mock analysis")
            return self._mock_analysis(doc)
        
        # Check if storage is available (needed for SAS URLs and chunk uploads)
        if not self.storage or not self.storage.is_available():
            logger.warning("Storage service not available, falling back to direct analysis")
            return self._analyze_single_blob(doc, blob_url, request)
        
        # ---- Determine if chunking is needed ----
        _page_count = 0
        try:
            from src.services import get_pdf_chunker
            chunker = get_pdf_chunker()
            
            # Download the PDF to check page count
            logger.info(f"⏱️ [{doc.get('filename')}] Downloading PDF from blob...")
            self._log_step(f"Downloading PDF: {doc.get('filename', 'unknown')}")
            _dl_start = _t.time()
            pdf_bytes = self.storage.download_document(blob_url)
            _dl_elapsed = round(_t.time()-_dl_start, 2)
            
            logger.info(f"⏱️ [{doc.get('filename')}] PDF downloaded in {_dl_elapsed}s ({len(pdf_bytes) if pdf_bytes else 0} bytes)")
            self._log_step(f"PDF downloaded ({_dl_elapsed}s, {len(pdf_bytes) if pdf_bytes else 0} bytes)")
            
            if pdf_bytes:
                _page_count = chunker.get_page_count(pdf_bytes)
            
            # Smart page limit: trim very large docs to first N pages
            # Cat bond key terms are typically on pages 5-10, so we don't
            # need to process 250+ page offering circulars in full.
            if pdf_bytes and chunker.smart_page_limit > 0 and _page_count > chunker.smart_page_limit:
                self._log_step(
                    f"📄 Large PDF ({_page_count} pages) — trimming to first "
                    f"{chunker.smart_page_limit} pages (key data is in first pages)"
                )
                pdf_bytes, _original_pages, _was_trimmed = chunker.trim_to_limit(pdf_bytes)
                _page_count_for_analysis = chunker.smart_page_limit
            else:
                _original_pages = _page_count
                _page_count_for_analysis = _page_count
                _was_trimmed = False
            
            if chunker.needs_chunking(pdf_bytes):
                logger.info(
                    f"⏱️ [{doc.get('filename')}] {_page_count_for_analysis} pages > chunk limit {chunker.max_pages} — "
                    f"splitting into chunks for parallel processing"
                )
                self._log_step(f"Splitting {_page_count_for_analysis} pages into chunks of {chunker.max_pages} for parallel analysis")
                result = self._analyze_chunked_document(doc, pdf_bytes, request)
                # Inject total page count so cost_tracker in the caller can pick it up
                if result:
                    result['_pages_analyzed'] = _page_count_for_analysis
                logger.info(f"⏱️ [{doc.get('filename')}] CHUNKED analysis total: {_t.time()-_method_start:.2f}s")
                return result
            else:
                logger.info(f"⏱️ [{doc.get('filename')}] {_page_count_for_analysis} pages <= chunk limit {chunker.max_pages} — analyzing as single file")
                self._log_step(f"PDF has {_page_count_for_analysis} pages — sending as single request to Azure CU")
        except Exception as e:
            logger.warning(f"Chunking check failed ({e}), falling back to single-file analysis")
        
        # Standard single-file analysis path — pass page count to avoid re-downloading
        return self._analyze_single_blob(doc, blob_url, request, page_count=_page_count)

    # ------------------------------------------------------------------
    # Single-blob analysis (original path, extracted to helper)
    # ------------------------------------------------------------------

    def _analyze_single_blob(self, doc: Dict[str, Any], blob_url: str, request: Dict[str, Any] = None, page_count: int = 0) -> Optional[Dict[str, Any]]:
        """Analyze a single blob URL via Azure Content Understanding (no chunking)."""
        import time as _t
        _blob_start = _t.time()
        
        try:
            logger.info(f"⏱️ [{doc.get('filename')}] Generating SAS URL...")
            _sas_start = _t.time()
            sas_url = self.storage.generate_sas_url(blob_url, expiry_hours=1)
            logger.info(f"⏱️ [{doc.get('filename')}] SAS URL generated in {_t.time()-_sas_start:.2f}s")
        except Exception as e:
            logger.error(f"Error generating SAS URL: {e}")
            return self._mock_analysis(doc)
        
        # Resolve analyzer ID
        azure_analyzer_id, internal_analyzer_id = self._resolve_analyzer_ids(request)
        logger.info(f"⏱️ [{doc.get('filename')}] Using analyzer: {azure_analyzer_id}")
        self._log_step(f"Using analyzer: {azure_analyzer_id}")
        
        try:
            logger.info(f"⏱️ [{doc.get('filename')}] Calling begin_analyze...")
            self._log_step(f"Submitting to Azure Content Understanding...")
            self._log_step(f"  Endpoint: {self.azure_client.endpoint}")
            self._log_step(f"  API version: {self.azure_client.api_version}")
            self._log_step(f"  Analyzer: {azure_analyzer_id}")
            self._log_step(f"  Document: {doc.get('filename')} ({page_count} pages)")
            _begin_start = _t.time()
            response = self.azure_client.begin_analyze(sas_url, analyzer_id=azure_analyzer_id)
            _begin_elapsed = round(_t.time()-_begin_start, 2)
            
            # Log response headers for debugging
            op_location = response.headers.get('operation-location', 'MISSING')
            logger.info(f"⏱️ [{doc.get('filename')}] begin_analyze returned {response.status_code} in {_begin_elapsed}s")
            logger.info(f"⏱️ [{doc.get('filename')}] operation-location: {op_location}")
            self._log_step(f"Azure CU accepted (HTTP {response.status_code}, {_begin_elapsed}s)")
            azure_op_id = self.azure_client.extract_operation_id(response)
            self._log_step(f"Operation ID: {azure_op_id or 'unknown'}")
            
            run_id = self.db.create_analysis_run(
                run_id=None,
                source_type='document',
                source_id=doc['id'],
                analyzer_id=internal_analyzer_id or azure_analyzer_id,
                triggered_by=request.get('created_by') if request else None,
                azure_operation_id=azure_op_id
            )
            
            _single_timeout = int(os.environ.get('AZURE_CU_SINGLE_TIMEOUT', '600'))
            logger.info(f"⏱️ [{doc.get('filename')}] Starting poll_result (timeout={_single_timeout}s)...")
            self._log_step(f"Polling Azure CU for results (timeout={_single_timeout}s)...")
            _poll_start = _t.time()
            result = self.azure_client.poll_result(
                response, 
                timeout_seconds=_single_timeout,
                step_log_callback=self._log_step
            )
            _poll_elapsed = round(_t.time()-_poll_start, 2)
            logger.info(f"⏱️ [{doc.get('filename')}] poll_result completed in {_poll_elapsed}s")
            self._log_step(f"✅ Azure CU analysis complete ({_poll_elapsed}s)")
            
            # Also capture the id from the result JSON if available
            if result and not azure_op_id:
                azure_op_id = result.get('id')
                if azure_op_id:
                    self.db.update_analysis_run_azure_op_id(run_id, azure_op_id)
            
            if result:
                fields_payload = self._extract_fields_only_payload(result)
                self.db.update_analysis_run(run_id=run_id, status='succeeded', analysis_payload=fields_payload)
                
                # Capture Azure CU's OCR text for LLM fallback.
                # This is critical for scanned/image-only PDFs where pypdf
                # returns nothing but Azure CU has OCR-extracted the text.
                cu_text = self._extract_text_from_cu_result(result)
                if cu_text and len(cu_text) > len(self._last_cu_document_text):
                    self._last_cu_document_text = cu_text
                    logger.info(f"Captured {len(cu_text)} chars of Azure CU text for LLM fallback")
                
                # Inject page count into result so caller can accumulate cost_tracker
                result['_pages_analyzed'] = page_count
                
                # ===== COST TRACKING =====
                cu_cost = page_count * 0.01
                logger.info(
                    f"💰 Azure CU — {doc.get('filename', doc['id'])}: "
                    f"{page_count} pages, ${cu_cost:.4f}"
                )
                # ===== END COST TRACKING =====
                
                _total_blob = round(_t.time()-_blob_start, 2)
                logger.info(f"⏱️ [{doc.get('filename')}] _analyze_single_blob TOTAL: {_total_blob}s")
                self._log_step(f"Document analysis complete ({_total_blob}s total)")
                return result
            else:
                self.db.update_analysis_run(run_id=run_id, status='failed', error_message='No result returned')
                self._log_step(f"⚠️ Azure returned no result for {doc.get('filename')}")
                return self._mock_analysis(doc)
        except TimeoutError as te:
            _fail_elapsed = round(_t.time()-_blob_start, 2)
            logger.error(f"⏱️ [{doc.get('filename')}] _analyze_single_blob TIMEOUT after {_fail_elapsed}s: {te}")
            self._log_step(f"❌ Azure CU timed out after {_fail_elapsed:.0f}s. The document may be too large or Azure is overloaded. Try again later or use a shorter document.")
            return None
        except Exception as e:
            _fail_elapsed = round(_t.time()-_blob_start, 2)
            logger.error(f"⏱️ [{doc.get('filename')}] _analyze_single_blob FAILED after {_fail_elapsed}s: {e}")
            self._log_step(f"❌ Analysis failed after {_fail_elapsed}s: {str(e)[:100]}")
            return None

    # ------------------------------------------------------------------
    # Chunked / parallel analysis
    # ------------------------------------------------------------------

    def _analyze_chunked_document(
        self,
        doc: Dict[str, Any],
        pdf_bytes: bytes,
        request: Dict[str, Any] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Split a large PDF into chunks and analyze all chunks in parallel
        via Azure Content Understanding's binary upload API.
        
        No temporary blob storage is used — each chunk's raw bytes are
        sent directly to the analyzeBinary endpoint in memory.
        
        A separate analysis_run record is created for EACH chunk so the
        work is fully auditable.  After all chunks complete, the per-chunk
        results are merged into a single consolidated JSON and a final
        "merged" analysis_run is stored.
        """
        from src.services import get_pdf_chunker
        chunker = get_pdf_chunker()
        
        chunks = chunker.split(pdf_bytes)
        if not chunks:
            logger.error(f"PDF chunking produced no chunks for document {doc['id']}")
            return self._mock_analysis(doc)
        
        azure_analyzer_id, internal_analyzer_id = self._resolve_analyzer_ids(request)
        effective_analyzer_id = internal_analyzer_id or azure_analyzer_id
        triggered_by = request.get('created_by') if request else None
        
        # Build chunk info list (all in-memory, no blob uploads)
        chunk_infos = [
            {
                'index': idx,
                'chunk_bytes': chunk_bytes,
                'start_page': start_page,
                'end_page': end_page,
            }
            for idx, (chunk_bytes, start_page, end_page) in enumerate(chunks)
        ]
        
        total_chunks = len(chunk_infos)
        total_pages_chunked = sum(ci['end_page'] - ci['start_page'] + 1 for ci in chunk_infos)
        logger.info(
            f"Document {doc['id']} split into {total_chunks} chunks "
            f"(max {chunker.max_pages} pages each)"
        )
        # ===== COST TRACKING =====
        cu_cost = total_pages_chunked * 0.01
        logger.info(
            f"\n{'='*60}\n"
            f"💰 COST TRACKING — Azure Content Understanding (CHUNKED)\n"
            f"{'='*60}\n"
            f"  Document:    {doc.get('filename', doc['id'])}\n"
            f"  Total pages: {total_pages_chunked}\n"
            f"  Chunks:      {total_chunks}\n"
            f"  Est. cost:   ${cu_cost:.4f}\n"
            f"  (at ~$0.01/page)\n"
            f"{'='*60}"
        )
        # ===== END COST TRACKING =====
        
        # ---- Parallel analysis of all chunks via binary upload ----
        chunk_results: List[Dict[str, Any]] = []
        
        def _analyze_one_chunk(chunk_info: Dict[str, Any]) -> Dict[str, Any]:
            """Analyze a single chunk — runs inside a thread.
            
            Creates its own analysis_run record so every chunk is auditable.
            """
            idx = chunk_info['index']
            start_page = chunk_info['start_page']
            end_page = chunk_info['end_page']
            
            # Create an analysis_run record for this chunk BEFORE sending
            try:
                run_id = self.db.create_analysis_run(
                    run_id=None,
                    source_type='document_chunk',
                    source_id=doc['id'],
                    analyzer_id=effective_analyzer_id,
                    triggered_by=triggered_by,
                    azure_operation_id=None
                )
                self.db.update_analysis_run(run_id=run_id, status='running')
                logger.info(
                    f"Chunk {idx+1}/{total_chunks} (pages {start_page}–{end_page}) "
                    f"→ analysis_run {run_id}"
                )
            except Exception as exc:
                logger.warning(f"Failed to create analysis_run for chunk {idx+1}: {exc}")
                run_id = None
            
            try:
                _chunk_timeout = int(os.environ.get('AZURE_CU_CHUNK_TIMEOUT', '300'))
                self._log_step(f"  Chunk {idx+1}/{total_chunks}: Submitting pages {start_page}–{end_page} to Azure CU...")
                resp = self.azure_client.begin_analyze_binary(
                    chunk_info['chunk_bytes'], analyzer_id=azure_analyzer_id
                )
                self._log_step(f"  Chunk {idx+1}/{total_chunks}: Accepted (HTTP {resp.status_code}), polling...")
                result = self.azure_client.poll_result(
                    resp, timeout_seconds=_chunk_timeout,
                    step_log_callback=self._log_step
                )
                
                # Update the analysis_run with the result
                if run_id:
                    azure_op_id = None
                    if result:
                        azure_op_id = result.get('id')
                    if azure_op_id:
                        self.db.update_analysis_run_azure_op_id(run_id, azure_op_id)
                    
                    if result:
                        fields_payload = self._extract_fields_only_payload(result)
                        self.db.update_analysis_run(
                            run_id=run_id,
                            status='succeeded',
                            analysis_payload=fields_payload
                        )
                        self._log_step(f"  ✅ Chunk {idx+1}/{total_chunks} succeeded")
                    else:
                        self.db.update_analysis_run(
                            run_id=run_id, status='failed',
                            error_message='No result returned from Azure'
                        )
                        self._log_step(f"  ⚠️ Chunk {idx+1}/{total_chunks}: No result returned")
                
                return {
                    'index': idx,
                    'start_page': start_page,
                    'end_page': end_page,
                    'result': result,
                    'success': result is not None,
                    'run_id': run_id,
                }
            except Exception as exc:
                logger.error(f"Chunk {idx+1} analysis failed: {exc}")
                self._log_step(f"  ❌ Chunk {idx+1}/{total_chunks} FAILED: {str(exc)[:100]}")
                if run_id:
                    self.db.update_analysis_run(
                        run_id=run_id, status='failed',
                        error_message=str(exc)[:500]
                    )
                return {
                    'index': idx,
                    'start_page': start_page,
                    'end_page': end_page,
                    'result': None,
                    'success': False,
                    'error': str(exc),
                    'run_id': run_id,
                }
        
        max_workers = min(total_chunks, 5)  # Cap at 5 parallel requests
        logger.info(
            f"Starting parallel binary analysis of {total_chunks} chunks "
            f"(max_workers={max_workers}) — each chunk gets its own analysis_run"
        )
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_analyze_one_chunk, ci): ci
                for ci in chunk_infos
            }
            for future in as_completed(futures):
                chunk_result = future.result()
                chunk_results.append(chunk_result)
                if chunk_result['success']:
                    logger.info(
                        f"Chunk {chunk_result['index']+1} (pages {chunk_result['start_page']}–"
                        f"{chunk_result['end_page']}) completed → run {chunk_result.get('run_id')}"
                    )
                else:
                    logger.warning(
                        f"Chunk {chunk_result['index']+1} failed: {chunk_result.get('error', 'unknown')}"
                    )
        
        # Sort results by chunk index so page offsets are applied correctly
        chunk_results.sort(key=lambda x: x['index'])
        
        successful_results = [cr for cr in chunk_results if cr['success'] and cr['result']]
        logger.info(f"{len(successful_results)}/{len(chunk_results)} chunks succeeded")
        
        if not successful_results:
            logger.error("All chunks failed — falling back to first-chunk-only analysis")
            self._log_step("⚠️ All chunks timed out. Retrying with first chunk only...")
            # Instead of retrying the full doc (which will also timeout),
            # try sending just the first chunk as a last resort
            first_chunk = chunk_infos[0] if chunk_infos else None
            if first_chunk:
                try:
                    _fallback_timeout = int(os.environ.get('AZURE_CU_CHUNK_TIMEOUT', '300'))
                    self._log_step(f"  Fallback: Submitting pages {first_chunk['start_page']}–{first_chunk['end_page']} only...")
                    resp = self.azure_client.begin_analyze_binary(
                        first_chunk['chunk_bytes'], analyzer_id=azure_analyzer_id
                    )
                    result = self.azure_client.poll_result(
                        resp, timeout_seconds=_fallback_timeout,
                        step_log_callback=self._log_step
                    )
                    if result:
                        self._log_step(f"  ✅ Fallback succeeded with first {first_chunk['end_page']} pages")
                        return result
                except Exception as fallback_err:
                    logger.error(f"Fallback first-chunk analysis also failed: {fallback_err}")
                    self._log_step(f"  ❌ Fallback also failed: {str(fallback_err)[:80]}")
            
            self._log_step("❌ Document analysis failed — Azure CU could not process this document. Try a shorter document or retry later.")
            return None
        
        # ---- Merge all chunk results into one consolidated JSON ----
        merged_result = self._merge_chunk_results(successful_results)

        # Capture Azure CU text from all chunks for LLM fallback
        # (critical for scanned/OCR PDFs where pypdf returns nothing)
        all_cu_text_parts = []
        for cr in successful_results:
            chunk_text = self._extract_text_from_cu_result(cr['result'])
            if chunk_text:
                all_cu_text_parts.append(chunk_text)
        if all_cu_text_parts:
            combined_cu_text = '\n\n'.join(all_cu_text_parts)
            if len(combined_cu_text) > len(self._last_cu_document_text):
                self._last_cu_document_text = combined_cu_text
                logger.info(f"Captured {len(combined_cu_text)} chars from {len(all_cu_text_parts)} chunks for LLM fallback")
        
        # Store the final merged analysis_run (source_type='document' for the full doc)
        try:
            chunk_run_ids = [cr.get('run_id') for cr in chunk_results if cr.get('run_id')]
            
            run_id = self.db.create_analysis_run(
                run_id=None,
                source_type='document',
                source_id=doc['id'],
                analyzer_id=effective_analyzer_id,
                triggered_by=triggered_by,
                azure_operation_id=None
            )
            fields_payload = self._extract_fields_only_payload(merged_result)
            self.db.update_analysis_run(
                run_id=run_id,
                status='succeeded',
                analysis_payload=fields_payload
            )
            logger.info(
                f"Created merged analysis_run {run_id} from {len(successful_results)} chunks "
                f"(chunk runs: {chunk_run_ids})"
            )
        except Exception as e:
            logger.warning(f"Failed to store merged analysis run: {e}")
        
        return merged_result

    # ------------------------------------------------------------------
    # Merge chunk results — highest confidence wins per field
    # ------------------------------------------------------------------

    def _merge_chunk_results(self, chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge multiple Azure Content Understanding results into a single
        result structure.  For every field that appears in more than one
        chunk, the value with the **highest confidence** is kept.
        
        Page numbers in the ``source`` bounding-box strings are offset so
        they refer to the original document's page numbering.
        """
        # Use the first successful result as the base structure
        base = chunk_results[0]['result']
        
        # Collect all fields across chunks, keyed by field name
        # Each entry: { field_name: [ (field_data_dict, page_offset) , … ] }
        all_fields: Dict[str, List[Dict[str, Any]]] = {}
        
        for cr in chunk_results:
            result = cr['result']
            page_offset = cr['start_page'] - 1  # offset to add to chunk-local page nums
            
            result_data = result.get('result', {})
            contents = result_data.get('contents', [])
            
            for content in contents:
                content_fields = content.get('fields', {})
                for field_name, field_data in content_fields.items():
                    if not isinstance(field_data, dict):
                        continue
                    
                    # Deep-copy field data so we can mutate page references
                    fd = copy.deepcopy(field_data)
                    
                    # Offset page numbers in the source bounding-box string
                    fd = self._offset_source_pages(fd, page_offset)
                    
                    if field_name not in all_fields:
                        all_fields[field_name] = []
                    all_fields[field_name].append(fd)
        
        # For each field, pick the best entry.
        # IMPORTANT: prefer non-empty values over empty ones, even if
        # the empty entry has higher confidence.  Azure returns high
        # confidence for empty values meaning "I'm confident this chunk
        # does NOT contain that field" – we must not let that shadow a
        # real extraction from another chunk.
        merged_fields: Dict[str, Any] = {}
        for field_name, candidates in all_fields.items():
            def _has_value(c):
                """Return True if the candidate contains a non-empty extracted value."""
                v = c.get('valueString') or c.get('value') or ''
                if isinstance(v, list):
                    return len(v) > 0
                return bool(str(v).strip())

            non_empty = [c for c in candidates if _has_value(c)]
            pool = non_empty if non_empty else candidates  # fall back to empty only if ALL are empty

            best = max(pool, key=lambda c: c.get('confidence', 0) or 0)
            merged_fields[field_name] = best
            
            if len(candidates) > 1:
                confidences = [c.get('confidence', 0) for c in candidates]
                picked_val = best.get('valueString') or best.get('value') or ''
                if isinstance(picked_val, list):
                    picked_val = f"[{len(picked_val)} items]"
                else:
                    picked_val = str(picked_val)[:50]
                logger.info(
                    f"Field '{field_name}': merged {len(candidates)} extractions, "
                    f"confidences={confidences}, picked confidence={best.get('confidence', 0)}, "
                    f"non_empty={len(non_empty)}/{len(candidates)}, value='{picked_val}'"
                )
        
        # Build the merged result in Azure CU format
        merged_result = copy.deepcopy(base)
        if 'result' in merged_result and 'contents' in merged_result['result']:
            if merged_result['result']['contents']:
                merged_result['result']['contents'][0]['fields'] = merged_fields
            else:
                merged_result['result']['contents'] = [{'fields': merged_fields}]
        else:
            merged_result.setdefault('result', {}).setdefault('contents', [{'fields': merged_fields}])
        
        logger.info(f"Merged {len(merged_fields)} fields from {len(chunk_results)} chunks")
        return merged_result

    def _offset_source_pages(self, field_data: Dict[str, Any], page_offset: int) -> Dict[str, Any]:
        """
        Adjust page numbers inside the ``source`` string ``D(pageNum, …)``
        by adding ``page_offset`` so they reference the original full document.
        Also handles list/array field values recursively.
        """
        import re
        
        def _offset_source_string(source: str) -> str:
            if not source or not isinstance(source, str):
                return source
            
            def _replace(m):
                original_page = int(m.group(1))
                new_page = original_page + page_offset
                return f"D({new_page},"
            
            return re.sub(r'D\((\d+),', _replace, source)
        
        # Offset top-level source
        if 'source' in field_data:
            field_data['source'] = _offset_source_string(field_data['source'])
        
        # Offset array items
        value = field_data.get('valueString') or field_data.get('value')
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and 'source' in item:
                    item['source'] = _offset_source_string(item['source'])
        
        return field_data

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_analyzer_ready(self, request: Dict[str, Any], job_id: str):
        """
        Check if the template has a built Azure CU analyzer. If not, build one
        synchronously before extraction starts. This ensures that auto-created
        templates (from custom prompts or no-match auto-create) have a working
        analyzer by the time we start document extraction.

        The build typically takes 30-120 seconds. The user sees progress updates
        in the plugin polling UI.
        """
        import json
        import uuid

        template_id = request.get('template_id')
        if not template_id:
            return

        template = self.db.get_template(template_id)
        if not template:
            return

        # Already has an analyzer — nothing to do
        if template.get('analyzer') and template['analyzer'].get('azure_analyzer_id'):
            logger.info(
                f"Template {template_id} already has analyzer: "
                f"{template['analyzer']['azure_analyzer_id']}"
            )
            return

        # No analyzer — build one now
        fields = template.get('fields') or []
        if not fields:
            logger.warning(f"Template {template_id} has no fields — skipping analyzer build")
            return

        self._append_step_log(job_id, f"🔨 Building custom analyzer for template '{template.get('name')}' ({len(fields)} fields)...")
        self._update_job_progress(job_id, 8, "Building custom analyzer...")

        try:
            from src.services.azure_service import get_azure_client
            azure_client = get_azure_client()
            if not azure_client or not azure_client.is_available():
                logger.warning("Azure CU not available — skipping analyzer build, will use prebuilt-layout")
                self._append_step_log(job_id, "⚠️ Azure CU not available — using prebuilt-layout fallback")
                return

            # Generate IDs
            suffix = str(uuid.uuid4())[:8]
            analyzer_record_id = f"anl_{suffix}"
            azure_analyzer_id = f"tmpl_{template_id}_{suffix}"

            # Build field schema for Azure CU
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
                self._append_step_log(job_id, "⚠️ No fields for analyzer — using prebuilt-layout")
                return

            logger.info(
                f"🔨 Building analyzer '{azure_analyzer_id}' for template "
                f"'{template.get('name')}' ({len(cu_fields)} fields)"
            )

            # 1) Create analyzer record in DB
            self.db.create_analyzer(
                analyzer_id=analyzer_record_id,
                org_id=request.get('organization_id'),
                name=f"{template.get('name', 'Template')} Analyzer",
                description=f"Auto-built analyzer for template '{template.get('name')}'",
                analyzer_type='azure_cu',
                azure_analyzer_id=azure_analyzer_id,
                configuration=json.dumps({
                    'template_id': template_id,
                    'field_count': len(cu_fields),
                    'created_via': 'job_processor_auto_build',
                })
            )

            # 2) Create analyzer in Azure CU
            import time as _t
            _build_start = _t.time()
            create_result = azure_client.create_custom_analyzer(
                analyzer_id=azure_analyzer_id,
                fields=cu_fields,
                description=f"Auto-built analyzer for template '{template.get('name')}'",
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
                self._append_step_log(job_id, f"Waiting for Azure CU to build analyzer (up to 180s)...")
                operation_payload = azure_client.poll_operation(
                    operation_location=operation_location,
                    timeout_seconds=180
                )

            _build_elapsed = round(_t.time() - _build_start, 1)
            build_status = (operation_payload or {}).get('status', 'succeeded')

            # 4) Mark analyzer active and link to template
            self.db.update_analyzer(
                analyzer_id=analyzer_record_id,
                is_active=True,
                configuration=json.dumps({
                    'template_id': template_id,
                    'field_count': len(cu_fields),
                    'created_via': 'job_processor_auto_build',
                    'operation_id': create_result.get('operation_id'),
                    'operation_status': build_status,
                    'build_time_seconds': _build_elapsed,
                })
            )
            self.db.link_template_to_analyzer(
                template_id=template_id,
                analyzer_id=analyzer_record_id
            )

            logger.info(
                f"✅ Analyzer '{azure_analyzer_id}' built in {_build_elapsed}s and "
                f"linked to template {template_id} ({len(cu_fields)} fields)"
            )
            self._append_step_log(
                job_id,
                f"✅ Custom analyzer ready ({len(cu_fields)} fields, {_build_elapsed}s)"
            )

        except Exception as e:
            logger.error(
                f"Failed to build analyzer for template {template_id}: {e}",
                exc_info=True
            )
            self._append_step_log(
                job_id,
                f"⚠️ Analyzer build failed — using prebuilt-layout fallback ({str(e)[:80]})"
            )
            # Extraction will fall back to prebuilt-layout + LLM — still works

    def _resolve_analyzer_ids(self, request: Dict[str, Any] = None):
        """Resolve the Azure and internal analyzer IDs from template or env.

        Priority:
        1. Template's linked custom CU analyzer (if template has one built)
        2. prebuilt-layout (if request has a template but no custom analyzer yet —
           this is the normal path for newly-classified/auto-created templates)
        3. prebuilt-layout (when NO template at all — classification found no match
           and cleared the default, or legacy requests)
        """
        azure_analyzer_id = None
        internal_analyzer_id = None
        has_template = False

        if request and request.get('template_id'):
            has_template = True
            template = self.db.get_template(request['template_id'])
            if template and template.get('analyzer'):
                azure_analyzer_id = template['analyzer'].get('azure_analyzer_id')
                internal_analyzer_id = template['analyzer'].get('id')
                logger.info(f"Using analyzer from template: {azure_analyzer_id} (internal: {internal_analyzer_id})")

        if not azure_analyzer_id:
            if has_template:
                # Template exists but has no custom analyzer built yet.
                # Use prebuilt-layout — NEVER fall back to an unrelated env-var
                # analyzer (e.g. catBondDocumentAnalyzerLatest) because that
                # analyzer's fields won't match the classified template's fields.
                azure_analyzer_id = 'prebuilt-layout'
                logger.info(
                    f"Template {request.get('template_id')} has no custom analyzer — "
                    f"using prebuilt-layout (generic OCR)"
                )
            else:
                # No template at all — always use prebuilt-layout.
                # This covers two cases:
                #   a) Classification found no match and cleared the default template
                #      → must NOT use env var (e.g. catBondDocumentAnalyzerLatest)
                #      because it would extract fields for the wrong document type.
                #   b) Legacy requests without templates → generic OCR is safest.
                azure_analyzer_id = 'prebuilt-layout'
                logger.info(
                    f"No template assigned — using prebuilt-layout for generic OCR "
                    f"(env AZURE_ANALYZER_ID='{self.azure_client.analyzer_id}' ignored)"
                )

        return azure_analyzer_id, internal_analyzer_id
    
    @staticmethod
    def _extract_fields_only_payload(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract only the 'fields' from an Azure Content Understanding result.
        The full response is: {id, status, result: {analyzerId, contents: [{fields: {...}}]}}
        We store only the combined fields dict from all contents to save space.
        Non-Azure results (e.g. OpenAI, mock) are returned as-is.
        """
        if not result or not isinstance(result, dict):
            return result

        # Non-Azure results (OpenAI / mock) — store as-is
        if result.get('mock') or result.get('success') is not None:
            return result

        result_data = result.get('result', {})
        contents = result_data.get('contents', [])

        all_fields = {}
        for content in contents:
            content_fields = content.get('fields', {})
            if isinstance(content_fields, dict):
                all_fields.update(content_fields)

        return {
            'fields': all_fields,
            'analyzer_id': result_data.get('analyzerId'),
            'created_at': result_data.get('createdAt'),
            'azure_operation_id': result.get('id'),
        }

    def _extract_fields_from_analysis(self, result: Dict[str, Any], doc_id: str, template_field_map: Dict[str, Dict] = None) -> List[Dict[str, Any]]:
        """Extract field data from analysis result"""
        fields = []
        template_field_map = template_field_map or {}
        
        # DEBUG: Log the full Azure response structure including all value keys
        logger.info(f"=== AZURE RESPONSE DEBUG ===")
        logger.info(f"Top-level keys: {list(result.keys())}")
        if 'result' in result:
            logger.info(f"result keys: {list(result['result'].keys())}")
            if 'contents' in result['result']:
                for i, content in enumerate(result['result']['contents']):
                    logger.info(f"contents[{i}] keys: {list(content.keys())}")
                    if 'fields' in content:
                        logger.info(f"contents[{i}]['fields'] has {len(content['fields'])} fields")
                        for fname, fdata in content['fields'].items():
                            if isinstance(fdata, dict):
                                # Log ALL keys so we can see which value* key Azure used
                                value_keys = [k for k in fdata.keys() if k.startswith('value')]
                                raw_value = (
                                    fdata.get('valueString')
                                    or fdata.get('valueDate')
                                    or fdata.get('valueNumber')
                                    or fdata.get('valueInteger')
                                    or fdata.get('valueCurrency')
                                    or fdata.get('valueBoolean')
                                    or fdata.get('value', '')
                                )
                                logger.info(
                                    f"  Field '{fname}': keys={list(fdata.keys())}, "
                                    f"value_keys={value_keys}, raw_value='{raw_value}', "
                                    f"confidence={fdata.get('confidence', 0)}"
                                )
                            else:
                                logger.info(f"  Field '{fname}': {fdata}")
        logger.info(f"=== END DEBUG ===")
        
        # Handle mock results
        if result.get('mock'):
            for f in result.get('fields', []):
                fields.append({
                    'field_name': f['name'],
                    'field_value': str(f['value']),
                    'confidence': f.get('confidence'),
                    'source_type': 'document',
                    'source_id': doc_id,
                    'page_number': f.get('page')
                })
            return fields
        
        # Handle Azure Content Understanding results
        # The API returns: result['result']['contents'][0]['fields']
        result_data = result.get('result', {})
        contents = result_data.get('contents', [])
        
        for content in contents:
            # Extract from fields (custom analyzer fields)
            content_fields = content.get('fields', {})
            for field_name, field_data in content_fields.items():
                if isinstance(field_data, dict):
                    # Azure CU returns typed value keys: valueString, valueDate,
                    # valueNumber, valueInteger, valueCurrency, valueBoolean, value
                    value = (
                        field_data.get('valueString')
                        or field_data.get('valueDate')
                        or field_data.get('valueNumber')
                        or field_data.get('valueInteger')
                        or field_data.get('valueCurrency')
                        or field_data.get('valueBoolean')
                        or field_data.get('value', '')
                    )
                    # Convert non-string values to string for consistent handling
                    if value is not None and not isinstance(value, str):
                        value = str(value)
                    confidence = field_data.get('confidence', 0)
                    
                    # If value is empty, confidence should be 0 regardless of what Azure reports
                    if not value or not str(value).strip():
                        confidence = 0
                    
                    # Extract source/bounding box data
                    # Azure returns: "source": "D(pageNum, x1,y1, x2,y2, x3,y3, x4,y4)"
                    source_location = field_data.get('source', '')
                    
                    page_number = None
                    
                    # Parse page number from source string D(pageNum, ...)
                    if isinstance(source_location, str) and source_location.startswith('D('):
                        try:
                            page_str = source_location[2:].split(',')[0]
                            page_number = int(page_str)
                        except (ValueError, IndexError):
                            pass
                    
                    # Handle array values
                    if isinstance(value, list):
                        for i, item in enumerate(value):
                            item_value = item.get('valueString') or item.get('value', '') if isinstance(item, dict) else str(item)
                            item_source = item.get('source', '') if isinstance(item, dict) else ''
                            item_page = None
                            if isinstance(item_source, str) and item_source.startswith('D('):
                                try:
                                    item_page = int(item_source[2:].split(',')[0])
                                except:
                                    pass
                            # Set confidence to 0 if item value is empty
                            item_conf = item.get('confidence', confidence) if isinstance(item, dict) else confidence
                            if not item_value or not str(item_value).strip():
                                item_conf = 0
                            
                            # Normalize the value based on field type
                            field_name_full = f"{field_name}_{i+1}"
                            template_field = template_field_map.get(field_name_full, {})
                            field_type = template_field.get('data_type', 'text')
                            field_values = template_field.get('field_values')
                            
                            normalizer = get_field_normalizer()
                            normalized_val, stored_normalized = normalizer.normalize_field(
                                str(item_value), 
                                field_type, 
                                field_values,
                                field_name_full
                            )
                            
                            fields.append({
                                'field_name': field_name_full,
                                'field_value': normalized_val,  # Display value (normalized)
                                'extracted_value': str(item_value),  # Raw AI extraction
                                'normalized_value': stored_normalized,  # Normalized for storage/search
                                'confidence': item_conf,
                                'source_type': 'document',
                                'source_id': doc_id,
                                'page_number': item_page or content.get('startPageNumber'),
                                'bounding_box': item_source
                            })
                    else:
                        # Normalize single values based on field type
                        template_field = template_field_map.get(field_name, {})
                        field_type = template_field.get('data_type', 'text')
                        field_values = template_field.get('field_values')
                        
                        normalizer = get_field_normalizer()
                        normalized_val, stored_normalized = normalizer.normalize_field(
                            str(value) if value else '', 
                            field_type, 
                            field_values,
                            field_name
                        )
                        
                        fields.append({
                            'field_name': field_name,
                            'field_value': normalized_val,  # Display value (normalized)
                            'extracted_value': str(value) if value else '',  # Raw AI extraction
                            'normalized_value': stored_normalized,  # Normalized for storage/search
                            'confidence': confidence,
                            'source_type': 'document',
                            'source_id': doc_id,
                            'page_number': page_number or content.get('startPageNumber'),
                            'bounding_box': source_location
                        })
            
            # Extract from key-value pairs (if present)
            for kv in content.get('keyValuePairs', []):
                key = kv.get('key', {}).get('content', '')
                value = kv.get('value', {}).get('content', '')
                confidence = kv.get('confidence', 0)
                
                if key and value:
                    # Get bounding regions from key-value pairs
                    kv_regions = kv.get('boundingRegions', [])
                    page_num = kv_regions[0].get('pageNumber') if kv_regions else None
                    
                    fields.append({
                        'field_name': key,
                        'field_value': value,
                        'confidence': confidence,
                        'source_type': 'document',
                        'source_id': doc_id,
                        'page_number': page_num,
                        'bounding_box': str(kv_regions) if kv_regions else None
                    })
            
            # Extract from tables (skip for now - too many fields)
            # Tables are already extracted as structured data above
        
        logger.info(f"Extracted {len(fields)} fields from document {doc_id}")
        return fields
    
    def _update_job_progress(self, job_id: str, progress: int, message: str):
        """Update job progress and append to processing log"""
        import time as _t
        self._central_db.update_async_job(
            job_id,
            progress_percent=progress,
            progress_message=message
        )
        # Also append to in-memory processing log (stored to result_data periodically)
        self._append_step_log(job_id, message, progress)

    def _append_step_log(self, job_id: str, message: str, progress: int = None):
        """
        Append a timestamped log entry to the job's processing log.
        Stored in result_data JSON so the frontend can poll it in real-time.
        """
        import time as _t
        import json as _json
        from datetime import datetime

        if not hasattr(self, '_processing_logs'):
            self._processing_logs = {}

        if job_id not in self._processing_logs:
            self._processing_logs[job_id] = {
                'start_time': _t.time(),
                'steps': []
            }

        log_entry = self._processing_logs[job_id]
        elapsed = round(_t.time() - log_entry['start_time'], 2)

        step = {
            'timestamp': datetime.utcnow().strftime('%H:%M:%S'),
            'elapsed_seconds': elapsed,
            'message': message,
        }
        if progress is not None:
            step['progress'] = progress

        log_entry['steps'].append(step)

        # Persist to DB every step so frontend can poll it
        try:
            log_data = _json.dumps({
                'processing_log': log_entry['steps'],
                'total_elapsed_seconds': elapsed,
            })
            self._central_db.update_async_job(job_id, result_data=log_data)
        except Exception:
            pass  # Don't fail the job because of log persistence

    def _log_step(self, message: str):
        """Convenience: append step log using the current job_id."""
        job_id = getattr(self, '_current_job_id', None)
        if job_id:
            self._append_step_log(job_id, message)

    def _update_parent_status(self, job: Dict[str, Any], status: str):
        """Update the parent entity status after job completion/failure"""
        entity_type = job.get('entity_type')
        entity_id = job.get('entity_id')
        
        if entity_type == 'request':
            self.db.update_request_status(entity_id, status)
        elif entity_type == 'document':
            self.db.update_document(entity_id, status_name=status)


# Shared (central) processor for querying pending jobs & fallback
_processor = None

# Cache of per-tenant processors to avoid re-creating on every poll
_tenant_processors: dict[str, RequestProcessor] = {}


def get_processor() -> RequestProcessor:
    """Get the shared request processor (uses central database)"""
    global _processor
    if _processor is None:
        _processor = RequestProcessor()
    return _processor


def _get_tenant_processor(org_id: str) -> RequestProcessor:
    """Get or create a tenant-aware processor for the given org_id.

    Falls back to the shared processor when *org_id* is None / empty.
    Processors are cached so that connection pools are reused across
    successive poll cycles.
    """
    if not org_id:
        return get_processor()
    if org_id not in _tenant_processors:
        _tenant_processors[org_id] = RequestProcessor(org_id=org_id)
    return _tenant_processors[org_id]


def process_pending_jobs(max_jobs: int = 10) -> int:
    """
    Process pending jobs.

    The *central* database is always queried for pending jobs (the async_jobs
    table lives in the central DB).  For each job the organisation id is
    resolved and a tenant-aware processor is used so that data reads/writes
    go to the correct tenant database / storage account.

    Returns the number of jobs processed.
    """
    # Always query central DB for the job queue
    shared = get_processor()
    pending_jobs = shared.db.get_pending_jobs(limit=max_jobs)
    
    processed = 0
    for job in pending_jobs:
        try:
            org_id = job.get('organization_id')
            processor = _get_tenant_processor(org_id)
            success = processor.process_job(job['id'])
            if success:
                processed += 1
        except Exception as e:
            logger.error(f"Error processing job {job['id']}: {e}")
    
    return processed
