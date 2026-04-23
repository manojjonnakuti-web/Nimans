"""
OpenAI Service for Email Body Analysis & AI Field Suggestion
Uses Azure OpenAI (GPT-4.1) via the same Azure AI Services resource
as the normalisation service.

Required environment variables:
    AZURE_AI_AGENT_API_KEY      – API key for Azure AI Services
    AZURE_AI_AGENT_ENDPOINT     – Azure OpenAI base URL
    AZURE_AI_FIELD_SUGGESTION_DEPLOYMENT – deployment name (default: gpt-4.1)
"""

import os
import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    from openai import AzureOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("openai package not installed")


class OpenAIService:
    """Service for email analysis and AI field suggestion using Azure OpenAI"""
    
    def __init__(self):
        self.api_key = os.getenv('AZURE_AI_AGENT_API_KEY')
        self.endpoint = os.getenv(
            'AZURE_AI_AGENT_ENDPOINT',
            'https://xtractfoundrydevaisky7a8j.services.ai.azure.com'
        )
        self.deployment = os.getenv('AZURE_AI_FIELD_SUGGESTION_DEPLOYMENT', 'gpt-4.1')
        self.api_version = '2024-12-01-preview'
        
        if not OPENAI_AVAILABLE:
            logger.warning("OpenAI package not available — service disabled")
            self.client = None
            return
            
        if not self.api_key:
            logger.warning(
                "Azure OpenAI service not configured — "
                "set AZURE_AI_AGENT_API_KEY env var"
            )
            self.client = None
            return
        
        try:
            self.client = AzureOpenAI(
                api_key=self.api_key,
                azure_endpoint=self.endpoint,
                api_version=self.api_version
            )
            logger.info(
                f"Azure OpenAI service initialized "
                f"(deployment={self.deployment}, endpoint={self.endpoint})"
            )
        except Exception as e:
            logger.error(f"Failed to initialize Azure OpenAI service: {e}", exc_info=True)
            self.client = None
    
    def is_available(self) -> bool:
        """Check if OpenAI service is available"""
        return self.client is not None
    
    def analyze_email_body(self, subject: str, body: str, template_fields: list = None) -> Optional[Dict[str, Any]]:
        """
        Analyze email body and extract structured information.
        If template_fields is provided, extract ONLY those fields.
        Otherwise, discover and extract ALL relevant fields from the email.
        Returns analysis in Azure Content Understanding compatible format.
        """
        if not self.is_available():
            logger.error("OpenAI service not available")
            return None
        
        try:
            # Truncate body to avoid token limits
            max_chars = 120000
            truncated_body = body[:max_chars] if len(body) > max_chars else body

            if template_fields:
                # Template-guided extraction: extract only the specified fields
                field_list = "\n".join(
                    f"- {f['field_name']}: {f.get('description', '')}" 
                    for f in template_fields
                )
                prompt = f"""Analyze the following email and extract values for these specific fields:

{field_list}

Email Subject: {subject}

Email Body:
{truncated_body}

For each field, extract the value if present in the email. If a field is not mentioned, skip it.

Return as JSON:
{{
    "fields": {{
        "FieldName": {{
            "valueString": "extracted value",
            "confidence": 0.95,
            "source": "Email Body"
        }}
    }},
    "summary": "Brief summary of what this email is about"
}}"""
            else:
                # Dynamic extraction: discover and extract ALL fields from the email
                prompt = f"""Analyze the following email and extract ALL structured data fields you can identify.

Email Subject: {subject}

Email Body:
{truncated_body}

Extract every meaningful data point from the email, such as:
- Names, companies, contacts, addresses
- Dates, deadlines, reference numbers
- Amounts, quantities, prices, currencies
- Product/service descriptions, codes, identifiers
- Any other structured information

For each field, provide:
1. The field name (use CamelCase, no spaces)
2. The extracted value as a string
3. Your confidence level (0.0 to 1.0)

Return as JSON:
{{
    "fields": {{
        "FieldName": {{
            "valueString": "extracted value",
            "confidence": 0.95,
            "source": "Email Body"
        }}
    }},
    "summary": "Brief summary of what this email is about"
}}"""

            response = self.client.chat.completions.create(
                model=self.deployment,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert document analyst. Extract structured data from emails and documents. Return valid JSON only."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=16000
            )
            
            # ===== COST TRACKING =====
            usage = getattr(response, 'usage', None)
            if usage:
                input_tokens = usage.prompt_tokens or 0
                output_tokens = usage.completion_tokens or 0
                total_tokens = usage.total_tokens or 0
                # Azure OpenAI GPT-4.1 pricing: $2.00/1M input, $8.00/1M output
                input_cost = (input_tokens / 1_000_000) * 2.00
                output_cost = (output_tokens / 1_000_000) * 8.00
                total_cost = input_cost + output_cost
                logger.info(
                    f"\n{'='*60}\n"
                    f"💰 COST TRACKING — Azure OpenAI Email Analysis (GPT-4.1)\n"
                    f"{'='*60}\n"
                    f"  Deployment:    {self.deployment}\n"
                    f"  Input tokens:  {input_tokens:,}\n"
                    f"  Output tokens: {output_tokens:,}\n"
                    f"  Total tokens:  {total_tokens:,}\n"
                    f"  Est. cost:     ${total_cost:.6f}\n"
                    f"    (input:  ${input_cost:.6f} | output: ${output_cost:.6f})\n"
                    f"{'='*60}"
                )
            else:
                logger.info("💰 COST TRACKING — Azure OpenAI Email Analysis: usage data not available")
            # ===== END COST TRACKING =====
            
            content = response.choices[0].message.content
            
            import json
            result = json.loads(content)
            
            # Wrap in Azure Content Understanding compatible format
            return {
                "success": True,
                "result": {
                    "contents": [{
                        "fields": result.get("fields", {})
                    }],
                    "summary": result.get("summary", "")
                },
                "analyzedAt": datetime.utcnow().isoformat(),
                "_cost_tracking": {
                    "service": "Azure OpenAI Email Analysis",
                    "deployment": self.deployment,
                    "input_tokens": usage.prompt_tokens if usage else 0,
                    "output_tokens": usage.completion_tokens if usage else 0,
                    "total_tokens": usage.total_tokens if usage else 0,
                    "estimated_cost_usd": round(((usage.prompt_tokens or 0) / 1_000_000 * 2.00 + (usage.completion_tokens or 0) / 1_000_000 * 8.00), 6) if usage else 0
                }
            }
            
        except Exception as e:
            logger.error(f"OpenAI analysis failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }

    def suggest_fields_from_text(self, document_text: str, user_prompt: str = None,
                                  page_count: int = None, max_fields: int = None) -> Dict[str, Any]:
        """
        Two-pass AI field suggestion:
        Pass 1: Enumerate ALL field:value pairs from the document (exhaustive scan)
        Pass 2: Generate proper template field definitions with descriptions

        This ensures nothing is missed — Pass 1 creates a checklist, Pass 2 adds quality.
        """
        if not self.is_available():
            return {"success": False, "error": "OpenAI service not available"}

        try:
            import json as _json

            user_instruction = ""
            if user_prompt and user_prompt.strip():
                user_instruction = f"\n\nADDITIONAL INSTRUCTIONS FROM USER:\n{user_prompt.strip()}\n"

            # =================================================================
            # PASS 1: Exhaustive enumeration of ALL field:value pairs
            # =================================================================
            # Field count target (user-controlled or default)
            if max_fields:
                field_target = f"Aim for approximately {max_fields} fields."
                if max_fields <= 20:
                    field_target += " Only extract the most critical data points (key parties, dates, amounts, identifiers)."
                elif max_fields >= 70:
                    field_target += " Be very thorough — include secondary details, table row data, and supplementary information."
            else:
                field_target = "Aim for 40-60 high-quality fields."

            pass1_prompt = f"""You are an expert document analyst. Your task is to scan this document and list the MOST IMPORTANT structured data points. Focus on quality over quantity.

Go through the document and extract only data points that a business user would actually need to review or act on.

INCLUDE (high-value data):
- Key parties and their roles (issuer, trustee, employee, employer, etc.)
- Important dates (agreement date, maturity, effective date, expiry, vesting, commencement)
- Monetary amounts and financial terms (price, salary, CTC, issue size, coupon, fee, premium)
- Critical terms and conditions (notice period, termination, non-compete duration, governing law)
- Identifiers (ISIN, CUSIP, document ID, contract number, policy number, reference)
- Key percentages, rates, and ratios
- Locations, jurisdictions, and registered addresses
- Contact information (names, addresses, emails, phone numbers)
- Table data: every distinct row/column value in structured tables (each row is a separate data point)
- Typed/printed signatory names, witness names, authorized representatives (ONLY if the name is clearly printed text)
- Coverage details, limits, deductibles, exclusions
- Obligations, covenants, warranties, representations

EXCLUDE (low-value noise):
- Generic boilerplate definitions that just restate legal language
- Duplicate or near-duplicate data points (keep the best version)
- Fields where the value would be >200 chars of legal prose
- Process descriptions or procedural steps
- Standard legal clauses with no specific extractable value (e.g., "Severability")
- HANDWRITTEN SIGNATURES, scribbles, initials, or stamp images — these cannot be reliably extracted by AI. Do NOT create fields for handwritten signatures or signature images. If a signature block has a typed/printed name (e.g. "Signed by: John Smith"), extract the TYPED NAME only, not the signature itself.

IMPORTANT: {field_target} Each field should have a clear, specific, short value (not a paragraph). Be thorough — extract ALL meaningful data points from tables, schedules, and annexures.
{user_instruction}
DOCUMENT TEXT ({page_count or 'unknown'} pages):
---
{document_text[:120000]}
---

Return JSON with this EXACT structure:
{{
    "document_type": "Brief document type",
    "data_points": [
        {{
            "field_name": "CamelCaseName",
            "value": "exact value from document",
            "location": "section or table where found"
        }}
    ],
    "total_found": 50
}}

IMPORTANT: Quality over quantity. Only include fields with clear, specific, extractable values. Skip boilerplate."""

            logger.info(f"Pass 1: Enumerating all fields from {len(document_text)} chars of text")

            # Pass 1 call
            pass1_response = self._call_with_retry(
                prompt=pass1_prompt,
                system_message=(
                    "You are a meticulous document scanner. You extract every single piece of "
                    "structured data from documents without missing anything. Tables, key-value "
                    "pairs, dates, amounts, names, identifiers — everything. "
                    "IMPORTANT: Skip handwritten signatures, scribbles, and stamp images — only "
                    "extract typed/printed signatory names. Return valid JSON only."
                ),
                max_tokens=16000,
                temperature=0.1,
            )

            pass1_content = pass1_response.choices[0].message.content
            pass1_result = _json.loads(pass1_content)
            data_points = pass1_result.get('data_points', [])

            # Track Pass 1 cost
            pass1_usage = getattr(pass1_response, 'usage', None)
            pass1_cost = 0.0
            if pass1_usage:
                pass1_cost = (pass1_usage.prompt_tokens / 1_000_000) * 2.00 + (pass1_usage.completion_tokens / 1_000_000) * 8.00

            logger.info(f"Pass 1 found {len(data_points)} data points (${pass1_cost:.4f})")

            # ── HARD FILTER: Remove signature/handwriting fields ──
            # GPT sometimes ignores prompt instructions about signatures.
            # Strip them out before they reach Pass 2 / template creation.
            _SIGNATURE_KEYWORDS = {'signature', 'signatory', 'signed', 'autograph', 'initial', 'initialled'}
            _SIGNATURE_JUNK_VALUES = {'r-rz', 'r·rz', 'rrz', '[signature]', '[signed]', 'illegible', 'scribble'}
            filtered_points = []
            sig_removed = 0
            for dp in data_points:
                name_lower = dp.get('field_name', '').lower()
                val_lower = str(dp.get('value', '')).lower().strip()
                # Check if field name is signature-related
                is_sig_field = any(kw in name_lower for kw in _SIGNATURE_KEYWORDS)
                # Check if value looks like OCR garbage from a handwritten signature
                is_junk_value = val_lower in _SIGNATURE_JUNK_VALUES or (
                    len(val_lower) <= 6 and not val_lower.isalpha()
                    and any(c in val_lower for c in '·-.')
                )
                if is_sig_field and (is_junk_value or len(val_lower) <= 5):
                    sig_removed += 1
                    logger.info(
                        f"Pass 1: Removed signature field '{dp.get('field_name')}' "
                        f"with junk value '{val_lower}'"
                    )
                    continue
                filtered_points.append(dp)
            if sig_removed:
                logger.info(f"Pass 1: Filtered out {sig_removed} signature/handwriting fields")
            data_points = filtered_points

            if not data_points:
                return {
                    "success": True,
                    "document_type": pass1_result.get("document_type", "Unknown"),
                    "document_summary": "",
                    "fields": [],
                    "total_fields": 0,
                    "notes": "Pass 1 found no data points",
                    "cost": {
                        "input_tokens": pass1_usage.prompt_tokens if pass1_usage else 0,
                        "output_tokens": pass1_usage.completion_tokens if pass1_usage else 0,
                        "estimated_cost_usd": round(pass1_cost, 6)
                    }
                }

            # =================================================================
            # PASS 2: Generate proper template fields from the enumerated list
            # =================================================================
            # Build a concise list of what Pass 1 found
            data_point_lines = []
            for dp in data_points:
                name = dp.get('field_name', 'Unknown')
                val = str(dp.get('value', ''))[:100]
                loc = dp.get('location', '')
                data_point_lines.append(f"  - {name}: \"{val}\" (found in: {loc})")
            data_point_text = "\n".join(data_point_lines)

            pass2_prompt = f"""You are an expert who designs extraction schemas for AI document processing (Azure Content Understanding).

A prior scan found the following {len(data_points)} data points in the document. Your job is to convert these into proper template field definitions with detailed extraction descriptions.

CRITICAL RULES:
1. Create a template field for each data point below.
2. MERGE duplicates aggressively: if two data points have the same or very similar values, keep ONE field with the better name.
   Examples of duplicates to merge into ONE field:
   - "Trigger Type" and "Trigger Type Definition" (same value) → keep "Trigger Type"
   - "Employee Role" and "Employee Position" (same value) → keep "Employee Role"
3. SKIP fields where the value is an entire paragraph of legal boilerplate (>200 chars of generic legal language).
4. SKIP fields for handwritten signatures, scribbles, stamp images, or any field where the value is a signature image. If a data point is named "Signature", "Authorized Signatory", or similar and its value is illegible scribbles or an image description, do NOT create a field for it. Only keep it if the value is a clearly typed/printed person's name.
5. Each description MUST include: what the value is, alternative labels (2-4), where to find it, format guidance.
6. All field_type values MUST be "text".
7. Group fields into logical categories that fit THIS document type. Choose 3-7 short category names based on what makes sense (e.g. for a bond: "Instrument Details", "Financial Terms", "Key Dates"; for a contract: "Parties", "Compensation", "Terms & Conditions"; for an invoice: "Vendor Details", "Line Items", "Payment Terms"). Use "Other" for anything that doesn't fit.
8. sample_value MUST be the actual value from the document (shown below).
9. Set is_required: false for ALL fields. The user will decide which fields are required later.
{user_instruction}
DATA POINTS FOUND IN DOCUMENT:
{data_point_text}

DOCUMENT TYPE: {pass1_result.get('document_type', 'Unknown')}

For EACH data point, generate a field with this structure:
{{
    "field_name": "CamelCaseName",
    "display_name": "Human Readable Name",
    "field_type": "text",
    "category": "Category Name",
    "description": "2-4 sentence extraction instruction with alternative labels, where to look, and format guidance.",
    "sample_value": "the actual value from the document",
    "is_required": true/false,
    "confidence": 0.0-1.0
}}

Return JSON:
{{
    "document_type": "{pass1_result.get('document_type', 'Unknown')}",
    "document_summary": "1-2 sentence summary",
    "fields": [ ... ],
    "total_fields": {len(data_points)},
    "notes": "observations"
}}"""

            logger.info(f"Pass 2: Generating field definitions for {len(data_points)} data points")

            pass2_response = self._call_with_retry(
                prompt=pass2_prompt,
                system_message=(
                    "You are an expert document analyst who designs extraction schemas. "
                    "Convert every data point into a proper template field with detailed "
                    "extraction instructions. Do NOT skip any data points. "
                    "ALL field_type values must be 'text'. Return valid JSON only."
                ),
                max_tokens=16000,
                temperature=0.2,
            )

            pass2_content = pass2_response.choices[0].message.content
            pass2_result = _json.loads(pass2_content)

            # Track Pass 2 cost
            pass2_usage = getattr(pass2_response, 'usage', None)
            pass2_cost = 0.0
            if pass2_usage:
                pass2_cost = (pass2_usage.prompt_tokens / 1_000_000) * 2.00 + (pass2_usage.completion_tokens / 1_000_000) * 8.00

            total_cost = pass1_cost + pass2_cost
            total_input = (pass1_usage.prompt_tokens if pass1_usage else 0) + (pass2_usage.prompt_tokens if pass2_usage else 0)
            total_output = (pass1_usage.completion_tokens if pass1_usage else 0) + (pass2_usage.completion_tokens if pass2_usage else 0)

            fields = pass2_result.get("fields", [])

            # --- Dedup: remove fields with same value where one name is a suffix of the other ---
            # e.g. "Trigger Type" vs "Trigger Type Definition" with same sample_value
            pre_dedup_count = len(fields)
            if fields:
                deduped = []
                seen_values = {}  # normalised_value -> field
                for f in fields:
                    sv = (f.get('sample_value') or '').strip().lower()
                    fn = (f.get('field_name') or '').strip()
                    dn = (f.get('display_name') or '').strip().lower()
                    if not sv or len(sv) < 3:
                        deduped.append(f)
                        continue
                    # Check if we already have a field with the same value
                    if sv in seen_values:
                        existing = seen_values[sv]
                        existing_dn = (existing.get('display_name') or '').strip().lower()
                        # Keep the shorter / cleaner name (drop "Definition" suffix)
                        if 'definition' in dn and 'definition' not in existing_dn:
                            # Current is the duplicate — skip it, add alt labels to existing description
                            logger.debug(f"Dedup: dropping '{f.get('display_name')}' (duplicate of '{existing.get('display_name')}')") 
                            continue
                        elif 'definition' in existing_dn and 'definition' not in dn:
                            # Existing is the duplicate — replace it
                            logger.debug(f"Dedup: replacing '{existing.get('display_name')}' with '{f.get('display_name')}'")
                            deduped = [d for d in deduped if d is not existing]
                            seen_values[sv] = f
                            deduped.append(f)
                            continue
                    seen_values[sv] = f
                    deduped.append(f)
                fields = deduped
                if pre_dedup_count != len(fields):
                    logger.info(f"Dedup: {pre_dedup_count} → {len(fields)} fields ({pre_dedup_count - len(fields)} duplicates removed)")

            logger.info(
                f"Two-pass complete: {len(data_points)} data points → {len(fields)} template fields "
                f"(${total_cost:.4f})"
            )

            return {
                "success": True,
                "document_type": pass2_result.get("document_type", pass1_result.get("document_type", "Unknown")),
                "document_summary": pass2_result.get("document_summary", ""),
                "fields": fields,
                "total_fields": len(fields),
                "notes": f"Pass 1: {len(data_points)} data points found. Pass 2: {len(fields)} template fields generated.",
                "cost": {
                    "input_tokens": total_input,
                    "output_tokens": total_output,
                    "estimated_cost_usd": round(total_cost, 6)
                }
            }

        except Exception as e:
            logger.error(f"Field suggestion failed: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    def _call_with_retry(self, prompt: str, system_message: str,
                          max_tokens: int = 8000, temperature: float = 0.2):
        """Helper: Call Azure OpenAI with retry logic for 429 rate limits."""
        max_retries = 3
        base_delay = 10

        for attempt in range(max_retries + 1):
            try:
                response = self.client.chat.completions.create(
                    model=self.deployment,
                    messages=[
                        {"role": "system", "content": system_message},
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                return response
            except Exception as api_err:
                error_str = str(api_err)
                is_rate_limit = '429' in error_str or 'Too Many Requests' in error_str or 'rate' in error_str.lower()
                if is_rate_limit and attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Rate limited (429). Retry {attempt + 1}/{max_retries} after {delay}s...")
                    time.sleep(delay)
                else:
                    raise


    def extract_pending_fields(
        self,
        document_text: str,
        pending_fields: list,
        context: dict = None,
    ) -> Dict[str, Any]:
        """
        LLM fallback extraction for fields that Azure CU couldn't extract.

        After Azure Content Understanding processes a document, some fields
        may come back empty (pending).  This method sends the document text
        + the field descriptions to GPT-4.1 and asks it to extract values
        that CU missed.

        Args:
            document_text: Full text / markdown of the document (from CU or OCR).
            pending_fields: List of dicts, each with at least:
                - field_name (str)
                - display_name (str)
                - description (str)
                - field_type (str, optional): e.g. 'date', 'number', 'currency'
                - category (str, optional): field category grouping
                - field_values (str, optional): enum/dropdown valid options
            context: Optional dict with extra info (template_name, extracted_fields_sample, etc.)

        Returns:
            Dict with 'fields' list containing extracted values.
        """
        if not self.is_available():
            return {"success": False, "error": "OpenAI service not available"}

        if not pending_fields:
            return {"success": True, "fields": [], "note": "No pending fields"}

        try:
            import json as _json

            # Build a rich field spec for the prompt — include type, category,
            # and enum options so the LLM knows exactly what to look for.
            field_specs = []
            for pf in pending_fields:
                # Base: name (display_name): description
                line = (
                    f"  - {pf['field_name']} ({pf.get('display_name', pf['field_name'])}): "
                    f"{pf.get('description', 'No description')}"
                )
                # Append data type if not plain text
                field_type = pf.get('field_type', 'text')
                if field_type and field_type != 'text':
                    line += f"  [type: {field_type}]"
                # Append category for grouping context
                category = pf.get('category', '')
                if category:
                    line += f"  [category: {category}]"
                # Append valid enum options if defined
                field_values = pf.get('field_values')
                if field_values:
                    line += f"  [valid values: {field_values}]"
                field_specs.append(line)
            field_spec_text = "\n".join(field_specs)

            ctx_info = ""
            if context:
                ctx_info = f"\nDocument context: {_json.dumps(context, default=str)[:2000]}\n"

            prompt = f"""You are an expert document analyst. An AI extraction engine has already processed this document but FAILED to extract the following fields. Your job is to find and extract these specific values from the document text below.

CRITICAL RULES — READ CAREFULLY:
1. You MUST ONLY extract values that are EXPLICITLY WRITTEN in the document text below.
2. NEVER invent, guess, or fabricate a value. If a value is not in the document, set "value" to null and "confidence" to 0.
3. NEVER use information from your training data, prior knowledge, or other documents. ONLY use what appears in the DOCUMENT TEXT section below.
4. Extract values EXACTLY as written in the document (don't paraphrase, correct, or improve).
5. For each extracted value, you MUST provide a direct quote from the document in your "reasoning" field to prove the value exists.
6. If a field asks for a name or identifier that does NOT appear anywhere in the document text, set it to null — do NOT guess.
7. For fields requiring inference (e.g. currency from '$' signs), make the inference and explain in "reasoning".
8. Be thorough — search the ENTIRE document text, not just the first few pages.
9. For long text fields, extract the relevant summary sentence(s), not entire paragraphs.
10. If you are even SLIGHTLY unsure whether a value is in the document, set confidence below 0.5 or set value to null.
11. SIGNATURES: Do NOT attempt to extract or interpret handwritten signatures, initials, or scribbles. These are unreliable and will be wrong. However, if a signature field has a TYPED/PRINTED name next to it (e.g. "Signature: John Smith" or "Signed by: Jane Doe"), you MAY extract that printed name. Only extract signature-related values when the name is clearly typed text, never from handwriting.
12. DATA TYPES: If a field has a [type: ...] annotation, respect it. For 'date' fields, extract dates. For 'number' or 'currency' fields, extract numeric values. For 'percentage' fields, extract percentages.
13. ENUM/DROPDOWN FIELDS: If a field has [valid values: ...], the extracted value MUST be one of the listed options. If the document value doesn't match any option, set value to null.
14. CONTEXT: The "Document context" section may include already-extracted fields from this document. Use them to understand what kind of document this is (e.g. if you see an extracted 'ClientName', you know who the client is). Do NOT re-extract those fields — only extract the PENDING fields listed below.
{ctx_info}
FIELDS TO EXTRACT:
{field_spec_text}

DOCUMENT TEXT:
---
{document_text[:120000]}
---

Return JSON with this exact structure:
{{
    "fields": [
        {{
            "field_name": "FieldName",
            "value": "extracted value or null if not found",
            "confidence": 0.85,
            "reasoning": "Brief explanation of where/how you found this value",
            "page_hint": "approximate location in document (e.g. 'page 6, Key Transaction Terms')"
        }}
    ],
    "fields_found": 10,
    "fields_not_found": 3
}}"""

            logger.info(
                f"LLM fallback: extracting {len(pending_fields)} pending fields "
                f"from {len(document_text)} chars of text"
            )

            # Retry with exponential backoff for 429 rate limits
            max_retries = 3
            base_delay = 10
            response = None

            for attempt in range(max_retries + 1):
                try:
                    response = self.client.chat.completions.create(
                        model=self.deployment,
                        messages=[
                            {
                                "role": "system",
                                "content": (
                                    "You are an expert document analyst. You extract specific field values "
                                    "from complex documents including financial, legal, insurance, and corporate filings. "
                                    "CRITICAL: You MUST ONLY return values that are EXPLICITLY WRITTEN in the provided document text. "
                                    "NEVER fabricate, hallucinate, or guess values. If a value is not in the document, return null. "
                                    "You have NO memory of previous documents — each request is completely independent. "
                                    "Search the ENTIRE document text thoroughly — check every page, table, schedule, and appendix. "
                                    "Return valid JSON only."
                                ),
                            },
                            {"role": "user", "content": prompt},
                        ],
                        response_format={"type": "json_object"},
                        temperature=0.1,
                        max_tokens=16000,
                    )
                    break
                except Exception as api_err:
                    error_str = str(api_err)
                    is_rate_limit = (
                        "429" in error_str
                        or "Too Many Requests" in error_str
                        or "rate" in error_str.lower()
                    )
                    if is_rate_limit and attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            f"LLM fallback rate limited (429). Retry {attempt + 1}/{max_retries} "
                            f"after {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        raise

            # Cost tracking
            usage = getattr(response, "usage", None)
            cost_usd = 0.0
            if usage:
                input_tokens = usage.prompt_tokens or 0
                output_tokens = usage.completion_tokens or 0
                cost_usd = (input_tokens / 1_000_000) * 2.00 + (output_tokens / 1_000_000) * 8.00
                logger.info(
                    f"💰 LLM fallback extraction: {input_tokens:,} in + {output_tokens:,} out = ${cost_usd:.4f}"
                )

            content = response.choices[0].message.content
            result = _json.loads(content)

            fields_out = result.get("fields", [])
            found = sum(1 for f in fields_out if f.get("value"))
            logger.info(
                f"LLM fallback: {found}/{len(pending_fields)} pending fields extracted"
            )

            return {
                "success": True,
                "fields": fields_out,
                "fields_found": found,
                "fields_not_found": len(pending_fields) - found,
                "cost": {
                    "input_tokens": usage.prompt_tokens if usage else 0,
                    "output_tokens": usage.completion_tokens if usage else 0,
                    "estimated_cost_usd": round(cost_usd, 6),
                },
            }

        except Exception as e:
            logger.error(f"LLM fallback extraction failed: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Comprehensive extraction – discover ALL fields in a document
    # ------------------------------------------------------------------
    def extract_all_fields(
        self,
        document_text: str,
        already_extracted_fields: list = None,
        user_prompt: str = None,
    ) -> Dict[str, Any]:
        """
        Comprehensive field discovery: extract EVERY piece of structured
        information from the document, not limited to a predefined template.

        This runs *after* the CU + LLM-fallback stages so that it can fill
        in anything those stages missed (tables, footnotes, schedules, etc.).

        Args:
            document_text: Full text of the document (from PDF/OCR).
            already_extracted_fields: List of field-name strings that are
                already extracted – the LLM will skip these.
            user_prompt: Optional user-supplied instruction to guide the
                extraction (e.g. "focus on the premium schedule").

        Returns:
            Dict with 'fields' list of {field_name, value, confidence, reasoning}.
        """
        if not self.is_available():
            return {"success": False, "error": "OpenAI service not available"}

        if not document_text or len(document_text) < 50:
            return {"success": True, "fields": [], "note": "No document text to analyse"}

        try:
            import json as _json

            already_names = sorted(set(already_extracted_fields or []))
            already_text = (
                "\n".join(f"  - {n}" for n in already_names)
                if already_names
                else "(none)"
            )

            user_instruction = ""
            if user_prompt:
                user_instruction = (
                    f"\n\nADDITIONAL USER INSTRUCTION:\n{user_prompt[:1000]}\n"
                )

            prompt = f"""You are an expert document analyst. A prior extraction pass already captured some fields from this document. Your task is to find ALL REMAINING structured data — every field/value pair, table entry, schedule item, footnote, party detail, date, amount, percentage, or any other factual data point that has NOT already been extracted.

IMPORTANT RULES:
1. Do NOT re-extract fields that are already captured (listed below).
2. For tables: extract each data cell as a separate field using the format "TableName_ColumnHeader_Row1", "TableName_ColumnHeader_Row2", etc.
3. Extract EXACT values as written in the document — do not paraphrase or infer unless the document explicitly implies it.
4. Include ALL schedules, annexes, appendices, and footnotes.
5. For every field, provide a descriptive field_name in PascalCase (e.g. "TrusteePartyName", "PremiumScheduleQ1Amount").
6. Search the ENTIRE document text thoroughly.
7. Group related data logically (e.g. all premium schedule rows together).
8. If a value spans multiple lines, combine into a single concise value.
9. Do NOT extract handwritten signatures, scribbles, initials, or stamp images. Only extract typed/printed signatory names.
{user_instruction}
FIELDS ALREADY EXTRACTED (skip these):
{already_text}

DOCUMENT TEXT:
---
{document_text[:120000]}
---

Return JSON with this exact structure:
{{
    "fields": [
        {{
            "field_name": "DescriptiveFieldName",
            "value": "extracted value",
            "confidence": 0.85,
            "reasoning": "Brief note on where this was found"
        }}
    ],
    "total_new_fields": 25,
    "sections_scanned": ["cover page", "key terms", "premium schedule", "risk analysis"]
}}"""

            logger.info(
                f"Comprehensive extraction: doc={len(document_text)} chars, "
                f"{len(already_names)} fields already extracted"
            )

            # Retry with exponential backoff for 429 rate limits
            max_retries = 3
            base_delay = 10
            response = None

            for attempt in range(max_retries + 1):
                try:
                    response = self.client.chat.completions.create(
                        model=self.deployment,
                        messages=[
                            {
                                "role": "system",
                                "content": (
                                    "You are an expert document analyst. You meticulously extract "
                                    "every piece of structured data from documents including tables, "
                                    "schedules, party lists, financial figures, dates, percentages, "
                                    "and all other factual information. "
                                    "Return valid JSON only."
                                ),
                            },
                            {"role": "user", "content": prompt},
                        ],
                        response_format={"type": "json_object"},
                        temperature=0.1,
                        max_tokens=16000,
                    )
                    break
                except Exception as api_err:
                    error_str = str(api_err)
                    is_rate_limit = (
                        "429" in error_str
                        or "Too Many Requests" in error_str
                        or "rate" in error_str.lower()
                    )
                    if is_rate_limit and attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            f"Comprehensive extraction rate limited (429). "
                            f"Retry {attempt + 1}/{max_retries} after {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        raise

            # Cost tracking
            usage = getattr(response, "usage", None)
            cost_usd = 0.0
            if usage:
                input_tokens = usage.prompt_tokens or 0
                output_tokens = usage.completion_tokens or 0
                # GPT-4.1 pricing: $2/M input, $8/M output
                cost_usd = (input_tokens / 1_000_000) * 2.00 + (output_tokens / 1_000_000) * 8.00
                logger.info(
                    f"💰 Comprehensive extraction: {input_tokens:,} in + "
                    f"{output_tokens:,} out = ${cost_usd:.4f}"
                )

            content = response.choices[0].message.content
            result = _json.loads(content)

            fields_out = result.get("fields", [])
            # Deduplicate against already-extracted
            already_lower = {n.lower() for n in already_names}
            fields_out = [
                f for f in fields_out
                if f.get("field_name", "").lower() not in already_lower
                and f.get("value") is not None
                and str(f.get("value", "")).strip()
            ]

            logger.info(
                f"Comprehensive extraction: {len(fields_out)} new fields discovered"
            )

            return {
                "success": True,
                "fields": fields_out,
                "total_new_fields": len(fields_out),
                "cost": {
                    "input_tokens": usage.prompt_tokens if usage else 0,
                    "output_tokens": usage.completion_tokens if usage else 0,
                    "estimated_cost_usd": round(cost_usd, 6),
                },
            }

        except Exception as e:
            logger.error(f"Comprehensive extraction failed: {e}", exc_info=True)
            return {"success": False, "error": str(e)}


# Singleton instance
_openai_service = None


def get_openai_service() -> OpenAIService:
    """Get the OpenAI service singleton"""
    global _openai_service
    if _openai_service is None:
        _openai_service = OpenAIService()
    return _openai_service
