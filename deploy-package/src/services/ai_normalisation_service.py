"""
AI Normalisation Service
Sends extracted field values + normalisation instructions to Azure OpenAI
(Chat Completions) and returns normalised values.

Uses the AzureOpenAI client with API-key authentication against the
Azure AI Services OpenAI deployment.

Input/Output JSON schema:
[
    {
        "template_field_id": 1,
        "field_name": "coupon_rate",
        "extracted_value": "5.75 percent per annum",
        "normalisation_instruction": "Convert to decimal percentage, e.g. 5.75",
        "normalised_value": ""          <-- filled by AI
    },
    ...
]

Required environment variables:
    AZURE_AI_AGENT_API_KEY          – API key for the Azure AI Services resource
    AZURE_AI_NORMALISATION_ENDPOINT – Azure OpenAI base URL
                                      (default: https://aif-instrument-extraction.services.ai.azure.com)
    AZURE_AI_NORMALISATION_DEPLOYMENT – deployment name
                                        (default: gpt-4.1-284553)
"""

import os
import json
import time
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

# OpenAI SDK import (AzureOpenAI supports api-key auth natively)
try:
    from openai import AzureOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning(
        "openai package not installed — AI normalisation will not work.  "
        "Install with:  pip install openai"
    )


# ---------------------------------------------------------------------------
# Data-type → auto-instruction mapping
# ---------------------------------------------------------------------------
_DATATYPE_INSTRUCTIONS = {
    'date': (
        'STRICT OUTPUT FORMAT RULE: The normalised_value MUST be exactly YYYY-MM-DD '
        '(e.g. 2025-06-01). If you cannot determine a valid calendar date from the '
        'extracted text, set normalised_value to "" (empty string). '
        'NEVER return free text, month names, partial dates, or anything other than '
        'YYYY-MM-DD or "".'
    ),
    'number': (
        'STRICT OUTPUT FORMAT RULE: The normalised_value MUST be a plain decimal '
        'number with no commas, currency symbols, units or text (e.g. 125000000). '
        'Use "." as the decimal separator. If you cannot extract a valid number, '
        'set normalised_value to "" (empty string).'
    ),
    'currency': (
        'STRICT OUTPUT FORMAT RULE: The normalised_value MUST be a plain decimal '
        'number with no commas, currency symbols, units or text (e.g. 125000000). '
        'Use "." as the decimal separator. If you cannot extract a valid number, '
        'set normalised_value to "" (empty string).'
    ),
    'percentage': (
        'STRICT OUTPUT FORMAT RULE: The normalised_value MUST be a plain decimal '
        'number representing the percentage (e.g. 5.75 — not 5.75%, not 0.0575). '
        'Use "." as the decimal separator. If you cannot extract a valid number, '
        'set normalised_value to "" (empty string).'
    ),
    # 'dropdown' is handled dynamically — see build_datatype_instruction()
    # 'text' has no special format rule
}


def build_datatype_instruction(data_type: str, field_values: str = None) -> str:
    """
    Return an auto-generated format instruction based on the template field's
    data_type (and allowed values for dropdowns).  Returns '' for text or
    unknown types.
    """
    if not data_type:
        return ''
    dt = data_type.strip().lower()
    if dt == 'dropdown' and field_values:
        return (
            f'STRICT OUTPUT FORMAT RULE: The normalised_value MUST be exactly one of '
            f'these allowed values: [{field_values}]. Pick the closest match '
            f'from this list. If none match, set normalised_value to "" (empty string).'
        )
    return _DATATYPE_INSTRUCTIONS.get(dt, '')


SYSTEM_PROMPT = """You are an expert data normalisation assistant for financial instrument fields (catastrophe bonds, ILS, reinsurance).

Your task is to normalise extracted field values into clean, machine-readable values that STRICTLY conform to the field's data_type format.

=== ABSOLUTE RULE — DATA TYPE COMPLIANCE ===
Every normalised_value MUST match the format required by the field's datatype_format_rule.
If you CANNOT produce a value that conforms to the required format, you MUST set normalised_value to "" (empty string).
NEVER return raw extracted text, partial sentences, parenthetical clauses, or free-form descriptions as normalised_value.
Examples of FORBIDDEN output for a date field: "July [.], 2029", "June 8, 2032 (or if such day is not a Business Day...)", "TBD".
Examples of FORBIDDEN output for a number field: "$100M - $150M", "approximately 50,000,000", "N/A".
The ONLY acceptable outputs are: a correctly formatted value OR "" (empty string).

Core Rules:
1. Read each object's "extracted_value", "normalisation_instruction", "data_type" and "datatype_format_rule".
2. Apply the normalisation_instruction to interpret the extracted_value.
3. The "datatype_format_rule" specifies the MANDATORY output format. It always takes precedence. If the result does not match the format, return "".
4. Write the result into "normalised_value".
5. If normalisation_instruction is empty, apply only the datatype_format_rule (if any); for text fields with no rule, copy extracted_value as-is.
6. If extracted_value is empty BUT normalisation_instruction says to derive/copy from another field, look at the other fields in the array and produce the value accordingly.
7. If extracted_value is empty and there is no derivation instruction, set normalised_value to "".

Intelligent Date Interpretation:
8. MISSING DAY — bracket placeholders: If the date text contains "[.]", "[●]", "[_]", "[__]", "[TBD]" or any similar bracket placeholder where the day should be, treat the day as the 1st of that month, then apply business-day logic.
   Example: "July [.], 2029" → start with July 1, 2029 → check if weekday → if not, roll to next Monday → output "2029-07-02" (since July 1, 2029 is a Sunday).
   Example: "January [.], 2026" → start with January 1, 2026 → Thursday → output "2026-01-01".
8b. MISSING MONTH — bracket placeholders: If the month position contains "[.]", "[●]", "[_]", "[__]", "[TBD]" or similar bracket placeholder, treat the month as January (1st month). If the day is also a placeholder, treat it as the 1st. Then apply business-day logic.
   Example: "[.] 15, 2029" → start with January 15, 2029 → Monday → output "2029-01-15".
   Example: "[.] [.], 2029" → start with January 1, 2029 → Monday → output "2029-01-01".
   Example: "[●] [●], 2030" → start with January 1, 2030 → Tuesday → output "2030-01-01".
9. BUSINESS DAY ADJUSTMENT: If the text contains language like "or if such day is not a Business Day, the next succeeding Business Day", check whether the candidate date falls on Saturday or Sunday. If Saturday, roll to Monday (+2 days). If Sunday, roll to Monday (+1 day). Assume Mon-Fri business-day calendar; ignore regional holidays.
   Example: "June 8, 2032" → June 8, 2032 is a Tuesday → output "2032-06-08".
   Example: "January 3, 2026 (or if such day is not a Business Day, the next succeeding Business Day)" → January 3, 2026 is a Saturday → roll to Monday January 5 → output "2026-01-05".
10. If the year is missing or completely ambiguous (no year anywhere in the text or derivable from context), return "".

Intelligent Value Interpretation:
11. Amounts with ranges: Use the lower bound. "$100M - $150M" → "100000000".
12. Percentages: Strip "per annum", "%" and similar qualifiers. Preserve numeric precision. "5.750% per annum" → "5.75".
13. Currency amounts: Strip commas, currency symbols, "M"/"B" multipliers. Return plain number.
14. Entity names (SPV, Trustee, Sponsors, etc.): Remove legal annotations like '(the "Issuer")' but keep the full legal name including "Ltd.", "SE", "Inc.".
15. Conditional/estimated placeholders: If the ENTIRE value is "TBD", "to be determined", "N/A", "expected to be [amount]" with no concrete data, return "".
    BUT if a concrete value is present alongside qualifications (e.g. "June 8, 2032 (or if such day is not a Business Day...)"), extract and normalise the concrete value — do NOT return "".
16. Use context from OTHER fields in the same array when it helps resolve ambiguity (e.g. derive OffRiskDate from OnRiskDate + risk period length).

Output Rules:
17. Return ONLY a JSON object with a single key "fields" containing the array.
18. Keep the array length and order exactly the same.
19. Do NOT modify request_field_id, template_field_id, field_name, extracted_value, data_type, datatype_format_rule or normalisation_instruction. The "request_field_id" field is the unique key for matching — always preserve it exactly.
20. Return valid JSON only — no markdown, no explanation, no code fences.
21. IMPORTANT: Multiple fields may share the same template_field_id (they are alternative extractions). Each must be normalised INDEPENDENTLY based on its own extracted_value. Use the "request_field_id" field (unique per field) to keep them separate."""


USER_MESSAGE_TEMPLATE = """Normalise the following extracted field values.
Today's date is {today_date} (for business-day calculations).

{payload_json}"""


class AINormalisationService:
    """
    Service that calls Azure OpenAI (Chat Completions) to normalise extracted
    field values based on per-field normalisation instructions stored on the
    template.  Uses API-key authentication.
    """

    def __init__(self):
        self.api_key = os.getenv('AZURE_AI_AGENT_API_KEY')
        self.endpoint = os.getenv(
            'AZURE_AI_NORMALISATION_ENDPOINT',
            'https://aif-instrument-extraction.services.ai.azure.com'
        )
        self.deployment = os.getenv(
            'AZURE_AI_NORMALISATION_DEPLOYMENT',
            'gpt-4.1-284553'
        )

        if not OPENAI_AVAILABLE:
            logger.warning("OpenAI package not available — AI normalisation disabled")
            self.client: Optional[AzureOpenAI] = None
            return

        if not self.api_key:
            logger.warning(
                "AI normalisation service not configured — "
                "set AZURE_AI_AGENT_API_KEY env var"
            )
            self.client = None
            return

        try:
            self.client = AzureOpenAI(
                api_key=self.api_key,
                azure_endpoint=self.endpoint,
                api_version="2024-06-01"
            )
            logger.info(
                f"AI normalisation service initialised "
                f"(deployment={self.deployment}, endpoint={self.endpoint})"
            )
        except Exception as e:
            logger.error(f"Failed to initialise AI normalisation service: {e}", exc_info=True)
            self.client = None

    def is_available(self) -> bool:
        return self.client is not None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def normalise_fields(self, fields: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Send an array of field dicts to Azure OpenAI and get back the same
        array with normalised_value filled in.

        Args:
            fields: list of dicts, each with:
                - template_field_id (int)
                - field_name (str)
                - extracted_value (str)
                - normalisation_instruction (str)
                - normalised_value (str, initially empty)

        Returns:
            The same list with normalised_value populated by the AI.
            On error, returns the input list unchanged (extracted_value copied
            into normalised_value as a safe fallback).
        """
        if not self.is_available():
            logger.error("AI normalisation service not available — falling back to extracted values")
            return self._fallback(fields)

        # Send ALL fields (including empty ones) so the AI can derive
        # values from sibling fields when the instruction says so.
        if not fields:
            logger.info("No fields to normalise")
            return []

        try:
            from datetime import date as _date

            # request_field_id is already in each field dict (unique per field)
            # — no synthetic index injection needed
            #
            # Payload JSON template per field:
            # {
            #   "request_field_id": 234,          // unique DB primary key — DO NOT modify
            #   "template_field_id": 12,           // shared across active + alternatives
            #   "field_name": "IssuanceDate",
            #   "extracted_value": "June 8, 2025",
            #   "data_type": "date",
            #   "normalisation_instruction": "Format as YYYY-MM-DD",
            #   "datatype_format_rule": "Must be a valid date in YYYY-MM-DD format...",
            #   "normalised_value": ""             // AI fills this in
            # }
            #
            payload_json = json.dumps(fields, indent=2)
            user_message = USER_MESSAGE_TEMPLATE.format(
                today_date=_date.today().isoformat(),
                payload_json=payload_json
            )
            logger.info(f"Sending {len(fields)} fields for AI normalisation")

            # --- Azure OpenAI Chat Completion with retry for 429 ---
            max_retries = 3
            base_delay = 10  # seconds
            response = None

            for attempt in range(max_retries + 1):
                try:
                    response = self.client.chat.completions.create(
                        model=self.deployment,
                        messages=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": user_message}
                        ],
                        temperature=0,
                        response_format={"type": "json_object"}
                    )
                    break  # Success
                except Exception as api_err:
                    error_str = str(api_err)
                    is_rate_limit = '429' in error_str or 'Too Many Requests' in error_str or 'rate' in error_str.lower()
                    if is_rate_limit and attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            f"Azure OpenAI rate limited (429) during normalisation. "
                            f"Retry {attempt + 1}/{max_retries} after {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        raise

            # ===== COST TRACKING =====
            usage = getattr(response, 'usage', None)
            if usage:
                input_tokens = usage.prompt_tokens or 0
                output_tokens = usage.completion_tokens or 0
                total_tokens = usage.total_tokens or 0
                # GPT-4.1 pricing: $2.00/1M input, $8.00/1M output
                input_cost = (input_tokens / 1_000_000) * 2.00
                output_cost = (output_tokens / 1_000_000) * 8.00
                total_cost = input_cost + output_cost
                logger.info(
                    f"\n{'='*60}\n"
                    f"💰 COST TRACKING — Azure OpenAI Normalisation (GPT-4.1)\n"
                    f"{'='*60}\n"
                    f"  Deployment:    {self.deployment}\n"
                    f"  Fields sent:   {len(fields)}\n"
                    f"  Input tokens:  {input_tokens:,}\n"
                    f"  Output tokens: {output_tokens:,}\n"
                    f"  Total tokens:  {total_tokens:,}\n"
                    f"  Est. cost:     ${total_cost:.6f}\n"
                    f"    (input:  ${input_cost:.6f} | output: ${output_cost:.6f})\n"
                    f"{'='*60}"
                )
            else:
                logger.info("💰 COST TRACKING — Azure OpenAI Normalisation: usage data not available")
            # ===== END COST TRACKING =====

            # Store usage for external consumers (e.g. cost_tracker in request_processor)
            self._last_usage = {
                'input': usage.prompt_tokens if usage else 0,
                'output': usage.completion_tokens if usage else 0,
                'cost': round(((usage.prompt_tokens or 0) / 1_000_000 * 2.00 + (usage.completion_tokens or 0) / 1_000_000 * 8.00), 6) if usage else 0.0
            }

            raw_output = response.choices[0].message.content
            if not raw_output:
                logger.error("Azure OpenAI returned empty response")
                return self._fallback(fields)

            raw_output = raw_output.strip()

            # Strip markdown code fences if the model wraps them
            if raw_output.startswith("```"):
                raw_output = raw_output.split("\n", 1)[1] if "\n" in raw_output else raw_output[3:]
                if raw_output.endswith("```"):
                    raw_output = raw_output[:-3].strip()

            parsed = json.loads(raw_output)

            # Handle both {"fields": [...]} and bare [...] responses
            if isinstance(parsed, dict):
                normalised_list = None
                for key in ('fields', 'normalised_fields', 'data', 'results'):
                    if key in parsed and isinstance(parsed[key], list):
                        normalised_list = parsed[key]
                        break
                if normalised_list is None:
                    for val in parsed.values():
                        if isinstance(val, list):
                            normalised_list = val
                            break
                if normalised_list is None:
                    logger.error(f"AI normalisation returned unexpected JSON structure: {list(parsed.keys())}")
                    return self._fallback(fields)
            elif isinstance(parsed, list):
                normalised_list = parsed
            else:
                logger.error(f"AI normalisation returned unexpected type: {type(parsed)}")
                return self._fallback(fields)

            # Build a lookup by request_field_id (unique) for reliable 1:1 merge
            normalised_by_rfid = {}
            normalised_by_tfid = {}
            for item in normalised_list:
                nv = item.get('normalised_value', '')
                if 'request_field_id' in item:
                    normalised_by_rfid[item['request_field_id']] = nv
                if 'template_field_id' in item:
                    normalised_by_tfid[item['template_field_id']] = nv

            # Merge back — prefer request_field_id, fall back to template_field_id
            result = []
            for f in fields:
                f_copy = dict(f)
                rfid = f_copy.get('request_field_id')
                if rfid and rfid in normalised_by_rfid:
                    f_copy['normalised_value'] = normalised_by_rfid[rfid]
                elif f_copy.get('template_field_id') in normalised_by_tfid:
                    f_copy['normalised_value'] = normalised_by_tfid[f_copy['template_field_id']]
                elif f_copy.get('extracted_value'):
                    f_copy['normalised_value'] = f_copy['extracted_value']
                else:
                    f_copy['normalised_value'] = ''
                result.append(f_copy)

            logger.info(f"AI normalisation complete — {len(normalised_by_rfid) or len(normalised_by_tfid)} fields normalised")
            return result

        except json.JSONDecodeError as e:
            logger.error(f"AI normalisation returned invalid JSON: {e}")
            return self._fallback(fields)
        except Exception as e:
            logger.error(f"AI normalisation failed: {e}", exc_info=True)
            return self._fallback(fields)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fallback(fields: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Safe fallback: copy extracted_value → normalised_value."""
        result = []
        for f in fields:
            f_copy = dict(f)
            f_copy['normalised_value'] = f_copy.get('extracted_value', '')
            result.append(f_copy)
        return result


# ---------------------------------------------------------------------------
# Singleton accessor
# ---------------------------------------------------------------------------
_ai_normalisation_service: Optional[AINormalisationService] = None


def get_ai_normalisation_service() -> AINormalisationService:
    global _ai_normalisation_service
    if _ai_normalisation_service is None:
        _ai_normalisation_service = AINormalisationService()
    return _ai_normalisation_service
