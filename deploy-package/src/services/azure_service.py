"""
Azure Content Understanding Client
Handles document analysis using Azure AI Content Understanding API
"""

import os
import json
import time
import logging
import requests
from typing import Dict, Any, Optional, Tuple
from urllib.parse import quote

logger = logging.getLogger(__name__)


class AzureContentUnderstandingClient:
    """Client for Azure Content Understanding API"""
    
    def __init__(self):
        self.endpoint = os.getenv('AZURE_CONTENT_UNDERSTANDING_ENDPOINT', '').rstrip('/')
        self.api_key = os.getenv('AZURE_CONTENT_UNDERSTANDING_API_KEY')
        self.api_version = os.getenv('AZURE_API_VERSION', '2025-11-01')
        self.analyzer_id = os.getenv('AZURE_ANALYZER_ID', 'prebuilt-layout')
        
        if not self.endpoint or not self.api_key:
            logger.warning("Azure Content Understanding not configured")
            self.is_configured = False
        else:
            self.is_configured = True
            logger.info(f"Azure Content Understanding initialized: {self.endpoint}")
        
        self.headers = {
            'Ocp-Apim-Subscription-Key': self.api_key,
            'x-ms-useragent': 'request-driven-extraction-system'
        } if self.api_key else {}
    
    def is_available(self) -> bool:
        """Check if the service is configured and available"""
        return self.is_configured
    
    def begin_analyze(self, file_url: str, analyzer_id: str = None) -> requests.Response:
        """Start document analysis via URL reference"""
        if not self.is_configured:
            raise ValueError("Azure Content Understanding not configured")
        
        analyzer = analyzer_id or self.analyzer_id
        url = f"{self.endpoint}/contentunderstanding/analyzers/{analyzer}:analyze"
        url += f"?api-version={self.api_version}&stringEncoding=utf16"
        
        headers = self.headers.copy()
        headers['Content-Type'] = 'application/json'
        
        # GA API format
        data = {'inputs': [{'url': file_url}]}
        
        logger.info(f"⏱️ begin_analyze: POST to {url} with analyzer={analyzer}")
        _t0 = time.time()
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"⏱️ begin_analyze: POST returned {response.status_code} in {time.time()-_t0:.2f}s")
        
        logger.info(f"Started analysis for file: {file_url}")
        return response
    
    def begin_analyze_binary(self, pdf_bytes: bytes, analyzer_id: str = None) -> requests.Response:
        """
        Start document analysis by uploading raw PDF bytes directly.
        Uses the analyzeBinary endpoint — no blob storage needed.
        Max 200 MB per the Azure Content Understanding service limits.
        """
        if not self.is_configured:
            raise ValueError("Azure Content Understanding not configured")
        
        analyzer = analyzer_id or self.analyzer_id
        url = f"{self.endpoint}/contentunderstanding/analyzers/{analyzer}:analyzeBinary"
        url += f"?api-version={self.api_version}&stringEncoding=utf16"
        
        headers = self.headers.copy()
        headers['Content-Type'] = 'application/pdf'
        
        response = requests.post(url, headers=headers, data=pdf_bytes)
        response.raise_for_status()
        
        logger.info(f"Started binary analysis ({len(pdf_bytes)} bytes)")
        return response
    
    @staticmethod
    def extract_operation_id(response: requests.Response) -> Optional[str]:
        """
        Extract the Azure operation/run ID from the begin_analyze response.
        The operation-location header looks like:
          .../operations/<OPERATION_ID>?api-version=...
        The poll result JSON also contains an 'id' field with the same value.
        """
        operation_location = response.headers.get('operation-location', '')
        if not operation_location:
            return None
        try:
            # Extract ID from URL path: .../operations/<ID>?...
            path = operation_location.split('?')[0]
            return path.rstrip('/').rsplit('/', 1)[-1]
        except Exception:
            return None

    def poll_result(self, response: requests.Response, 
                    timeout_seconds: int = 1200, 
                    polling_interval_seconds: int = 2,
                    step_log_callback=None) -> Dict[str, Any]:
        """
        Poll for analysis results
        Raises TimeoutError if operation doesn't complete in time
        
        Polling strategy:
        - First 10 minutes: poll every 2 seconds
        - After 10 minutes: poll every 30 seconds
        
        step_log_callback: optional callable(message: str) to report
        progress to the UI processing log.
        """
        operation_location = response.headers.get('operation-location', '')
        if not operation_location:
            raise ValueError("Operation location not found in response headers")
        
        # Log the full operation URL for debugging
        logger.info(f"⏱️ poll_result: operation-location = {operation_location}")
        if step_log_callback:
            step_log_callback(f"Azure operation URL: ...{operation_location[-80:]}")
        
        headers = self.headers.copy()
        start_time = time.time()
        last_log_time = start_time
        last_step_log_time = start_time
        poll_count = 0
        last_status = None
        
        while True:
            elapsed_time = time.time() - start_time
            
            if elapsed_time > timeout_seconds:
                logger.warning(f"Polling timeout after {timeout_seconds}s")
                if step_log_callback:
                    step_log_callback(f"❌ TIMEOUT after {timeout_seconds}s — Azure never returned 'succeeded'. Last status: '{last_status}'")
                raise TimeoutError(f"Operation timed out after {timeout_seconds} seconds")
            
            try:
                result_response = requests.get(operation_location, headers=headers, timeout=30)
                result_response.raise_for_status()
                result = result_response.json()
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️ poll_result: HTTP GET timed out at {elapsed_time:.0f}s, retrying...")
                if step_log_callback and time.time() - last_step_log_time >= 30:
                    step_log_callback(f"⚠️ Azure poll HTTP timeout at {elapsed_time:.0f}s, retrying...")
                    last_step_log_time = time.time()
                time.sleep(5)
                continue
            except Exception as e:
                logger.warning(f"⏱️ poll_result: HTTP error at {elapsed_time:.0f}s: {e}")
                if step_log_callback and time.time() - last_step_log_time >= 30:
                    step_log_callback(f"⚠️ Azure poll error at {elapsed_time:.0f}s: {str(e)[:80]}")
                    last_step_log_time = time.time()
                time.sleep(5)
                continue
            
            poll_count += 1
            status = result.get('status', '').lower()
            last_status = status
            
            if status == 'succeeded':
                logger.info(f"⏱️ poll_result: SUCCEEDED after {elapsed_time:.2f}s ({poll_count} polls)")
                if step_log_callback:
                    step_log_callback(f"✅ Azure CU returned 'succeeded' after {elapsed_time:.0f}s ({poll_count} polls)")
                return result
            elif status == 'failed':
                error_details = result.get('error', {})
                logger.error(f"⏱️ poll_result: FAILED after {elapsed_time:.2f}s — {error_details}")
                if step_log_callback:
                    step_log_callback(f"❌ Azure CU returned 'failed' after {elapsed_time:.0f}s — {str(error_details)[:100]}")
                raise RuntimeError(f"Analysis failed: {error_details}")
            else:
                # Log to Python logger
                log_interval = 10 if elapsed_time < 600 else 60
                if time.time() - last_log_time >= log_interval:
                    logger.info(f"⏱️ poll_result: waiting... {elapsed_time:.0f}s elapsed (status: {status}, polls: {poll_count})")
                    last_log_time = time.time()
                
                # Log to step log every 30 seconds so UI shows we're alive
                if step_log_callback and time.time() - last_step_log_time >= 30:
                    step_log_callback(f"⏳ Waiting for Azure CU... {elapsed_time:.0f}s elapsed (status: '{status}', poll #{poll_count})")
                    last_step_log_time = time.time()
            
            # Adaptive polling: 2 seconds for first 10 min, then 30 seconds
            current_interval = polling_interval_seconds if elapsed_time < 600 else 30
            time.sleep(current_interval)
    
    def check_operation_status(self, operation_location: str) -> Tuple[str, Dict[str, Any]]:
        """Check the status of a running operation"""
        headers = self.headers.copy()
        result_response = requests.get(operation_location, headers=headers)
        result_response.raise_for_status()
        result = result_response.json()
        status = result.get('status', '').lower()
        return status, result

    def delete_custom_analyzer(self, azure_analyzer_id: str) -> bool:
        """
        Delete a custom analyzer from Azure Content Understanding.
        Uses the REST API: DELETE /contentunderstanding/analyzers/{analyzerId}
        Returns True if deleted or already gone, False on unexpected failure.
        """
        if not self.is_configured:
            logger.warning("Azure CU not configured — cannot delete analyzer")
            return False

        encoded_id = quote(azure_analyzer_id, safe='._-')
        url = f"{self.endpoint}/contentunderstanding/analyzers/{encoded_id}?api-version={self.api_version}"

        headers = self.headers.copy()
        try:
            response = requests.delete(url, headers=headers, timeout=30)
            if response.status_code in (200, 204):
                logger.info(f"Deleted CU analyzer '{azure_analyzer_id}' successfully")
                return True
            elif response.status_code == 404:
                logger.info(f"CU analyzer '{azure_analyzer_id}' already gone (404)")
                return True
            else:
                logger.error(f"Failed to delete CU analyzer '{azure_analyzer_id}': {response.status_code} {response.text[:500]}")
                return False
        except Exception as e:
            logger.error(f"Error deleting CU analyzer '{azure_analyzer_id}': {e}", exc_info=True)
            return False

    def create_custom_analyzer(self, analyzer_id: str,
                               fields: list,
                               description: str = '',
                               config: Optional[Dict[str, Any]] = None,
                               model_config: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Create a custom analyzer via Azure Content Understanding REST API.
        Returns operation metadata (operation_location, operation_id).
        """
        if not self.is_configured:
            raise ValueError("Azure Content Understanding not configured")

        if not fields:
            raise ValueError("At least one field is required to create an analyzer")

        encoded_analyzer_id = quote(analyzer_id, safe='._-')
        url = f"{self.endpoint}/contentunderstanding/analyzers/{encoded_analyzer_id}?api-version={self.api_version}"

        headers = self.headers.copy()
        headers['Content-Type'] = 'application/json'

        field_schema = {}
        for field in fields:
            name = field.get('field_name')
            if not name:
                continue

            field_type = self._map_field_type(field.get('field_type'))
            method = field.get('method') or self._default_method_for_type(field_type)

            field_def = {
                'type': field_type,
                'description': field.get('description') or field.get('display_name') or name,
                'method': method,
            }

            field_schema[name] = field_def

        body = {
            'description': description or f'Custom analyzer for {analyzer_id}',
            'baseAnalyzerId': 'prebuilt-document',
            'models': model_config or {
                'completion': 'gpt-4.1',
                'embedding': 'text-embedding-3-large',
            },
            'config': {
                'returnDetails': True,
                'estimateFieldSourceAndConfidence': True,
                'enableOcr': True,
                'enableLayout': True,
                **(config or {})
            },
            'fieldSchema': {
                'fields': field_schema
            }
        }

        logger.info(f"Creating analyzer {analyzer_id}: {len(field_schema)} fields, url={url}")
        logger.debug(f"Analyzer body: {json.dumps(body, indent=2)[:2000]}")

        response = requests.put(url, headers=headers, json=body)
        if response.status_code >= 400:
            error_body = response.text[:1000] if response.text else '(empty)'
            logger.error(
                f"Azure CU analyzer creation failed: "
                f"status={response.status_code}, body={error_body}"
            )
            response.raise_for_status()

        operation_location = response.headers.get('Operation-Location') or response.headers.get('operation-location')
        operation_id = self.extract_operation_id(response)

        logger.info(f"Started custom analyzer creation: {analyzer_id}")
        return {
            'analyzer_id': analyzer_id,
            'operation_location': operation_location,
            'operation_id': operation_id,
            'status_code': response.status_code,
            'response_json': response.json() if response.content else {}
        }

    def poll_operation(self, operation_location: str,
                       timeout_seconds: int = 180,
                       polling_interval_seconds: int = 2) -> Dict[str, Any]:
        """
        Poll a generic Azure CU operation (e.g., analyzer creation) until completion.
        Returns final operation payload.
        """
        if not operation_location:
            raise ValueError("operation_location is required")

        start_time = time.time()
        headers = self.headers.copy()

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(f"Operation timed out after {timeout_seconds} seconds")

            resp = requests.get(operation_location, headers=headers)
            resp.raise_for_status()
            payload = resp.json()

            status = (payload.get('status') or '').lower()
            if status == 'succeeded':
                return payload
            if status == 'failed':
                raise RuntimeError(f"Analyzer creation failed: {payload.get('error')}")

            time.sleep(polling_interval_seconds)

    @staticmethod
    def _map_field_type(field_type: Optional[str]) -> str:
        """Map app field types to CU supported schema types.
        
        IMPORTANT: We map ALL types to 'string' because Azure CU extracts
        far more reliably when fields are typed as string. The working
        catbond analyzer (catBondDocumentAnalyzer-v2) uses 'string' for
        every field including dates, numbers, and percentages.
        
        Our normalization layer handles type conversion after extraction.
        """
        # Azure CU works best with string type for all fields
        # Dates, numbers, percentages are normalized post-extraction
        return 'string'

    @staticmethod
    def _default_method_for_type(field_type: str) -> str:
        """Choose sensible default extraction method per type."""
        if field_type in {'string', 'number', 'date', 'boolean'}:
            return 'extract'
        return 'generate'

    def analyze_pdf_with_prebuilt_layout(self, pdf_bytes: bytes,
                                          timeout_seconds: int = 120) -> Dict[str, Any]:
        """
        Run a PDF through the prebuilt-layout analyzer and return the full
        structured result including text, tables, key-value pairs, and
        bounding regions.

        This is used for:
        - AI Suggest Fields (better than raw pypdf text)
        - Prebuilt-layout + LLM extraction mode (no custom analyzer needed)

        Returns the raw Azure CU result dict with 'result.contents[]'.
        """
        if not self.is_configured:
            raise ValueError("Azure Content Understanding not configured")

        response = self.begin_analyze_binary(pdf_bytes, analyzer_id='prebuilt-layout')
        result = self.poll_result(response, timeout_seconds=timeout_seconds)
        return result

    @staticmethod
    def format_prebuilt_layout_as_text(result: Dict[str, Any]) -> str:
        """
        Convert a prebuilt-layout result into a structured text representation
        that includes tables formatted as markdown tables, key-value pairs,
        and page text — much richer than raw pypdf extraction.

        Returns a single string suitable for sending to an LLM.
        """
        parts = []
        contents = result.get('result', {}).get('contents', [])

        for page_idx, content in enumerate(contents):
            page_num = content.get('startPageNumber', page_idx + 1)
            parts.append(f"\n{'='*60}")
            parts.append(f"PAGE {page_num}")
            parts.append(f"{'='*60}\n")

            # Plain text / markdown from the page
            page_text = content.get('markdown') or content.get('content') or ''
            if page_text.strip():
                parts.append(page_text.strip())

            # Key-value pairs
            kvps = content.get('keyValuePairs', [])
            if kvps:
                parts.append("\n--- KEY-VALUE PAIRS ---")
                for kv in kvps:
                    key = kv.get('key', {}).get('content', '').strip()
                    val = kv.get('value', {}).get('content', '').strip()
                    if key:
                        parts.append(f"  {key}: {val}")

            # Tables — format as markdown
            tables = content.get('tables', [])
            for t_idx, table in enumerate(tables):
                row_count = table.get('rowCount', 0)
                col_count = table.get('columnCount', 0)
                if row_count == 0 or col_count == 0:
                    continue

                # Build grid
                grid: Dict[int, Dict[int, str]] = {}
                for cell in table.get('cells', []):
                    r = cell.get('rowIndex', 0)
                    c = cell.get('columnIndex', 0)
                    grid.setdefault(r, {})[c] = cell.get('content', '').strip()

                parts.append(f"\n--- TABLE {t_idx + 1} ({row_count} rows × {col_count} cols) ---")
                # Header row
                headers = [grid.get(0, {}).get(c, '') for c in range(col_count)]
                parts.append("| " + " | ".join(h or '—' for h in headers) + " |")
                parts.append("| " + " | ".join('---' for _ in headers) + " |")
                # Data rows
                for r in range(1, row_count):
                    row_vals = [grid.get(r, {}).get(c, '') for c in range(col_count)]
                    parts.append("| " + " | ".join(v or '' for v in row_vals) + " |")

        return "\n".join(parts)


# Singleton instance
_azure_client = None


def get_azure_client() -> AzureContentUnderstandingClient:
    """Get the Azure Content Understanding client singleton"""
    global _azure_client
    if _azure_client is None:
        _azure_client = AzureContentUnderstandingClient()
    return _azure_client
