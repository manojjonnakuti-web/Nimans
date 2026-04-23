"""
Storage Service
Handles Azure Blob Storage operations for document management
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple
from urllib.parse import quote, unquote

from azure.storage.blob import BlobServiceClient, BlobClient, generate_blob_sas, BlobSasPermissions, ContentSettings

logger = logging.getLogger(__name__)


class StorageService:
    """Azure Blob Storage service for document management"""
    
    def __init__(self):
        self.connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = os.getenv('AZURE_STORAGE_CONTAINER', 'documents')
        self._container_verified = False  # Cache container existence check
        
        if not self.connection_string:
            logger.warning("Azure Storage not configured")
            self.blob_service_client = None
        else:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
            logger.info(f"Azure Storage initialized with container: {self.container_name}")
    
    def is_available(self) -> bool:
        """Check if the service is available"""
        return self.blob_service_client is not None
    
    def check_connectivity(self) -> dict:
        """Test actual connectivity to Azure Storage (for health checks)"""
        if not self.blob_service_client:
            return {'ok': False, 'error': 'Not configured'}
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            exists = container_client.exists()
            return {'ok': True, 'container_exists': exists}
        except Exception as e:
            return {'ok': False, 'error': str(e)}
    
    def _get_container_client(self):
        """Get the container client, creating container if needed"""
        if not self.blob_service_client:
            raise ValueError("Azure Storage not configured")
        
        container_client = self.blob_service_client.get_container_client(self.container_name)
        # Only check existence once per process lifetime
        if not self._container_verified:
            if not container_client.exists():
                container_client.create_container()
                logger.info(f"Created container: {self.container_name}")
            self._container_verified = True
        return container_client
    
    def upload_document(
        self,
        file_content: bytes,
        filename: str,
        organization_id: str = None,
        document_id: int = None,
        request_id: str = None,
        content_type: str = 'application/pdf'
    ) -> str:
        """
        Upload a document to Azure Blob Storage
        Returns the blob path
        
        All paths are prefixed with organization_id for tenant isolation:
        - With request_id: {organization_id}/requests/{request_id}/{filename}
        - Without request_id: {organization_id}/{document_id}/{filename}
        """
        if not organization_id:
            raise ValueError("organization_id is required for storage isolation")
        
        container_client = self._get_container_client()
        
        # Construct blob path - always prefixed with organization_id for tenant isolation
        safe_filename = quote(filename, safe='')
        
        if request_id:
            # Tenant-isolated request path
            blob_path = f"{organization_id}/requests/{request_id}/{safe_filename}"
        else:
            # Tenant-isolated document path
            blob_path = f"{organization_id}/{document_id}/{safe_filename}"
        
        blob_client = container_client.get_blob_client(blob_path)
        
        blob_client.upload_blob(
            file_content,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type)
        )
        
        logger.info(f"Uploaded document: {blob_path}")
        return blob_path
    
    def download_document(self, blob_path: str) -> bytes:
        """Download a document from Azure Blob Storage"""
        container_client = self._get_container_client()
        blob_client = container_client.get_blob_client(blob_path)
        
        download_stream = blob_client.download_blob()
        content = download_stream.readall()
        
        logger.info(f"Downloaded document: {blob_path} ({len(content)} bytes)")
        return content
    
    @staticmethod
    def sanitize_blob_filename(blob_path: str) -> str:
        """
        Sanitize non-ASCII characters in the filename portion of a blob path,
        matching the Outlook plugin's SanitizeFileName() / ToAsciiSafe() behavior:
        any character with code-point > 127 is replaced with '_'.
        """
        parts = blob_path.rsplit('/', 1)
        if len(parts) == 2:
            directory, filename = parts
            sanitized = ''.join(c if ord(c) < 128 else '_' for c in filename)
            return f"{directory}/{sanitized}"
        else:
            return ''.join(c if ord(c) < 128 else '_' for c in blob_path)

    def resolve_blob_path(self, blob_path: str) -> tuple:
        """
        Try to find the actual blob, handling path mismatches caused by the
        Outlook plugin's filename sanitisation (non-ASCII chars → underscore).
        
        Returns (resolved_path, was_corrected) tuple.
        Tries: 1) original DB path, 2) non-ASCII-sanitised path.
        """
        container_client = self._get_container_client()

        # 1. Try the original path stored in the DB
        try:
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.get_blob_properties()
            return blob_path, False  # Original path is fine
        except Exception:
            pass

        # 2. Try with non-ASCII chars replaced by underscore
        sanitized = self.sanitize_blob_filename(blob_path)
        if sanitized != blob_path:
            try:
                blob_client = container_client.get_blob_client(sanitized)
                blob_client.get_blob_properties()
                logger.info(f"Resolved blob path via sanitisation: '{blob_path}' → '{sanitized}'")
                return sanitized, True
            except Exception:
                pass

        # Nothing matched — return original so caller can raise the proper error
        return blob_path, False

    def download_document_stream(self, blob_path: str):
        """
        Stream a document from Azure Blob Storage.
        Returns (generator, properties) tuple.
        The generator yields chunks — uses constant memory regardless of file size.
        """
        container_client = self._get_container_client()
        blob_client = container_client.get_blob_client(blob_path)
        
        # Get properties first (small metadata request)
        properties = blob_client.get_blob_properties()
        
        # Return a streaming generator + properties
        download_stream = blob_client.download_blob()
        
        def chunk_generator():
            for chunk in download_stream.chunks():
                yield chunk
        
        logger.info(f"Streaming document: {blob_path} ({properties.size} bytes)")
        return chunk_generator(), properties
    
    def delete_document(self, blob_path: str) -> bool:
        """Delete a document from Azure Blob Storage"""
        try:
            container_client = self._get_container_client()
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.delete_blob()
            logger.info(f"Deleted document: {blob_path}")
            return True
        except Exception as e:
            logger.error(f"Error deleting document {blob_path}: {e}")
            return False
    
    def generate_sas_url(
        self,
        blob_path: str,
        expiry_hours: int = 24
    ) -> str:
        """
        Generate a SAS URL for document access
        Returns a URL with time-limited access
        """
        if not self.blob_service_client:
            raise ValueError("Azure Storage not configured")
        
        container_client = self._get_container_client()
        blob_client = container_client.get_blob_client(blob_path)
        
        # Parse account info from connection string
        account_name = None
        account_key = None
        for part in self.connection_string.split(';'):
            if part.startswith('AccountName='):
                account_name = part.split('=', 1)[1]
            elif part.startswith('AccountKey='):
                account_key = part.split('=', 1)[1]
        
        if not account_name or not account_key:
            raise ValueError("Cannot parse account info from connection string")
        
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=self.container_name,
            blob_name=blob_path,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
        )
        
        sas_url = f"{blob_client.url}?{sas_token}"
        logger.info(f"Generated SAS URL for: {blob_path} (expires in {expiry_hours}h)")
        return sas_url
    
    def get_document_metadata(self, blob_path: str) -> Optional[dict]:
        """Get document metadata including size and content type"""
        try:
            container_client = self._get_container_client()
            blob_client = container_client.get_blob_client(blob_path)
            properties = blob_client.get_blob_properties()
            
            return {
                'size': properties.size,
                'content_type': properties.content_settings.content_type,
                'created_at': properties.creation_time,
                'last_modified': properties.last_modified,
                'etag': properties.etag
            }
        except Exception as e:
            logger.error(f"Error getting metadata for {blob_path}: {e}")
            return None
    
    def copy_document(
        self,
        source_blob_path: str,
        destination_blob_path: str
    ) -> str:
        """Copy a document to a new location"""
        container_client = self._get_container_client()
        
        source_client = container_client.get_blob_client(source_blob_path)
        dest_client = container_client.get_blob_client(destination_blob_path)
        
        # Generate SAS for source
        source_url = self.generate_sas_url(source_blob_path, expiry_hours=1)
        
        dest_client.start_copy_from_url(source_url)
        logger.info(f"Copied document from {source_blob_path} to {destination_blob_path}")
        
        return destination_blob_path
    
    def list_documents(
        self,
        prefix: Optional[str] = None,
        max_results: int = 1000
    ) -> list:
        """List documents in storage with optional prefix filter"""
        container_client = self._get_container_client()
        
        blobs = container_client.list_blobs(name_starts_with=prefix)
        
        results = []
        for blob in blobs:
            if len(results) >= max_results:
                break
            results.append({
                'name': blob.name,
                'size': blob.size,
                'last_modified': blob.last_modified,
                'content_type': blob.content_settings.content_type if blob.content_settings else None
            })
        
        return results


# Singleton instance
_storage_service = None


def get_storage_service() -> StorageService:
    """Get the storage service singleton"""
    global _storage_service
    if _storage_service is None:
        _storage_service = StorageService()
    return _storage_service
