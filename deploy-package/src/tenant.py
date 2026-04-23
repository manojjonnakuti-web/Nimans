"""
Tenant-Aware Multi-Tenant Service Registry

Provides per-tenant database connections, storage clients, and AI service clients.
Falls back to shared (default) services when no tenant config exists for an org.

This module is the CORE of the multi-tenant data hosting system.
"""

import os
import time
import logging
import threading
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

logger = logging.getLogger(__name__)


# ============================================
# TENANT CONFIG DATA CLASS
# ============================================

@dataclass
class TenantConfig:
    """Holds connection details for a single tenant's resources."""
    organization_id: str
    status: str = 'active'
    
    # Database
    db_connection_string: Optional[str] = None
    
    # Storage
    storage_connection_string: Optional[str] = None
    storage_container: str = 'documents'
    
    # Content Understanding
    cu_endpoint: Optional[str] = None
    cu_api_key: Optional[str] = None
    cu_api_version: str = '2025-11-01'
    
    # OpenAI (optional per-tenant override)
    openai_endpoint: Optional[str] = None
    openai_api_key: Optional[str] = None
    openai_deployment: Optional[str] = None
    
    # Metadata
    region: Optional[str] = None
    loaded_at: float = field(default_factory=time.time)
    
    @property
    def has_own_database(self) -> bool:
        return bool(self.db_connection_string)
    
    @property
    def has_own_storage(self) -> bool:
        return bool(self.storage_connection_string)
    
    @property
    def has_own_cu(self) -> bool:
        return bool(self.cu_endpoint and self.cu_api_key)
    
    @property
    def has_own_openai(self) -> bool:
        return bool(self.openai_endpoint and self.openai_api_key)
    
    @property
    def is_active(self) -> bool:
        return self.status == 'active'


# ============================================
# TENANT CONFIG CACHE
# ============================================

class TenantConfigCache:
    """
    Thread-safe in-memory cache for tenant configurations.
    Loads from central database, caches with TTL.
    """
    
    CACHE_TTL_SECONDS = 300  # 5 minutes
    
    def __init__(self):
        self._cache: Dict[str, TenantConfig] = {}
        self._lock = threading.Lock()
    
    def get(self, org_id: str) -> Optional[TenantConfig]:
        """Get a cached tenant config, or None if expired/missing."""
        with self._lock:
            config = self._cache.get(org_id)
            if config is None:
                return None
            # Check TTL
            if (time.time() - config.loaded_at) > self.CACHE_TTL_SECONDS:
                del self._cache[org_id]
                return None
            return config
    
    def put(self, config: TenantConfig):
        """Cache a tenant config."""
        config.loaded_at = time.time()
        with self._lock:
            self._cache[config.organization_id] = config
    
    def invalidate(self, org_id: str):
        """Remove a specific tenant from cache."""
        with self._lock:
            self._cache.pop(org_id, None)
    
    def invalidate_all(self):
        """Clear entire cache."""
        with self._lock:
            self._cache.clear()
    
    def get_all_active_org_ids(self) -> list:
        """Return all org_ids with active cached configs."""
        with self._lock:
            now = time.time()
            return [
                org_id for org_id, config in self._cache.items()
                if config.is_active and (now - config.loaded_at) <= self.CACHE_TTL_SECONDS
            ]


# ============================================
# TENANT CONFIG LOADER (from central DB)
# ============================================

def load_tenant_config(org_id: str) -> Optional[TenantConfig]:
    """
    Load tenant configuration from the central database.
    Returns None if no tenant_configs row exists for this org
    (meaning: use shared/default resources).
    """
    from .repositories import get_database_repository
    
    try:
        db = get_database_repository()
        config_row = db.get_tenant_config(org_id)
        
        if config_row is None:
            logger.debug(f"No tenant config for {org_id} — will use shared resources")
            return None
        
        config = TenantConfig(
            organization_id=org_id,
            status=config_row.get('status', 'active'),
            db_connection_string=config_row.get('db_connection_string'),
            storage_connection_string=config_row.get('storage_connection_string'),
            storage_container=config_row.get('storage_container', 'documents'),
            cu_endpoint=config_row.get('cu_endpoint'),
            cu_api_key=config_row.get('cu_api_key'),
            cu_api_version=config_row.get('cu_api_version', '2025-11-01'),
            openai_endpoint=config_row.get('openai_endpoint'),
            openai_api_key=config_row.get('openai_api_key'),
            openai_deployment=config_row.get('openai_deployment'),
            region=config_row.get('region'),
        )
        
        logger.info(
            f"Loaded tenant config for {org_id}: "
            f"db={'own' if config.has_own_database else 'shared'}, "
            f"storage={'own' if config.has_own_storage else 'shared'}, "
            f"cu={'own' if config.has_own_cu else 'shared'}, "
            f"openai={'own' if config.has_own_openai else 'shared'}"
        )
        return config
        
    except Exception as e:
        logger.warning(f"Failed to load tenant config for {org_id}: {e} — falling back to shared")
        return None


def load_all_active_tenant_configs() -> list:
    """
    Load all active tenant configurations from central DB.
    Used by background worker to know which tenant DBs to check.
    """
    from .repositories import get_database_repository
    
    try:
        db = get_database_repository()
        rows = db.get_all_tenant_configs(status='active')
        configs = []
        for row in rows:
            config = TenantConfig(
                organization_id=row.get('organization_id'),
                status=row.get('status', 'active'),
                db_connection_string=row.get('db_connection_string'),
                storage_connection_string=row.get('storage_connection_string'),
                storage_container=row.get('storage_container', 'documents'),
                cu_endpoint=row.get('cu_endpoint'),
                cu_api_key=row.get('cu_api_key'),
                cu_api_version=row.get('cu_api_version', '2025-11-01'),
                openai_endpoint=row.get('openai_endpoint'),
                openai_api_key=row.get('openai_api_key'),
                openai_deployment=row.get('openai_deployment'),
                region=row.get('region'),
            )
            configs.append(config)
        return configs
    except Exception as e:
        logger.warning(f"Failed to load active tenant configs: {e}")
        return []


# ============================================
# TENANT DATABASE ENGINE REGISTRY
# ============================================

class TenantDatabaseRegistry:
    """
    Manages SQLAlchemy engines per-tenant.
    Falls back to the shared engine when no tenant config exists.
    """
    
    def __init__(self):
        self._engines: Dict[str, Any] = {}  # org_id -> engine
        self._session_factories: Dict[str, Any] = {}  # org_id -> sessionmaker
        self._lock = threading.Lock()
        self._last_access: Dict[str, float] = {}  # org_id -> timestamp
    
    def get_engine(self, org_id: str, tenant_config: Optional[TenantConfig] = None):
        """
        Get the SQLAlchemy engine for a tenant.
        Returns None if no tenant-specific DB (caller should use shared).
        """
        if tenant_config is None or not tenant_config.has_own_database:
            return None
        
        with self._lock:
            self._last_access[org_id] = time.time()
            
            if org_id in self._engines:
                return self._engines[org_id]
            
            # Create a new engine for this tenant
            logger.info(f"Creating database engine for tenant: {org_id}")
            engine = create_engine(
                tenant_config.db_connection_string,
                echo=False,
                pool_pre_ping=True,
                fast_executemany=True,
                pool_size=3,          # Smaller per-tenant pool
                max_overflow=5,
                pool_recycle=1800,
                pool_timeout=30,
            )
            
            # Verify connectivity
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    conn.commit()
                logger.info(f"Tenant database connected: {org_id}")
            except Exception as e:
                logger.error(f"Failed to connect to tenant database for {org_id}: {e}")
                engine.dispose()
                raise
            
            self._engines[org_id] = engine
            self._session_factories[org_id] = sessionmaker(bind=engine)
            return engine
    
    def get_session_factory(self, org_id: str, tenant_config: Optional[TenantConfig] = None):
        """Get sessionmaker for a tenant. Returns None if no tenant-specific DB."""
        engine = self.get_engine(org_id, tenant_config)
        if engine is None:
            return None
        with self._lock:
            return self._session_factories.get(org_id)
    
    def dispose_idle(self, max_idle_seconds: int = 1800):
        """Dispose engines that haven't been accessed recently."""
        now = time.time()
        with self._lock:
            to_remove = [
                org_id for org_id, last in self._last_access.items()
                if (now - last) > max_idle_seconds
            ]
            for org_id in to_remove:
                logger.info(f"Disposing idle tenant engine: {org_id}")
                if org_id in self._engines:
                    self._engines[org_id].dispose()
                    del self._engines[org_id]
                self._session_factories.pop(org_id, None)
                self._last_access.pop(org_id, None)
    
    def dispose_all(self):
        """Dispose all tenant engines (for shutdown)."""
        with self._lock:
            for org_id, engine in self._engines.items():
                engine.dispose()
            self._engines.clear()
            self._session_factories.clear()
            self._last_access.clear()


# ============================================
# TENANT STORAGE REGISTRY
# ============================================

class TenantStorageRegistry:
    """
    Manages StorageService instances per-tenant.
    Falls back to shared StorageService when no tenant config exists.
    """
    
    def __init__(self):
        self._clients: Dict[str, Any] = {}  # org_id -> StorageService
        self._lock = threading.Lock()
    
    def get_storage_service(self, org_id: str, tenant_config: Optional[TenantConfig] = None):
        """
        Get StorageService for a tenant.
        Returns None if no tenant-specific storage (caller should use shared).
        """
        if tenant_config is None or not tenant_config.has_own_storage:
            return None
        
        with self._lock:
            if org_id in self._clients:
                return self._clients[org_id]
            
            from .services.storage_service import StorageService
            
            logger.info(f"Creating storage client for tenant: {org_id}")
            service = StorageService.__new__(StorageService)
            service.connection_string = tenant_config.storage_connection_string
            service.container_name = tenant_config.storage_container
            service._container_verified = False
            
            from azure.storage.blob import BlobServiceClient
            service.blob_service_client = BlobServiceClient.from_connection_string(
                tenant_config.storage_connection_string
            )
            
            self._clients[org_id] = service
            return service
    
    def invalidate(self, org_id: str):
        """Remove a tenant's storage client from cache."""
        with self._lock:
            self._clients.pop(org_id, None)


# ============================================
# TENANT CU CLIENT REGISTRY
# ============================================

class TenantCURegistry:
    """
    Manages AzureContentUnderstandingClient instances per-tenant.
    Falls back to shared CU client when no tenant config exists.
    """
    
    def __init__(self):
        self._clients: Dict[str, Any] = {}
        self._lock = threading.Lock()
    
    def get_cu_client(self, org_id: str, tenant_config: Optional[TenantConfig] = None):
        """
        Get CU client for a tenant.
        Returns None if no tenant-specific CU (caller should use shared).
        """
        if tenant_config is None or not tenant_config.has_own_cu:
            return None
        
        with self._lock:
            if org_id in self._clients:
                return self._clients[org_id]
            
            from .services.azure_service import AzureContentUnderstandingClient
            
            logger.info(f"Creating CU client for tenant: {org_id}")
            client = AzureContentUnderstandingClient.__new__(AzureContentUnderstandingClient)
            client.endpoint = tenant_config.cu_endpoint.rstrip('/')
            client.api_key = tenant_config.cu_api_key
            client.api_version = tenant_config.cu_api_version
            client.analyzer_id = os.getenv('AZURE_ANALYZER_ID', 'prebuilt-layout')
            client.is_configured = True
            client.headers = {
                'Ocp-Apim-Subscription-Key': tenant_config.cu_api_key,
                'x-ms-useragent': 'request-driven-extraction-system'
            }
            
            self._clients[org_id] = client
            return client
    
    def invalidate(self, org_id: str):
        with self._lock:
            self._clients.pop(org_id, None)


# ============================================
# TENANT OPENAI REGISTRY
# ============================================

class TenantOpenAIRegistry:
    """
    Manages OpenAI client instances per-tenant.
    Falls back to shared OpenAI when no tenant config exists.
    """
    
    def __init__(self):
        self._clients: Dict[str, Any] = {}
        self._lock = threading.Lock()
    
    def get_openai_service(self, org_id: str, tenant_config: Optional[TenantConfig] = None):
        """
        Get OpenAI service for a tenant.
        Returns None if no tenant-specific OpenAI (caller should use shared).
        """
        if tenant_config is None or not tenant_config.has_own_openai:
            return None
        
        with self._lock:
            if org_id in self._clients:
                return self._clients[org_id]
            
            from .services.openai_service import OpenAIService
            
            logger.info(f"Creating OpenAI client for tenant: {org_id}")
            service = OpenAIService.__new__(OpenAIService)
            service.api_key = tenant_config.openai_api_key
            service.endpoint = tenant_config.openai_endpoint
            service.deployment = tenant_config.openai_deployment or 'gpt-4.1'
            service.api_version = '2024-12-01-preview'
            
            try:
                from openai import AzureOpenAI
                service.client = AzureOpenAI(
                    api_key=tenant_config.openai_api_key,
                    azure_endpoint=tenant_config.openai_endpoint,
                    api_version=service.api_version
                )
            except Exception as e:
                logger.error(f"Failed to create tenant OpenAI client for {org_id}: {e}")
                service.client = None
            
            self._clients[org_id] = service
            return service
    
    def invalidate(self, org_id: str):
        with self._lock:
            self._clients.pop(org_id, None)


# ============================================
# GLOBAL REGISTRY INSTANCES
# ============================================

_tenant_config_cache = TenantConfigCache()
_tenant_db_registry = TenantDatabaseRegistry()
_tenant_storage_registry = TenantStorageRegistry()
_tenant_cu_registry = TenantCURegistry()
_tenant_openai_registry = TenantOpenAIRegistry()


# ============================================
# PUBLIC API — Tenant-Aware Service Accessors
# ============================================

def get_tenant_config(org_id: str) -> Optional[TenantConfig]:
    """
    Get tenant configuration with caching.
    Returns None if no tenant config exists (use shared resources).
    """
    config = _tenant_config_cache.get(org_id)
    if config is not None:
        return config
    
    # Cache miss — load from DB
    config = load_tenant_config(org_id)
    if config is not None:
        _tenant_config_cache.put(config)
    
    return config


def get_tenant_database_repository(org_id: str):
    """
    Get a DatabaseRepository connected to the tenant's database.
    Falls back to the shared database if no tenant config.
    
    Usage:
        db = get_tenant_database_repository(org_id)
        # db is a standard DatabaseRepository, just connected to a different DB
    """
    from .repositories.database_repository import DatabaseRepository, get_database_repository
    
    config = get_tenant_config(org_id)
    
    if config is None or not config.has_own_database or not config.is_active:
        # No tenant config or no own DB — use shared
        return get_database_repository()
    
    # Get tenant-specific engine
    try:
        engine = _tenant_db_registry.get_engine(org_id, config)
        if engine is None:
            return get_database_repository()
        
        # Create a DatabaseRepository instance with the tenant's engine
        session_factory = _tenant_db_registry.get_session_factory(org_id, config)
        repo = DatabaseRepository.__new__(DatabaseRepository)
        repo._engine = engine
        repo._SessionLocal = session_factory
        return repo
        
    except Exception as e:
        logger.error(f"Failed to get tenant DB for {org_id}: {e} — falling back to shared")
        return get_database_repository()


def get_tenant_storage_service(org_id: str):
    """
    Get StorageService for the tenant.
    Falls back to shared StorageService if no tenant config.
    """
    from .services import get_storage_service
    
    config = get_tenant_config(org_id)
    
    if config is None or not config.has_own_storage or not config.is_active:
        return get_storage_service()
    
    try:
        service = _tenant_storage_registry.get_storage_service(org_id, config)
        return service if service is not None else get_storage_service()
    except Exception as e:
        logger.error(f"Failed to get tenant storage for {org_id}: {e} — falling back to shared")
        return get_storage_service()


def get_tenant_cu_client(org_id: str):
    """
    Get CU client for the tenant.
    Falls back to shared CU client if no tenant config.
    """
    from .services import get_azure_client
    
    config = get_tenant_config(org_id)
    
    if config is None or not config.has_own_cu or not config.is_active:
        return get_azure_client()
    
    try:
        client = _tenant_cu_registry.get_cu_client(org_id, config)
        return client if client is not None else get_azure_client()
    except Exception as e:
        logger.error(f"Failed to get tenant CU client for {org_id}: {e} — falling back to shared")
        return get_azure_client()


def get_tenant_openai_service(org_id: str):
    """
    Get OpenAI service for the tenant.
    Falls back to shared OpenAI if no tenant config.
    """
    from .services import get_openai_service
    
    config = get_tenant_config(org_id)
    
    if config is None or not config.has_own_openai or not config.is_active:
        return get_openai_service()
    
    try:
        service = _tenant_openai_registry.get_openai_service(org_id, config)
        return service if service is not None else get_openai_service()
    except Exception as e:
        logger.error(f"Failed to get tenant OpenAI for {org_id}: {e} — falling back to shared")
        return get_openai_service()


def invalidate_tenant(org_id: str):
    """Invalidate all cached resources for a tenant (e.g., after config change)."""
    _tenant_config_cache.invalidate(org_id)
    _tenant_storage_registry.invalidate(org_id)
    _tenant_cu_registry.invalidate(org_id)
    _tenant_openai_registry.invalidate(org_id)
    logger.info(f"Invalidated all cached resources for tenant: {org_id}")


def cleanup_idle_resources(max_idle_seconds: int = 1800):
    """Dispose idle tenant database engines. Call periodically."""
    _tenant_db_registry.dispose_idle(max_idle_seconds)


def shutdown_all():
    """Dispose all tenant resources. Call on app shutdown."""
    _tenant_db_registry.dispose_all()
    _tenant_config_cache.invalidate_all()
    logger.info("All tenant resources disposed")
