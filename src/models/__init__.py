"""
SQLAlchemy Models for Request-Driven Extraction System
All tables use SINGLE SQL DATABASE - No NoSQL dependencies

Naming Convention:
- Table names: lowercase, plural (e.g., users, templates, requests)
- Column names: {table_singular}_{column_name} (e.g., user_id, template_name)
- Primary keys: {table_singular}_id
- Foreign keys: {table_singular}_{referenced_table_singular}_id
- Timestamps: {table_singular}_created_at, {table_singular}_modified_at
"""

from datetime import datetime
from typing import Optional, List
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, Index, ForeignKey, Float, BigInteger, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, deferred

Base = declarative_base()


# ============================================
# LOOKUP: PARENT RECORD TYPES
# ============================================
class ParentRecordType(Base):
    """Lookup table for polymorphic parent types (entities like Request, Email, Document)"""
    __tablename__ = 'parent_record_types'
    
    parent_record_type_id = Column(Integer, primary_key=True, autoincrement=True)
    parent_record_type_value = Column(String(100), nullable=False, unique=True)
    parent_record_type_description = Column(String(500))
    parent_record_type_created_at = Column(DateTime, default=datetime.utcnow)
    parent_record_type_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    status_types = relationship("StatusType", back_populates="parent_record_type")
    audit_logs = relationship("AuditLog", back_populates="parent_record_type")


# ============================================
# LOOKUP: STATUSES
# ============================================
class Status(Base):
    """Lookup table for all possible status values"""
    __tablename__ = 'statuses'
    
    status_id = Column(Integer, primary_key=True, autoincrement=True)
    status_value = Column(String(100), nullable=False, unique=True)
    status_display_name = Column(String(100), nullable=False)
    status_description = Column(String(500))
    status_created_at = Column(DateTime, default=datetime.utcnow)
    status_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    status_types = relationship("StatusType", back_populates="status")


# ============================================
# LOOKUP: STATUS TYPES (STATUS × RECORD TYPE)
# ============================================
class StatusType(Base):
    """Maps valid statuses to each parent record type"""
    __tablename__ = 'status_types'
    
    status_type_id = Column(Integer, primary_key=True, autoincrement=True)
    status_type_status_id = Column(Integer, ForeignKey('statuses.status_id'), nullable=False)
    status_type_parent_record_type_id = Column(Integer, ForeignKey('parent_record_types.parent_record_type_id'), nullable=False)
    status_type_sort_order = Column(Integer, default=0)
    status_type_is_terminal = Column(Boolean, default=False)
    status_type_created_at = Column(DateTime, default=datetime.utcnow)
    status_type_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    status = relationship("Status", back_populates="status_types")
    parent_record_type = relationship("ParentRecordType", back_populates="status_types")
    
    # Relationships for entities using this status_type
    emails = relationship("Email", back_populates="status_type")
    documents = relationship("Document", back_populates="status_type")
    requests = relationship("Request", back_populates="status_type")
    document_versions = relationship("DocumentVersion", back_populates="status_type")
    async_jobs = relationship("AsyncJob", back_populates="status_type")
    
    __table_args__ = (
        Index('idx_status_types_status', 'status_type_status_id'),
        Index('idx_status_types_parent', 'status_type_parent_record_type_id'),
    )


# ============================================
# LOOKUP: DOCUMENT TYPES
# ============================================
class DocumentType(Base):
    """Lookup table for document types (email_body, email_attachment, manual_upload)"""
    __tablename__ = 'document_types'
    
    document_type_id = Column(Integer, primary_key=True, autoincrement=True)
    document_type_value = Column(String(100), nullable=False, unique=True)
    document_type_description = Column(String(500))
    document_type_created_at = Column(DateTime, default=datetime.utcnow)
    document_type_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    documents = relationship("Document", back_populates="document_type")


# ============================================
# ORGANIZATIONS
# ============================================
class Organization(Base):
    """Organizations (tenants)"""
    __tablename__ = 'organizations'
    
    organization_id = Column(String(50), primary_key=True)
    organization_name = Column(String(200), nullable=False)
    organization_azure_tenant_id = Column(String(36), unique=True, nullable=True)  # Azure AD tenant GUID
    organization_tier = Column(String(50))
    organization_settings = Column(Text)  # JSON
    organization_created_at = Column(DateTime, default=datetime.utcnow)
    organization_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    users = relationship("User", back_populates="organization")
    emails = relationship("Email", back_populates="organization")
    documents = relationship("Document", back_populates="organization")
    requests = relationship("Request", back_populates="organization")
    templates = relationship("Template", back_populates="organization")
    analyzers = relationship("Analyzer", back_populates="organization")
    subscriptions = relationship("Subscription", back_populates="organization")


# ============================================
# SUBSCRIPTIONS (Marketplace SaaS)
# ============================================
class Subscription(Base):
    """Marketplace subscription tracking per organization"""
    __tablename__ = 'subscriptions'
    
    subscription_id = Column(String(100), primary_key=True)
    subscription_organization_id = Column(String(50), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False)
    subscription_plan = Column(String(50), nullable=False, default='free_trial')  # free_trial | enterprise
    subscription_status = Column(String(30), nullable=False, default='active')    # active | suspended | cancelled | expired
    subscription_marketplace_id = Column(String(255), unique=True, nullable=True)
    subscription_started_at = Column(DateTime, default=datetime.utcnow)
    subscription_expires_at = Column(DateTime, nullable=True)
    subscription_cancelled_at = Column(DateTime, nullable=True)
    subscription_created_at = Column(DateTime, default=datetime.utcnow)
    subscription_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization", back_populates="subscriptions")


# ============================================
# USERS
# ============================================
class User(Base):
    """User accounts"""
    __tablename__ = 'users'
    
    user_id = Column(String(50), primary_key=True)
    user_organization_id = Column(String(50), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False)
    user_auth0_id = Column(String(255), unique=True, nullable=True)
    user_email = Column(String(255), nullable=False)
    user_username = Column(String(200), nullable=False)
    user_password_hash = Column(String(255))
    user_role_types = Column(String(50), default='user')
    user_is_active = Column(Boolean, default=True)
    user_last_login = Column(DateTime)
    user_created_at = Column(DateTime, default=datetime.utcnow)
    user_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization", back_populates="users")
    
    __table_args__ = (
        Index('idx_users_email', 'user_email'),
        Index('idx_users_org', 'user_organization_id'),
    )


# ============================================
# ANALYZERS
# ============================================
class Analyzer(Base):
    """Document Intelligence Analyzers"""
    __tablename__ = 'analyzers'
    
    analyzer_id = Column(String(50), primary_key=True)
    analyzer_organization_id = Column(String(50), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False)
    analyzer_name = Column(String(255), nullable=False)
    analyzer_description = Column(Text)
    analyzer_type = Column(String(100), nullable=False)  # 'azure_di', 'openai', 'custom'
    analyzer_azure_id = Column(String(255))  # Azure Document Intelligence analyzer ID
    analyzer_endpoint_url = Column(String(500))
    analyzer_configuration = Column(Text)  # JSON
    analyzer_is_active = Column(Boolean, default=True)
    analyzer_created_at = Column(DateTime, default=datetime.utcnow)
    analyzer_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization", back_populates="analyzers")
    templates = relationship("Template", back_populates="analyzer")
    analysis_runs = relationship("AnalysisRun", back_populates="analyzer")
    
    __table_args__ = (
        Index('idx_analyzers_org', 'analyzer_organization_id'),
        Index('idx_analyzers_active', 'analyzer_organization_id', 'analyzer_is_active'),
        Index('idx_analyzers_type', 'analyzer_type'),
    )


# ============================================
# TEMPLATES
# ============================================
class Template(Base):
    """Analysis templates"""
    __tablename__ = 'templates'
    
    template_id = Column(Integer, primary_key=True, autoincrement=True)
    template_organization_id = Column(String(50), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False)
    template_analyzer_id = Column(String(50), ForeignKey('analyzers.analyzer_id', ondelete='SET NULL'))
    template_name = Column(String(255), nullable=False)
    template_internal_name = Column(String(255))  # Maps to content understanding analyzer
    template_description = Column(Text)
    template_is_active = Column(Boolean, default=True)
    template_allow_reprocessing = Column(Boolean, default=True)
    template_created_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    template_created_at = Column(DateTime, default=datetime.utcnow)
    template_modified_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    template_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    # NOTE: template_source_documents, template_creation_prompt, template_creation_method
    # are optional columns added via database/add_template_metadata_columns.sql.
    # They are NOT mapped as ORM columns to avoid crashes if migration hasn't run.
    # Read/write is handled via raw SQL in database_repository.py.
    
    organization = relationship("Organization", back_populates="templates")
    analyzer = relationship("Analyzer", back_populates="templates")
    fields = relationship("TemplateField", back_populates="template", cascade="all, delete-orphan")
    category_orders = relationship("TemplateCategoryOrder", back_populates="template", cascade="all, delete-orphan")
    requests = relationship("Request", back_populates="template")
    created_by_user = relationship("User", foreign_keys=[template_created_by])
    modified_by_user = relationship("User", foreign_keys=[template_modified_by])
    
    __table_args__ = (
        Index('idx_templates_org', 'template_organization_id'),
        Index('idx_templates_active', 'template_organization_id', 'template_is_active'),
        Index('idx_templates_analyzer', 'template_analyzer_id'),
    )


# ============================================
# TEMPLATE FIELD CATEGORIES
# ============================================
class TemplateFieldCategory(Base):
    """Template field category definitions"""
    __tablename__ = 'template_field_categories'
    
    template_field_category_id = Column(String(50), primary_key=True)
    template_field_category_name = Column(String(100), nullable=False, unique=True)
    template_field_category_display_name = Column(String(255), nullable=False)
    template_field_category_description = Column(Text)
    template_field_category_icon = Column(String(50))
    template_field_category_color = Column(String(50))
    template_field_category_is_active = Column(Boolean, default=True)
    template_field_category_created_at = Column(DateTime, default=datetime.utcnow)
    template_field_category_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    template_fields = relationship("TemplateField", back_populates="category")
    template_orders = relationship("TemplateCategoryOrder", back_populates="category")
    
    __table_args__ = (
        Index('idx_template_field_categories_name', 'template_field_category_name'),
        Index('idx_template_field_categories_active', 'template_field_category_is_active'),
    )


# ============================================
# TEMPLATE CATEGORY ORDERS
# ============================================
class TemplateCategoryOrder(Base):
    """Category ordering per template"""
    __tablename__ = 'template_category_orders'
    
    template_category_order_id = Column(String(50), primary_key=True)
    template_category_order_template_id = Column(Integer, ForeignKey('templates.template_id', ondelete='CASCADE'), nullable=False)
    template_category_order_category_id = Column(String(50), ForeignKey('template_field_categories.template_field_category_id', ondelete='CASCADE'), nullable=False)
    template_category_order_sort_order = Column(Integer, default=0)
    template_category_order_is_visible = Column(Boolean, default=True)
    template_category_order_created_at = Column(DateTime, default=datetime.utcnow)
    template_category_order_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    template = relationship("Template", back_populates="category_orders")
    category = relationship("TemplateFieldCategory", back_populates="template_orders")
    
    __table_args__ = (
        Index('idx_template_category_orders_template', 'template_category_order_template_id'),
        Index('idx_template_category_orders_sort', 'template_category_order_template_id', 'template_category_order_sort_order'),
    )


# ============================================
# TEMPLATE FIELDS
# ============================================
class TemplateField(Base):
    """Template field definitions"""
    __tablename__ = 'template_fields'
    
    template_field_id = Column(Integer, primary_key=True, autoincrement=True)
    template_field_template_id = Column(Integer, ForeignKey('templates.template_id', ondelete='CASCADE'), nullable=False)
    template_field_category_id = Column(String(50), ForeignKey('template_field_categories.template_field_category_id', ondelete='SET NULL'))
    template_field_field_name = Column(String(255), nullable=False)
    template_field_display_name = Column(String(255), nullable=False)
    template_field_field_definition = Column(Text)
    template_field_data_type = Column(String(50), default='text')
    template_field_field_values = Column(String(500))
    template_field_precision_threshold = Column(Float, default=0.60)  # Min confidence to save field value (0-1)
    template_field_is_required = Column(Boolean, default=False)
    template_field_extraction_is_required = Column(Boolean, default=False)
    template_field_normalisation_instruction = Column(Text)  # AI normalisation instruction per field
    template_field_is_active = Column(Boolean, default=True)
    template_field_sort_order = Column(Integer, default=0)
    template_field_validation_rules = Column(Text)  # JSON
    template_field_date_effective_from = Column(DateTime)
    template_field_date_effective_to = Column(DateTime)
    template_field_created_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    template_field_created_at = Column(DateTime, default=datetime.utcnow)
    template_field_modified_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    template_field_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    template = relationship("Template", back_populates="fields")
    category = relationship("TemplateFieldCategory", back_populates="template_fields")
    request_fields = relationship("RequestField", back_populates="template_field")
    created_by_user = relationship("User", foreign_keys=[template_field_created_by])
    modified_by_user = relationship("User", foreign_keys=[template_field_modified_by])
    
    __table_args__ = (
        Index('idx_template_fields_template', 'template_field_template_id'),
        Index('idx_template_fields_active', 'template_field_template_id', 'template_field_is_active'),
        Index('idx_template_fields_category', 'template_field_template_id', 'template_field_category_id'),
    )


# ============================================
# EMAILS
# ============================================
class Email(Base):
    """Ingested emails (input source)"""
    __tablename__ = 'emails'
    
    email_id = Column(Integer, primary_key=True, autoincrement=True)
    email_organization_id = Column(String(50), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False)
    email_message_id = Column(String(255))
    email_conversation_id = Column(String(255))
    email_sender = Column(String(255))
    email_recipients = Column(String(500))
    email_subject = Column(String(500), nullable=False)
    email_body_text = Column(Text, nullable=False)
    email_received_at = Column(DateTime, default=datetime.utcnow)
    email_mailbox_email = Column(String(255))
    email_is_shared_mailbox = Column(Boolean, default=False)
    email_status_type_id = Column(Integer, ForeignKey('status_types.status_type_id'), nullable=False)
    email_created_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    email_created_at = Column(DateTime, default=datetime.utcnow)
    email_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization", back_populates="emails")
    status_type = relationship("StatusType", back_populates="emails")
    created_by_user = relationship("User", foreign_keys=[email_created_by])
    documents = relationship("Document", back_populates="email")
    email_requests = relationship("EmailRequest", back_populates="email", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_emails_org_received', 'email_organization_id', 'email_received_at'),
        Index('idx_emails_status', 'email_status_type_id'),
        Index('idx_emails_message_id', 'email_message_id'),
    )


# ============================================
# DOCUMENTS
# ============================================
class Document(Base):
    """Documents (PDFs, attachments)"""
    __tablename__ = 'documents'
    
    document_id = Column(Integer, primary_key=True, autoincrement=True)
    document_organization_id = Column(String(50), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False)
    document_request_id = Column(Integer, ForeignKey('requests.request_id', ondelete='NO ACTION'))  # Direct link to request
    document_email_id = Column(Integer, ForeignKey('emails.email_id', ondelete='NO ACTION'))
    document_document_type_id = Column(Integer, ForeignKey('document_types.document_type_id'), nullable=False)
    document_file_name = Column(String(500), nullable=False)
    document_file_path = Column(Text, nullable=False)
    document_content_type = Column(String(100))
    document_file_size_bytes = Column(Integer)
    document_status_type_id = Column(Integer, ForeignKey('status_types.status_type_id'), nullable=False)
    document_current_version_id = Column(Integer)  # FK added after document_versions
    document_created_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    document_created_at = Column(DateTime, default=datetime.utcnow)
    document_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization", back_populates="documents")
    request = relationship("Request", back_populates="documents", foreign_keys=[document_request_id])  # Direct relationship
    email = relationship("Email", back_populates="documents")
    document_type = relationship("DocumentType", back_populates="documents")
    status_type = relationship("StatusType", back_populates="documents")
    created_by_user = relationship("User", foreign_keys=[document_created_by])
    versions = relationship("DocumentVersion", back_populates="document", cascade="all, delete-orphan")
    request_documents = relationship("RequestDocument", back_populates="document")
    annotations = relationship("Annotation", back_populates="document")
    request_fields = relationship("RequestField", back_populates="source_document")
    
    __table_args__ = (
        Index('idx_documents_org_created', 'document_organization_id', 'document_created_at'),
        Index('idx_documents_status', 'document_status_type_id'),
        Index('idx_documents_email', 'document_email_id'),
        Index('idx_documents_request', 'document_request_id'),
    )


# ============================================
# REQUESTS
# ============================================
class Request(Base):
    """Requests - PRIMARY AGGREGATE ROOT"""
    __tablename__ = 'requests'
    
    request_id = Column(Integer, primary_key=True, autoincrement=True)
    request_organization_id = Column(String(50), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False)
    request_ref = Column(String(100))  # External reference number
    request_title = Column(String(500), nullable=False)
    request_description = Column(Text)
    request_issuer = Column(String(500))  # Issuer name for the request
    request_template_id = Column(Integer, ForeignKey('templates.template_id', ondelete='NO ACTION'))
    request_status_type_id = Column(Integer, ForeignKey('status_types.status_type_id'), nullable=False)
    request_current_version_id = Column(Integer)  # FK added after request_versions
    request_created_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    request_created_at = Column(DateTime, default=datetime.utcnow)
    request_modified_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    request_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    request_published_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    request_published_at = Column(DateTime)
    request_cancelled_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    request_cancelled_at = Column(DateTime)
    request_cancellation_reason = Column(Text)
    request_extraction_prompt = deferred(Column(Text))  # Optional user prompt to guide AI extraction (deferred: not in default SELECT)
    
    organization = relationship("Organization", back_populates="requests")
    template = relationship("Template", back_populates="requests")
    status_type = relationship("StatusType", back_populates="requests")
    created_by_user = relationship("User", foreign_keys=[request_created_by])
    modified_by_user = relationship("User", foreign_keys=[request_modified_by])
    published_by_user = relationship("User", foreign_keys=[request_published_by])
    cancelled_by_user = relationship("User", foreign_keys=[request_cancelled_by])
    versions = relationship("RequestVersion", back_populates="request", cascade="all, delete-orphan")
    request_documents = relationship("RequestDocument", back_populates="request", cascade="all, delete-orphan")
    documents = relationship("Document", back_populates="request", foreign_keys="Document.document_request_id")  # Direct relationship via document_request_id
    fields = relationship("RequestField", back_populates="request", cascade="all, delete-orphan")
    annotations = relationship("Annotation", back_populates="request")
    email_requests = relationship("EmailRequest", back_populates="request", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_requests_org_created', 'request_organization_id', 'request_created_at'),
        Index('idx_requests_status', 'request_status_type_id'),
        Index('idx_requests_template', 'request_template_id'),
        Index('idx_requests_ref', 'request_ref'),
    )


# ============================================
# EMAIL REQUESTS (Junction Table)
# ============================================
class EmailRequest(Base):
    """Email-Request junction table"""
    __tablename__ = 'emailrequests'
    
    emailrequest_id = Column(Integer, primary_key=True, autoincrement=True)
    emailrequest_request_id = Column(Integer, ForeignKey('requests.request_id', ondelete='CASCADE'), nullable=False)
    emailrequest_email_id = Column(Integer, ForeignKey('emails.email_id', ondelete='NO ACTION'), nullable=False)
    emailrequest_created_at = Column(DateTime, default=datetime.utcnow)
    
    request = relationship("Request", back_populates="email_requests")
    email = relationship("Email", back_populates="email_requests")
    
    __table_args__ = (
        Index('idx_emailrequests_request', 'emailrequest_request_id'),
        Index('idx_emailrequests_email', 'emailrequest_email_id'),
    )


# ============================================
# REQUEST DOCUMENTS (Junction Table)
# ============================================
class RequestDocument(Base):
    """Request-Document junction table"""
    __tablename__ = 'request_documents'
    
    request_document_id = Column(Integer, primary_key=True, autoincrement=True)
    request_document_request_id = Column(Integer, ForeignKey('requests.request_id', ondelete='CASCADE'), nullable=False)
    request_document_document_id = Column(Integer, ForeignKey('documents.document_id', ondelete='NO ACTION'), nullable=False)
    request_document_source_type = Column(String(50), default='attachment')
    request_document_created_at = Column(DateTime, default=datetime.utcnow)
    
    request = relationship("Request", back_populates="request_documents")
    document = relationship("Document", back_populates="request_documents")
    
    __table_args__ = (
        Index('idx_request_documents_request', 'request_document_request_id'),
        Index('idx_request_documents_document', 'request_document_document_id'),
    )


# ============================================
# REQUEST VERSIONS
# ============================================
class RequestVersion(Base):
    """Request versions"""
    __tablename__ = 'request_versions'
    
    request_version_id = Column(Integer, primary_key=True, autoincrement=True)
    request_version_request_id = Column(Integer, ForeignKey('requests.request_id', ondelete='CASCADE'), nullable=False)
    request_version_number = Column(Integer, nullable=False)
    request_version_label = Column(String(200))
    request_version_consolidated_fields = Column(Text)  # JSON snapshot
    request_version_created_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    request_version_created_at = Column(DateTime, default=datetime.utcnow)
    
    request = relationship("Request", back_populates="versions")
    created_by_user = relationship("User", foreign_keys=[request_version_created_by])
    fields = relationship("RequestField", back_populates="request_version")
    
    __table_args__ = (
        Index('idx_request_versions_request', 'request_version_request_id'),
    )


# ============================================
# DOCUMENT VERSIONS
# ============================================
class DocumentVersion(Base):
    """Document versions"""
    __tablename__ = 'document_versions'
    
    document_version_id = Column(Integer, primary_key=True, autoincrement=True)
    document_version_document_id = Column(Integer, ForeignKey('documents.document_id', ondelete='CASCADE'), nullable=False)
    document_version_number = Column(Integer, nullable=False)
    document_version_analysis_run_id = Column(Integer)  # Reference to analysis_runs
    document_version_operation_location = Column(Text)  # Azure async operation URL
    document_version_status_type_id = Column(Integer, ForeignKey('status_types.status_type_id'), nullable=False)
    document_version_error_message = Column(Text)
    document_version_created_at = Column(DateTime, default=datetime.utcnow)
    
    document = relationship("Document", back_populates="versions")
    status_type = relationship("StatusType", back_populates="document_versions")
    artifact_overrides = relationship("ArtifactOverride", back_populates="document_version")
    
    __table_args__ = (
        Index('idx_document_versions_document', 'document_version_document_id'),
        Index('idx_document_versions_analysis_run', 'document_version_analysis_run_id'),
    )


# ============================================
# REQUEST FIELDS (Extracted Fields)
# ============================================
class RequestField(Base):
    """Extracted/consolidated fields for requests"""
    __tablename__ = 'requestfields'
    
    requestfield_id = Column(Integer, primary_key=True, autoincrement=True)
    requestfield_request_id = Column(Integer, ForeignKey('requests.request_id', ondelete='CASCADE'), nullable=False)
    requestfield_request_version_id = Column(Integer, ForeignKey('request_versions.request_version_id', ondelete='NO ACTION'), nullable=False)
    requestfield_template_field_id = Column(Integer, ForeignKey('template_fields.template_field_id', ondelete='NO ACTION'))
    requestfield_field_name = Column(String(500), nullable=False)
    requestfield_field_value = Column(Text)
    requestfield_extracted_value = Column(Text)
    requestfield_normalized_value = Column(Text)
    requestfield_precision = Column(String(50))
    requestfield_confidence = Column(Float)
    requestfield_source_document_id = Column(Integer, ForeignKey('documents.document_id', ondelete='NO ACTION'))
    requestfield_source_type = Column(String(50), default='pending')
    requestfield_source_location = Column(Text)  # JSON for bounding box, page number
    requestfield_is_active = Column(Boolean, default=True)
    requestfield_is_selected = Column(Boolean, default=False)
    requestfield_is_manually_edited = Column(Boolean, default=False)
    requestfield_created_at = Column(DateTime, default=datetime.utcnow)
    requestfield_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    request = relationship("Request", back_populates="fields")
    request_version = relationship("RequestVersion", back_populates="fields")
    template_field = relationship("TemplateField", back_populates="request_fields")
    source_document = relationship("Document", back_populates="request_fields")
    
    __table_args__ = (
        Index('idx_requestfields_request', 'requestfield_request_id'),
        Index('idx_requestfields_version', 'requestfield_request_version_id'),
        Index('idx_requestfields_template_field', 'requestfield_template_field_id'),
        Index('idx_requestfields_name', 'requestfield_field_name'),
        Index('idx_requestfields_active', 'requestfield_request_id', 'requestfield_field_name', 'requestfield_is_active'),
    )


# ============================================
# ANALYSIS RUNS
# ============================================
class AnalysisRun(Base):
    """Analysis runs - stored in SQL"""
    __tablename__ = 'analysis_runs'
    
    analysis_run_id = Column(Integer, primary_key=True, autoincrement=True)
    analysis_run_azure_operation_id = Column(String(255))  # Azure Content Understanding operation/run ID
    analysis_run_source_type = Column(String(50), nullable=False)
    analysis_run_source_id = Column(Integer, nullable=False)
    analysis_run_document_version_id = Column(Integer, ForeignKey('document_versions.document_version_id', ondelete='CASCADE'))
    analysis_run_analyzer_id = Column(String(50), ForeignKey('analyzers.analyzer_id'), nullable=False)
    analysis_run_status = Column(String(50), default='pending')
    analysis_run_payload = Column(Text)  # JSON
    analysis_run_is_chunked = Column(Boolean, default=False)
    analysis_run_chunk_count = Column(Integer, default=0)
    analysis_run_payload_size_bytes = Column(BigInteger, default=0)
    analysis_run_error_message = Column(Text)
    analysis_run_triggered_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    analysis_run_started_at = Column(DateTime)
    analysis_run_completed_at = Column(DateTime)
    analysis_run_created_at = Column(DateTime, default=datetime.utcnow)
    analysis_run_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    document_version = relationship("DocumentVersion")
    analyzer = relationship("Analyzer", back_populates="analysis_runs")
    triggered_by_user = relationship("User", foreign_keys=[analysis_run_triggered_by])
    artifacts = relationship("AnalysisArtifact", back_populates="analysis_run", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_analysis_runs_source', 'analysis_run_source_type', 'analysis_run_source_id'),
        Index('idx_analysis_runs_version', 'analysis_run_document_version_id'),
        Index('idx_analysis_runs_status', 'analysis_run_status'),
    )


# ============================================
# ANALYSIS ARTIFACTS
# ============================================
class AnalysisArtifact(Base):
    """Extracted artifacts from analysis"""
    __tablename__ = 'analysis_artifacts'
    
    analysis_artifact_id = Column(Integer, primary_key=True, autoincrement=True)
    analysis_artifact_analysis_run_id = Column(Integer, ForeignKey('analysis_runs.analysis_run_id', ondelete='CASCADE'), nullable=False)
    analysis_artifact_source_type = Column(String(50), nullable=False)
    analysis_artifact_source_id = Column(Integer, nullable=False)
    analysis_artifact_type = Column(String(50), nullable=False)  # 'field', 'table', 'figure'
    analysis_artifact_name = Column(String(500))
    analysis_artifact_data = Column(Text)  # JSON
    analysis_artifact_created_at = Column(DateTime, default=datetime.utcnow)
    
    analysis_run = relationship("AnalysisRun", back_populates="artifacts")
    overrides = relationship("ArtifactOverride", back_populates="artifact")
    
    __table_args__ = (
        Index('idx_analysis_artifacts_run', 'analysis_artifact_analysis_run_id'),
        Index('idx_analysis_artifacts_source', 'analysis_artifact_source_type', 'analysis_artifact_source_id'),
        Index('idx_analysis_artifacts_type', 'analysis_artifact_type'),
    )


# ============================================
# ARTIFACT OVERRIDES
# ============================================
class ArtifactOverride(Base):
    """Manual overrides for artifacts"""
    __tablename__ = 'artifact_overrides'
    
    artifact_override_id = Column(Integer, primary_key=True, autoincrement=True)
    artifact_override_analysis_artifact_id = Column(Integer, ForeignKey('analysis_artifacts.analysis_artifact_id', ondelete='CASCADE'), nullable=False)
    artifact_override_document_version_id = Column(Integer, ForeignKey('document_versions.document_version_id', ondelete='NO ACTION'))
    artifact_override_field_path = Column(String(500), nullable=False)
    artifact_override_original_value = Column(Text)
    artifact_override_value = Column(Text)
    artifact_override_reason = Column(Text)
    artifact_override_created_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    artifact_override_created_at = Column(DateTime, default=datetime.utcnow)
    
    artifact = relationship("AnalysisArtifact", back_populates="overrides")
    document_version = relationship("DocumentVersion", back_populates="artifact_overrides")
    created_by_user = relationship("User", foreign_keys=[artifact_override_created_by])
    
    __table_args__ = (
        Index('idx_artifact_overrides_artifact', 'artifact_override_analysis_artifact_id'),
        Index('idx_artifact_overrides_version', 'artifact_override_document_version_id'),
    )


# ============================================
# ASYNC JOBS
# ============================================
class AsyncJob(Base):
    """Async job tracking"""
    __tablename__ = 'async_jobs'
    
    async_job_id = Column(Integer, primary_key=True, autoincrement=True)
    async_job_organization_id = Column(String(50), ForeignKey('organizations.organization_id'))
    async_job_type = Column(String(50), nullable=False)
    async_job_entity_id = Column(Integer, nullable=False)
    async_job_entity_type = Column(String(50), nullable=False)
    async_job_status_type_id = Column(Integer, ForeignKey('status_types.status_type_id'), nullable=False)
    async_job_progress_percent = Column(Integer, default=0)
    async_job_progress_message = Column(Text)
    async_job_result_data = Column(Text)  # JSON
    async_job_error_message = Column(Text)
    async_job_retry_count = Column(Integer, default=0)
    async_job_max_retries = Column(Integer, default=3)
    async_job_started_at = Column(DateTime)
    async_job_completed_at = Column(DateTime)
    async_job_created_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    async_job_created_at = Column(DateTime, default=datetime.utcnow)
    async_job_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    status_type = relationship("StatusType", back_populates="async_jobs")
    created_by_user = relationship("User", foreign_keys=[async_job_created_by])
    organization = relationship("Organization", foreign_keys=[async_job_organization_id])
    
    __table_args__ = (
        Index('idx_async_jobs_entity', 'async_job_entity_id', 'async_job_entity_type'),
        Index('idx_async_jobs_status', 'async_job_status_type_id'),
        Index('idx_async_jobs_type', 'async_job_type'),
        Index('idx_async_jobs_created', 'async_job_created_at'),
        Index('idx_async_jobs_organization', 'async_job_organization_id', 'async_job_status_type_id', 'async_job_created_at'),
    )


# ============================================
# ANNOTATIONS
# ============================================
class Annotation(Base):
    """Document annotations"""
    __tablename__ = 'annotations'
    
    annotation_id = Column(Integer, primary_key=True, autoincrement=True)
    annotation_document_id = Column(Integer, ForeignKey('documents.document_id', ondelete='CASCADE'), nullable=False)
    annotation_request_id = Column(Integer, ForeignKey('requests.request_id', ondelete='NO ACTION'))
    annotation_user_id = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'))
    annotation_page_number = Column(Integer)
    annotation_bounding_box = Column(Text)  # JSON
    annotation_text = Column(Text)
    annotation_type = Column(String(50))
    annotation_created_at = Column(DateTime, default=datetime.utcnow)
    
    document = relationship("Document", back_populates="annotations")
    request = relationship("Request", back_populates="annotations")
    user = relationship("User")
    
    __table_args__ = (
        Index('idx_annotations_document', 'annotation_document_id'),
        Index('idx_annotations_request', 'annotation_request_id'),
        Index('idx_annotations_user', 'annotation_user_id'),
    )


# ============================================
# AUDIT LOGS
# ============================================
class AuditLog(Base):
    """
    Comprehensive audit logging with JSON-based change tracking.
    
    Uses parent_record_types for polymorphic entity references.
    audit_json format: { "field_name": { "old": value, "new": value } }
    
    Actions: INSERT, UPDATE, DELETE
    """
    __tablename__ = 'auditlogs'
    
    auditlog_id = Column(Integer, primary_key=True, autoincrement=True)
    auditlog_parent_record_type_id = Column(Integer, ForeignKey('parent_record_types.parent_record_type_id'), nullable=False)
    auditlog_entity_id = Column(String(100), nullable=False)  # Entity primary key as string
    auditlog_action = Column(String(10), nullable=False)  # INSERT, UPDATE, DELETE
    auditlog_audit_json = Column(Text, nullable=False)  # JSON with old/new values per field
    auditlog_reason = Column(String(500))  # Optional reason for the change
    auditlog_created_by = Column(String(50), ForeignKey('users.user_id', ondelete='NO ACTION'), nullable=False)
    auditlog_created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    auditlog_ip_address = Column(String(45))  # IPv4/IPv6 compatible
    auditlog_user_agent = Column(String(500))
    auditlog_request_trace_id = Column(String(100))  # For distributed tracing
    
    parent_record_type = relationship("ParentRecordType", back_populates="audit_logs")
    created_by_user = relationship("User")
    
    __table_args__ = (
        Index('idx_auditlogs_entity', 'auditlog_parent_record_type_id', 'auditlog_entity_id'),
        Index('idx_auditlogs_created_at', 'auditlog_created_at'),
        Index('idx_auditlogs_created_by', 'auditlog_created_by'),
    )


# ============================================
# METERED USAGE (Azure Marketplace Billing)
# ============================================
class MeteredUsage(Base):
    """Tracks per-org metered usage for Azure Marketplace billing.
    
    Dimensions:
        pages_processed    — pages sent through Azure Content Understanding
        fields_normalised  — fields sent through Azure OpenAI normalisation
    """
    __tablename__ = 'metered_usage'
    
    metered_usage_id = Column(Integer, primary_key=True, autoincrement=True)
    metered_usage_organization_id = Column(String(50), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False)
    metered_usage_subscription_id = Column(String(100), ForeignKey('subscriptions.subscription_id', ondelete='NO ACTION'), nullable=False)
    metered_usage_dimension = Column(String(50), nullable=False)  # 'pages_processed' | 'fields_normalised'
    metered_usage_quantity = Column(Numeric(18, 4), nullable=False, default=0)
    metered_usage_request_id = Column(Integer, nullable=True)
    metered_usage_job_id = Column(Integer, nullable=True)
    metered_usage_period_start = Column(DateTime, nullable=False)
    metered_usage_period_end = Column(DateTime, nullable=False)
    metered_usage_reported = Column(Boolean, nullable=False, default=False)
    metered_usage_reported_at = Column(DateTime, nullable=True)
    metered_usage_marketplace_response = Column(Text, nullable=True)
    metered_usage_created_at = Column(DateTime, default=datetime.utcnow)
    metered_usage_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization")
    subscription = relationship("Subscription")
    
    __table_args__ = (
        Index('idx_metered_usage_unreported', 'metered_usage_reported', 'metered_usage_organization_id', 'metered_usage_dimension'),
        Index('idx_metered_usage_org_period', 'metered_usage_organization_id', 'metered_usage_dimension', 'metered_usage_period_start', 'metered_usage_period_end'),
    )


# ============================================
# TENANT CONFIGURATION (Multi-Tenant Routing)
# ============================================
class TenantConfig(Base):
    """Tenant-specific resource connection details for multi-tenant data hosting.
    
    When a row exists for an organization, customer data operations route
    to the tenant's own database, storage, and AI services.
    When no row exists, shared (default) resources are used.
    """
    __tablename__ = 'tenant_configs'
    
    tenant_config_id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_config_organization_id = Column(String(50), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False, unique=True)
    
    # Database
    tenant_config_db_connection_string = Column(Text, nullable=True)
    
    # Blob Storage
    tenant_config_storage_connection_string = Column(Text, nullable=True)
    tenant_config_storage_container = Column(String(100), default='documents')
    
    # Content Understanding
    tenant_config_cu_endpoint = Column(String(500), nullable=True)
    tenant_config_cu_api_key = Column(Text, nullable=True)
    tenant_config_cu_api_version = Column(String(50), default='2025-11-01')
    
    # OpenAI (optional per-tenant override)
    tenant_config_openai_endpoint = Column(String(500), nullable=True)
    tenant_config_openai_api_key = Column(Text, nullable=True)
    tenant_config_openai_deployment = Column(String(100), nullable=True)
    
    # Status and metadata
    tenant_config_status = Column(String(30), nullable=False, default='active')
    tenant_config_region = Column(String(50), nullable=True)
    tenant_config_notes = Column(Text, nullable=True)
    tenant_config_created_at = Column(DateTime, default=datetime.utcnow)
    tenant_config_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization")
    
    __table_args__ = (
        Index('idx_tenant_configs_org', 'tenant_config_organization_id'),
        Index('idx_tenant_configs_status', 'tenant_config_status'),
    )


# ============================================
# ORGANIZATION BRANDING (White-labelling)
# ============================================
class OrganizationBranding(Base):
    """Per-tenant branding / white-label configuration."""
    __tablename__ = 'organization_branding'
    
    branding_id = Column(Integer, primary_key=True, autoincrement=True)
    branding_organization_id = Column(String(255), ForeignKey('organizations.organization_id', ondelete='CASCADE'), nullable=False, unique=True)
    branding_app_name = Column(String(100), nullable=False, default='Xtract')
    branding_subtitle = Column(String(100), nullable=False, default='Synapx AI')
    branding_logo_url = Column(String(500), nullable=True)
    branding_favicon_url = Column(String(500), nullable=True)
    branding_primary_color = Column(String(7), nullable=False, default='#1e2a3b')
    branding_accent_color = Column(String(7), nullable=False, default='#2563eb')
    branding_copyright_text = Column(String(200), nullable=True)
    branding_login_tagline = Column(String(200), nullable=True)
    branding_apply_to_plugin = Column(Boolean, nullable=False, default=False)
    branding_plugin_body_text = Column(String(500), nullable=True)
    branding_plugin_footer_text = Column(String(200), nullable=True)
    branding_created_at = Column(DateTime, default=datetime.utcnow)
    branding_modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization")
    
    __table_args__ = (
        Index('idx_branding_org', 'branding_organization_id'),
    )
