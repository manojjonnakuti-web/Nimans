"""
Database Repository Layer
Handles all database operations with proper transaction management
SINGLE SQL DATABASE - No NoSQL dependencies
"""

import os
import json
import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager

from sqlalchemy import create_engine, and_, or_, desc, text, func
from sqlalchemy.orm import sessionmaker, Session, joinedload
from sqlalchemy.exc import IntegrityError

from ..models import (
    Base, Organization, Subscription, User, Email, Document, EmailRequest,
    Request, RequestDocument, RequestVersion, DocumentVersion,
    RequestField, AnalysisRun, AnalysisArtifact,
    ArtifactOverride, AsyncJob, Template, TemplateField, TemplateFieldCategory, 
    TemplateCategoryOrder, Annotation, AuditLog, Status, StatusType, 
    Analyzer, ParentRecordType, DocumentType, MeteredUsage, TenantConfig,
    OrganizationBranding
)

logger = logging.getLogger(__name__)

# Global engine reference
_engine = None
_SessionLocal = None


def init_database(connection_string: str):
    """Initialize the database engine for Azure SQL Server"""
    global _engine, _SessionLocal
    if not connection_string:
        raise ValueError("Database connection string is required")
    
    # Azure SQL Server connection with optimized pool settings for cloud
    # Note: connect_args with attrs_before causes HY024 errors on Linux ODBC Driver 18
    _engine = create_engine(
        connection_string, 
        echo=False, 
        pool_pre_ping=True,           # Verify connections are alive
        fast_executemany=True,        # Batch insert optimization
        pool_size=10,                 # Increased pool size for cloud (connections are pre-warmed)
        max_overflow=20,              # Allow 20 more under load for bursts
        pool_recycle=1800,            # Recycle connections every 30 min (Azure drops idle after ~30min)
        pool_timeout=30,              # Increased timeout for cloud latency
    )
    _SessionLocal = sessionmaker(bind=_engine)
    
    # Pre-warm multiple connections in the pool for faster first requests
    logger.info("Pre-warming database connection pool...")
    warmed = 0
    for i in range(5):  # Warm up 5 connections for better cloud performance
        try:
            with _engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                conn.commit()  # Explicitly commit to avoid rollback on close
                warmed += 1
        except Exception as e:
            logger.warning(f"Failed to pre-warm connection {i}: {e}")
    
    # Schema is managed externally for Azure SQL - no auto-creation
    logger.info(f"Database initialized successfully (Azure SQL Server with {warmed} pre-warmed connections)")


def _seed_initial_data():
    """Seed initial required data (statuses, org, etc.)"""
    global _SessionLocal
    if _SessionLocal is None:
        return
    
    session = _SessionLocal()
    try:
        # Check if data already exists
        existing_org = session.query(Organization).filter_by(organization_id='org_xtractai').first()
        if existing_org:
            # Backfill azure_tenant_id if missing (migration support)
            if not existing_org.organization_azure_tenant_id:
                home_tenant = os.environ.get('AZURE_TENANT_ID', '')
                if home_tenant:
                    existing_org.organization_azure_tenant_id = home_tenant
                    session.commit()
                    logger.info(f"Backfilled azure_tenant_id for org_xtractai: {home_tenant}")
            logger.info("Seed data already exists, skipping")
            session.close()
            return
        
        # Create default organization (linked to home Azure AD tenant)
        home_tenant = os.environ.get('AZURE_TENANT_ID', '')
        org = Organization(
            organization_id='org_xtractai',
            organization_name='Xtract',
            organization_azure_tenant_id=home_tenant or None,
            organization_tier='enterprise'
        )
        session.add(org)
        
        # Create parent record types (auto-increment IDs)
        parent_types_data = [
            ('request', 'A document extraction request'),
            ('email', 'An ingested email'),
            ('document', 'A document/PDF attachment'),
            ('request_field', 'An extracted field from a request'),
            ('job', 'An async processing job'),
        ]
        for value, desc in parent_types_data:
            pt = ParentRecordType(parent_record_type_value=value, parent_record_type_description=desc)
            session.add(pt)
        session.flush()  # Get IDs
        
        # Create statuses (auto-increment IDs)
        statuses_data = [
            ('pending', 'Pending', 'Awaiting processing'),
            ('processing', 'Processing', 'Currently being processed'),
            ('extracted', 'Extracted', 'Fields extracted, pending review'),
            ('reviewing', 'Reviewing', 'Under review'),
            ('approved', 'Approved', 'Has been approved'),
            ('rejected', 'Rejected', 'Has been rejected'),
            ('failed', 'Failed', 'Processing failed'),
            ('uploaded', 'Uploaded', 'Uploaded to storage'),
            ('processed', 'Processed', 'Analysis complete'),
            ('running', 'Running', 'Currently executing'),
            ('completed', 'Completed', 'Completed successfully'),
            ('cancelled', 'Cancelled', 'Cancelled by user'),
        ]
        for value, display, desc in statuses_data:
            s = Status(status_value=value, status_display_name=display, status_description=desc)
            session.add(s)
        session.flush()
        
        # Get parent record type IDs
        request_type = session.query(ParentRecordType).filter_by(parent_record_type_value='request').first()
        email_type = session.query(ParentRecordType).filter_by(parent_record_type_value='email').first()
        doc_type = session.query(ParentRecordType).filter_by(parent_record_type_value='document').first()
        job_type = session.query(ParentRecordType).filter_by(parent_record_type_value='job').first()
        
        # Get status IDs
        status_map = {s.status_value: s.status_id for s in session.query(Status).all()}
        
        # Create status_types (mapping statuses to parent record types)
        status_types_data = [
            # Request statuses
            (status_map['pending'], request_type.parent_record_type_id, 1, False),
            (status_map['processing'], request_type.parent_record_type_id, 2, False),
            (status_map['extracted'], request_type.parent_record_type_id, 3, False),
            (status_map['reviewing'], request_type.parent_record_type_id, 4, False),
            (status_map['approved'], request_type.parent_record_type_id, 5, True),
            (status_map['rejected'], request_type.parent_record_type_id, 6, True),
            (status_map['failed'], request_type.parent_record_type_id, 7, True),
            (status_map['completed'], request_type.parent_record_type_id, 8, True),
            (status_map['cancelled'], request_type.parent_record_type_id, 9, True),
            # Email statuses
            (status_map['pending'], email_type.parent_record_type_id, 1, False),
            (status_map['processing'], email_type.parent_record_type_id, 2, False),
            (status_map['processed'], email_type.parent_record_type_id, 3, True),
            (status_map['failed'], email_type.parent_record_type_id, 4, True),
            # Document statuses
            (status_map['uploaded'], doc_type.parent_record_type_id, 1, False),
            (status_map['processing'], doc_type.parent_record_type_id, 2, False),
            (status_map['processed'], doc_type.parent_record_type_id, 3, True),
            (status_map['failed'], doc_type.parent_record_type_id, 4, True),
            # Job statuses
            (status_map['pending'], job_type.parent_record_type_id, 1, False),
            (status_map['running'], job_type.parent_record_type_id, 2, False),
            (status_map['completed'], job_type.parent_record_type_id, 3, True),
            (status_map['failed'], job_type.parent_record_type_id, 4, True),
        ]
        for status_id, parent_id, sort_order, is_terminal in status_types_data:
            st = StatusType(
                status_type_status_id=status_id,
                status_type_parent_record_type_id=parent_id,
                status_type_sort_order=sort_order,
                status_type_is_terminal=is_terminal
            )
            session.add(st)
        session.flush()
        
        # Create document types
        doc_types_data = [
            ('email_body', 'Email body content converted to document'),
            ('email_attachment', 'Email attachment file'),
            ('manual_upload', 'Manually uploaded document'),
        ]
        for value, desc in doc_types_data:
            dt = DocumentType(document_type_value=value, document_type_description=desc)
            session.add(dt)
        session.flush()
        
        # Create template field categories
        categories_data = [
            ('cat_instrument', 'instrument', 'Instrument Details', 'Fields related to bond/instrument identification'),
            ('cat_financial', 'financial', 'Financial Details', 'Fields related to financial terms and amounts'),
            ('cat_date', 'date', 'Important Dates', 'Fields related to dates and timelines'),
            ('cat_counterparty', 'counterparty', 'Contact Information', 'Fields related to parties and counterparties'),
            ('cat_coverage', 'coverage', 'Coverage Details', 'Fields related to insurance/risk coverage'),
            ('cat_other', 'other', 'Other Fields', 'Miscellaneous fields'),
        ]
        for cat_id, name, display, desc in categories_data:
            cat = TemplateFieldCategory(
                template_field_category_id=cat_id,
                template_field_category_name=name,
                template_field_category_display_name=display,
                template_field_category_description=desc,
                template_field_category_is_active=True
            )
            session.add(cat)
        session.flush()
        
        # Create default analyzer for CAT Bond documents
        analyzer = Analyzer(
            analyzer_id='anlz_catbond_001',
            analyzer_organization_id='org_xtractai',
            analyzer_name='CAT Bond Analyzer',
            analyzer_description='Azure Document Intelligence analyzer for Catastrophe Bond documents',
            analyzer_type='azure_di',
            analyzer_azure_id='catbond-analyzer-v1',
            analyzer_is_active=True
        )
        session.add(analyzer)
        session.flush()
        
        # Create default template for CAT Bond analysis (linked to analyzer)
        template = Template(
            template_organization_id='org_xtractai',
            template_analyzer_id='anlz_catbond_001',
            template_name='CAT Bond Template',
            template_internal_name='catbond-template-v1',
            template_description='Standard template for Catastrophe Bond document extraction',
            template_is_active=True
        )
        session.add(template)
        session.flush()
        
        # Create template category orders
        category_orders = [
            ('cat_date', 1),
            ('cat_instrument', 2),
            ('cat_financial', 3),
            ('cat_counterparty', 4),
            ('cat_coverage', 5),
            ('cat_other', 6),
        ]
        for cat_id, sort_order in category_orders:
            tco = TemplateCategoryOrder(
                template_category_order_id=f'tco_{template.template_id}_{cat_id}',
                template_category_order_template_id=template.template_id,
                template_category_order_category_id=cat_id,
                template_category_order_sort_order=sort_order,
                template_category_order_is_visible=True
            )
            session.add(tco)
        session.flush()
        
        # Create default template fields for CAT Bond analysis
        # Tuple: (field_name, display_name, category_id, description, data_type, is_required, sort_order, field_values, normalisation_instruction)
        template_fields_data = [
            # ── Instrument Details (cat_instrument) ──
            ('SPV', 'SPV / Issuer', 'cat_instrument',
             'The name of the Special Purpose Vehicle (Issuer) established to issue the notes',
             'text', True, 1, None,
             'Extract the full legal name of the SPV/Issuer. Remove surrounding quotes and annotations like (the "Issuer"). Example: "3264 Re Ltd." not "3264 Re Ltd. (the \\"Issuer\\")".'),
            ('BondSeries', 'Bond Series', 'cat_instrument',
             'The specific series identifier assigned to the notes (e.g., Series 2025-1). Combination of Issuer & Series name (excluding "Notes")',
             'text', True, 2, None,
             'Combine the Issuer name and Series identifier, excluding the word "Notes". Example: "3264 Re Ltd. 2025-1".'),
            ('Class', 'Class', 'cat_instrument',
             'The class or tranche identifier of the notes',
             'text', True, 3, None,
             'Extract the class/tranche letter or number. Return only the class identifier. Examples: "A", "B-1", "D-2".'),
            ('ISIN', 'ISIN', 'cat_instrument',
             'The International Securities Identification Number assigned to the specific class of notes',
             'text', False, 4, None,
             'Extract the 12-character ISIN code. Remove any spaces or dashes. Return uppercase alphanumeric only.'),
            ('CUSIP', 'CUSIP', 'cat_instrument',
             'The value of the CUSIP identifier',
             'text', False, 5, None,
             'Extract the 9-character CUSIP code. Remove any spaces or dashes. Return uppercase alphanumeric only.'),
            ('InstrumentName', 'Instrument Name', 'cat_instrument',
             'The full name or title of the instrument as stated in the offering documents',
             'text', False, 6, None,
             'Extract the full instrument name/title as it appears in the document. Remove extraneous formatting.'),
            ('Issuer', 'Issuer', 'cat_instrument',
             'The legal entity that issues the notes (may be same as SPV)',
             'text', False, 7, None,
             'Extract the full legal name of the issuing entity. Remove annotations like (the "Issuer").'),
            ('InstrumentType', 'Instrument Type', 'cat_instrument',
             'The classification of the financial instrument (e.g., Cat Bond, ILS Note)',
             'text', False, 8, None,
             'Normalise to a standard type: "Cat Bond", "ILS Note", "Catastrophe Bond", etc. Use title case.'),
            ('ContractType', 'Contract Type', 'cat_instrument',
             'The classification of the contract type',
             'text', True, 9,
             'Per Occurrence,Per Occurrence with AAD,Aggregate,Aggregate with per Occ Cap,Umbrella,Cascade,Kth Event,Pillar,Term Aggregate',
             'Map the extracted value to one of these allowed values exactly: "Per Occurrence", "Per Occurrence with AAD", "Aggregate", "Aggregate with per Occ Cap", "Umbrella", "Cascade", "Kth Event", "Pillar", "Term Aggregate". If uncertain, return the closest match.'),
            ('TriggerType', 'Trigger Type', 'cat_instrument',
             'The mechanism determining loss payout (e.g., Indemnity, Parametric, Industry Loss Index)',
             'text', False, 10, None,
             'Normalise to one of: "Indemnity", "Parametric", "Industry Loss Index", "Modeled Loss", "Hybrid". Use title case.'),
            ('LimitType', 'Limit Type', 'cat_instrument',
             'The structure of the limit (e.g., Per Occurrence, Annual Aggregate)',
             'dropdown', False, 11, None,
             'Normalise to "Per Occurrence" or "Annual Aggregate". Use title case.'),
            ('RiskCoverage', 'Risk Coverage', 'cat_instrument',
             'A summary of the risk coverage provided by the instrument',
             'text', False, 12, None,
             'Provide a concise description of the risk coverage. Example: "U.S. Hurricane and Earthquake".'),
            ('ListingExchange', 'Listing Exchange', 'cat_instrument',
             'The stock exchange where the notes are listed (e.g., Euronext Dublin, BSX)',
             'text', False, 13, None,
             'Extract the exchange name. Remove asterisks, extra spaces and quotes. Example: "Bermuda Stock Exchange" not "* Bermuda Stock Exchange".'),

            # ── Financial Details (cat_financial) ──
            ('TotalIssueSize', 'Total Issue Size', 'cat_financial',
             'The total initial principal amount of the notes offered in this issuance',
             'text', True, 1, None,
             'Extract the numeric amount with currency symbol. Format as plain number without commas. Example: "125000000" not "125,000,000". Include currency code prefix if present e.g. "USD 125000000".'),
            ('AmountOutstanding', 'Amount Outstanding', 'cat_financial',
             'The current principal amount outstanding (initially equal to Total Issue Size). Can be sourced from the Capitalization table under "As Of Issuance"',
             'text', True, 2, None,
             'Extract the numeric amount. Remove currency symbols, commas and whitespace. Return as plain number e.g. "125000000".'),
            ('CouponRate', 'Coupon Rate', 'cat_financial',
             'The interest rate spread or fixed coupon percentage payable to noteholders. Usually under Initial Interest Spread within the body of email.',
             'percentage', True, 3, None,
             'Extract the percentage value as a decimal number. Remove "%" and "per annum". If a range is given, take the minimum value. Example: extract "6.00" from "6.00% per annum". Example: extract "5.50" from "5.50% - 7.25%".'),
            ('ReferenceRate', 'Reference Rate', 'cat_financial',
             'The benchmark money market rate used to calculate floating interest (e.g., SOFR, EURIBOR, T-Bills)',
             'text', False, 4, None,
             'Normalise to standard abbreviation. "Secured Overnight Financing Rate" → "SOFR", "Euro Interbank Offered Rate" → "EURIBOR". Use uppercase.'),
            ('IssuanceCurrency', 'Issuance Currency', 'cat_financial',
             'The currency in which the notes are denominated and issued. Must be in ISO code',
             'text', True, 5, None,
             'Convert to 3-letter ISO 4217 currency code. Always return uppercase 3-letter code. Common mappings: "US Dollars"/"United States Dollar"/"$" → "USD", "Euros"/"Euro"/"€" → "EUR", "British Pounds"/"Pound Sterling"/"£" → "GBP", "Japanese Yen"/"¥" → "JPY", "Swiss Franc"/"CHF" → "CHF", "Canadian Dollar"/"C$" → "CAD", "Australian Dollar"/"A$" → "AUD", "New Zealand Dollar"/"NZ$" → "NZD", "Chinese Yuan"/"Renminbi"/"CNY" → "CNY", "Hong Kong Dollar"/"HK$" → "HKD", "Singapore Dollar"/"S$" → "SGD", "Swedish Krona" → "SEK", "Norwegian Krone" → "NOK", "Danish Krone" → "DKK", "South Korean Won"/"₩" → "KRW", "Indian Rupee"/"₹" → "INR", "Brazilian Real"/"R$" → "BRL", "Mexican Peso"/"MX$" → "MXN", "South African Rand"/"R" → "ZAR", "Turkish Lira"/"₺" → "TRY", "Russian Ruble"/"₽" → "RUB", "Polish Zloty"/"zł" → "PLN", "Thai Baht"/"฿" → "THB", "Indonesian Rupiah" → "IDR", "Malaysian Ringgit" → "MYR", "Philippine Peso" → "PHP", "Taiwan Dollar" → "TWD", "Israeli Shekel"/"₪" → "ILS", "Saudi Riyal" → "SAR", "UAE Dirham" → "AED", "Czech Koruna" → "CZK", "Hungarian Forint" → "HUF", "Romanian Leu" → "RON", "Colombian Peso" → "COP", "Chilean Peso" → "CLP", "Peruvian Sol" → "PEN", "Argentine Peso" → "ARS", "Bermudian Dollar" → "BMD", "Cayman Islands Dollar" → "KYD".'),
            ('PrincipalAmount', 'Principal Amount', 'cat_financial',
             'The face value or principal amount of the notes',
             'text', False, 6, None,
             'Extract the numeric amount. Remove currency symbols and commas. Return as plain number.'),
            ('PerOccurrenceLimit', 'Per Occurrence Limit', 'cat_financial',
             'The maximum payout limit for a single loss occurrence event',
             'text', False, 7, None,
             'Extract the numeric amount. Remove currency symbols and commas. Return as plain number. Per occurrence limit should match contract type unless specifically given.'),
            ('PerOccurrenceAttachment', 'Per Occurrence Attachment', 'cat_financial',
             'The attachment point or deductible amount that must be reached before the bond principal is at risk for a single occurrence',
             'text', False, 8, None,
             'Extract the numeric amount. Remove currency symbols and commas. Return as plain number.'),
            ('ExpectedLoss', 'Expected Loss', 'cat_financial',
             'The modeled expected loss expressed as a percentage of the principal',
             'percentage', False, 9, None,
             'Extract the percentage value as a decimal number. Remove "%" symbol. Example: extract "2.45" from "2.45%".'),

            # ── Important Dates (cat_date) ──
            ('IssuanceDate', 'Issuance Date', 'cat_date',
             'The date on which the notes are issued and settlement occurs (Closing Date)',
             'date', False, 1, None,
             'Convert to YYYY-MM-DD format. May appear as "Closing Date" or "Settlement Date".'),
            ('MaturityDate', 'Maturity Date', 'cat_date',
             'The final legal maturity date of the notes, including any potential extension periods. Usually given as "Redemption Date"',
             'date', True, 2, None,
             'Convert to YYYY-MM-DD format. This is the final/legal maturity, may differ from Scheduled Maturity Date.'),
            ('ScheduledMaturityDate', 'Scheduled Maturity Date', 'cat_date',
             'The date on which the notes are expected to be redeemed, absent any extension events',
             'date', True, 3, None,
             'Convert to YYYY-MM-DD format. May appear as "Scheduled Redemption Date". Example: "January 8, 2029" → "2029-01-08".'),
            ('OnRiskDate', 'On Risk Date', 'cat_date',
             'The effective date or inception date from which the risk coverage period begins',
             'date', False, 4, None,
             'Convert to YYYY-MM-DD format. Parse from "Risk Period: The period commencing at..." text. Example: "January 1, 2026" → "2026-01-01".'),
            ('OffRiskDate', 'Off Risk Date', 'cat_date',
             'The date on which the risk coverage period terminates',
             'date', True, 5, None,
             'Convert to YYYY-MM-DD format. Parse from "Risk Period: ... up to and including..." text. Example: "December 31, 2028" → "2028-12-31".'),
            ('SettlementDate', 'Settlement Date', 'cat_date',
             'The date on which the transaction settles and funds are exchanged',
             'date', False, 6, None,
             'Convert to YYYY-MM-DD format. May appear as "Closing Date" or "Settlement Date".'),
            ('CoverageStartDate', 'Coverage Start Date', 'cat_date',
             'The start date of the coverage or risk period',
             'date', False, 7, None,
             'Convert to YYYY-MM-DD format. May also be referred to as "Inception Date" or "Effective Date".'),
            ('CoverageEndDate', 'Coverage End Date', 'cat_date',
             'The end date of the coverage or risk period',
             'date', False, 8, None,
             'Convert to YYYY-MM-DD format. May also be referred to as "Expiry Date" or "Termination Date".'),

            # ── Contact Information (cat_counterparty) ──
            ('Sponsors', 'Sponsors', 'cat_counterparty',
             'The entity transferring the risk (Ceding Insurer/Reinsurer) or the beneficiary of the coverage',
             'text', False, 1, None,
             'Extract the full legal name of the sponsor/cedent. Remove annotations like (the "Sponsor").'),
            ('LeadManagers', 'Lead Managers', 'cat_counterparty',
             'The investment banks or firms acting as lead managers, bookrunners, or structuring agents',
             'text', False, 2, None,
             'Extract all lead manager names. Separate multiple names with semicolons. Example: "Aon Securities; GC Securities".'),
            ('Structurers', 'Structurers', 'cat_counterparty',
             'The firms responsible for structuring the transaction and risk modeling',
             'text', False, 3, None,
             'Extract all structurer names. Separate multiple names with semicolons.'),
            ('Trustee', 'Trustee', 'cat_counterparty',
             'The financial institution acting as the Indenture Trustee or Reinsurance Trustee',
             'text', False, 4, None,
             'Extract the full legal name of the trustee. Remove role descriptions like "as Indenture Trustee".'),
            ('SPVAdministrator', 'SPV Administrator', 'cat_counterparty',
             'The corporate services provider managing the administration of the SPV',
             'text', False, 5, None,
             'Extract the full legal name of the administrator.'),
            ('ClearingHouse', 'Clearing House', 'cat_counterparty',
             'The entity responsible for clearing and settlement of the notes (e.g., Euroclear, Clearstream, DTC)',
             'text', False, 6, None,
             'Extract the clearing house name. If multiple, separate with semicolons. Example: "DTC; Euroclear; Clearstream".'),
            ('LegalCounsel', 'Legal Counsel', 'cat_counterparty',
             'The law firms providing legal advice on the transaction',
             'text', False, 7, None,
             'Extract all legal counsel names. Separate multiple names with semicolons.'),
            ('ModelingFirm', 'Modeling Firm', 'cat_counterparty',
             'The firms responsible for catastrophe risk modeling and analysis',
             'text', False, 8, None,
             'Extract all modeling firm names. Separate multiple names with semicolons. Example: "AIR Worldwide; RMS".'),

            # ── Coverage Details (cat_coverage) ──
            ('CoveredPeril', 'Covered Peril', 'cat_coverage',
             'The specific natural catastrophe perils covered by the instrument',
             'text', False, 1, None,
             'Extract the peril types. Normalise to standard names: "Hurricane", "Earthquake", "Flood", "Wildfire", "Severe Storm". Separate multiple with semicolons.'),
            ('CoveredRegion', 'Covered Region', 'cat_coverage',
             'The geographic regions where covered perils apply',
             'text', False, 2, None,
             'Extract the geographic regions. Use standard names: "United States", "Japan", "Europe", etc. Separate multiple with semicolons.'),
            ('AnnualAggregateLimit', 'Annual Aggregate Limit', 'cat_coverage',
             'The maximum total payout across all events in a single annual period',
             'text', False, 3, None,
             'Extract the numeric amount. Remove currency symbols and commas. Return as plain number.'),
            ('AnnualAggregateAttachment', 'Annual Aggregate Attachment', 'cat_coverage',
             'The aggregate attachment point that must be reached before aggregate losses trigger payouts',
             'text', False, 4, None,
             'Extract the numeric amount. Remove currency symbols and commas. Return as plain number.'),
            ('ExposureDescription', 'Exposure Description', 'cat_coverage',
             'A description of the covered perils and geographic regions (e.g., U.S. Named Storm, Japan Earthquake)',
             'text', False, 5, None,
             'Provide a concise summary of covered perils and regions. Example: "U.S. Named Storm and Earthquake".'),
            ('ExposureLineOfBusinessSplit', 'Exposure Line of Business Split', 'cat_coverage',
             'The breakdown of the subject business portfolio by line of business (e.g., Residential, Commercial, Industrial percentages)',
             'text', False, 6, None,
             'Extract the percentage breakdown. Format as "Residential: 60%, Commercial: 30%, Industrial: 10%". Use consistent formatting.'),
        ]
        for item in template_fields_data:
            field_name, display_name, category_id, definition, data_type, is_required, sort_order = item[0:7]
            field_values = item[7] if len(item) > 7 else None
            norm_instruction = item[8] if len(item) > 8 else None
            tf = TemplateField(
                template_field_template_id=template.template_id,
                template_field_category_id=category_id,
                template_field_field_name=field_name,
                template_field_display_name=display_name,
                template_field_field_definition=definition,
                template_field_data_type=data_type,
                template_field_field_values=field_values,
                template_field_is_required=is_required,
                template_field_extraction_is_required=is_required,
                template_field_is_active=True,
                template_field_sort_order=sort_order,
                template_field_precision_threshold=0.60,
                template_field_normalisation_instruction=norm_instruction
            )
            session.add(tf)
        session.flush()
        
        session.commit()
        logger.info("Seed data created successfully (with default template)")
    except Exception as e:
        session.rollback()
        logger.warning(f"Error seeding data (may already exist): {e}")
    finally:
        session.close()


class DatabaseRepository:
    """Repository for all database operations - SINGLE SQL DATABASE"""
    
    def __init__(self):
        global _engine, _SessionLocal
        
        if _engine is None:
            # Fallback to environment variable
            connection_string = os.getenv('MSSQL_URI')
            if connection_string:
                init_database(connection_string)
            else:
                raise ValueError("Database not initialized. Call init_database first or set MSSQL_URI")
        
        self.engine = _engine
        self.SessionLocal = _SessionLocal
        logger.info("Database repository initialized")
    
    @contextmanager
    def get_session(self) -> Session:
        """Get a database session with proper cleanup"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    def get_system_health(self) -> Dict[str, Any]:
        """Get system health metrics with real timing data."""
        import time

        # -- Database latency --
        db_status = 'connected'
        db_latency_ms = 0
        try:
            with self.get_session() as session:
                t0 = time.perf_counter()
                session.execute(text("SELECT 1"))
                db_latency_ms = round((time.perf_counter() - t0) * 1000)
        except Exception as e:
            logger.error(f"Health: DB check failed: {e}")
            db_status = 'disconnected'
            db_latency_ms = -1

        # -- Job queue stats --
        queue_status = 'normal'
        queue_pending = 0
        queue_running = 0
        queue_failed = 0
        try:
            job_stats = self.get_job_stats()
            queue_pending = job_stats.get('pending', 0)
            queue_running = job_stats.get('running', 0)
            queue_failed = job_stats.get('failed', 0)
            if queue_failed > 5:
                queue_status = 'degraded'
            elif queue_pending > 50:
                queue_status = 'busy'
        except Exception as e:
            logger.error(f"Health: job stats failed: {e}")
            queue_status = 'unknown'

        # -- Connection pool --
        pool_size = 0
        pool_checked_out = 0
        try:
            pool_size = self.engine.pool.size()
            pool_checked_out = self.engine.pool.checkedout()
        except Exception:
            pass

        # -- Overall status --
        if db_status == 'disconnected':
            overall = 'unhealthy'
        elif queue_status == 'degraded' or db_latency_ms > 500:
            overall = 'degraded'
        else:
            overall = 'healthy'

        return {
            'overall': overall,
            'database': {
                'status': db_status,
                'latency_ms': db_latency_ms,
            },
            'queue': {
                'status': queue_status,
                'pending': queue_pending,
                'running': queue_running,
                'failed': queue_failed,
            },
            'pool': {
                'size': pool_size,
                'in_use': pool_checked_out,
            },
        }
    
    # ==========================================
    # STATUS OPERATIONS
    # ==========================================
    
    def get_status_by_value(self, status_value: str) -> Optional[Status]:
        """Get status by value"""
        with self.get_session() as session:
            return session.query(Status).filter_by(status_value=status_value).first()
    
    def get_status_type_for_entity(self, status_value: str, parent_record_type_value: str) -> Optional[StatusType]:
        """Get status_type by status value and parent record type value"""
        with self.get_session() as session:
            return self._get_status_type_in_session(session, status_value, parent_record_type_value)
    
    def _get_status_type_in_session(self, session: Session, status_value: str, parent_record_type_value: str) -> Optional[StatusType]:
        """Get status_type within an existing session context"""
        status = session.query(Status).filter_by(status_value=status_value).first()
        parent_type = session.query(ParentRecordType).filter_by(parent_record_type_value=parent_record_type_value).first()
        if status and parent_type:
            return session.query(StatusType).filter_by(
                status_type_status_id=status.status_id,
                status_type_parent_record_type_id=parent_type.parent_record_type_id
            ).first()
        return None
    
    def _get_status_type_id_in_session(self, session: Session, status_value: str, parent_record_type_value: str) -> Optional[int]:
        """Get status_type_id within an existing session context - returns just the ID"""
        status_type = self._get_status_type_in_session(session, status_value, parent_record_type_value)
        return status_type.status_type_id if status_type else None
    
    # ==========================================
    # ORGANIZATION OPERATIONS
    # ==========================================
    
    def get_organization(self, org_id: str) -> Optional[Dict[str, Any]]:
        """Get organization by ID"""
        with self.get_session() as session:
            org = session.query(Organization).filter_by(organization_id=org_id).first()
            if org:
                return self._org_to_dict(org)
            return None
    
    def get_organization_by_tenant_id(self, azure_tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get organization by Azure AD tenant ID"""
        with self.get_session() as session:
            org = session.query(Organization).filter_by(
                organization_azure_tenant_id=azure_tenant_id
            ).first()
            if org:
                return self._org_to_dict(org)
            return None
    
    def create_organization(self, org_id: str, name: str, azure_tenant_id: str,
                            tier: str = 'free_trial') -> Dict[str, Any]:
        """Create a new organization linked to an Azure AD tenant"""
        with self.get_session() as session:
            org = Organization(
                organization_id=org_id,
                organization_name=name,
                organization_azure_tenant_id=azure_tenant_id,
                organization_tier=tier,
                organization_settings=json.dumps({'features': ['document_analysis']})
            )
            session.add(org)
            session.flush()
            return self._org_to_dict(org)
    
    def update_organization_settings(self, org_id: str, settings: dict) -> bool:
        """Update organization_settings JSON for an org."""
        with self.get_session() as session:
            org = session.query(Organization).filter_by(
                organization_id=org_id
            ).first()
            if not org:
                return False
            org.organization_settings = json.dumps(settings)
            return True
    
    def _org_to_dict(self, org: Organization) -> Dict[str, Any]:
        return {
            'id': org.organization_id,
            'organization_id': org.organization_id,
            'name': org.organization_name,
            'azure_tenant_id': org.organization_azure_tenant_id,
            'tier': org.organization_tier,
            'settings': json.loads(org.organization_settings) if org.organization_settings else {},
            'created_at': org.organization_created_at.isoformat() if org.organization_created_at else None,
            'updated_at': org.organization_modified_at.isoformat() if org.organization_modified_at else None
        }
    
    # ==========================================
    # SUBSCRIPTION OPERATIONS
    # ==========================================
    
    def get_active_subscription(self, org_id: str) -> Optional[Dict[str, Any]]:
        """Get the active subscription for an organization"""
        with self.get_session() as session:
            sub = session.query(Subscription).filter_by(
                subscription_organization_id=org_id,
                subscription_status='active'
            ).first()
            if sub:
                return self._subscription_to_dict(sub)
            return None
    
    def get_subscription_by_marketplace_id(self, marketplace_id: str) -> Optional[Dict[str, Any]]:
        """Get subscription by marketplace ID"""
        with self.get_session() as session:
            sub = session.query(Subscription).filter_by(
                subscription_marketplace_id=marketplace_id
            ).first()
            if sub:
                return self._subscription_to_dict(sub)
            return None
    
    def create_subscription(self, sub_id: str, org_id: str, plan: str = 'free_trial',
                            marketplace_id: str = None) -> Dict[str, Any]:
        """Create a new subscription for an organization"""
        with self.get_session() as session:
            sub = Subscription(
                subscription_id=sub_id,
                subscription_organization_id=org_id,
                subscription_plan=plan,
                subscription_status='active',
                subscription_marketplace_id=marketplace_id
            )
            session.add(sub)
            session.flush()
            return self._subscription_to_dict(sub)
    
    def update_subscription_status(self, sub_id: str, status: str, plan: str = None) -> bool:
        """Update subscription status (active, suspended, cancelled, expired) and optionally plan"""
        with self.get_session() as session:
            sub = session.query(Subscription).filter_by(subscription_id=sub_id).first()
            if sub:
                sub.subscription_status = status
                if plan:
                    sub.subscription_plan = plan
                if status == 'cancelled':
                    sub.subscription_cancelled_at = datetime.utcnow()
                sub.subscription_modified_at = datetime.utcnow()
                return True
            return False
    
    def _subscription_to_dict(self, sub: Subscription) -> Dict[str, Any]:
        return {
            'id': sub.subscription_id,
            'subscription_id': sub.subscription_id,
            'organization_id': sub.subscription_organization_id,
            'plan': sub.subscription_plan,
            'status': sub.subscription_status,
            'marketplace_id': sub.subscription_marketplace_id,
            'started_at': sub.subscription_started_at.isoformat() if sub.subscription_started_at else None,
            'expires_at': sub.subscription_expires_at.isoformat() if sub.subscription_expires_at else None,
            'cancelled_at': sub.subscription_cancelled_at.isoformat() if sub.subscription_cancelled_at else None,
            'created_at': sub.subscription_created_at.isoformat() if sub.subscription_created_at else None,
        }
    
    # ==========================================
    # USER OPERATIONS
    # ==========================================
    
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        with self.get_session() as session:
            user = session.query(User).filter_by(user_id=user_id).first()
            if user:
                return self._user_to_dict(user)
            return None
    
    def get_user_by_email(self, email: str, org_id: str = None) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        with self.get_session() as session:
            query = session.query(User).filter_by(user_email=email)
            if org_id:
                query = query.filter_by(user_organization_id=org_id)
            user = query.first()
            if user:
                return self._user_to_dict(user)
            return None
    
    def get_user_by_auth0_id(self, auth0_id: str) -> Optional[Dict[str, Any]]:
        """Get user by Auth0 ID"""
        with self.get_session() as session:
            user = session.query(User).filter_by(user_auth0_id=auth0_id).first()
            if user:
                return self._user_to_dict(user)
            return None
    
    def create_user(self, user_id: str, org_id: str, email: str, username: str,
                    auth0_id: str = None, role: str = 'user') -> Dict[str, Any]:
        """Create a new user"""
        with self.get_session() as session:
            user = User(
                user_id=user_id,
                user_organization_id=org_id,
                user_email=email,
                user_username=username,
                user_auth0_id=auth0_id,
                user_role_types=role,
                user_is_active=True
            )
            session.add(user)
            session.flush()
            return self._user_to_dict(user)
    
    def update_user_last_login(self, user_id: str) -> bool:
        """Update user's last login timestamp"""
        with self.get_session() as session:
            user = session.query(User).filter_by(user_id=user_id).first()
            if user:
                user.user_last_login = datetime.utcnow()
                return True
            return False
    
    def _user_to_dict(self, user: User) -> Dict[str, Any]:
        return {
            'id': user.user_id,
            'organization_id': user.user_organization_id,
            'auth0_id': user.user_auth0_id,
            'email': user.user_email,
            'username': user.user_username,
            'role': user.user_role_types,
            'is_active': user.user_is_active,
            'last_login': user.user_last_login.isoformat() if user.user_last_login else None,
            'created_at': user.user_created_at.isoformat() if user.user_created_at else None
        }
    
    # ==========================================
    # EMAIL OPERATIONS
    # ==========================================
    
    def create_email(self, email_id: str, org_id: str, subject: str, body: str,
                     sender: str = None, ingested_by: str = None,
                     mailbox_email: str = None, is_shared_mailbox: bool = False) -> Dict[str, Any]:
        """Create a new email record"""
        with self.get_session() as session:
            # Get the pending status_type for emails (using session-aware helper)
            status_type_id = self._get_status_type_id_in_session(session, 'pending', 'email') or 1
            
            email = Email(
                email_organization_id=org_id,
                email_subject=subject,
                email_body_text=body,
                email_sender=sender,
                email_mailbox_email=mailbox_email,
                email_is_shared_mailbox=is_shared_mailbox,
                email_status_type_id=status_type_id,
                email_created_by=ingested_by,
                email_received_at=datetime.utcnow()
            )
            session.add(email)
            session.flush()
            
            result = self._email_to_dict_internal(session, email)
            return result
    
    def get_email(self, email_id) -> Optional[Dict[str, Any]]:
        """Get email with attached documents"""
        with self.get_session() as session:
            email = session.query(Email).filter_by(email_id=int(email_id) if isinstance(email_id, str) and email_id.isdigit() else email_id).first()
            if not email:
                return None
            
            result = self._email_to_dict_internal(session, email)
            
            # Get attached documents via documents relationship
            docs = []
            for doc in email.documents:
                docs.append({
                    'id': doc.document_id,
                    'filename': doc.document_file_name,
                    'blob_url': doc.document_file_path,
                    'content_type': doc.document_content_type,
                    'file_size_bytes': doc.document_file_size_bytes,
                    'status': self._get_status_value_from_type(session, doc.document_status_type_id),
                })
            result['documents'] = docs
            
            # Get linked requests via emailrequests junction table
            requests_list = []
            for er in email.email_requests:
                req = er.request
                if req:
                    status_value = self._get_status_value_from_type(session, req.request_status_type_id)
                    requests_list.append({
                        'id': req.request_id,
                        'title': req.request_title,
                        'description': req.request_description,
                        'issuer': req.request_issuer,
                        'status': status_value,
                        'status_display': status_value.replace('_', ' ').title() if status_value else None,
                        'created_at': req.request_created_at.isoformat() if req.request_created_at else None,
                        'updated_at': req.request_modified_at.isoformat() if req.request_modified_at else None,
                    })
            result['requests'] = requests_list
            
            return result
    
    def _get_status_value_from_type(self, session, status_type_id: int) -> Optional[str]:
        """Get status value from status_type_id"""
        if not status_type_id:
            return None
        st = session.query(StatusType).filter_by(status_type_id=status_type_id).first()
        if st and st.status:
            return st.status.status_value
        return None

    @staticmethod
    def _safe_get_extraction_prompt(req) -> Optional[str]:
        """Safely get extraction_prompt — returns None if column doesn't exist in DB yet."""
        try:
            return req.request_extraction_prompt
        except Exception:
            return None
    
    def _build_email_query(self, session, org_id, status_name=None, search=None, date_from=None):
        """Build base email query with filters (shared by list and stats)"""
        query = session.query(Email).filter_by(email_organization_id=org_id)

        if status_name:
            status_type_id = self._get_status_type_id_in_session(session, status_name, 'email')
            if status_type_id:
                query = query.filter_by(email_status_type_id=status_type_id)

        if search:
            query = query.filter(
                or_(
                    Email.email_subject.ilike(f'%{search}%'),
                    Email.email_sender.ilike(f'%{search}%')
                )
            )

        if date_from:
            query = query.filter(Email.email_received_at >= date_from)

        return query

    def list_emails(self, org_id: str, status_name: str = None, search: str = None,
                    date_from: datetime = None, page: int = 1, per_page: int = 25) -> Tuple[List[Dict[str, Any]], int]:
        """List emails with pagination"""
        with self.get_session() as session:
            query = self._build_email_query(session, org_id, status_name, search, date_from)
            
            total = query.count()
            emails = query.order_by(desc(Email.email_received_at))\
                .offset((page - 1) * per_page).limit(per_page).all()
            
            result = []
            for email in emails:
                email_dict = self._email_to_dict_internal(session, email)
                # Use explicit COUNT query - lazy-loaded relationship may not resolve
                doc_count = session.query(func.count(Document.document_id)).filter(
                    Document.document_email_id == email.email_id
                ).scalar() or 0
                email_dict['document_count'] = doc_count
                result.append(email_dict)
            
            return result, total

    def get_email_stats(self, org_id: str, search: str = None, date_from: datetime = None) -> Dict[str, int]:
        """Get email counts grouped by status"""
        with self.get_session() as session:
            query = self._build_email_query(session, org_id, search=search, date_from=date_from)
            total = query.count()

            stats = {'total': total, 'pending': 0, 'processing': 0, 'processed': 0, 'failed': 0}
            for status_name in ['pending', 'processing', 'processed', 'failed']:
                stid = self._get_status_type_id_in_session(session, status_name, 'email')
                if stid:
                    stats[status_name] = query.filter(Email.email_status_type_id == stid).count()
            return stats
    
    def update_email_status(self, email_id, status_name: str, user_id: str = None) -> bool:
        """Update email status"""
        with self.get_session() as session:
            email = session.query(Email).filter_by(email_id=int(email_id) if isinstance(email_id, str) and email_id.isdigit() else email_id).first()
            if email:
                status_type_id = self._get_status_type_id_in_session(session, status_name, 'email')
                if status_type_id:
                    email.email_status_type_id = status_type_id
                    email.email_modified_at = datetime.utcnow()
                    return True
            return False
    
    def delete_email(self, email_id, user_id: str = None) -> bool:
        """Delete an email"""
        with self.get_session() as session:
            email = session.query(Email).filter_by(email_id=int(email_id) if isinstance(email_id, str) and email_id.isdigit() else email_id).first()
            if email:
                session.delete(email)
                return True
            return False
    
    def _email_to_dict_internal(self, session: Session, email: Email) -> Dict[str, Any]:
        status_value = self._get_status_value_from_type(session, email.email_status_type_id)
        return {
            'id': email.email_id,
            'organization_id': email.email_organization_id,
            'subject': email.email_subject,
            'body': email.email_body_text,
            'sender': email.email_sender,
            'recipients': email.email_recipients,
            'status': status_value,
            'status_display': status_value.replace('_', ' ').title() if status_value else None,
            'mailbox_email': getattr(email, 'email_mailbox_email', None),
            'is_shared_mailbox': getattr(email, 'email_is_shared_mailbox', False) or False,
            'ingested_by': email.email_created_by,
            'received_at': email.email_received_at.isoformat() if email.email_received_at else None,
            'created_at': email.email_created_at.isoformat() if email.email_created_at else None,
            'updated_at': email.email_modified_at.isoformat() if email.email_modified_at else None
        }
    
    # ==========================================
    # DOCUMENT OPERATIONS
    # ==========================================
    
    def create_document(self, doc_id: str, org_id: str, filename: str, blob_url: str,
                        content_type: str = None, file_size_bytes: int = None,
                        uploaded_by: str = None, document_type: str = 'manual_upload',
                        email_id: int = None, request_id: int = None) -> Dict[str, Any]:
        """Create a new document record with direct link to request"""
        with self.get_session() as session:
            # Get status_type for uploaded document (using session-aware helper)
            status_type_id = self._get_status_type_id_in_session(session, 'uploaded', 'document') or 1
            
            # Get document type id
            doc_type = session.query(DocumentType).filter_by(document_type_value=document_type).first()
            doc_type_id = doc_type.document_type_id if doc_type else 1
            
            # Convert request_id to int if needed
            req_id_int = None
            if request_id:
                req_id_int = int(request_id) if isinstance(request_id, str) and str(request_id).isdigit() else request_id
            
            document = Document(
                document_organization_id=org_id,
                document_request_id=req_id_int,  # Direct link to request
                document_email_id=email_id,
                document_document_type_id=doc_type_id,
                document_file_name=filename,
                document_file_path=blob_url,
                document_content_type=content_type,
                document_file_size_bytes=file_size_bytes,
                document_status_type_id=status_type_id,
                document_created_by=uploaded_by
            )
            session.add(document)
            session.flush()
            
            return self._doc_to_dict_internal(session, document)
    
    def get_document(self, doc_id) -> Optional[Dict[str, Any]]:
        """Get document by ID"""
        with self.get_session() as session:
            doc = session.query(Document).filter_by(document_id=int(doc_id) if isinstance(doc_id, str) and doc_id.isdigit() else doc_id).first()
            if doc:
                return self._doc_to_dict_internal(session, doc)
            return None
    
    def list_documents(self, org_id: str, search: str = None, source: str = None,
                       page: int = 1, per_page: int = 25) -> Tuple[List[Dict[str, Any]], int]:
        """List documents with pagination - deduplicated by filename (latest per name)"""
        with self.get_session() as session:
            # Subquery: pick the max document_id per unique filename for this org
            base_filter = [Document.document_organization_id == org_id]
            if search:
                base_filter.append(Document.document_file_name.ilike(f'%{search}%'))
            
            latest_per_name = session.query(
                func.max(Document.document_id).label('max_id')
            ).filter(
                *base_filter
            ).group_by(Document.document_file_name).subquery()
            
            query = session.query(Document).filter(
                Document.document_id.in_(
                    session.query(latest_per_name.c.max_id)
                )
            )
            
            total = query.count()
            documents = query.order_by(desc(Document.document_created_at))\
                .offset((page - 1) * per_page).limit(per_page).all()
            
            result = [self._doc_to_dict_internal(session, doc) for doc in documents]
            return result, total
    
    def update_document(self, doc_id, blob_url: str = None, status_name: str = None,
                        current_version_id: int = None, user_id: str = None) -> bool:
        """Update document"""
        with self.get_session() as session:
            doc = session.query(Document).filter_by(document_id=int(doc_id) if isinstance(doc_id, str) and doc_id.isdigit() else doc_id).first()
            if doc:
                if blob_url:
                    doc.document_file_path = blob_url
                if status_name:
                    status_type_id = self._get_status_type_id_in_session(session, status_name, 'document')
                    if status_type_id:
                        doc.document_status_type_id = status_type_id
                if current_version_id:
                    doc.document_current_version_id = current_version_id
                
                doc.document_modified_at = datetime.utcnow()
                return True
            return False
    
    def delete_document(self, doc_id, user_id: str = None) -> bool:
        """Delete a document"""
        with self.get_session() as session:
            doc = session.query(Document).filter_by(document_id=int(doc_id) if isinstance(doc_id, str) and doc_id.isdigit() else doc_id).first()
            if doc:
                session.delete(doc)
                return True
            return False
    
    def link_email_document(self, email_id, doc_id, attachment_order: int = 0) -> bool:
        """Link a document to an email by updating document's email_id"""
        with self.get_session() as session:
            doc = session.query(Document).filter_by(document_id=int(doc_id) if isinstance(doc_id, str) and doc_id.isdigit() else doc_id).first()
            if doc:
                doc.document_email_id = int(email_id) if isinstance(email_id, str) and email_id.isdigit() else email_id
                return True
            return False
    
    def _doc_to_dict_internal(self, session: Session, doc: Document) -> Dict[str, Any]:
        status_value = self._get_status_value_from_type(session, doc.document_status_type_id)
        doc_type = session.query(DocumentType).filter_by(document_type_id=doc.document_document_type_id).first()
        return {
            'id': doc.document_id,
            'organization_id': doc.document_organization_id,
            'request_id': doc.document_request_id,  # Direct link to request
            'email_id': doc.document_email_id,
            'filename': doc.document_file_name,
            'blob_url': doc.document_file_path,
            'content_type': doc.document_content_type,
            'file_size_bytes': doc.document_file_size_bytes,
            'document_type': doc_type.document_type_value if doc_type else None,
            'status': status_value,
            'status_display': status_value.replace('_', ' ').title() if status_value else None,
            'current_version_id': doc.document_current_version_id,
            'uploaded_by': doc.document_created_by,
            'created_at': doc.document_created_at.isoformat() if doc.document_created_at else None,
            'updated_at': doc.document_modified_at.isoformat() if doc.document_modified_at else None
        }
    
    # ==========================================
    # REQUEST OPERATIONS (PRIMARY AGGREGATE)
    # ==========================================
    
    def create_request(self, request_id: str, org_id: str, title: str,
                       email_id = None, template_id = None, description: str = None,
                       created_by: str = None, extraction_prompt: str = None) -> Dict[str, Any]:
        """Create a new request (primary aggregate root)"""
        with self.get_session() as session:
            # Get status_type for pending request (using session-aware helper)
            status_type_id = self._get_status_type_id_in_session(session, 'pending', 'request') or 1
            
            req = Request(
                request_organization_id=org_id,
                request_template_id=int(template_id) if template_id and str(template_id).isdigit() else None,
                request_title=title,
                request_description=description,
                request_status_type_id=status_type_id,
                request_created_by=created_by
            )
            session.add(req)
            session.flush()
            
            # Set extraction_prompt separately — column may not exist yet
            if extraction_prompt:
                try:
                    req.request_extraction_prompt = extraction_prompt
                    session.flush()
                except Exception:
                    pass  # Column doesn't exist in DB yet — skip silently
            
            # Create initial version
            version = RequestVersion(
                request_version_request_id=req.request_id,
                request_version_number=1,
                request_version_label='Initial',
                request_version_created_by=created_by
            )
            session.add(version)
            session.flush()
            
            req.request_current_version_id = version.request_version_id
            
            # Link email if provided (via emailrequests junction)
            if email_id:
                email_req = EmailRequest(
                    emailrequest_request_id=req.request_id,
                    emailrequest_email_id=int(email_id) if isinstance(email_id, str) and email_id.isdigit() else email_id
                )
                session.add(email_req)
            
            return self._request_to_dict_internal(session, req)
    
    def get_request(self, request_id) -> Optional[Dict[str, Any]]:
        """Get request with all related data"""
        with self.get_session() as session:
            req = session.query(Request).filter_by(request_id=int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id).first()
            if not req:
                return None
            
            result = self._request_to_dict_internal(session, req)
            
            # Get documents - merge from both direct link and junction table
            docs = []
            seen_doc_ids = set()
            
            # 1. Get documents via direct document_request_id (new approach)
            direct_docs = session.query(Document).filter_by(document_request_id=req.request_id).all()
            for doc in direct_docs:
                if doc.document_id not in seen_doc_ids:
                    seen_doc_ids.add(doc.document_id)
                    doc_type = session.query(DocumentType).filter_by(document_type_id=doc.document_document_type_id).first()
                    docs.append({
                        'id': doc.document_id,
                        'filename': doc.document_file_name,
                        'blob_url': doc.document_file_path,
                        'content_type': doc.document_content_type,
                        'file_size_bytes': doc.document_file_size_bytes,
                        'status': self._get_status_value_from_type(session, doc.document_status_type_id),
                        'source_type': doc_type.document_type_value if doc_type else 'attachment'
                    })
            
            # 2. Get documents via request_documents junction (legacy, for backward compatibility)
            for rd in req.request_documents:
                doc = rd.document
                if doc and doc.document_id not in seen_doc_ids:
                    seen_doc_ids.add(doc.document_id)
                    docs.append({
                        'id': doc.document_id,
                        'filename': doc.document_file_name,
                        'blob_url': doc.document_file_path,
                        'content_type': doc.document_content_type,
                        'file_size_bytes': doc.document_file_size_bytes,
                        'status': self._get_status_value_from_type(session, doc.document_status_type_id),
                        'source_type': rd.request_document_source_type
                    })
            result['documents'] = docs
            
            # Get email if linked via email_requests
            if req.email_requests:
                email_req = req.email_requests[0]
                email = email_req.email
                if email:
                    result['email'] = {
                        'id': email.email_id,
                        'subject': email.email_subject,
                        'sender': email.email_sender,
                        'received_at': email.email_received_at.isoformat() if email.email_received_at else None
                    }
            
            # Get template info if linked - include full template with fields
            if req.request_template_id and req.template:
                result['template'] = self._template_to_dict(session, req.template, include_fields=True)
            else:
                result['template'] = None
            
            # Get current version fields - only active fields
            if req.request_current_version_id:
                fields = session.query(RequestField).options(
                    joinedload(RequestField.source_document)
                ).filter_by(
                    requestfield_request_version_id=req.request_current_version_id,
                    requestfield_is_active=True
                ).all()
                
                # Get alternative counts in a SINGLE grouped query instead of N+1
                alt_counts_query = session.query(
                    RequestField.requestfield_field_name,
                    func.count(RequestField.requestfield_id)
                ).filter(
                    RequestField.requestfield_request_id == req.request_id,
                    RequestField.requestfield_request_version_id == req.request_current_version_id,
                    RequestField.requestfield_is_active == False
                ).group_by(RequestField.requestfield_field_name).all()
                alt_counts_map = {name: count for name, count in alt_counts_query}
                
                # Build fields list with alternative counts
                fields_list = []
                for f in fields:
                    field_dict = self._field_to_dict(f)
                    alt_count = alt_counts_map.get(f.requestfield_field_name, 0)
                    field_dict['alternative_count'] = alt_count
                    field_dict['has_alternatives'] = alt_count > 0
                    fields_list.append(field_dict)
                
                result['fields'] = fields_list
            else:
                result['fields'] = []
            
            # Check for active (pending/running) extraction job for this request
            pending_job_status = self._get_status_type_id_in_session(session, 'pending', 'job')
            running_job_status = self._get_status_type_id_in_session(session, 'running', 'job')
            active_statuses = [s for s in [pending_job_status, running_job_status] if s]
            
            active_job = None
            if active_statuses:
                job = session.query(AsyncJob).filter(
                    AsyncJob.async_job_entity_id == req.request_id,
                    AsyncJob.async_job_entity_type == 'request',
                    AsyncJob.async_job_status_type_id.in_(active_statuses)
                ).order_by(AsyncJob.async_job_created_at.desc()).first()
                if job:
                    active_job = {
                        'id': job.async_job_id,
                        'status': self._get_status_value_from_type(session, job.async_job_status_type_id),
                        'progress_percent': job.async_job_progress_percent,
                        'progress_message': job.async_job_progress_message,
                    }
            result['active_job'] = active_job
            
            # Also return the most recent job (completed or failed) so frontend can access logs
            last_job_obj = session.query(AsyncJob).filter(
                AsyncJob.async_job_entity_id == req.request_id,
                AsyncJob.async_job_entity_type == 'request',
            ).order_by(AsyncJob.async_job_created_at.desc()).first()
            if last_job_obj:
                import json as _json
                result_data_raw = last_job_obj.async_job_result_data
                result['last_job'] = {
                    'id': last_job_obj.async_job_id,
                    'status': self._get_status_value_from_type(session, last_job_obj.async_job_status_type_id),
                    'progress_percent': last_job_obj.async_job_progress_percent,
                    'progress_message': last_job_obj.async_job_progress_message,
                    'error_message': last_job_obj.async_job_error_message,
                    'started_at': last_job_obj.async_job_started_at.isoformat() if last_job_obj.async_job_started_at else None,
                    'completed_at': last_job_obj.async_job_completed_at.isoformat() if last_job_obj.async_job_completed_at else None,
                    'result_data': _json.loads(result_data_raw) if result_data_raw else None,
                }
            else:
                result['last_job'] = None
            
            return result
    
    def list_requests(self, org_id, status_name: str = None, search: str = None,
                      issuer: str = None, date_from: str = None, date_to: str = None,
                      email_id: int = None,
                      page: int = 1, per_page: int = 25) -> Tuple[List[Dict[str, Any]], int]:
        """List requests with pagination"""
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
            query = session.query(Request).filter_by(request_organization_id=org_id_int)
            
            if email_id:
                email_id_int = int(email_id) if isinstance(email_id, str) and str(email_id).isdigit() else email_id
                email_req_subquery = session.query(EmailRequest.emailrequest_request_id).filter(
                    EmailRequest.emailrequest_email_id == email_id_int
                ).distinct()
                query = query.filter(Request.request_id.in_(email_req_subquery))
            
            if status_name:
                # Support comma-separated status values for multi-select
                status_names = [s.strip() for s in status_name.split(',') if s.strip()]
                if len(status_names) == 1:
                    status_type_id = self._get_status_type_id_in_session(session, status_names[0], 'request')
                    if status_type_id:
                        query = query.filter_by(request_status_type_id=status_type_id)
                elif len(status_names) > 1:
                    status_type_ids = []
                    for sn in status_names:
                        stid = self._get_status_type_id_in_session(session, sn, 'request')
                        if stid:
                            status_type_ids.append(stid)
                    if status_type_ids:
                        query = query.filter(Request.request_status_type_id.in_(status_type_ids))
            
            if search:
                # Search in title, description, and document filenames
                search_filter = or_(
                    Request.request_title.ilike(f'%{search}%'),
                    Request.request_description.ilike(f'%{search}%')
                )
                # Also search in document filenames via subquery
                doc_subquery = session.query(Document.document_request_id).filter(
                    Document.document_file_name.ilike(f'%{search}%')
                ).distinct()
                search_filter = or_(search_filter, Request.request_id.in_(doc_subquery))
                query = query.filter(search_filter)
            
            if issuer:
                query = query.filter(Request.request_issuer.ilike(f'%{issuer}%'))
            
            if date_from:
                try:
                    from_date = datetime.strptime(date_from, '%Y-%m-%d')
                    query = query.filter(Request.request_created_at >= from_date)
                except ValueError:
                    pass
            
            if date_to:
                try:
                    to_date = datetime.strptime(date_to, '%Y-%m-%d')
                    # Add one day to include the entire end date
                    to_date = to_date + timedelta(days=1)
                    query = query.filter(Request.request_created_at < to_date)
                except ValueError:
                    pass
            
            total = query.count()
            requests = query.order_by(desc(Request.request_created_at))\
                .offset((page - 1) * per_page).limit(per_page).all()
            
            result = []
            for req in requests:
                req_dict = self._request_to_dict_internal(session, req)
                # Collect documents from both direct and junction table relationships
                doc_names = []
                seen_ids = set()
                # Direct documents via document_request_id
                for doc in (req.documents or []):
                    if doc.document_id not in seen_ids:
                        seen_ids.add(doc.document_id)
                        doc_names.append(doc.document_file_name)
                # Junction table documents (legacy)
                for rd in (req.request_documents or []):
                    if rd.document and rd.document.document_id not in seen_ids:
                        seen_ids.add(rd.document.document_id)
                        doc_names.append(rd.document.document_file_name)
                req_dict['document_count'] = len(doc_names)
                req_dict['document_names'] = doc_names
                
                result.append(req_dict)
            
            return result, total
    
    def update_request(self, request_id, title: str = None, description: str = None,
                       issuer: str = None, user_id: str = None) -> bool:
        """Update request metadata"""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            req = session.query(Request).filter_by(request_id=req_id_int).first()
            if req:
                if title:
                    req.request_title = title
                if description is not None:
                    req.request_description = description
                if issuer is not None:
                    req.request_issuer = issuer
                
                req.request_modified_at = datetime.utcnow()
                if user_id:
                    req.request_modified_by = int(user_id) if str(user_id).isdigit() else user_id
                return True
            return False
    
    def update_request_status(self, request_id, status_name: str,
                              user_id: str = None, reason: str = None) -> bool:
        """Update request status"""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            req = session.query(Request).filter_by(request_id=req_id_int).first()
            if req:
                status_type_id = self._get_status_type_id_in_session(session, status_name, 'request')
                if status_type_id:
                    req.request_status_type_id = status_type_id
                    req.request_modified_at = datetime.utcnow()
                    if user_id:
                        req.request_modified_by = int(user_id) if str(user_id).isdigit() else user_id
                    
                    if status_name in ('completed', 'approved'):
                        req.request_published_by = int(user_id) if user_id and str(user_id).isdigit() else user_id
                        req.request_published_at = datetime.utcnow()
                    elif status_name == 'cancelled':
                        req.request_cancelled_by = int(user_id) if user_id and str(user_id).isdigit() else user_id
                        req.request_cancelled_at = datetime.utcnow()
                        req.request_cancellation_reason = reason
                    
                    return True
            return False
    
    def delete_request(self, request_id, user_id: str = None) -> bool:
        """Delete a request"""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            req = session.query(Request).filter_by(request_id=req_id_int).first()
            if req:
                session.delete(req)
                return True
            return False
    
    def link_request_document(self, request_id, doc_id,
                              source_type: str = 'attachment') -> bool:
        """Link a document to a request"""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            doc_id_int = int(doc_id) if isinstance(doc_id, str) and doc_id.isdigit() else doc_id
            
            # Check if already linked
            existing = session.query(RequestDocument).filter_by(
                request_document_request_id=req_id_int, 
                request_document_document_id=doc_id_int
            ).first()
            if existing:
                return True
            
            link = RequestDocument(
                request_document_request_id=req_id_int,
                request_document_document_id=doc_id_int,
                request_document_source_type=source_type
            )
            session.add(link)
            return True
    
    def unlink_request_document(self, request_id, doc_id, user_id: str = None) -> bool:
        """Unlink a document from a request"""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            doc_id_int = int(doc_id) if isinstance(doc_id, str) and doc_id.isdigit() else doc_id
            
            link = session.query(RequestDocument).filter_by(
                request_document_request_id=req_id_int, 
                request_document_document_id=doc_id_int
            ).first()
            if link:
                session.delete(link)
                return True
            return False
    
    def get_request_documents(self, request_id) -> List[Dict[str, Any]]:
        """
        Get all documents linked to a request.
        
        This method supports both:
        1. Direct link via document_request_id (new approach)
        2. Junction table request_documents (legacy, for backward compatibility)
        
        Documents from both sources are merged and deduplicated.
        """
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            result = []
            seen_doc_ids = set()
            
            # 1. Get documents via direct document_request_id (primary/new approach)
            direct_docs = session.query(Document).filter_by(document_request_id=req_id_int).all()
            for doc in direct_docs:
                if doc.document_id not in seen_doc_ids:
                    seen_doc_ids.add(doc.document_id)
                    # Determine source_type based on document_type
                    doc_type = session.query(DocumentType).filter_by(document_type_id=doc.document_document_type_id).first()
                    source_type = doc_type.document_type_value if doc_type else 'attachment'
                    result.append({
                        'id': None,  # No junction table entry
                        'request_id': req_id_int,
                        'document_id': doc.document_id,
                        'source_type': source_type,
                        'document': {
                            'id': doc.document_id,
                            'filename': doc.document_file_name,
                            'blob_url': doc.document_file_path,
                            'content_type': doc.document_content_type,
                            'file_size_bytes': doc.document_file_size_bytes,
                            'status': self._get_status_value_from_type(session, doc.document_status_type_id)
                        },
                        'created_at': doc.document_created_at.isoformat() if doc.document_created_at else None
                    })
            
            # 2. Get documents via junction table (legacy, for backward compatibility)
            links = session.query(RequestDocument).filter_by(request_document_request_id=req_id_int).all()
            for link in links:
                doc = link.document
                if doc and doc.document_id not in seen_doc_ids:
                    seen_doc_ids.add(doc.document_id)
                    result.append({
                        'id': link.request_document_id,
                        'request_id': link.request_document_request_id,
                        'document_id': link.request_document_document_id,
                        'source_type': link.request_document_source_type,
                        'document': {
                            'id': doc.document_id,
                            'filename': doc.document_file_name,
                            'blob_url': doc.document_file_path,
                            'content_type': doc.document_content_type,
                            'file_size_bytes': doc.document_file_size_bytes,
                            'status': self._get_status_value_from_type(session, doc.document_status_type_id)
                        } if doc else None,
                        'created_at': link.request_document_created_at.isoformat() if link.request_document_created_at else None
                    })
            return result
    
    def get_requests_for_document(self, document_id, org_id) -> List[Dict[str, Any]]:
        """Get all requests linked to a document via direct FK or junction table"""
        with self.get_session() as session:
            doc_id_int = int(document_id) if isinstance(document_id, str) and str(document_id).isdigit() else document_id
            org_id_int = int(org_id) if isinstance(org_id, str) and str(org_id).isdigit() else org_id

            # Find request IDs from direct document_request_id link
            direct_subquery = session.query(Document.document_request_id).filter(
                Document.document_id == doc_id_int,
                Document.document_request_id.isnot(None)
            ).distinct()

            # Find request IDs from junction table
            junction_subquery = session.query(RequestDocument.request_document_request_id).filter(
                RequestDocument.request_document_document_id == doc_id_int
            ).distinct()

            # Union both sets of request IDs
            requests = session.query(Request).filter(
                Request.request_organization_id == org_id_int,
                or_(
                    Request.request_id.in_(direct_subquery),
                    Request.request_id.in_(junction_subquery)
                )
            ).order_by(desc(Request.request_created_at)).all()

            result = []
            for req in requests:
                req_dict = self._request_to_dict_internal(session, req)
                # Collect documents
                doc_names = []
                seen_ids = set()
                for doc in (req.documents or []):
                    if doc.document_id not in seen_ids:
                        seen_ids.add(doc.document_id)
                        doc_names.append(doc.document_file_name)
                for rd in (req.request_documents or []):
                    if rd.document and rd.document.document_id not in seen_ids:
                        seen_ids.add(rd.document.document_id)
                        doc_names.append(rd.document.document_file_name)
                req_dict['document_count'] = len(doc_names)
                req_dict['document_names'] = doc_names
                result.append(req_dict)

            return result

    def _request_to_dict_internal(self, session: Session, req: Request) -> Dict[str, Any]:
        # Get email_id from email_requests junction table
        email_id = None
        if req.email_requests:
            email_id = req.email_requests[0].emailrequest_email_id
        
        # Compute ai_confidence: average of extracted field confidences * 100, clamped 0-100
        ai_confidence = 0
        if req.request_current_version_id:
            active_fields = session.query(RequestField.requestfield_confidence).filter(
                RequestField.requestfield_request_version_id == req.request_current_version_id,
                RequestField.requestfield_is_active == True,
                RequestField.requestfield_confidence.isnot(None),
                RequestField.requestfield_confidence > 0,
                RequestField.requestfield_source_type.in_(['document', 'email_body', 'llm_fallback'])
            ).all()
            if active_fields:
                avg_conf = sum(f[0] for f in active_fields) / len(active_fields)
                ai_confidence = min(max(round(avg_conf * 100), 0), 100)
        
        return {
            'id': req.request_id,
            'organization_id': req.request_organization_id,
            'email_id': email_id,
            'template_id': req.request_template_id,
            'title': req.request_title,
            'description': req.request_description,
            'issuer': req.request_issuer,
            'status': self._get_status_value_from_type(session, req.request_status_type_id),
            'status_display': self._get_status_value_from_type(session, req.request_status_type_id),
            'current_version_id': req.request_current_version_id,
            'created_by': req.request_created_by,
            'created_by_name': req.created_by_user.user_username if req.created_by_user else None,
            'published_by': req.request_published_by,
            'published_at': req.request_published_at.isoformat() if req.request_published_at else None,
            'cancelled_by': req.request_cancelled_by,
            'cancelled_at': req.request_cancelled_at.isoformat() if req.request_cancelled_at else None,
            'cancellation_reason': req.request_cancellation_reason,
            'extraction_prompt': self._safe_get_extraction_prompt(req),
            'created_at': req.request_created_at.isoformat() if req.request_created_at else None,
            'updated_at': req.request_modified_at.isoformat() if req.request_modified_at else None,
            'ai_confidence': ai_confidence,
            'ref': req.request_ref,
        }
    
    def get_request_header(self, request_id) -> Optional[Dict[str, Any]]:
        """Lightweight request fetch - only core columns for auth/permission checks.
        Does NOT load documents, fields, template, email relations."""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            req = session.query(
                Request.request_id,
                Request.request_organization_id,
                Request.request_current_version_id,
                Request.request_status_type_id
            ).filter_by(request_id=req_id_int).first()
            if not req:
                return None
            return {
                'id': req.request_id,
                'organization_id': req.request_organization_id,
                'current_version_id': req.request_current_version_id,
                'status_type_id': req.request_status_type_id
            }

    def get_field_alternatives_with_active(self, request_id, version_id, field_name: str) -> Dict[str, Any]:
        """Get both active field and alternatives in a SINGLE query/session.
        Much faster than two separate get_active_field + get_field_alternatives calls."""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            version_id_int = int(version_id) if isinstance(version_id, str) and version_id.isdigit() else version_id
            
            # Single query - get ALL fields (active + inactive) with this name
            all_fields = session.query(RequestField).options(
                joinedload(RequestField.source_document)
            ).filter(
                RequestField.requestfield_request_id == req_id_int,
                RequestField.requestfield_request_version_id == version_id_int,
                RequestField.requestfield_field_name == field_name
            ).order_by(
                RequestField.requestfield_is_active.desc(),  # Active first
                RequestField.requestfield_confidence.desc()
            ).all()
            
            active = None
            alternatives = []
            for f in all_fields:
                fd = self._field_to_dict(f)
                if f.requestfield_is_active:
                    active = fd
                else:
                    alternatives.append(fd)
            
            return {
                'active': active,
                'alternatives': alternatives
            }

    # ==========================================
    # REQUEST VERSION OPERATIONS
    # ==========================================
    
    def create_request_version(self, request_id, version_label: str = None,
                               fields: List[Dict] = None, user_id: str = None) -> Dict[str, Any]:
        """Create a new request version"""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            
            # Get next version number
            max_version = session.query(func.max(RequestVersion.request_version_number))\
                .filter_by(request_version_request_id=req_id_int).scalar() or 0
            
            version = RequestVersion(
                request_version_request_id=req_id_int,
                request_version_number=max_version + 1,
                request_version_label=version_label or f'Version {max_version + 1}',
                request_version_consolidated_fields=json.dumps(fields) if fields else None,
                request_version_created_by=int(user_id) if user_id and str(user_id).isdigit() else user_id
            )
            session.add(version)
            session.flush()
            
            # Create fields if provided
            if fields:
                for field_data in fields:
                    # Build source_location JSON from bounding_box and page_number
                    source_location = None
                    if field_data.get('bounding_box') or field_data.get('page_number'):
                        source_location = json.dumps({
                            'bounding_box': field_data.get('bounding_box'),
                            'page_number': field_data.get('page_number')
                        })
                    
                    field = RequestField(
                        requestfield_request_id=req_id_int,
                        requestfield_request_version_id=version.request_version_id,
                        requestfield_template_field_id=int(field_data.get('template_field_id')) if field_data.get('template_field_id') and str(field_data.get('template_field_id')).isdigit() else None,
                        requestfield_field_name=field_data.get('field_name'),
                        requestfield_field_value=field_data.get('field_value'),
                        requestfield_extracted_value=field_data.get('extracted_value'),
                        requestfield_normalized_value=field_data.get('normalized_value'),
                        requestfield_precision=field_data.get('precision'),
                        requestfield_confidence=field_data.get('confidence'),
                        requestfield_source_document_id=int(field_data.get('source_id') or field_data.get('source_document_id')) if (field_data.get('source_id') or field_data.get('source_document_id')) and str(field_data.get('source_id') or field_data.get('source_document_id')).isdigit() else None,
                        requestfield_source_type=field_data.get('source_type', 'pending'),
                        requestfield_source_location=source_location,
                        requestfield_is_active=field_data.get('is_active', True),
                        requestfield_is_selected=field_data.get('is_selected', False),
                        requestfield_is_manually_edited=field_data.get('is_manually_edited', False)
                    )
                    session.add(field)
            
            return {
                'id': version.request_version_id,
                'request_id': version.request_version_request_id,
                'version_number': version.request_version_number,
                'version_label': version.request_version_label,
                'created_by': version.request_version_created_by,
                'created_at': version.request_version_created_at.isoformat() if version.request_version_created_at else None
            }
    
    def get_request_versions(self, request_id) -> List[Dict[str, Any]]:
        """Get all versions for a request"""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            versions = session.query(RequestVersion).filter_by(request_version_request_id=req_id_int)\
                .order_by(desc(RequestVersion.request_version_number)).all()
            
            result = []
            for v in versions:
                result.append({
                    'id': v.request_version_id,
                    'request_id': v.request_version_request_id,
                    'version_number': v.request_version_number,
                    'version_label': v.request_version_label,
                    'created_by': v.request_version_created_by,
                    'created_at': v.request_version_created_at.isoformat() if v.request_version_created_at else None
                })
            return result
    
    def set_current_version(self, request_id, version_id, user_id: str = None) -> bool:
        """Set the current version for a request"""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            version_id_int = int(version_id) if isinstance(version_id, str) and version_id.isdigit() else version_id
            req = session.query(Request).filter_by(request_id=req_id_int).first()
            if req:
                req.request_current_version_id = version_id_int
                req.request_modified_at = datetime.utcnow()
                if user_id:
                    req.request_modified_by = int(user_id) if str(user_id).isdigit() else user_id
                return True
            return False
    
    def merge_fields_into_version(self, request_id, version_id, new_fields: List[Dict],
                                   field_thresholds: Dict[str, float] = None,
                                   default_threshold: float = 0.60) -> Dict[str, Any]:
        """
        Merge newly extracted fields into an existing version incrementally.
        
        For each new field:
        - If the field_name doesn't exist in this version yet, insert it
        - If it already exists, compare confidence:
            - If new field has higher confidence and meets threshold, it becomes active
              and the old active field becomes an alternative
            - If new field has lower confidence, it's added as an inactive alternative
        - Empty 'pending' placeholders are replaced by real extractions
        
        Returns:
            Dict with merge stats (updated_count, inserted_count, skipped_count)
        """
        field_thresholds = field_thresholds or {}
        
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            version_id_int = int(version_id) if isinstance(version_id, str) and version_id.isdigit() else version_id
            
            # Load all existing fields for this version, grouped by field_name
            existing_fields = session.query(RequestField).filter_by(
                requestfield_request_id=req_id_int,
                requestfield_request_version_id=version_id_int
            ).all()
            
            # Group existing by field_name
            existing_by_name: Dict[str, List[RequestField]] = {}
            for ef in existing_fields:
                name = ef.requestfield_field_name
                if name not in existing_by_name:
                    existing_by_name[name] = []
                existing_by_name[name].append(ef)
            
            updated_count = 0
            inserted_count = 0
            skipped_count = 0
            
            for field_data in new_fields:
                field_name = field_data.get('field_name')
                if not field_name:
                    continue
                
                new_confidence = field_data.get('confidence', 0) or 0
                threshold = field_thresholds.get(field_name, default_threshold)
                new_value = field_data.get('field_value')
                has_value = new_value and str(new_value).strip()
                
                # Skip if new extraction has no value or is below threshold
                if not has_value or new_confidence < threshold:
                    skipped_count += 1
                    continue
                
                # Build source_location JSON
                source_location = None
                if field_data.get('bounding_box') or field_data.get('page_number'):
                    source_location = json.dumps({
                        'bounding_box': field_data.get('bounding_box'),
                        'page_number': field_data.get('page_number')
                    })
                
                existing_group = existing_by_name.get(field_name, [])
                
                # Find the currently active field for this name
                active_field = None
                for ef in existing_group:
                    if ef.requestfield_is_active:
                        active_field = ef
                        break
                
                if active_field and active_field.requestfield_source_type == 'pending':
                    # Replace the empty placeholder with the real extraction
                    active_field.requestfield_field_value = field_data.get('field_value')
                    active_field.requestfield_extracted_value = field_data.get('extracted_value')
                    active_field.requestfield_normalized_value = field_data.get('normalized_value')
                    active_field.requestfield_confidence = new_confidence
                    active_field.requestfield_source_type = field_data.get('source_type', 'document')
                    active_field.requestfield_source_document_id = int(field_data.get('source_id') or field_data.get('source_document_id')) if (field_data.get('source_id') or field_data.get('source_document_id')) and str(field_data.get('source_id') or field_data.get('source_document_id')).isdigit() else None
                    active_field.requestfield_source_location = source_location
                    active_field.requestfield_is_active = True
                    active_field.requestfield_modified_at = datetime.utcnow()
                    updated_count += 1
                    
                elif active_field:
                    # There's already an extracted active field — compare confidence
                    active_confidence = active_field.requestfield_confidence or 0
                    
                    if new_confidence > active_confidence:
                        # New field wins — demote the old active to alternative
                        active_field.requestfield_is_active = False
                        active_field.requestfield_modified_at = datetime.utcnow()
                        
                        # Insert new field as the new active
                        new_record = RequestField(
                            requestfield_request_id=req_id_int,
                            requestfield_request_version_id=version_id_int,
                            requestfield_template_field_id=int(field_data.get('template_field_id')) if field_data.get('template_field_id') and str(field_data.get('template_field_id')).isdigit() else active_field.requestfield_template_field_id,
                            requestfield_field_name=field_name,
                            requestfield_field_value=field_data.get('field_value'),
                            requestfield_extracted_value=field_data.get('extracted_value'),
                            requestfield_normalized_value=field_data.get('normalized_value'),
                            requestfield_confidence=new_confidence,
                            requestfield_source_document_id=int(field_data.get('source_id') or field_data.get('source_document_id')) if (field_data.get('source_id') or field_data.get('source_document_id')) and str(field_data.get('source_id') or field_data.get('source_document_id')).isdigit() else None,
                            requestfield_source_type=field_data.get('source_type', 'document'),
                            requestfield_source_location=source_location,
                            requestfield_is_active=True,
                            requestfield_is_selected=False,
                            requestfield_is_manually_edited=False
                        )
                        session.add(new_record)
                        updated_count += 1
                    else:
                        # New field is weaker — add as inactive alternative
                        alt_record = RequestField(
                            requestfield_request_id=req_id_int,
                            requestfield_request_version_id=version_id_int,
                            requestfield_template_field_id=int(field_data.get('template_field_id')) if field_data.get('template_field_id') and str(field_data.get('template_field_id')).isdigit() else active_field.requestfield_template_field_id,
                            requestfield_field_name=field_name,
                            requestfield_field_value=field_data.get('field_value'),
                            requestfield_extracted_value=field_data.get('extracted_value'),
                            requestfield_normalized_value=field_data.get('normalized_value'),
                            requestfield_confidence=new_confidence,
                            requestfield_source_document_id=int(field_data.get('source_id') or field_data.get('source_document_id')) if (field_data.get('source_id') or field_data.get('source_document_id')) and str(field_data.get('source_id') or field_data.get('source_document_id')).isdigit() else None,
                            requestfield_source_type=field_data.get('source_type', 'document'),
                            requestfield_source_location=source_location,
                            requestfield_is_active=False,
                            requestfield_is_selected=False,
                            requestfield_is_manually_edited=False
                        )
                        session.add(alt_record)
                        inserted_count += 1
                else:
                    # No existing field for this name at all — insert as active
                    new_record = RequestField(
                        requestfield_request_id=req_id_int,
                        requestfield_request_version_id=version_id_int,
                        requestfield_template_field_id=int(field_data.get('template_field_id')) if field_data.get('template_field_id') and str(field_data.get('template_field_id')).isdigit() else None,
                        requestfield_field_name=field_name,
                        requestfield_field_value=field_data.get('field_value'),
                        requestfield_extracted_value=field_data.get('extracted_value'),
                        requestfield_normalized_value=field_data.get('normalized_value'),
                        requestfield_confidence=new_confidence,
                        requestfield_source_document_id=int(field_data.get('source_id') or field_data.get('source_document_id')) if (field_data.get('source_id') or field_data.get('source_document_id')) and str(field_data.get('source_id') or field_data.get('source_document_id')).isdigit() else None,
                        requestfield_source_type=field_data.get('source_type', 'document'),
                        requestfield_source_location=source_location,
                        requestfield_is_active=True,
                        requestfield_is_selected=False,
                        requestfield_is_manually_edited=False
                    )
                    session.add(new_record)
                    inserted_count += 1
            
            # Update version modified timestamp
            version = session.query(RequestVersion).filter_by(
                request_version_id=version_id_int
            ).first()
            if version:
                version.request_version_modified_at = datetime.utcnow() if hasattr(version, 'request_version_modified_at') else None
            
            # Update request modified timestamp
            req = session.query(Request).filter_by(request_id=req_id_int).first()
            if req:
                req.request_modified_at = datetime.utcnow()
            
            session.flush()
            
            return {
                'updated_count': updated_count,
                'inserted_count': inserted_count,
                'skipped_count': skipped_count,
                'version_id': version_id_int
            }
    
    # ==========================================
    # REQUEST FIELD OPERATIONS
    # ==========================================
    
    def get_request_fields(self, request_id, version_id = None, 
                           include_inactive: bool = False) -> List[Dict[str, Any]]:
        """Get all fields for a request. By default returns only active fields."""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            
            if version_id:
                version_id_int = int(version_id) if isinstance(version_id, str) and version_id.isdigit() else version_id
                query = session.query(RequestField).options(
                    joinedload(RequestField.source_document),
                    joinedload(RequestField.template_field)
                ).filter_by(
                    requestfield_request_id=req_id_int, 
                    requestfield_request_version_id=version_id_int
                )
            else:
                # Get from current version
                req = session.query(Request).filter_by(request_id=req_id_int).first()
                if req and req.request_current_version_id:
                    query = session.query(RequestField).options(
                        joinedload(RequestField.source_document),
                        joinedload(RequestField.template_field)
                    ).filter_by(
                        requestfield_request_version_id=req.request_current_version_id
                    )
                    version_id_int = req.request_current_version_id
                else:
                    return []
            
            # Filter by is_active unless include_inactive is True
            if not include_inactive:
                query = query.filter(RequestField.requestfield_is_active == True)
            
            fields = query.all()
            
            # Get alternative counts in a SINGLE grouped query instead of N+1
            alt_counts_query = session.query(
                RequestField.requestfield_field_name,
                func.count(RequestField.requestfield_id)
            ).filter(
                RequestField.requestfield_request_id == req_id_int,
                RequestField.requestfield_request_version_id == version_id_int,
                RequestField.requestfield_is_active == False
            ).group_by(RequestField.requestfield_field_name).all()
            alt_counts_map = {name: count for name, count in alt_counts_query}
            
            result = []
            for f in fields:
                field_dict = self._field_to_dict(f)
                # Add alternative count for active fields
                if f.requestfield_is_active:
                    alt_count = alt_counts_map.get(f.requestfield_field_name, 0)
                    field_dict['alternative_count'] = alt_count
                    field_dict['has_alternatives'] = alt_count > 0
                result.append(field_dict)
            
            return result
    
    def create_request_field(self, field_id, request_id, version_id,
                             field_name: str, field_value: str, 
                             extracted_value: str = None, normalized_value: str = None,
                             confidence: float = None,
                             source_type: str = None, source_id = None,
                             page_number: int = None, bounding_box: str = None) -> Dict[str, Any]:
        """
        Create a request field
        
        Args:
            field_value: Normalized display value (what user sees/edits)
            extracted_value: Raw AI extraction value
            normalized_value: Standardized value for storage/search
        """
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            version_id_int = int(version_id) if isinstance(version_id, str) and version_id.isdigit() else version_id
            
            # Build source_location JSON from bounding_box and page_number
            source_location = None
            if bounding_box or page_number:
                source_location = json.dumps({
                    'bounding_box': bounding_box,
                    'page_number': page_number
                })
            
            field = RequestField(
                requestfield_request_id=req_id_int,
                requestfield_request_version_id=version_id_int,
                requestfield_field_name=field_name,
                requestfield_field_value=field_value,
                requestfield_extracted_value=extracted_value or field_value,
                requestfield_normalized_value=normalized_value or field_value,
                requestfield_confidence=confidence,
                requestfield_source_type=source_type or 'extraction',
                requestfield_source_document_id=int(source_id) if source_id and str(source_id).isdigit() else None,
                requestfield_source_location=source_location,
                requestfield_is_selected=False,
                requestfield_is_manually_edited=False
            )
            session.add(field)
            session.flush()
            return self._field_to_dict(field)
    
    def update_request_field(self, field_id, field_value: str = None,
                             is_selected: bool = None, edited_by: str = None) -> bool:
        """Update a request field"""
        with self.get_session() as session:
            field_id_int = int(field_id) if isinstance(field_id, str) and field_id.isdigit() else field_id
            field = session.query(RequestField).filter_by(requestfield_id=field_id_int).first()
            if field:
                if field_value is not None:
                    field.requestfield_field_value = field_value
                    field.requestfield_is_manually_edited = True
                
                if is_selected is not None:
                    field.requestfield_is_selected = is_selected
                
                field.requestfield_modified_at = datetime.utcnow()
                return True
            return False
    
    def update_request_field_normalised_value(self, field_id, normalised_value: str) -> bool:
        """Update the normalised_value of a request field and also update
        field_value so the normalised result is visible in the UI.
        Skips updating field_value if the user has manually edited it."""
        with self.get_session() as session:
            field_id_int = int(field_id) if isinstance(field_id, str) and field_id.isdigit() else field_id
            field = session.query(RequestField).filter_by(requestfield_id=field_id_int).first()
            if field:
                field.requestfield_normalized_value = normalised_value
                # Also update the display value so the frontend sees the
                # normalised result — but only if the user hasn't manually
                # overridden it.
                if not field.requestfield_is_manually_edited:
                    field.requestfield_field_value = normalised_value
                field.requestfield_modified_at = datetime.utcnow()
                return True
            return False
    
    def update_request_fields_in_place(self, request_id, fields: List[Dict], user_id: str = None) -> Dict[str, Any]:
        """
        Update existing request fields in place without creating a new version.
        This maintains field IDs and preserves audit log linkages.
        
        Args:
            request_id: The request ID
            fields: List of field dicts with 'id' and values to update
            user_id: User making the changes
            
        Returns:
            Dict with updated fields and stats
        """
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            
            # Get current request and version
            req = session.query(Request).filter_by(request_id=req_id_int).first()
            if not req:
                raise ValueError(f"Request {request_id} not found")
            
            current_version_id = req.request_current_version_id
            if not current_version_id:
                raise ValueError(f"Request {request_id} has no current version")
            
            # Build lookup of incoming fields by ID and field_name
            fields_by_id = {f['id']: f for f in fields if f.get('id')}
            fields_by_name = {f['field_name']: f for f in fields if f.get('field_name')}
            
            updated_count = 0
            created_count = 0
            
            # Update existing fields
            for field_id, field_data in fields_by_id.items():
                field_id_int = int(field_id) if isinstance(field_id, str) and field_id.isdigit() else field_id
                field = session.query(RequestField).filter_by(requestfield_id=field_id_int).first()
                
                if field:
                    # Update the existing field
                    if 'field_value' in field_data and field_data['field_value'] != field.requestfield_field_value:
                        field.requestfield_field_value = field_data['field_value']
                        field.requestfield_is_manually_edited = True
                    
                    # Update other properties
                    if 'is_selected' in field_data:
                        field.requestfield_is_selected = field_data['is_selected']
                    if 'is_active' in field_data:
                        field.requestfield_is_active = field_data['is_active']
                    if 'confidence' in field_data:
                        field.requestfield_confidence = field_data['confidence']
                    if 'extracted_value' in field_data:
                        field.requestfield_extracted_value = field_data['extracted_value']
                    if 'normalized_value' in field_data:
                        field.requestfield_normalized_value = field_data['normalized_value']
                    
                    # Handle source_location
                    if field_data.get('bounding_box') or field_data.get('page_number'):
                        source_location = json.dumps({
                            'bounding_box': field_data.get('bounding_box'),
                            'page_number': field_data.get('page_number')
                        })
                        field.requestfield_source_location = source_location
                    
                    field.requestfield_modified_at = datetime.utcnow()
                    updated_count += 1
            
            # Create new fields that don't have IDs (truly new fields)
            for field_data in fields:
                if not field_data.get('id'):
                    # This is a new field
                    source_location = None
                    if field_data.get('bounding_box') or field_data.get('page_number'):
                        source_location = json.dumps({
                            'bounding_box': field_data.get('bounding_box'),
                            'page_number': field_data.get('page_number')
                        })
                    
                    new_field = RequestField(
                        requestfield_request_id=req_id_int,
                        requestfield_request_version_id=current_version_id,
                        requestfield_template_field_id=int(field_data.get('template_field_id')) if field_data.get('template_field_id') and str(field_data.get('template_field_id')).isdigit() else None,
                        requestfield_field_name=field_data.get('field_name'),
                        requestfield_field_value=field_data.get('field_value'),
                        requestfield_extracted_value=field_data.get('extracted_value'),
                        requestfield_normalized_value=field_data.get('normalized_value'),
                        requestfield_precision=field_data.get('precision'),
                        requestfield_confidence=field_data.get('confidence'),
                        requestfield_source_document_id=int(field_data.get('source_id') or field_data.get('source_document_id')) if (field_data.get('source_id') or field_data.get('source_document_id')) and str(field_data.get('source_id') or field_data.get('source_document_id')).isdigit() else None,
                        requestfield_source_type=field_data.get('source_type', 'pending'),
                        requestfield_source_location=source_location,
                        requestfield_is_active=field_data.get('is_active', True),
                        requestfield_is_selected=field_data.get('is_selected', False),
                        requestfield_is_manually_edited=field_data.get('is_manually_edited', False)
                    )
                    session.add(new_field)
                    created_count += 1
            
            # Update request modified timestamp
            req.request_modified_at = datetime.utcnow()
            if user_id:
                req.request_modified_by = int(user_id) if str(user_id).isdigit() else user_id
            
            session.flush()
            
            return {
                'updated_count': updated_count,
                'created_count': created_count,
                'current_version_id': current_version_id
            }
    
    def _field_to_dict(self, field: RequestField) -> Dict[str, Any]:
        # Parse source_location JSON for bounding_box and page_number
        bounding_box = None
        page_number = None
        source_location_raw = field.requestfield_source_location if hasattr(field, 'requestfield_source_location') else None
        if source_location_raw:
            try:
                source_loc = json.loads(source_location_raw)
                bounding_box = source_loc.get('bounding_box')
                page_number = source_loc.get('page_number')
            except (json.JSONDecodeError, TypeError):
                # Keep as string if not valid JSON
                bounding_box = source_location_raw
        
        # Get source document filename if available
        source_document_filename = None
        if hasattr(field, 'source_document') and field.source_document:
            source_document_filename = field.source_document.document_file_name
        
        result = {
            'id': field.requestfield_id,
            'request_id': field.requestfield_request_id,
            'request_version_id': field.requestfield_request_version_id,
            'template_field_id': field.requestfield_template_field_id if hasattr(field, 'requestfield_template_field_id') else None,
            'field_name': field.requestfield_field_name,
            'field_value': field.requestfield_field_value,
            'extracted_value': field.requestfield_extracted_value if hasattr(field, 'requestfield_extracted_value') else None,
            'normalized_value': field.requestfield_normalized_value if hasattr(field, 'requestfield_normalized_value') else None,
            'precision': field.requestfield_precision if hasattr(field, 'requestfield_precision') else None,
            'confidence': float(field.requestfield_confidence) if field.requestfield_confidence else None,
            'source_type': field.requestfield_source_type,
            'source_id': field.requestfield_source_document_id,
            'source_document_id': field.requestfield_source_document_id,
            'source_document_filename': source_document_filename,
            'bounding_box': bounding_box,
            'page_number': page_number,
            'source_location': source_location_raw,
            'is_active': field.requestfield_is_active if hasattr(field, 'requestfield_is_active') else True,
            'is_selected': field.requestfield_is_selected,
            'is_manually_edited': field.requestfield_is_manually_edited,
            'is_extracted': field.requestfield_source_type in ('document', 'email_body'),
            'created_at': field.requestfield_created_at.isoformat() if field.requestfield_created_at else None
        }
        
        # Add template field info if available
        if hasattr(field, 'template_field') and field.template_field:
            result['template_field'] = {
                'id': field.template_field.template_field_id,
                'display_name': field.template_field.template_field_display_name,
                'field_type': field.template_field.template_field_data_type,
                'data_type': field.template_field.template_field_data_type,
                'field_values': field.template_field.template_field_field_values,
                'category': field.template_field.category.template_field_category_name if field.template_field.category else None,
                'is_required': field.template_field.template_field_is_required,
                'sort_order': field.template_field.template_field_sort_order,
                'normalisation_instruction': field.template_field.template_field_normalisation_instruction
            }
        
        return result
    
    def get_field_alternatives(self, request_id, version_id, field_name: str) -> List[Dict[str, Any]]:
        """
        Get all alternative (inactive) values for a specific field.
        Returns inactive rows with the same field_name for user selection.
        """
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            version_id_int = int(version_id) if isinstance(version_id, str) and version_id.isdigit() else version_id
            
            fields = session.query(RequestField).options(
                joinedload(RequestField.source_document)
            ).filter(
                RequestField.requestfield_request_id == req_id_int,
                RequestField.requestfield_request_version_id == version_id_int,
                RequestField.requestfield_field_name == field_name,
                RequestField.requestfield_is_active == False
            ).order_by(RequestField.requestfield_confidence.desc()).all()
            return [self._field_to_dict(f) for f in fields]
    
    def get_active_field(self, request_id, version_id, field_name: str) -> Optional[Dict[str, Any]]:
        """Get the active value for a specific field."""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            version_id_int = int(version_id) if isinstance(version_id, str) and version_id.isdigit() else version_id
            
            field = session.query(RequestField).options(
                joinedload(RequestField.source_document)
            ).filter(
                RequestField.requestfield_request_id == req_id_int,
                RequestField.requestfield_request_version_id == version_id_int,
                RequestField.requestfield_field_name == field_name,
                RequestField.requestfield_is_active == True
            ).first()
            if field:
                return self._field_to_dict(field)
            return None
    
    def count_field_alternatives(self, request_id, version_id, field_name: str) -> int:
        """Count the number of alternative values for a field."""
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and request_id.isdigit() else request_id
            version_id_int = int(version_id) if isinstance(version_id, str) and version_id.isdigit() else version_id
            
            return session.query(RequestField).filter(
                RequestField.requestfield_request_id == req_id_int,
                RequestField.requestfield_request_version_id == version_id_int,
                RequestField.requestfield_field_name == field_name,
                RequestField.requestfield_is_active == False
            ).count()
    
    def set_field_as_active(self, field_id, user_id: str = None) -> Dict[str, Any]:
        """
        Set a specific field as active and deactivate the current active field.
        Used when user selects an alternative value for a field.
        
        Returns:
            Dict with 'success', 'old_value', 'new_value', 'field_name', 'old_field_id'
            or None on failure.
        """
        with self.get_session() as session:
            field_id_int = int(field_id) if isinstance(field_id, str) and field_id.isdigit() else field_id
            
            # Get the field to be made active
            new_active = session.query(RequestField).filter_by(requestfield_id=field_id_int).first()
            if not new_active:
                return None
            
            field_name = new_active.requestfield_field_name
            new_value = new_active.requestfield_field_value or new_active.requestfield_extracted_value
            old_value = None
            old_field_id = None
            
            # Find and deactivate current active field with same name
            current_active = session.query(RequestField).filter(
                RequestField.requestfield_request_id == new_active.requestfield_request_id,
                RequestField.requestfield_request_version_id == new_active.requestfield_request_version_id,
                RequestField.requestfield_field_name == new_active.requestfield_field_name,
                RequestField.requestfield_is_active == True,
                RequestField.requestfield_id != field_id_int
            ).first()
            
            if current_active:
                old_value = current_active.requestfield_field_value or current_active.requestfield_extracted_value
                old_field_id = current_active.requestfield_id
                current_active.requestfield_is_active = False
                current_active.requestfield_modified_at = datetime.utcnow()
            
            # Activate the new field
            new_active.requestfield_is_active = True
            new_active.requestfield_is_selected = True
            new_active.requestfield_modified_at = datetime.utcnow()
            
            return {
                'success': True,
                'field_name': field_name,
                'old_value': old_value,
                'new_value': new_value,
                'old_field_id': str(old_field_id) if old_field_id else None,
                'new_field_id': str(field_id_int)
            }
    
    # ==========================================
    # ASYNC JOB OPERATIONS
    # ==========================================
    
    def create_async_job(self, job_id: str, job_type: str, entity_id,
                         entity_type: str, created_by: str = None,
                         org_id: str = None) -> Dict[str, Any]:
        """Create an async job"""
        with self.get_session() as session:
            # Get status_type for pending job (using session-aware helper)
            status_type_id = self._get_status_type_id_in_session(session, 'pending', 'job') or 1
            
            job = AsyncJob(
                async_job_organization_id=org_id,
                async_job_type=job_type,
                async_job_entity_id=int(entity_id) if isinstance(entity_id, str) and entity_id.isdigit() else entity_id,
                async_job_entity_type=entity_type,
                async_job_status_type_id=status_type_id,
                async_job_progress_percent=0,
                async_job_created_by=int(created_by) if created_by and str(created_by).isdigit() else created_by
            )
            session.add(job)
            session.flush()
            return self._job_to_dict(session, job)
    
    def get_async_job(self, job_id) -> Optional[Dict[str, Any]]:
        """Get async job by ID"""
        with self.get_session() as session:
            job_id_int = int(job_id) if isinstance(job_id, str) and job_id.isdigit() else job_id
            job = session.query(AsyncJob).filter_by(async_job_id=job_id_int).first()
            if job:
                return self._job_to_dict(session, job)
            return None
    
    def list_async_jobs(self, entity_type: str = None, status: str = None,
                        org_id: str = None,
                        page: int = 1, per_page: int = 25) -> Tuple[List[Dict[str, Any]], int]:
        """List async jobs with pagination, optionally filtered by organization"""
        with self.get_session() as session:
            query = session.query(AsyncJob)
            
            if org_id:
                query = query.filter_by(async_job_organization_id=org_id)
            
            if entity_type:
                query = query.filter_by(async_job_entity_type=entity_type)
            
            if status:
                status_type_id = self._get_status_type_id_in_session(session, status, 'job')
                if status_type_id:
                    query = query.filter_by(async_job_status_type_id=status_type_id)
            
            total = query.count()
            jobs = query.order_by(desc(AsyncJob.async_job_created_at))\
                .offset((page - 1) * per_page).limit(per_page).all()
            
            return [self._job_to_dict(session, j) for j in jobs], total
    
    def get_pending_jobs(self, limit: int = 10, org_id: str = None) -> List[Dict[str, Any]]:
        """Get pending jobs for processing, optionally filtered by organization"""
        with self.get_session() as session:
            status_type_id = self._get_status_type_id_in_session(session, 'pending', 'job')
            if not status_type_id:
                return []
            query = session.query(AsyncJob).filter_by(async_job_status_type_id=status_type_id)
            if org_id:
                query = query.filter_by(async_job_organization_id=org_id)
            jobs = query.order_by(AsyncJob.async_job_created_at).limit(limit).all()
            return [self._job_to_dict(session, j) for j in jobs]
    
    def update_async_job(self, job_id, status: str = None, progress_percent: int = None,
                         progress_message: str = None, result_data: str = None,
                         error_message: str = None) -> bool:
        """Update async job"""
        with self.get_session() as session:
            job_id_int = int(job_id) if isinstance(job_id, str) and job_id.isdigit() else job_id
            job = session.query(AsyncJob).filter_by(async_job_id=job_id_int).first()
            if job:
                if status:
                    status_type_id = self._get_status_type_id_in_session(session, status, 'job')
                    if status_type_id:
                        job.async_job_status_type_id = status_type_id
                    
                    if status == 'running' and not job.async_job_started_at:
                        job.async_job_started_at = datetime.utcnow()
                    elif status in ['completed', 'failed']:
                        job.async_job_completed_at = datetime.utcnow()
                
                if progress_percent is not None:
                    job.async_job_progress_percent = progress_percent
                if progress_message is not None:
                    job.async_job_progress_message = progress_message
                if result_data is not None:
                    job.async_job_result_data = result_data
                if error_message is not None:
                    job.async_job_error_message = error_message
                
                job.async_job_updated_at = datetime.utcnow()
                return True
            return False
    
    def cancel_async_job(self, job_id, user_id: str = None) -> bool:
        """Cancel an async job"""
        with self.get_session() as session:
            job_id_int = int(job_id) if isinstance(job_id, str) and job_id.isdigit() else job_id
            job = session.query(AsyncJob).filter_by(async_job_id=job_id_int).first()
            if job:
                # Check if job is in pending or running state (using session-aware helpers)
                pending_id = self._get_status_type_id_in_session(session, 'pending', 'job')
                running_id = self._get_status_type_id_in_session(session, 'running', 'job')
                failed_id = self._get_status_type_id_in_session(session, 'failed', 'job')
                
                if job.async_job_status_type_id in [pending_id, running_id]:
                    job.async_job_status_type_id = failed_id if failed_id else job.async_job_status_type_id
                    job.async_job_error_message = 'Cancelled by user'
                    job.async_job_completed_at = datetime.utcnow()
                    job.async_job_updated_at = datetime.utcnow()
                    return True
            return False
    
    def retry_async_job(self, job_id, user_id: str = None) -> Optional[int]:
        """Retry a failed job by creating a new one"""
        with self.get_session() as session:
            job_id_int = int(job_id) if isinstance(job_id, str) and job_id.isdigit() else job_id
            job = session.query(AsyncJob).filter_by(async_job_id=job_id_int).first()
            if job:
                # Get status IDs using session-aware helpers
                failed_id = self._get_status_type_id_in_session(session, 'failed', 'job')
                pending_id = self._get_status_type_id_in_session(session, 'pending', 'job') or 1
                
                if job.async_job_status_type_id == failed_id:
                    new_job = AsyncJob(
                        async_job_organization_id=job.async_job_organization_id,
                        async_job_type=job.async_job_type,
                        async_job_entity_id=job.async_job_entity_id,
                        async_job_entity_type=job.async_job_entity_type,
                        async_job_status_type_id=pending_id,
                        async_job_progress_percent=0,
                        async_job_retry_count=job.async_job_retry_count + 1 if job.async_job_retry_count else 1,
                        async_job_created_by=int(user_id) if user_id and str(user_id).isdigit() else (job.async_job_created_by)
                    )
                    session.add(new_job)
                    session.flush()
                    return new_job.async_job_id
            return None
    
    def get_job_stats(self, org_id: str = None) -> Dict[str, int]:
        """Get job statistics, optionally filtered by organization"""
        with self.get_session() as session:
            # Get status IDs using session-aware helpers
            pending_id = self._get_status_type_id_in_session(session, 'pending', 'job')
            running_id = self._get_status_type_id_in_session(session, 'running', 'job')
            completed_id = self._get_status_type_id_in_session(session, 'completed', 'job')
            failed_id = self._get_status_type_id_in_session(session, 'failed', 'job')
            
            base_query = session.query(AsyncJob)
            if org_id:
                base_query = base_query.filter_by(async_job_organization_id=org_id)
            
            pending = base_query.filter_by(async_job_status_type_id=pending_id).count() if pending_id else 0
            running = base_query.filter_by(async_job_status_type_id=running_id).count() if running_id else 0
            completed = base_query.filter_by(async_job_status_type_id=completed_id).count() if completed_id else 0
            failed = base_query.filter_by(async_job_status_type_id=failed_id).count() if failed_id else 0
            
            return {
                'pending': pending,
                'running': running,
                'completed': completed,
                'failed': failed,
                'total': pending + running + completed + failed
            }
    
    def _job_to_dict(self, session: Session, job: AsyncJob) -> Dict[str, Any]:
        status_name = self._get_status_value_from_type(session, job.async_job_status_type_id)
        return {
            'id': job.async_job_id,
            'organization_id': job.async_job_organization_id,
            'job_type': job.async_job_type,
            'entity_id': job.async_job_entity_id,
            'entity_type': job.async_job_entity_type,
            'status': status_name,
            'status_display': status_name,
            'progress_percent': job.async_job_progress_percent,
            'progress_message': job.async_job_progress_message,
            'result_data': json.loads(job.async_job_result_data) if job.async_job_result_data else None,
            'error_message': job.async_job_error_message,
            'retry_count': job.async_job_retry_count,
            'max_retries': job.async_job_max_retries,
            'started_at': job.async_job_started_at.isoformat() if job.async_job_started_at else None,
            'completed_at': job.async_job_completed_at.isoformat() if job.async_job_completed_at else None,
            'created_by': job.async_job_created_by,
            'created_at': job.async_job_created_at.isoformat() if job.async_job_created_at else None
        }
    
    # ==========================================
    # ANALYSIS OPERATIONS
    # ==========================================
    
    def create_analysis_run(self, run_id: str, source_type: str, source_id,
                            analyzer_id: str, document_version_id = None,
                            triggered_by: str = None,
                            azure_operation_id: str = None) -> int:
        """Create an analysis run"""
        with self.get_session() as session:
            run = AnalysisRun(
                analysis_run_source_type=source_type,
                analysis_run_source_id=int(source_id) if isinstance(source_id, str) and source_id.isdigit() else source_id,
                analysis_run_document_version_id=int(document_version_id) if document_version_id and str(document_version_id).isdigit() else None,
                analysis_run_analyzer_id=analyzer_id,
                analysis_run_status='pending',
                analysis_run_triggered_by=int(triggered_by) if triggered_by and str(triggered_by).isdigit() else triggered_by,
                analysis_run_azure_operation_id=azure_operation_id
            )
            session.add(run)
            session.flush()
            return run.analysis_run_id
    
    def update_analysis_run(self, run_id, status: str = None,
                            analysis_payload: Dict = None, error_message: str = None) -> bool:
        """Update analysis run with results"""
        with self.get_session() as session:
            run_id_int = int(run_id) if isinstance(run_id, str) and run_id.isdigit() else run_id
            run = session.query(AnalysisRun).filter_by(analysis_run_id=run_id_int).first()
            if run:
                if status:
                    run.analysis_run_status = status
                    if status == 'running':
                        run.analysis_run_started_at = datetime.utcnow()
                    elif status in ['succeeded', 'failed']:
                        run.analysis_run_completed_at = datetime.utcnow()
                
                if analysis_payload:
                    payload_str = json.dumps(analysis_payload)
                    payload_size = len(payload_str.encode('utf-8'))
                    run.analysis_run_payload = payload_str
                    run.analysis_run_is_chunked = False
                    run.analysis_run_payload_size_bytes = payload_size
                
                if error_message:
                    run.analysis_run_error_message = error_message
                
                run.analysis_run_modified_at = datetime.utcnow()
                return True
            return False
    
    def update_analysis_run_azure_op_id(self, run_id, azure_operation_id: str) -> bool:
        """Update the Azure operation ID on an existing analysis run"""
        with self.get_session() as session:
            run_id_int = int(run_id) if isinstance(run_id, str) and run_id.isdigit() else run_id
            run = session.query(AnalysisRun).filter_by(analysis_run_id=run_id_int).first()
            if run:
                run.analysis_run_azure_operation_id = azure_operation_id
                run.analysis_run_modified_at = datetime.utcnow()
                return True
            return False
    
    def get_analysis_run(self, run_id) -> Optional[Dict[str, Any]]:
        """Get analysis run with full payload"""
        with self.get_session() as session:
            run_id_int = int(run_id) if isinstance(run_id, str) and run_id.isdigit() else run_id
            run = session.query(AnalysisRun).filter_by(analysis_run_id=run_id_int).first()
            if not run:
                return None
            
            result = {
                'id': run.analysis_run_id,
                'azure_operation_id': run.analysis_run_azure_operation_id,
                'source_type': run.analysis_run_source_type,
                'source_id': run.analysis_run_source_id,
                'document_version_id': run.analysis_run_document_version_id,
                'analyzer_id': run.analysis_run_analyzer_id,
                'status': run.analysis_run_status,
                'is_chunked': run.analysis_run_is_chunked,
                'payload_size_bytes': run.analysis_run_payload_size_bytes,
                'error_message': run.analysis_run_error_message,
                'triggered_by': run.analysis_run_triggered_by,
                'started_at': run.analysis_run_started_at.isoformat() if run.analysis_run_started_at else None,
                'completed_at': run.analysis_run_completed_at.isoformat() if run.analysis_run_completed_at else None,
                'created_at': run.analysis_run_created_at.isoformat() if run.analysis_run_created_at else None
            }
            
            # Read payload directly
            result['analysis_payload'] = json.loads(run.analysis_run_payload) if run.analysis_run_payload else None
            
            return result
    
    def get_document_analysis(self, doc_id) -> Optional[Dict[str, Any]]:
        """Get latest analysis for a document"""
        with self.get_session() as session:
            doc_id_int = int(doc_id) if isinstance(doc_id, str) and doc_id.isdigit() else doc_id
            run = session.query(AnalysisRun).filter_by(
                analysis_run_source_type='document', analysis_run_source_id=doc_id_int
            ).order_by(desc(AnalysisRun.analysis_run_created_at)).first()
            
            if run:
                return self.get_analysis_run(run.analysis_run_id)
            return None
    
    # ==========================================
    # DASHBOARD STATISTICS
    # ==========================================
    
    def get_dashboard_stats(self, org_id, date_from: str = None, date_to: str = None) -> Dict[str, Any]:
        """Get dashboard statistics - optimized single query for cloud performance.
        Optionally filtered by date range (date_from, date_to in YYYY-MM-DD format).
        """
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
            
            # Build optional date filter clause for requests
            date_clause = ""
            params = {'org_id': org_id_int}
            
            if date_from:
                date_clause += " AND r.request_created_at >= :date_from"
                params['date_from'] = date_from
            if date_to:
                date_clause += " AND r.request_created_at < DATEADD(day, 1, CAST(:date_to AS DATE))"
                params['date_to'] = date_to
            
            # For the total count (no status join needed)
            total_date_clause = date_clause.replace('r.', 'requests.')
            if date_from:
                total_date_clause_raw = " AND requests.request_created_at >= :date_from"
            else:
                total_date_clause_raw = ""
            if date_to:
                total_date_clause_raw += " AND requests.request_created_at < DATEADD(day, 1, CAST(:date_to AS DATE))"
            
            stats_query = text(f"""
                SELECT 
                    (SELECT COUNT(*) FROM requests WHERE request_organization_id = :org_id{total_date_clause_raw}) as total_requests,
                    (SELECT COUNT(*) FROM requests r 
                     JOIN status_types st ON r.request_status_type_id = st.status_type_id
                     JOIN statuses s ON st.status_type_status_id = s.status_id
                     WHERE r.request_organization_id = :org_id AND s.status_value = 'pending'{date_clause}) as pending_requests,
                    (SELECT COUNT(*) FROM requests r 
                     JOIN status_types st ON r.request_status_type_id = st.status_type_id
                     JOIN statuses s ON st.status_type_status_id = s.status_id
                     WHERE r.request_organization_id = :org_id AND s.status_value = 'processing'{date_clause}) as processing_requests,
                    (SELECT COUNT(*) FROM requests r 
                     JOIN status_types st ON r.request_status_type_id = st.status_type_id
                     JOIN statuses s ON st.status_type_status_id = s.status_id
                     WHERE r.request_organization_id = :org_id AND s.status_value IN ('extracted', 'reviewing'){date_clause}) as reviewing_requests,
                    (SELECT COUNT(*) FROM requests r 
                     JOIN status_types st ON r.request_status_type_id = st.status_type_id
                     JOIN statuses s ON st.status_type_status_id = s.status_id
                     WHERE r.request_organization_id = :org_id AND s.status_value IN ('approved', 'completed'){date_clause}) as completed_requests,
                    (SELECT COUNT(*) FROM requests r 
                     JOIN status_types st ON r.request_status_type_id = st.status_type_id
                     JOIN statuses s ON st.status_type_status_id = s.status_id
                     WHERE r.request_organization_id = :org_id AND s.status_value = 'cancelled'{date_clause}) as cancelled_requests,
                    (SELECT COUNT(*) FROM emails WHERE email_organization_id = :org_id) as total_emails,
                    (SELECT COUNT(*) FROM documents WHERE document_organization_id = :org_id) as total_documents
            """)
            
            result = session.execute(stats_query, params).fetchone()
            
            # Get daily request counts for the last 7 days (for trend chart)
            daily_counts_query = text("""
                SELECT 
                    CAST(r.request_created_at AS DATE) as request_date,
                    COUNT(*) as request_count
                FROM requests r
                WHERE r.request_organization_id = :org_id
                    AND r.request_created_at >= DATEADD(day, -6, CAST(GETDATE() AS DATE))
                    AND r.request_created_at < DATEADD(day, 1, CAST(GETDATE() AS DATE))
                GROUP BY CAST(r.request_created_at AS DATE)
                ORDER BY request_date
            """)
            
            daily_results = session.execute(daily_counts_query, {'org_id': org_id_int}).fetchall()
            
            # Build a dict of date -> count from query results
            from datetime import datetime, timedelta
            daily_map = {}
            for row in daily_results:
                date_key = row.request_date
                if isinstance(date_key, str):
                    date_key = datetime.strptime(date_key, '%Y-%m-%d').date()
                elif hasattr(date_key, 'date'):
                    date_key = date_key.date()
                daily_map[date_key] = row.request_count
            
            # Fill in all 7 days (including zeros for days with no requests)
            today = datetime.utcnow().date()
            daily_counts = []
            for i in range(6, -1, -1):
                day = today - timedelta(days=i)
                daily_counts.append(daily_map.get(day, 0))
            
            return {
                'requests': {
                    'total': result.total_requests or 0,
                    'pending': result.pending_requests or 0,
                    'processing': result.processing_requests or 0,
                    'reviewing': result.reviewing_requests or 0,
                    'completed': result.completed_requests or 0,
                    'cancelled': result.cancelled_requests or 0
                },
                'emails': {
                    'total': result.total_emails or 0
                },
                'documents': {
                    'total': result.total_documents or 0
                },
                'daily_counts': daily_counts
            }
    
    def get_recent_requests(self, org_id, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent requests - optimized with single query for cloud performance"""
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
            
            # Use raw SQL for optimal performance - single query with all joins
            query = text("""
                SELECT TOP(:limit)
                    r.request_id,
                    r.request_organization_id,
                    r.request_template_id,
                    r.request_title,
                    r.request_description,
                    r.request_current_version_id,
                    r.request_created_by,
                    r.request_published_by,
                    r.request_published_at,
                    r.request_cancelled_by,
                    r.request_cancelled_at,
                    r.request_cancellation_reason,
                    r.request_created_at,
                    r.request_modified_at,
                    s.status_value,
                    s.status_display_name,
                    u.user_username as created_by_name,
                    (SELECT TOP 1 er.emailrequest_email_id FROM emailrequests er WHERE er.emailrequest_request_id = r.request_id) as email_id,
                    (SELECT COUNT(*) FROM documents d WHERE d.document_request_id = r.request_id) as document_count,
                    (SELECT STRING_AGG(d.document_file_name, '||') FROM documents d WHERE d.document_request_id = r.request_id) as document_names_str,
                    (SELECT CASE WHEN COUNT(*) > 0 THEN ROUND(AVG(rf.requestfield_confidence) * 100, 0) ELSE 0 END
                     FROM requestfields rf
                     WHERE rf.requestfield_request_version_id = r.request_current_version_id
                       AND rf.requestfield_is_active = 1
                       AND rf.requestfield_confidence IS NOT NULL
                       AND rf.requestfield_confidence > 0
                       AND rf.requestfield_source_type IN ('document', 'email_body')
                    ) as ai_confidence
                FROM requests r
                JOIN status_types st ON r.request_status_type_id = st.status_type_id
                JOIN statuses s ON st.status_type_status_id = s.status_id
                LEFT JOIN users u ON r.request_created_by = u.user_id
                WHERE r.request_organization_id = :org_id
                ORDER BY r.request_modified_at DESC
            """)
            
            results = session.execute(query, {'org_id': org_id_int, 'limit': limit}).fetchall()
            
            return [{
                'id': row.request_id,
                'organization_id': row.request_organization_id,
                'email_id': row.email_id,
                'template_id': row.request_template_id,
                'title': row.request_title,
                'description': row.request_description,
                'status': row.status_value,
                'status_display': row.status_display_name,
                'current_version_id': row.request_current_version_id,
                'created_by': row.request_created_by,
                'created_by_name': row.created_by_name,
                'published_by': row.request_published_by,
                'published_at': row.request_published_at.isoformat() if row.request_published_at else None,
                'cancelled_by': row.request_cancelled_by,
                'cancelled_at': row.request_cancelled_at.isoformat() if row.request_cancelled_at else None,
                'cancellation_reason': row.request_cancellation_reason,
                'created_at': row.request_created_at.isoformat() if row.request_created_at else None,
                'updated_at': row.request_modified_at.isoformat() if row.request_modified_at else None,
                'document_count': row.document_count or 0,
                'document_names': row.document_names_str.split('||') if row.document_names_str else [],
                'ai_confidence': min(max(int(row.ai_confidence or 0), 0), 100)
            } for row in results]
    
    def get_pending_review_requests(self, org_id, limit: int = 10) -> List[Dict[str, Any]]:
        """Get requests pending review - optimized with single query"""
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
            
            # Optimized single query with status filter
            query = text("""
                SELECT TOP(:limit)
                    r.request_id,
                    r.request_organization_id,
                    r.request_template_id,
                    r.request_title,
                    r.request_description,
                    r.request_current_version_id,
                    r.request_created_by,
                    r.request_published_by,
                    r.request_published_at,
                    r.request_cancelled_by,
                    r.request_cancelled_at,
                    r.request_cancellation_reason,
                    r.request_created_at,
                    r.request_modified_at,
                    s.status_value,
                    s.status_display_name,
                    u.user_username as created_by_name,
                    (SELECT TOP 1 er.emailrequest_email_id FROM emailrequests er WHERE er.emailrequest_request_id = r.request_id) as email_id,
                    (SELECT COUNT(*) FROM documents d WHERE d.document_request_id = r.request_id) as document_count,
                    (SELECT STRING_AGG(d.document_file_name, '||') FROM documents d WHERE d.document_request_id = r.request_id) as document_names_str,
                    (SELECT CASE WHEN COUNT(*) > 0 THEN ROUND(AVG(rf.requestfield_confidence) * 100, 0) ELSE 0 END
                     FROM requestfields rf
                     WHERE rf.requestfield_request_version_id = r.request_current_version_id
                       AND rf.requestfield_is_active = 1
                       AND rf.requestfield_confidence IS NOT NULL
                       AND rf.requestfield_confidence > 0
                       AND rf.requestfield_source_type IN ('document', 'email_body')
                    ) as ai_confidence
                FROM requests r
                JOIN status_types st ON r.request_status_type_id = st.status_type_id
                JOIN statuses s ON st.status_type_status_id = s.status_id
                LEFT JOIN users u ON r.request_created_by = u.user_id
                WHERE r.request_organization_id = :org_id 
                  AND s.status_value IN ('extracted', 'reviewing')
                ORDER BY r.request_created_at ASC
            """)
            
            results = session.execute(query, {'org_id': org_id_int, 'limit': limit}).fetchall()
            
            return [{
                'id': row.request_id,
                'organization_id': row.request_organization_id,
                'email_id': row.email_id,
                'template_id': row.request_template_id,
                'title': row.request_title,
                'description': row.request_description,
                'status': row.status_value,
                'status_display': row.status_display_name,
                'current_version_id': row.request_current_version_id,
                'created_by': row.request_created_by,
                'created_by_name': row.created_by_name,
                'published_by': row.request_published_by,
                'published_at': row.request_published_at.isoformat() if row.request_published_at else None,
                'cancelled_by': row.request_cancelled_by,
                'cancelled_at': row.request_cancelled_at.isoformat() if row.request_cancelled_at else None,
                'cancellation_reason': row.request_cancellation_reason,
                'created_at': row.request_created_at.isoformat() if row.request_created_at else None,
                'updated_at': row.request_modified_at.isoformat() if row.request_modified_at else None,
                'document_count': row.document_count or 0,
                'document_names': row.document_names_str.split('||') if row.document_names_str else [],
                'ai_confidence': min(max(int(row.ai_confidence or 0), 0), 100)
            } for row in results]
    
    def get_processing_requests(self, org_id, limit: int = 10) -> List[Dict[str, Any]]:
        """Get currently processing requests - optimized with single query"""
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
            
            # Optimized single query with status filter
            query = text("""
                SELECT TOP(:limit)
                    r.request_id,
                    r.request_organization_id,
                    r.request_template_id,
                    r.request_title,
                    r.request_description,
                    r.request_current_version_id,
                    r.request_created_by,
                    r.request_published_by,
                    r.request_published_at,
                    r.request_cancelled_by,
                    r.request_cancelled_at,
                    r.request_cancellation_reason,
                    r.request_created_at,
                    r.request_modified_at,
                    s.status_value,
                    s.status_display_name,
                    u.user_username as created_by_name,
                    (SELECT TOP 1 er.emailrequest_email_id FROM emailrequests er WHERE er.emailrequest_request_id = r.request_id) as email_id,
                    (SELECT COUNT(*) FROM documents d WHERE d.document_request_id = r.request_id) as document_count,
                    (SELECT STRING_AGG(d.document_file_name, '||') FROM documents d WHERE d.document_request_id = r.request_id) as document_names_str,
                    (SELECT CASE WHEN COUNT(*) > 0 THEN ROUND(AVG(rf.requestfield_confidence) * 100, 0) ELSE 0 END
                     FROM requestfields rf
                     WHERE rf.requestfield_request_version_id = r.request_current_version_id
                       AND rf.requestfield_is_active = 1
                       AND rf.requestfield_confidence IS NOT NULL
                       AND rf.requestfield_confidence > 0
                       AND rf.requestfield_source_type IN ('document', 'email_body')
                    ) as ai_confidence
                FROM requests r
                JOIN status_types st ON r.request_status_type_id = st.status_type_id
                JOIN statuses s ON st.status_type_status_id = s.status_id
                LEFT JOIN users u ON r.request_created_by = u.user_id
                WHERE r.request_organization_id = :org_id 
                  AND s.status_value = 'processing'
                ORDER BY r.request_created_at ASC
            """)
            
            results = session.execute(query, {'org_id': org_id_int, 'limit': limit}).fetchall()
            
            return [{
                'id': row.request_id,
                'organization_id': row.request_organization_id,
                'email_id': row.email_id,
                'template_id': row.request_template_id,
                'title': row.request_title,
                'description': row.request_description,
                'status': row.status_value,
                'status_display': row.status_display_name,
                'current_version_id': row.request_current_version_id,
                'created_by': row.request_created_by,
                'created_by_name': row.created_by_name,
                'published_by': row.request_published_by,
                'published_at': row.request_published_at.isoformat() if row.request_published_at else None,
                'cancelled_by': row.request_cancelled_by,
                'cancelled_at': row.request_cancelled_at.isoformat() if row.request_cancelled_at else None,
                'cancellation_reason': row.request_cancellation_reason,
                'created_at': row.request_created_at.isoformat() if row.request_created_at else None,
                'updated_at': row.request_modified_at.isoformat() if row.request_modified_at else None,
                'document_count': row.document_count or 0,
                'document_names': row.document_names_str.split('||') if row.document_names_str else [],
                'ai_confidence': min(max(int(row.ai_confidence or 0), 0), 100)
            } for row in results]
    
    def get_recent_emails(self, org_id, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent emails for an organization - optimized with single query"""
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
            
            # Optimized single query with all needed data
            query = text("""
                SELECT TOP(:limit)
                    e.email_id,
                    e.email_organization_id,
                    e.email_message_id,
                    e.email_subject,
                    e.email_sender,
                    e.email_received_at,
                    e.email_body_preview,
                    e.email_created_at,
                    e.email_modified_at,
                    s.status_value,
                    s.status_display_name,
                    (SELECT COUNT(*) FROM documents d WHERE d.document_email_id = e.email_id) as attachment_count
                FROM emails e
                JOIN status_types st ON e.email_status_type_id = st.status_type_id
                JOIN statuses s ON st.status_type_status_id = s.status_id
                WHERE e.email_organization_id = :org_id
                ORDER BY e.email_created_at DESC
            """)
            
            results = session.execute(query, {'org_id': org_id_int, 'limit': limit}).fetchall()
            
            return [{
                'id': row.email_id,
                'organization_id': row.email_organization_id,
                'message_id': row.email_message_id,
                'subject': row.email_subject,
                'sender': row.email_sender,
                'received_at': row.email_received_at.isoformat() if row.email_received_at else None,
                'body_preview': row.email_body_preview,
                'status': row.status_value,
                'status_display': row.status_display_name,
                'attachment_count': row.attachment_count or 0,
                'document_count': row.attachment_count or 0,
                'created_at': row.email_created_at.isoformat() if row.email_created_at else None,
                'updated_at': row.email_modified_at.isoformat() if row.email_modified_at else None
            } for row in results]
    
    # ==========================================
    # TEMPLATE OPERATIONS
    # ==========================================
    
    def list_templates(self, org_id, include_inactive: bool = False, 
                        include_fields: bool = False) -> List[Dict[str, Any]]:
        """List all templates for an organization"""
        from sqlalchemy.orm import joinedload
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
            query = session.query(Template).filter_by(template_organization_id=org_id_int)
            
            if not include_inactive:
                query = query.filter(Template.template_is_active == True)
            
            # Eagerly load relationships needed for _template_to_dict
            query = query.options(
                joinedload(Template.analyzer),
                joinedload(Template.category_orders).joinedload(TemplateCategoryOrder.category),
                joinedload(Template.fields).joinedload(TemplateField.category),
                joinedload(Template.created_by_user),
                joinedload(Template.modified_by_user),
            )
            
            templates = query.order_by(Template.template_name).all()
            return [self._template_to_dict(session, t, include_fields=include_fields) for t in templates]

    def get_template(self, template_id) -> Optional[Dict[str, Any]]:
        """Get a template with its fields"""
        with self.get_session() as session:
            template_id_int = int(template_id) if isinstance(template_id, str) and template_id.isdigit() else template_id
            template = session.query(Template).filter_by(template_id=template_id_int).first()
            if template:
                return self._template_to_dict(session, template, include_fields=True)
            return None
    
    def create_template(self, template_id, org_id, name: str,
                        description: str = None, user_id: str = None) -> Dict[str, Any]:
        """Create a new template"""
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
            template = Template(
                template_organization_id=org_id_int,
                template_name=name,
                template_description=description,
                template_is_active=True,
                template_created_by=user_id,
                template_modified_by=user_id,
            )
            session.add(template)
            session.flush()
            
            return self._template_to_dict(session, template)
    
    def update_template(self, template_id, name: str = None, description: str = None,
                        is_active: bool = None, user_id: str = None,
                        source_documents: str = None, creation_prompt: str = None,
                        creation_method: str = None, allow_reprocessing: bool = None) -> bool:
        """Update a template"""
        with self.get_session() as session:
            template_id_int = int(template_id) if isinstance(template_id, str) and template_id.isdigit() else template_id
            template = session.query(Template).filter_by(template_id=template_id_int).first()
            if template:
                if name is not None:
                    template.template_name = name
                if description is not None:
                    template.template_description = description
                if is_active is not None:
                    template.template_is_active = is_active
                if allow_reprocessing is not None:
                    template.template_allow_reprocessing = allow_reprocessing
                
                # Update optional metadata columns via raw SQL (may not exist if migration hasn't run)
                meta_updates = {}
                if source_documents is not None:
                    meta_updates['template_source_documents'] = source_documents
                if creation_prompt is not None:
                    meta_updates['template_creation_prompt'] = creation_prompt
                if creation_method is not None:
                    meta_updates['template_creation_method'] = creation_method
                if meta_updates:
                    try:
                        set_clauses = ', '.join(f'{k} = :{k}' for k in meta_updates)
                        meta_updates['tid'] = template_id_int
                        session.execute(
                            text(f"UPDATE templates SET {set_clauses} WHERE template_id = :tid"),
                            meta_updates
                        )
                    except Exception:
                        pass  # Columns don't exist yet — silently skip
                
                if user_id:
                    template.template_modified_by = user_id
                template.template_modified_at = datetime.utcnow()
                return True
            return False
    
    def get_template_fields(self, template_id, category_name: str = None,
                            include_inactive: bool = False) -> List[Dict[str, Any]]:
        """Get all fields for a template"""
        with self.get_session() as session:
            template_id_int = int(template_id) if isinstance(template_id, str) and template_id.isdigit() else template_id
            query = session.query(TemplateField).filter_by(template_field_template_id=template_id_int)
            
            if not include_inactive:
                query = query.filter(TemplateField.template_field_is_active == True)
            
            if category_name:
                # Join with TemplateFieldCategory to filter by name
                query = query.join(TemplateFieldCategory).filter(TemplateFieldCategory.template_field_category_name == category_name)
            
            fields = query.order_by(TemplateField.template_field_category_id, TemplateField.template_field_sort_order).all()
            return [self._template_field_to_dict(f) for f in fields]
    
    def get_template_field(self, field_id) -> Optional[Dict[str, Any]]:
        """Get a single template field"""
        with self.get_session() as session:
            field_id_int = int(field_id) if isinstance(field_id, str) and field_id.isdigit() else field_id
            field = session.query(TemplateField).filter_by(template_field_id=field_id_int).first()
            if field:
                return self._template_field_to_dict(field)
            return None
    
    def create_template_field(self, field_id, template_id, field_name: str,
                              display_name: str, field_type: str = 'text', category_id = None,
                              is_required: bool = False, extraction_is_required: bool = False,
                              sort_order: int = 0,
                              description: str = None, validation_rules: str = None,
                              normalisation_instruction: str = None) -> Dict[str, Any]:
        """Create a template field"""
        with self.get_session() as session:
            template_id_int = int(template_id) if isinstance(template_id, str) and template_id.isdigit() else template_id
            
            field = TemplateField(
                template_field_template_id=template_id_int,
                template_field_category_id=category_id,
                template_field_field_name=field_name,
                template_field_display_name=display_name,
                template_field_data_type=field_type,
                template_field_is_required=is_required,
                template_field_extraction_is_required=extraction_is_required,
                template_field_is_active=True,
                template_field_sort_order=sort_order,
                template_field_field_definition=description,
                template_field_validation_rules=validation_rules,
                template_field_normalisation_instruction=normalisation_instruction
            )
            session.add(field)
            session.flush()
            return self._template_field_to_dict(field)
    
    def update_template_field(self, field_id, display_name: str = None,
                              field_type: str = None, category_id = None,
                              is_required: bool = None, extraction_is_required: bool = None,
                              is_active: bool = None,
                              sort_order: int = None, description: str = None,
                              validation_rules: str = None,
                              normalisation_instruction: str = None) -> bool:
        """Update a template field"""
        with self.get_session() as session:
            field_id_int = int(field_id) if isinstance(field_id, str) and field_id.isdigit() else field_id
            field = session.query(TemplateField).filter_by(template_field_id=field_id_int).first()
            if field:
                if display_name is not None:
                    field.template_field_display_name = display_name
                if field_type is not None:
                    field.template_field_data_type = field_type
                if category_id is not None:
                    field.template_field_category_id = category_id
                if is_required is not None:
                    field.template_field_is_required = is_required
                if extraction_is_required is not None:
                    field.template_field_extraction_is_required = extraction_is_required
                if is_active is not None:
                    field.template_field_is_active = is_active
                if sort_order is not None:
                    field.template_field_sort_order = sort_order
                if description is not None:
                    field.template_field_field_definition = description
                if validation_rules is not None:
                    field.template_field_validation_rules = validation_rules
                if normalisation_instruction is not None:
                    field.template_field_normalisation_instruction = normalisation_instruction
                
                field.template_field_modified_at = datetime.utcnow()
                return True
            return False

    # ==========================================
    # ANALYZER CRUD
    # ==========================================

    def create_analyzer(self, analyzer_id: str, org_id, name: str,
                        description: str = None, analyzer_type: str = 'azure_cu',
                        azure_analyzer_id: str = None,
                        configuration: str = None) -> Dict[str, Any]:
        """Create a new analyzer record"""
        with self.get_session() as session:
            analyzer = Analyzer(
                analyzer_id=analyzer_id,
                analyzer_organization_id=org_id,
                analyzer_name=name,
                analyzer_description=description,
                analyzer_type=analyzer_type,
                analyzer_azure_id=azure_analyzer_id,
                analyzer_configuration=configuration,
                analyzer_is_active=False,  # Not active until build succeeds
            )
            session.add(analyzer)
            session.flush()
            return self._analyzer_to_dict(analyzer)

    def get_analyzer(self, analyzer_id: str) -> Optional[Dict[str, Any]]:
        """Get a single analyzer by ID"""
        with self.get_session() as session:
            analyzer = session.query(Analyzer).filter_by(analyzer_id=analyzer_id).first()
            if analyzer:
                return self._analyzer_to_dict(analyzer)
            return None

    def get_analyzers_for_org(self, org_id, include_inactive: bool = False) -> List[Dict[str, Any]]:
        """List all analyzers for an organization"""
        with self.get_session() as session:
            query = session.query(Analyzer).filter_by(analyzer_organization_id=org_id)
            if not include_inactive:
                query = query.filter(Analyzer.analyzer_is_active == True)
            analyzers = query.order_by(Analyzer.analyzer_name).all()
            return [self._analyzer_to_dict(a) for a in analyzers]

    def update_analyzer(self, analyzer_id: str, **kwargs) -> bool:
        """
        Update an analyzer. Supported kwargs:
            name, description, azure_analyzer_id, configuration, is_active, endpoint_url
        """
        with self.get_session() as session:
            analyzer = session.query(Analyzer).filter_by(analyzer_id=analyzer_id).first()
            if not analyzer:
                return False

            field_map = {
                'name': 'analyzer_name',
                'description': 'analyzer_description',
                'azure_analyzer_id': 'analyzer_azure_id',
                'configuration': 'analyzer_configuration',
                'is_active': 'analyzer_is_active',
                'endpoint_url': 'analyzer_endpoint_url',
            }

            for key, col in field_map.items():
                if key in kwargs and kwargs[key] is not None:
                    setattr(analyzer, col, kwargs[key])

            analyzer.analyzer_modified_at = datetime.utcnow()
            return True

    def link_template_to_analyzer(self, template_id, analyzer_id: str) -> bool:
        """Link a template to an analyzer"""
        with self.get_session() as session:
            template_id_int = int(template_id) if isinstance(template_id, str) and template_id.isdigit() else template_id
            template = session.query(Template).filter_by(template_id=template_id_int).first()
            if not template:
                return False
            template.template_analyzer_id = analyzer_id
            template.template_modified_at = datetime.utcnow()
            return True

    def delete_analyzer(self, analyzer_id: str) -> bool:
        """Soft-delete an analyzer (set inactive). Also unlinks any templates."""
        with self.get_session() as session:
            analyzer = session.query(Analyzer).filter_by(analyzer_id=analyzer_id).first()
            if not analyzer:
                return False
            analyzer.analyzer_is_active = False
            analyzer.analyzer_modified_at = datetime.utcnow()
            # Unlink templates pointing to this analyzer
            session.query(Template).filter_by(template_analyzer_id=analyzer_id).update(
                {Template.template_analyzer_id: None}, synchronize_session='fetch'
            )
            return True

    def get_field_categories(self) -> List[Dict[str, Any]]:
        """Get all active template field categories"""
        with self.get_session() as session:
            cats = session.query(TemplateFieldCategory).filter_by(
                template_field_category_is_active=True
            ).order_by(TemplateFieldCategory.template_field_category_name).all()
            return [
                {
                    'id': c.template_field_category_id,
                    'name': c.template_field_category_name,
                    'display_name': c.template_field_category_display_name,
                    'description': c.template_field_category_description,
                    'icon': c.template_field_category_icon,
                    'color': c.template_field_category_color,
                }
                for c in cats
            ]

    def _analyzer_to_dict(self, analyzer: Analyzer) -> Dict[str, Any]:
        """Convert analyzer to dictionary"""
        import json
        config = None
        if analyzer.analyzer_configuration:
            try:
                config = json.loads(analyzer.analyzer_configuration)
            except (json.JSONDecodeError, TypeError):
                config = analyzer.analyzer_configuration
        return {
            'id': analyzer.analyzer_id,
            'organization_id': analyzer.analyzer_organization_id,
            'name': analyzer.analyzer_name,
            'description': analyzer.analyzer_description,
            'analyzer_type': analyzer.analyzer_type,
            'azure_analyzer_id': analyzer.analyzer_azure_id,
            'endpoint_url': analyzer.analyzer_endpoint_url,
            'configuration': config,
            'is_active': analyzer.analyzer_is_active,
            'created_at': analyzer.analyzer_created_at.isoformat() if analyzer.analyzer_created_at else None,
            'updated_at': analyzer.analyzer_modified_at.isoformat() if analyzer.analyzer_modified_at else None,
        }

    def _template_to_dict(self, session: Session, template: Template,
                          include_fields: bool = False) -> Dict[str, Any]:
        """Convert template to dictionary"""
        # Resolve creator / modifier display names
        created_by_name = None
        modified_by_name = None
        try:
            if template.created_by_user:
                created_by_name = template.created_by_user.user_username
            elif template.template_created_by:
                # Fallback: query user if relationship wasn't loaded
                u = session.query(User).filter_by(user_id=template.template_created_by).first()
                created_by_name = u.user_username if u else template.template_created_by
        except Exception:
            created_by_name = template.template_created_by
        try:
            if template.modified_by_user:
                modified_by_name = template.modified_by_user.user_username
            elif template.template_modified_by:
                u = session.query(User).filter_by(user_id=template.template_modified_by).first()
                modified_by_name = u.user_username if u else template.template_modified_by
        except Exception:
            modified_by_name = template.template_modified_by

        result = {
            'id': template.template_id,
            'organization_id': template.template_organization_id,
            'analyzer_id': template.template_analyzer_id,
            'name': template.template_name,
            'description': template.template_description,
            'is_active': template.template_is_active,
            'allow_reprocessing': getattr(template, 'template_allow_reprocessing', True),
            'created_by': template.template_created_by,
            'created_by_name': created_by_name,
            'modified_by': template.template_modified_by,
            'modified_by_name': modified_by_name,
            'created_at': template.template_created_at.isoformat() if template.template_created_at else None,
            'updated_at': template.template_modified_at.isoformat() if template.template_modified_at else None,
        }

        # Try to read optional metadata columns (may not exist if migration hasn't run)
        try:
            meta_row = session.execute(
                text("SELECT template_source_documents, template_creation_prompt, template_creation_method "
                     "FROM templates WHERE template_id = :tid"),
                {'tid': template.template_id}
            ).fetchone()
            if meta_row:
                result['source_documents'] = meta_row[0]
                result['creation_prompt'] = meta_row[1]
                result['creation_method'] = meta_row[2]
        except Exception:
            result['source_documents'] = None
            result['creation_prompt'] = None
            result['creation_method'] = None
        
        # Include analyzer info if available
        if template.template_analyzer_id and hasattr(template, 'analyzer') and template.analyzer:
            result['analyzer'] = {
                'id': template.analyzer.analyzer_id,
                'name': template.analyzer.analyzer_name,
                'analyzer_type': template.analyzer.analyzer_type,
                'azure_analyzer_id': template.analyzer.analyzer_azure_id,
                'is_active': template.analyzer.analyzer_is_active
            }
        
        if include_fields:
            fields = session.query(TemplateField).filter_by(
                template_field_template_id=template.template_id, template_field_is_active=True
            ).order_by(TemplateField.template_field_category_id, TemplateField.template_field_sort_order).all()
            
            result['fields'] = [self._template_field_to_dict(f) for f in fields]
            result['field_count'] = len(fields)
            
            # Group fields by category
            categories = {}
            category_display_names = {}
            for f in fields:
                cat_name = f.category.template_field_category_name if f.category else 'other'
                cat_display = f.category.template_field_category_display_name if f.category else 'Other Fields'
                if cat_name not in categories:
                    categories[cat_name] = []
                    category_display_names[cat_name] = cat_display
                categories[cat_name].append(self._template_field_to_dict(f))
            result['fields_by_category'] = categories
            result['category_display_names'] = category_display_names
            
            # Get category order from template_category_order table
            category_order = []
            if hasattr(template, 'category_orders') and template.category_orders:
                for co in sorted(template.category_orders, key=lambda x: x.template_category_order_sort_order):
                    if co.category:
                        category_order.append({
                            'id': co.category.template_field_category_id,
                            'name': co.category.template_field_category_name,
                            'display_name': co.category.template_field_category_display_name,
                            'description': co.category.template_field_category_description,
                            'icon': co.category.template_field_category_icon,
                            'color': co.category.template_field_category_color,
                            'sort_order': co.template_category_order_sort_order,
                            'is_visible': co.template_category_order_is_visible
                        })
            result['category_order'] = category_order
        
        return result
    
    def _template_field_to_dict(self, field: TemplateField) -> Dict[str, Any]:
        """Convert template field to dictionary"""
        return {
            'id': field.template_field_id,
            'template_id': field.template_field_template_id,
            'category_id': field.template_field_category_id,
            'category': field.category.template_field_category_name if field.category else None,
            'category_display_name': field.category.template_field_category_display_name if field.category else None,
            'category_icon': field.category.template_field_category_icon if field.category else None,
            'category_color': field.category.template_field_category_color if field.category else None,
            'field_name': field.template_field_field_name,
            'display_name': field.template_field_display_name,
            'field_type': field.template_field_data_type,
            'field_values': field.template_field_field_values,  # For dropdown/enum fields
            'precision_threshold': field.template_field_precision_threshold if hasattr(field, 'template_field_precision_threshold') else 0.60,
            'is_required': field.template_field_is_required,
            'extraction_is_required': field.template_field_extraction_is_required if hasattr(field, 'template_field_extraction_is_required') else False,
            'normalisation_instruction': field.template_field_normalisation_instruction if hasattr(field, 'template_field_normalisation_instruction') else None,
            'is_active': field.template_field_is_active,
            'sort_order': field.template_field_sort_order,
            'description': field.template_field_field_definition,
            'validation_rules': field.template_field_validation_rules,
            'created_at': field.template_field_created_at.isoformat() if field.template_field_created_at else None,
            'updated_at': field.template_field_modified_at.isoformat() if field.template_field_modified_at else None
        }
    
    # ==========================================
    # ACTIVITY FEED
    # ==========================================

    def get_activity_feed_paginated(self, org_id, page: int = 1, page_size: int = 20,
                                     search: str = None, activity_type: str = None) -> Dict[str, Any]:
        """
        Build a paginated, searchable unified activity feed.
        Returns { items: [...], total: int, page: int, page_size: int, total_pages: int }
        """
        all_items = self._build_activity_list(org_id, cap_per_query=50)

        # Filter by type
        if activity_type and activity_type != 'all':
            all_items = [a for a in all_items if a['type'] == activity_type]

        # Filter by search term
        if search:
            q = search.lower()
            all_items = [a for a in all_items if q in (a.get('title') or '').lower()]

        total = len(all_items)
        total_pages = max(1, (total + page_size - 1) // page_size)
        start = (page - 1) * page_size
        end = start + page_size

        return {
            'items': all_items[start:end],
            'total': total,
            'page': page,
            'page_size': page_size,
            'total_pages': total_pages,
        }

    def get_activity_feed(self, org_id, limit: int = 8) -> List[Dict[str, Any]]:
        """
        Build a unified activity feed from real data (dashboard widget version).
        """
        return self._build_activity_list(org_id, cap_per_query=4)[:limit]

    def _build_activity_list(self, org_id, cap_per_query: int = 4) -> List[Dict[str, Any]]:
        """
        Internal helper: gathers activity events from multiple data sources.
        Gathers different event types (publish, cancel, uploads, edits,
        extractions, new requests) and merges them into one time-sorted list.
        """
        try:
            with self.get_session() as session:
                org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
                activities = []

                # 1. Recently published (completed) requests
                try:
                    published_q = text("""
                        SELECT TOP(:cap)
                            r.request_id, r.request_title, r.request_modified_at AS event_time,
                            u.user_username AS actor_name
                        FROM requests r
                        JOIN status_types st ON r.request_status_type_id = st.status_type_id
                        JOIN statuses s ON st.status_type_status_id = s.status_id
                        LEFT JOIN users u ON r.request_modified_by = u.user_id
                        WHERE r.request_organization_id = :org_id
                          AND s.status_value = 'completed'
                        ORDER BY r.request_modified_at DESC
                    """)
                    for row in session.execute(published_q, {'org_id': org_id_int, 'cap': cap_per_query}).fetchall():
                        actor = row.actor_name or 'A user'
                        activities.append({
                            'type': 'publish',
                            'title': f"{actor} published {row.request_title}",
                            'time': row.event_time.isoformat() if row.event_time else None,
                            'entity_id': row.request_id,
                        })
                except Exception as e:
                    logger.error(f"Error fetching published requests: {e}")

                # 2. Recently cancelled requests
                try:
                    cancelled_q = text("""
                        SELECT TOP(:cap)
                            r.request_id, r.request_title, r.request_modified_at AS event_time,
                            u.user_username AS actor_name
                        FROM requests r
                        JOIN status_types st ON r.request_status_type_id = st.status_type_id
                        JOIN statuses s ON st.status_type_status_id = s.status_id
                        LEFT JOIN users u ON r.request_modified_by = u.user_id
                        WHERE r.request_organization_id = :org_id
                          AND s.status_value = 'cancelled'
                        ORDER BY r.request_modified_at DESC
                    """)
                    for row in session.execute(cancelled_q, {'org_id': org_id_int, 'cap': cap_per_query}).fetchall():
                        actor = row.actor_name or 'A user'
                        activities.append({
                            'type': 'cancel',
                            'title': f"{actor} cancelled {row.request_title}",
                            'time': row.event_time.isoformat() if row.event_time else None,
                            'entity_id': row.request_id,
                        })
                except Exception as e:
                    logger.error(f"Error fetching cancelled requests: {e}")

                # 3. Recently uploaded documents
                try:
                    docs_q = text("""
                        SELECT TOP(:cap)
                            d.document_id, d.document_file_name, d.document_created_at AS event_time,
                            r.request_title
                        FROM documents d
                        LEFT JOIN requests r ON d.document_request_id = r.request_id
                        WHERE d.document_organization_id = :org_id
                        ORDER BY d.document_created_at DESC
                    """)
                    for row in session.execute(docs_q, {'org_id': org_id_int, 'cap': cap_per_query}).fetchall():
                        target = row.request_title or row.document_file_name
                        activities.append({
                            'type': 'upload',
                            'title': f"New document uploaded: {target}",
                            'time': row.event_time.isoformat() if row.event_time else None,
                            'entity_id': row.document_id,
                        })
                except Exception as e:
                    logger.error(f"Error fetching document uploads: {e}")

                # 4. AI extraction completed (requests with status 'extracted' or 'processed')
                try:
                    extraction_q = text("""
                        SELECT TOP(:cap)
                            r.request_id, r.request_title, r.request_modified_at AS event_time
                        FROM requests r
                        JOIN status_types st ON r.request_status_type_id = st.status_type_id
                        JOIN statuses s ON st.status_type_status_id = s.status_id
                        WHERE r.request_organization_id = :org_id
                          AND s.status_value IN ('extracted', 'processed')
                        ORDER BY r.request_modified_at DESC
                    """)
                    for row in session.execute(extraction_q, {'org_id': org_id_int, 'cap': cap_per_query}).fetchall():
                        activities.append({
                            'type': 'extraction',
                            'title': f"AI extraction completed for {row.request_title}",
                            'time': row.event_time.isoformat() if row.event_time else None,
                            'entity_id': row.request_id,
                        })
                except Exception as e:
                    logger.error(f"Error fetching extraction activities: {e}")

                # 5. Requests recently edited (modified != created, in reviewing/pending status)
                try:
                    edit_q = text("""
                        SELECT TOP(:cap)
                            r.request_id, r.request_title, r.request_modified_at AS event_time,
                            u.user_username AS actor_name
                        FROM requests r
                        LEFT JOIN users u ON r.request_modified_by = u.user_id
                        JOIN status_types st ON r.request_status_type_id = st.status_type_id
                        JOIN statuses s ON st.status_type_status_id = s.status_id
                        WHERE r.request_organization_id = :org_id
                          AND s.status_value IN ('reviewing', 'pending')
                          AND r.request_modified_at > r.request_created_at
                        ORDER BY r.request_modified_at DESC
                    """)
                    for row in session.execute(edit_q, {'org_id': org_id_int, 'cap': cap_per_query}).fetchall():
                        actor = row.actor_name or 'A user'
                        activities.append({
                            'type': 'edit',
                            'title': f"{actor} edited {row.request_title}",
                            'time': row.event_time.isoformat() if row.event_time else None,
                            'entity_id': row.request_id,
                        })
                except Exception as e:
                    logger.error(f"Error fetching edited requests: {e}")

                # 6. Newly created requests (fallback so there's always something)
                try:
                    new_q = text("""
                        SELECT TOP(:cap)
                            r.request_id, r.request_title, r.request_created_at AS event_time,
                            u.user_username AS actor_name
                        FROM requests r
                        LEFT JOIN users u ON r.request_created_by = u.user_id
                        WHERE r.request_organization_id = :org_id
                        ORDER BY r.request_created_at DESC
                    """)
                    for row in session.execute(new_q, {'org_id': org_id_int, 'cap': cap_per_query}).fetchall():
                        actor = row.actor_name or 'A user'
                        activities.append({
                            'type': 'new_request',
                            'title': f"{actor} created request {row.request_title}",
                            'time': row.event_time.isoformat() if row.event_time else None,
                            'entity_id': row.request_id,
                        })
                except Exception as e:
                    logger.error(f"Error fetching new requests: {e}")

                # De-duplicate by (type, entity_id), keep latest per combo
                seen = set()
                unique = []
                for a in activities:
                    key = (a['type'], a['entity_id'])
                    if key not in seen:
                        seen.add(key)
                        unique.append(a)

                # Sort descending by time
                unique.sort(key=lambda x: x.get('time') or '', reverse=True)
                return unique
        except Exception as e:
            logger.error(f"Error building activity list: {e}", exc_info=True)
            return []

    # ==========================================
    # ISSUER STATS
    # ==========================================

    def get_top_issuers(self, org_id, limit: int = 4) -> List[Dict[str, Any]]:
        """Get top issuers by request count for the dashboard widget."""
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id
            query = text("""
                SELECT TOP(:limit)
                    r.request_issuer AS issuer_name,
                    COUNT(*) AS request_count
                FROM requests r
                WHERE r.request_organization_id = :org_id
                  AND r.request_issuer IS NOT NULL
                  AND r.request_issuer != ''
                GROUP BY r.request_issuer
                ORDER BY COUNT(*) DESC
            """)
            total_q = text("""
                SELECT COUNT(*) AS total
                FROM requests r
                WHERE r.request_organization_id = :org_id
                  AND r.request_issuer IS NOT NULL
                  AND r.request_issuer != ''
            """)
            total = session.execute(total_q, {'org_id': org_id_int}).scalar() or 1
            rows = session.execute(query, {'org_id': org_id_int, 'limit': limit}).fetchall()
            return [{
                'name': row.issuer_name,
                'request_count': row.request_count,
                'percentage': round((row.request_count / total) * 100),
            } for row in rows]

    def get_issuers_paginated(self, org_id, page: int = 1, page_size: int = 20,
                               search: str = None) -> Dict[str, Any]:
        """Get paginated list of issuers with request counts."""
        with self.get_session() as session:
            org_id_int = int(org_id) if isinstance(org_id, str) and org_id.isdigit() else org_id

            base_where = """
                WHERE r.request_organization_id = :org_id
                  AND r.request_issuer IS NOT NULL
                  AND r.request_issuer != ''
            """
            params = {'org_id': org_id_int}
            if search:
                base_where += " AND r.request_issuer LIKE :search"
                params['search'] = f'%{search}%'

            # Total distinct issuers
            count_q = text(f"""
                SELECT COUNT(DISTINCT r.request_issuer) AS cnt
                FROM requests r
                {base_where}
            """)
            total = session.execute(count_q, params).scalar() or 0

            # Total requests for percentage calc
            total_reqs_q = text(f"""
                SELECT COUNT(*) AS total
                FROM requests r
                {base_where}
            """)
            total_reqs = session.execute(total_reqs_q, params).scalar() or 1

            total_pages = max(1, (total + page_size - 1) // page_size)
            offset = (page - 1) * page_size

            data_q = text(f"""
                SELECT
                    r.request_issuer AS issuer_name,
                    COUNT(*) AS request_count,
                    MIN(r.request_created_at) AS first_seen,
                    MAX(r.request_created_at) AS last_seen
                FROM requests r
                {base_where}
                GROUP BY r.request_issuer
                ORDER BY COUNT(*) DESC
                OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """)
            params['offset'] = offset
            params['page_size'] = page_size

            rows = session.execute(data_q, params).fetchall()
            items = [{
                'name': row.issuer_name,
                'request_count': row.request_count,
                'percentage': round((row.request_count / total_reqs) * 100),
                'first_seen': row.first_seen.isoformat() if row.first_seen else None,
                'last_seen': row.last_seen.isoformat() if row.last_seen else None,
            } for row in rows]

            return {
                'items': items,
                'total': total,
                'page': page,
                'page_size': page_size,
                'total_pages': total_pages,
            }

    # ==========================================
    # AUDIT LOG OPERATIONS
    # ==========================================
    
    def _create_audit_log(self, session: Session, entity_type: str, entity_id,
                          action: str, before_state: Any, after_state: Any,
                          changed_by = None, reason: str = None,
                          changed_fields: List[str] = None):
        """Create an audit log entry (legacy method)"""
        # Build audit_json from before/after states
        audit_json = {}
        if action == 'INSERT' and after_state:
            for k, v in (after_state if isinstance(after_state, dict) else {}).items():
                audit_json[k] = {'new': v}
        elif action == 'DELETE' and before_state:
            for k, v in (before_state if isinstance(before_state, dict) else {}).items():
                audit_json[k] = {'old': v}
        elif action == 'UPDATE':
            before = before_state if isinstance(before_state, dict) else {}
            after = after_state if isinstance(after_state, dict) else {}
            for k in set(before.keys()) | set(after.keys()):
                if before.get(k) != after.get(k):
                    entry = {}
                    if k in before:
                        entry['old'] = before[k]
                    if k in after:
                        entry['new'] = after[k]
                    audit_json[k] = entry
        
        # Get parent_record_type_id
        parent_type = session.query(ParentRecordType).filter_by(
            parent_record_type_value=entity_type
        ).first()
        
        if not parent_type:
            logger.warning(f"Unknown entity type for audit: {entity_type}")
            return
        
        log = AuditLog(
            auditlog_parent_record_type_id=parent_type.parent_record_type_id,
            auditlog_entity_id=str(entity_id),
            auditlog_action=action,
            auditlog_audit_json=json.dumps(audit_json) if audit_json else '{}',
            auditlog_reason=reason,
            auditlog_created_by=str(changed_by) if changed_by else None
        )
        session.add(log)
    
    def create_audit_log_v2(self, parent_record_type_id: int, entity_id: str,
                           action: str, audit_json: Dict[str, Any],
                           created_by: str, reason: str = None,
                           ip_address: str = None, user_agent: str = None,
                           request_trace_id: str = None) -> Optional[int]:
        """
        Create an audit log entry with JSON-based change tracking.
        
        Args:
            parent_record_type_id: FK to parent_record_types
            entity_id: Primary key of the entity (as string)
            action: 'INSERT', 'UPDATE', or 'DELETE'
            audit_json: Dict of changes { "field": { "old": x, "new": y } }
            created_by: User ID who made the change
            reason: Optional reason for the change
            ip_address: Client IP address
            user_agent: Client user agent
            request_trace_id: Distributed tracing ID
            
        Returns:
            The audit log ID
        """
        with self.get_session() as session:
            log = AuditLog(
                auditlog_parent_record_type_id=parent_record_type_id,
                auditlog_entity_id=str(entity_id),
                auditlog_action=action,
                auditlog_audit_json=json.dumps(audit_json) if audit_json else '{}',
                auditlog_reason=reason,
                auditlog_created_by=str(created_by) if created_by else None,
                auditlog_ip_address=ip_address,
                auditlog_user_agent=user_agent[:500] if user_agent else None,
                auditlog_request_trace_id=request_trace_id
            )
            session.add(log)
            session.flush()
            return log.auditlog_id
    
    def get_audit_logs_v2(self, parent_record_type_id: int, entity_id: str,
                         limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get audit logs for an entity.
        
        Args:
            parent_record_type_id: FK to parent_record_types
            entity_id: Primary key of the entity (as string)
            limit: Max records to return
            
        Returns:
            List of audit log dicts with parsed audit_json
        """
        with self.get_session() as session:
            logs = session.query(AuditLog).options(
                joinedload(AuditLog.created_by_user),
                joinedload(AuditLog.parent_record_type)
            ).filter(
                AuditLog.auditlog_parent_record_type_id == parent_record_type_id,
                AuditLog.auditlog_entity_id == str(entity_id)
            ).order_by(desc(AuditLog.auditlog_created_at)).limit(limit).all()
            
            return [{
                'id': log.auditlog_id,
                'parent_record_type_id': log.auditlog_parent_record_type_id,
                'parent_record_type': log.parent_record_type.parent_record_type_value if log.parent_record_type else None,
                'entity_id': log.auditlog_entity_id,
                'action': log.auditlog_action,
                'audit_json': json.loads(log.auditlog_audit_json) if log.auditlog_audit_json else {},
                'reason': log.auditlog_reason,
                'created_by': log.auditlog_created_by,
                'created_by_name': log.created_by_user.user_username if log.created_by_user else None,
                'created_at': log.auditlog_created_at.isoformat() if log.auditlog_created_at else None,
                'ip_address': log.auditlog_ip_address,
                'user_agent': log.auditlog_user_agent,
                'request_trace_id': log.auditlog_request_trace_id
            } for log in logs]
    
    def get_audit_logs_by_entity_type(self, entity_type: str, entity_id: str,
                                      limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get audit logs by entity type name.
        
        Args:
            entity_type: Type name (e.g., 'request_field', 'request')
            entity_id: Primary key of the entity (as string)
            limit: Max records to return
        """
        with self.get_session() as session:
            parent_type = session.query(ParentRecordType).filter_by(
                parent_record_type_value=entity_type
            ).first()
            
            if not parent_type:
                return []
            
            return self.get_audit_logs_v2(
                parent_type.parent_record_type_id, entity_id, limit
            )
    
    def get_audit_logs(self, entity_type: str, entity_id,
                       limit: int = 50) -> List[Dict[str, Any]]:
        """Get audit logs for an entity (legacy compatibility method)"""
        return self.get_audit_logs_by_entity_type(entity_type, str(entity_id), limit)
    
    def get_parent_record_type_id(self, entity_type: str) -> Optional[int]:
        """Get parent_record_type_id for an entity type name"""
        with self.get_session() as session:
            parent_type = session.query(ParentRecordType).filter_by(
                parent_record_type_value=entity_type
            ).first()
            return parent_type.parent_record_type_id if parent_type else None

    def get_audit_logs_for_request_fields(self, request_id: int, 
                                          limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get all audit logs for request fields belonging to a request across ALL versions.
        This is necessary because fields have different IDs in different versions.
        
        Args:
            request_id: The request ID
            limit: Max records to return
            
        Returns:
            List of audit log dicts with parsed audit_json
        """
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and str(request_id).isdigit() else request_id
            
            # Get the parent_record_type_id for request_field (should be 4)
            parent_type = session.query(ParentRecordType).filter_by(
                parent_record_type_value='request_field'
            ).first()
            
            if not parent_type:
                return []
            
            # Get all field IDs for this request (all versions)
            field_ids = session.query(RequestField.requestfield_id).filter(
                RequestField.requestfield_request_id == req_id_int
            ).all()
            field_id_strs = [str(f[0]) for f in field_ids]
            
            if not field_id_strs:
                return []
            
            # Get audit logs for all these field IDs
            logs = session.query(AuditLog).options(
                joinedload(AuditLog.created_by_user),
                joinedload(AuditLog.parent_record_type)
            ).filter(
                AuditLog.auditlog_parent_record_type_id == parent_type.parent_record_type_id,
                AuditLog.auditlog_entity_id.in_(field_id_strs)
            ).order_by(desc(AuditLog.auditlog_created_at)).limit(limit).all()
            
            return [{
                'id': log.auditlog_id,
                'parent_record_type_id': log.auditlog_parent_record_type_id,
                'parent_record_type': log.parent_record_type.parent_record_type_value if log.parent_record_type else None,
                'entity_id': log.auditlog_entity_id,
                'action': log.auditlog_action,
                'audit_json': json.loads(log.auditlog_audit_json) if log.auditlog_audit_json else {},
                'reason': log.auditlog_reason,
                'created_by': log.auditlog_created_by,
                'created_by_name': log.created_by_user.user_username if log.created_by_user else None,
                'created_at': log.auditlog_created_at.isoformat() if log.auditlog_created_at else None,
                'ip_address': log.auditlog_ip_address,
                'user_agent': log.auditlog_user_agent,
                'request_trace_id': log.auditlog_request_trace_id
            } for log in logs]

    def get_fields_with_audit_history(self, request_id: int) -> Dict[str, bool]:
        """
        Batch check which fields in a request have audit history.
        Returns a dict mapping field_id (as string) -> True for fields that have audit logs.
        Single query instead of N individual checks.
        """
        with self.get_session() as session:
            req_id_int = int(request_id) if isinstance(request_id, str) and str(request_id).isdigit() else request_id
            
            # Get the parent_record_type_id for request_field
            parent_type = session.query(ParentRecordType).filter_by(
                parent_record_type_value='request_field'
            ).first()
            
            if not parent_type:
                return {}
            
            # Get all field IDs for this request
            field_ids = session.query(RequestField.requestfield_id).filter(
                RequestField.requestfield_request_id == req_id_int
            ).all()
            field_id_strs = [str(f[0]) for f in field_ids]
            
            if not field_id_strs:
                return {}
            
            # Single query: get distinct entity_ids that have audit logs
            fields_with_logs = session.query(
                AuditLog.auditlog_entity_id
            ).filter(
                AuditLog.auditlog_parent_record_type_id == parent_type.parent_record_type_id,
                AuditLog.auditlog_entity_id.in_(field_id_strs)
            ).distinct().all()
            
            return {row[0]: True for row in fields_with_logs}

    # ==========================================
    # METERED USAGE OPERATIONS (Marketplace Billing)
    # ==========================================

    def record_metered_usage(self, organization_id: str, subscription_id: str,
                             dimension: str, quantity: float,
                             request_id: int = None, job_id: int = None) -> Dict[str, Any]:
        """Record a usage event for marketplace metered billing.
        
        Args:
            organization_id: The org that incurred the usage
            subscription_id: The marketplace subscription to bill against
            dimension: 'pages_processed' or 'fields_normalised'
            quantity: Number of pages or fields consumed
            request_id: Optional link to the request
            job_id: Optional link to the async job
            
        Returns:
            Dict with the created usage record
        """
        with self.get_session() as session:
            # Determine current billing period (UTC month boundaries)
            now = datetime.utcnow()
            period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            # Next month start
            if now.month == 12:
                period_end = period_start.replace(year=now.year + 1, month=1)
            else:
                period_end = period_start.replace(month=now.month + 1)
            
            usage = MeteredUsage(
                metered_usage_organization_id=organization_id,
                metered_usage_subscription_id=subscription_id,
                metered_usage_dimension=dimension,
                metered_usage_quantity=quantity,
                metered_usage_request_id=request_id,
                metered_usage_job_id=job_id,
                metered_usage_period_start=period_start,
                metered_usage_period_end=period_end,
                metered_usage_reported=False,
            )
            session.add(usage)
            session.flush()
            
            return {
                'id': usage.metered_usage_id,
                'organization_id': usage.metered_usage_organization_id,
                'dimension': usage.metered_usage_dimension,
                'quantity': float(usage.metered_usage_quantity),
                'period_start': period_start.isoformat(),
                'period_end': period_end.isoformat(),
            }

    def get_unreported_usage(self, organization_id: str = None) -> List[Dict[str, Any]]:
        """Get aggregated unreported usage per org + dimension + period.
        
        Returns rows ready to be reported to the Marketplace Metering API:
        one row per (org, subscription, dimension, period).
        """
        with self.get_session() as session:
            query = session.query(
                MeteredUsage.metered_usage_organization_id,
                MeteredUsage.metered_usage_subscription_id,
                MeteredUsage.metered_usage_dimension,
                MeteredUsage.metered_usage_period_start,
                MeteredUsage.metered_usage_period_end,
                func.sum(MeteredUsage.metered_usage_quantity).label('total_quantity'),
                func.count(MeteredUsage.metered_usage_id).label('event_count'),
            ).filter(
                MeteredUsage.metered_usage_reported == False
            )
            
            if organization_id:
                query = query.filter(
                    MeteredUsage.metered_usage_organization_id == organization_id
                )
            
            query = query.group_by(
                MeteredUsage.metered_usage_organization_id,
                MeteredUsage.metered_usage_subscription_id,
                MeteredUsage.metered_usage_dimension,
                MeteredUsage.metered_usage_period_start,
                MeteredUsage.metered_usage_period_end,
            )
            
            results = []
            for row in query.all():
                results.append({
                    'organization_id': row[0],
                    'subscription_id': row[1],
                    'dimension': row[2],
                    'period_start': row[3].isoformat() if row[3] else None,
                    'period_end': row[4].isoformat() if row[4] else None,
                    'total_quantity': float(row[5]),
                    'event_count': row[6],
                })
            return results

    def mark_usage_reported(self, organization_id: str, subscription_id: str,
                            dimension: str, period_start: datetime,
                            period_end: datetime,
                            marketplace_response: str = None) -> int:
        """Mark all usage records for a given org+dimension+period as reported.
        
        Returns:
            Number of records marked as reported
        """
        with self.get_session() as session:
            now = datetime.utcnow()
            count = session.query(MeteredUsage).filter(
                MeteredUsage.metered_usage_organization_id == organization_id,
                MeteredUsage.metered_usage_subscription_id == subscription_id,
                MeteredUsage.metered_usage_dimension == dimension,
                MeteredUsage.metered_usage_period_start == period_start,
                MeteredUsage.metered_usage_period_end == period_end,
                MeteredUsage.metered_usage_reported == False,
            ).update({
                MeteredUsage.metered_usage_reported: True,
                MeteredUsage.metered_usage_reported_at: now,
                MeteredUsage.metered_usage_marketplace_response: marketplace_response,
                MeteredUsage.metered_usage_modified_at: now,
            }, synchronize_session='fetch')
            
            return count

    def get_usage_summary(self, organization_id: str = None,
                          period_start: datetime = None) -> List[Dict[str, Any]]:
        """Get usage summary for dashboard / admin view.
        
        Groups by org + dimension + period + reported status.
        """
        with self.get_session() as session:
            query = session.query(
                MeteredUsage.metered_usage_organization_id,
                MeteredUsage.metered_usage_subscription_id,
                MeteredUsage.metered_usage_dimension,
                MeteredUsage.metered_usage_period_start,
                MeteredUsage.metered_usage_period_end,
                MeteredUsage.metered_usage_reported,
                func.sum(MeteredUsage.metered_usage_quantity).label('total_quantity'),
                func.count(MeteredUsage.metered_usage_id).label('event_count'),
            )
            
            if organization_id:
                query = query.filter(
                    MeteredUsage.metered_usage_organization_id == organization_id
                )
            if period_start:
                query = query.filter(
                    MeteredUsage.metered_usage_period_start >= period_start
                )
            
            query = query.group_by(
                MeteredUsage.metered_usage_organization_id,
                MeteredUsage.metered_usage_subscription_id,
                MeteredUsage.metered_usage_dimension,
                MeteredUsage.metered_usage_period_start,
                MeteredUsage.metered_usage_period_end,
                MeteredUsage.metered_usage_reported,
            ).order_by(
                desc(MeteredUsage.metered_usage_period_start)
            )
            
            results = []
            for row in query.all():
                results.append({
                    'organization_id': row[0],
                    'subscription_id': row[1],
                    'dimension': row[2],
                    'period_start': row[3].isoformat() if row[3] else None,
                    'period_end': row[4].isoformat() if row[4] else None,
                    'reported': row[5],
                    'total_quantity': float(row[6]),
                    'event_count': row[7],
                })
            return results

    def get_active_subscription_for_org(self, organization_id: str) -> Optional[Dict[str, Any]]:
        """Get the active subscription for an organization.
        
        Used by the metering pipeline to determine which subscription
        to report usage against.
        """
        with self.get_session() as session:
            sub = session.query(Subscription).filter(
                Subscription.subscription_organization_id == organization_id,
                Subscription.subscription_status == 'active',
            ).first()
            
            if sub:
                return {
                    'id': sub.subscription_id,
                    'organization_id': sub.subscription_organization_id,
                    'plan': sub.subscription_plan,
                    'status': sub.subscription_status,
                    'marketplace_id': sub.subscription_marketplace_id,
                }
            return None


    # ── TENANT CONFIG METHODS (for multi-tenant routing) ──────────────────

    def get_tenant_config(self, org_id: str) -> Optional[Dict[str, Any]]:
        """Load tenant configuration for an organization.
        Returns None if no config exists (use shared resources)."""
        with self.get_session() as session:
            tc = session.query(TenantConfig).filter(
                TenantConfig.tenant_config_organization_id == org_id
            ).first()
            
            if tc is None:
                return None
            
            return {
                'organization_id': tc.tenant_config_organization_id,
                'status': tc.tenant_config_status,
                'db_connection_string': tc.tenant_config_db_connection_string,
                'storage_connection_string': tc.tenant_config_storage_connection_string,
                'storage_container': tc.tenant_config_storage_container,
                'cu_endpoint': tc.tenant_config_cu_endpoint,
                'cu_api_key': tc.tenant_config_cu_api_key,
                'cu_api_version': tc.tenant_config_cu_api_version,
                'openai_endpoint': tc.tenant_config_openai_endpoint,
                'openai_api_key': tc.tenant_config_openai_api_key,
                'openai_deployment': tc.tenant_config_openai_deployment,
                'region': tc.tenant_config_region,
            }

    def get_all_tenant_configs(self, status: str = 'active') -> List[Dict[str, Any]]:
        """Load all tenant configurations with the given status."""
        with self.get_session() as session:
            configs = session.query(TenantConfig).filter(
                TenantConfig.tenant_config_status == status
            ).all()
            
            return [{
                'organization_id': tc.tenant_config_organization_id,
                'status': tc.tenant_config_status,
                'db_connection_string': tc.tenant_config_db_connection_string,
                'storage_connection_string': tc.tenant_config_storage_connection_string,
                'storage_container': tc.tenant_config_storage_container,
                'cu_endpoint': tc.tenant_config_cu_endpoint,
                'cu_api_key': tc.tenant_config_cu_api_key,
                'cu_api_version': tc.tenant_config_cu_api_version,
                'openai_endpoint': tc.tenant_config_openai_endpoint,
                'openai_api_key': tc.tenant_config_openai_api_key,
                'openai_deployment': tc.tenant_config_openai_deployment,
                'region': tc.tenant_config_region,
            } for tc in configs]

    def upsert_tenant_config(self, org_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create or update a tenant configuration."""
        with self.get_session() as session:
            tc = session.query(TenantConfig).filter(
                TenantConfig.tenant_config_organization_id == org_id
            ).first()
            
            if tc is None:
                tc = TenantConfig(tenant_config_organization_id=org_id)
                session.add(tc)
            
            # Update fields
            if 'db_connection_string' in config:
                tc.tenant_config_db_connection_string = config['db_connection_string']
            if 'storage_connection_string' in config:
                tc.tenant_config_storage_connection_string = config['storage_connection_string']
            if 'storage_container' in config:
                tc.tenant_config_storage_container = config['storage_container']
            if 'cu_endpoint' in config:
                tc.tenant_config_cu_endpoint = config['cu_endpoint']
            if 'cu_api_key' in config:
                tc.tenant_config_cu_api_key = config['cu_api_key']
            if 'cu_api_version' in config:
                tc.tenant_config_cu_api_version = config['cu_api_version']
            if 'openai_endpoint' in config:
                tc.tenant_config_openai_endpoint = config['openai_endpoint']
            if 'openai_api_key' in config:
                tc.tenant_config_openai_api_key = config['openai_api_key']
            if 'openai_deployment' in config:
                tc.tenant_config_openai_deployment = config['openai_deployment']
            if 'status' in config:
                tc.tenant_config_status = config['status']
            if 'region' in config:
                tc.tenant_config_region = config['region']
            if 'notes' in config:
                tc.tenant_config_notes = config['notes']
            
            session.flush()
            return {
                'organization_id': tc.tenant_config_organization_id,
                'status': tc.tenant_config_status,
            }


    # ── ORGANIZATION BRANDING (White-labelling) ────────────────────────

    def get_organization_branding(self, org_id: str) -> Optional[Dict[str, Any]]:
        """Load branding config for an organization. Returns None if none exists."""
        with self.get_session() as session:
            b = session.query(OrganizationBranding).filter(
                OrganizationBranding.branding_organization_id == org_id
            ).first()
            if b is None:
                return None
            return {
                'organization_id': b.branding_organization_id,
                'app_name': b.branding_app_name,
                'subtitle': b.branding_subtitle,
                'logo_url': b.branding_logo_url,
                'favicon_url': b.branding_favicon_url,
                'primary_color': (b.branding_primary_color or '').strip(),
                'accent_color': (b.branding_accent_color or '').strip(),
                'login_tagline': b.branding_login_tagline,
                'apply_to_plugin': bool(getattr(b, 'branding_apply_to_plugin', False)),
                'plugin_body_text': getattr(b, 'branding_plugin_body_text', None),
                'plugin_footer_text': getattr(b, 'branding_plugin_footer_text', None),
            }

    def upsert_organization_branding(self, org_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create or update branding for an organization."""
        field_map = {
            'app_name': 'branding_app_name',
            'subtitle': 'branding_subtitle',
            'logo_url': 'branding_logo_url',
            'favicon_url': 'branding_favicon_url',
            'primary_color': 'branding_primary_color',
            'accent_color': 'branding_accent_color',
            'login_tagline': 'branding_login_tagline',
            'apply_to_plugin': 'branding_apply_to_plugin',
            'plugin_body_text': 'branding_plugin_body_text',
            'plugin_footer_text': 'branding_plugin_footer_text',
        }
        with self.get_session() as session:
            b = session.query(OrganizationBranding).filter(
                OrganizationBranding.branding_organization_id == org_id
            ).first()
            if b is None:
                b = OrganizationBranding(branding_organization_id=org_id)
                session.add(b)
            for key, val in data.items():
                col = field_map.get(key)
                if col:
                    setattr(b, col, val)
            session.flush()
            return {
                'organization_id': b.branding_organization_id,
                'app_name': b.branding_app_name,
                'subtitle': b.branding_subtitle,
                'logo_url': b.branding_logo_url,
                'favicon_url': b.branding_favicon_url,
                'primary_color': (b.branding_primary_color or '').strip(),
                'accent_color': (b.branding_accent_color or '').strip(),
                'login_tagline': b.branding_login_tagline,
                'apply_to_plugin': bool(getattr(b, 'branding_apply_to_plugin', False)),
                'plugin_body_text': getattr(b, 'branding_plugin_body_text', None),
                'plugin_footer_text': getattr(b, 'branding_plugin_footer_text', None),
            }


# Singleton instance
_db_repository = None


def get_database_repository() -> DatabaseRepository:
    """Get the database repository singleton"""
    global _db_repository
    if _db_repository is None:
        _db_repository = DatabaseRepository()
    return _db_repository
