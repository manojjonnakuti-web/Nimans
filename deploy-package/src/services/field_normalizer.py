"""
Field Normalization Service
Normalizes extracted field values based on template field data types
"""

import re
import logging
from datetime import datetime
from typing import Any, Optional, Dict, List
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


class FieldNormalizer:
    """
    Normalizes field values based on template field data types
    
    Handles normalization for:
    - text: basic string cleanup
    - date: converts various date formats to YYYY-MM-DD
    - number/integer: extracts numeric values
    - currency: format as "SYMBOL AMOUNT"
    - percentage: extracts percentage values
    - dropdown: maps to closest valid option
    - boolean: converts to true/false
    - textarea: preserves multi-line text
    """
    
    # Common currency symbols and codes
    CURRENCY_SYMBOLS = ['$', '€', '£', '¥', '₹', '¢', '₽', '₩', '₪', '฿', '₦', '₡', '₱', '﷼']
    CURRENCY_CODES = [
        'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 
        'CNY', 'INR', 'BRL', 'ZAR', 'SGD', 'HKD', 'MXN', 'SEK', 
        'NOK', 'DKK', 'PLN', 'THB', 'IDR', 'MYR', 'PHP', 'TRY', 
        'RUB', 'KRW', 'TWD', 'SAR', 'AED', 'CLP', 'COP', 'ARS'
    ]
    
    @staticmethod
    def normalize_field(
        extracted_value: Any,
        field_type: str,
        field_values: Optional[str] = None,
        field_name: str = None
    ) -> tuple[str, str]:
        """
        Normalize a field value based on its data type
        
        Args:
            extracted_value: Raw value extracted from AI
            field_type: Data type from template (text, date, currency, etc.)
            field_values: Comma-separated allowed values for dropdown fields
            field_name: Field name for logging
            
        Returns:
            Tuple of (field_value, normalized_value)
            - field_value: Used for display and editing
            - normalized_value: Standardized format for storage/search
        """
        if extracted_value is None or extracted_value == '':
            return ('', '')
        
        value_str = str(extracted_value).strip()
        if not value_str:
            return ('', '')
        
        try:
            if field_type in ('text', None):
                return FieldNormalizer._normalize_text(value_str)
            elif field_type == 'date':
                return FieldNormalizer._normalize_date(value_str)
            elif field_type in ('number', 'integer'):
                return FieldNormalizer._normalize_number(value_str)
            elif field_type in ('currency', 'money'):
                return FieldNormalizer._normalize_currency(value_str)
            elif field_type in ('percentage', 'percent'):
                return FieldNormalizer._normalize_percentage(value_str)
            elif field_type in ('dropdown', 'select', 'enum'):
                return FieldNormalizer._normalize_dropdown(value_str, field_values)
            elif field_type in ('boolean', 'bool'):
                return FieldNormalizer._normalize_boolean(value_str)
            elif field_type in ('textarea', 'text_area', 'multiline'):
                return FieldNormalizer._normalize_textarea(value_str)
            else:
                # Unknown type - treat as text
                logger.warning(f"Unknown field type '{field_type}' for field '{field_name}', treating as text")
                return FieldNormalizer._normalize_text(value_str)
        except Exception as e:
            logger.error(f"Error normalizing field '{field_name}' (type: {field_type}): {e}")
            # Return original value on error
            return (value_str, value_str)
    
    @staticmethod
    def _normalize_text(value: str) -> tuple[str, str]:
        """Normalize text field - basic cleanup"""
        # Remove extra whitespace
        normalized = ' '.join(value.split())
        return (normalized, normalized)
    
    @staticmethod
    def _normalize_date(value: str) -> tuple[str, str]:
        """
        Normalize date to YYYY-MM-DD format
        Handles various formats: MM/DD/YYYY, DD-MM-YYYY, Month DD, YYYY, etc.
        """
        # Already in correct format
        if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
            return (value, value)
        
        # Try common date formats
        date_formats = [
            '%Y-%m-%d',      # 2025-01-15
            '%m/%d/%Y',      # 01/15/2025
            '%d/%m/%Y',      # 15/01/2025
            '%m-%d-%Y',      # 01-15-2025
            '%d-%m-%Y',      # 15-01-2025
            '%B %d, %Y',     # January 15, 2025
            '%b %d, %Y',     # Jan 15, 2025
            '%d %B %Y',      # 15 January 2025
            '%d %b %Y',      # 15 Jan 2025
            '%Y/%m/%d',      # 2025/01/15
            '%m.%d.%Y',      # 01.15.2025
            '%d.%m.%Y',      # 15.01.2025
        ]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(value, fmt)
                normalized = dt.strftime('%Y-%m-%d')
                return (normalized, normalized)
            except ValueError:
                continue
        
        # Could not parse - return original
        logger.warning(f"Could not parse date: '{value}'")
        return (value, value)
    
    @staticmethod
    def _normalize_number(value: str) -> tuple[str, str]:
        """
        Normalize number - remove commas, handle decimals
        Examples: "1,000,000" -> "1000000", "1.5M" -> "1500000"
        """
        # Remove commas and spaces
        cleaned = value.replace(',', '').replace(' ', '')
        
        # Handle million/billion suffixes
        multiplier = 1
        if cleaned.upper().endswith('M'):
            multiplier = 1_000_000
            cleaned = cleaned[:-1]
        elif cleaned.upper().endswith('B'):
            multiplier = 1_000_000_000
            cleaned = cleaned[:-1]
        elif cleaned.upper().endswith('K'):
            multiplier = 1_000
            cleaned = cleaned[:-1]
        
        try:
            # Try to parse as float first
            num_value = float(cleaned) * multiplier
            
            # If it's a whole number, format without decimals
            if num_value.is_integer():
                normalized = str(int(num_value))
            else:
                normalized = str(num_value)
            
            return (normalized, normalized)
        except ValueError:
            logger.warning(f"Could not parse number: '{value}'")
            return (value, value)
    
    @staticmethod
    def _normalize_currency(value: str) -> tuple[str, str]:
        """
        Normalize currency to "SYMBOL AMOUNT" format
        If no symbol found, store as " AMOUNT" (space before number)
        
        Examples:
            "$55,000,000" -> "$ 55000000"
            "USD 1.5M" -> "USD 1500000"
            "55000000" -> " 55000000"
            "€ 100,000" -> "€ 100000"
        """
        value = value.strip()
        
        # Build currency pattern
        symbols_escaped = [re.escape(s) for s in FieldNormalizer.CURRENCY_SYMBOLS]
        pattern = r'^([' + ''.join(symbols_escaped) + r']|' + '|'.join(FieldNormalizer.CURRENCY_CODES) + r')\s*'
        
        match = re.match(pattern, value, re.IGNORECASE)
        
        if match:
            symbol = match.group(1).upper() if match.group(1).isalpha() else match.group(1)
            amount_str = value[match.end():].strip()
        else:
            # No symbol found
            symbol = ''
            amount_str = value
        
        # Normalize the amount using number normalization
        amount_normalized, _ = FieldNormalizer._normalize_number(amount_str)
        
        # Format: "SYMBOL AMOUNT" or " AMOUNT" if no symbol
        if symbol:
            field_value = f"{symbol} {amount_normalized}"
        else:
            field_value = f" {amount_normalized}"
        
        # Normalized value is the same
        return (field_value, field_value)
    
    @staticmethod
    def _normalize_percentage(value: str) -> tuple[str, str]:
        """
        Normalize percentage - extract numeric value, keep % sign
        Examples: "6.5%" -> "6.5", "650 basis points" -> "6.5"
        """
        # Remove % sign if present
        cleaned = value.replace('%', '').replace(' ', '').replace(',', '')
        
        # Handle "basis points" (1 bp = 0.01%)
        if 'basis' in value.lower() or 'bp' in value.lower() or 'bps' in value.lower():
            cleaned = re.sub(r'[^\d.]', '', cleaned)
            try:
                bp_value = float(cleaned)
                percentage = bp_value / 100
                return (str(percentage), str(percentage))
            except ValueError:
                pass
        
        # Try to extract number
        try:
            num_value = float(cleaned)
            normalized = str(num_value)
            return (normalized, normalized)
        except ValueError:
            logger.warning(f"Could not parse percentage: '{value}'")
            return (value, value)
    
    @staticmethod
    def _normalize_dropdown(value: str, field_values: Optional[str]) -> tuple[str, str]:
        """
        Normalize dropdown - map to closest valid option
        Uses fuzzy matching to find best match from allowed values
        """
        if not field_values:
            # No options defined - return as is
            return (value, value)
        
        # Parse allowed options (comma-separated)
        options = [opt.strip() for opt in field_values.split(',') if opt.strip()]
        
        if not options:
            return (value, value)
        
        # Exact match (case-insensitive)
        value_lower = value.lower()
        for option in options:
            if option.lower() == value_lower:
                return (option, option)
        
        # Fuzzy match - find closest option
        best_match = None
        best_ratio = 0.0
        
        for option in options:
            ratio = SequenceMatcher(None, value_lower, option.lower()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = option
        
        # Use fuzzy match if confidence >= 0.6
        if best_match and best_ratio >= 0.6:
            logger.info(f"Fuzzy matched '{value}' to '{best_match}' (confidence: {best_ratio:.2f})")
            return (best_match, best_match)
        
        # No good match - return original but log warning
        logger.warning(f"Could not map '{value}' to any option in: {options}")
        return (value, value)
    
    @staticmethod
    def _normalize_boolean(value: str) -> tuple[str, str]:
        """
        Normalize boolean - convert to 'true' or 'false'
        """
        value_lower = value.lower().strip()
        
        # True values
        if value_lower in ('true', 'yes', 'y', '1', 'on', 'enabled', 'active'):
            return ('true', 'true')
        
        # False values
        if value_lower in ('false', 'no', 'n', '0', 'off', 'disabled', 'inactive'):
            return ('false', 'false')
        
        # Unknown - default to original
        logger.warning(f"Could not parse boolean: '{value}'")
        return (value, value)
    
    @staticmethod
    def _normalize_textarea(value: str) -> tuple[str, str]:
        """
        Normalize textarea - preserve line breaks, clean extra whitespace
        """
        # Preserve line breaks but normalize spacing within lines
        lines = value.split('\n')
        normalized_lines = [' '.join(line.split()) for line in lines]
        normalized = '\n'.join(normalized_lines)
        return (normalized, normalized)


# Singleton instance
_normalizer = None


def get_field_normalizer() -> FieldNormalizer:
    """Get the field normalizer singleton"""
    global _normalizer
    if _normalizer is None:
        _normalizer = FieldNormalizer()
    return _normalizer
