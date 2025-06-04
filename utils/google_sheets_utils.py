"""
Google Sheets utilities for importing data from Google Sheets documents.
"""

import pandas as pd
import requests
import streamlit as st
from typing import Optional
import re


def convert_google_sheets_url_to_csv(sheets_url: str) -> Optional[str]:
    """
    Convert Google Sheets URL to CSV export URL.
    
    Args:
        sheets_url: Google Sheets URL (edit or view link)
    
    Returns:
        CSV export URL or None if conversion fails
    """
    if not sheets_url:
        return None
    
    # Extract spreadsheet ID from various Google Sheets URL formats
    patterns = [
        r'/spreadsheets/d/([a-zA-Z0-9-_]+)',
        r'key=([a-zA-Z0-9-_]+)',
        r'id=([a-zA-Z0-9-_]+)'
    ]
    
    sheet_id = None
    for pattern in patterns:
        match = re.search(pattern, sheets_url)
        if match:
            sheet_id = match.group(1)
            break
    
    if not sheet_id:
        return None
    
    # Extract gid (sheet tab) if present
    gid_match = re.search(r'gid=(\d+)', sheets_url)
    gid = gid_match.group(1) if gid_match else '0'
    
    # Construct CSV export URL
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    return csv_url


def read_google_sheets_as_dataframe(sheets_url: str) -> Optional[pd.DataFrame]:
    """
    Read Google Sheets document as pandas DataFrame.
    
    Args:
        sheets_url: Google Sheets URL
    
    Returns:
        pandas DataFrame or None if reading fails
    """
    try:
        # Convert to CSV export URL
        csv_url = convert_google_sheets_url_to_csv(sheets_url)
        if not csv_url:
            st.error("Не удалось извлечь ID документа из URL Google Sheets")
            return None
        
        # Attempt to read the CSV data with proper UTF-8 encoding
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(csv_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Try multiple encoding approaches
        df = None
        
        # Method 1: Force UTF-8 encoding on response
        try:
            response.encoding = 'utf-8'
            from io import StringIO
            csv_content = StringIO(response.text)
            df = pd.read_csv(csv_content)
        except (UnicodeDecodeError, UnicodeError):
            # Method 2: Decode bytes content as UTF-8
            try:
                content_decoded = response.content.decode('utf-8')
                csv_content = StringIO(content_decoded)
                df = pd.read_csv(csv_content)
            except (UnicodeDecodeError, UnicodeError):
                # Method 3: Try with UTF-8 BOM handling
                try:
                    content_decoded = response.content.decode('utf-8-sig')
                    csv_content = StringIO(content_decoded)
                    df = pd.read_csv(csv_content)
                except (UnicodeDecodeError, UnicodeError):
                    # Method 4: Last resort - try to handle as bytes directly
                    from io import BytesIO
                    csv_bytes = BytesIO(response.content)
                    df = pd.read_csv(csv_bytes, encoding='utf-8')
        
        # Basic validation
        if df is None or df.empty:
            st.warning("Google Sheets документ пуст или не содержит данных")
            return None
        
        return df
        
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка при загрузке Google Sheets: {e}")
        return None
    except pd.errors.EmptyDataError:
        st.error("Google Sheets документ не содержит данных")
        return None
    except UnicodeDecodeError as e:
        st.error(f"Ошибка декодирования символов: {e}. Проверьте кодировку документа.")
        return None
    except Exception as e:
        st.error(f"Неожиданная ошибка при чтении Google Sheets: {e}")
        return None


def validate_google_sheets_url(sheets_url: str) -> bool:
    """
    Validate if the provided URL is a valid Google Sheets URL.
    
    Args:
        sheets_url: URL to validate
    
    Returns:
        True if URL appears to be a valid Google Sheets URL
    """
    if not sheets_url:
        return False
    
    # Check if URL contains Google Sheets domain and required patterns
    google_domains = ['docs.google.com', 'drive.google.com']
    has_google_domain = any(domain in sheets_url for domain in google_domains)
    has_spreadsheet_pattern = 'spreadsheet' in sheets_url or '/d/' in sheets_url
    
    # Additional check: try to extract spreadsheet ID
    if has_google_domain and has_spreadsheet_pattern:
        patterns = [
            r'/spreadsheets/d/([a-zA-Z0-9-_]+)',
            r'key=([a-zA-Z0-9-_]+)',
            r'id=([a-zA-Z0-9-_]+)'
        ]
        
        import re
        for pattern in patterns:
            match = re.search(pattern, sheets_url)
            if match and len(match.group(1)) > 10:  # Google Sheets IDs are typically longer
                return True
    
    return False


def test_google_sheets_access(sheets_url: str) -> bool:
    """
    Test if Google Sheets document is accessible.
    
    Args:
        sheets_url: Google Sheets URL to test
    
    Returns:
        True if document is accessible, False otherwise
    """
    try:
        csv_url = convert_google_sheets_url_to_csv(sheets_url)
        if not csv_url:
            return False
        
        # Use GET request with timeout instead of HEAD since Google Sheets 
        # CSV export doesn't always respond correctly to HEAD requests
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(csv_url, headers=headers, timeout=10)
        # Check if we got a successful response and some content
        return response.status_code == 200 and len(response.content) > 0
        
    except Exception:
        return False


def diagnose_google_sheets_encoding(sheets_url: str) -> dict:
    """
    Diagnose encoding issues with Google Sheets document.
    Returns diagnostic information about the document and encoding.
    
    Args:
        sheets_url: Google Sheets URL to diagnose
    
    Returns:
        Dictionary with diagnostic information
    """
    result = {
        'accessible': False,
        'content_type': None,
        'encoding_detected': None,
        'sample_content': None,
        'has_cyrillic': False,
        'recommendations': []
    }
    
    try:
        csv_url = convert_google_sheets_url_to_csv(sheets_url)
        if not csv_url:
            result['recommendations'].append("Невозможно извлечь ID документа из URL")
            return result
        
        # Test accessibility
        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()
        result['accessible'] = True
        result['content_type'] = response.headers.get('content-type', 'unknown')
        
        # Test different encoding methods
        try:
            # Try UTF-8 decoding
            content_utf8 = response.content.decode('utf-8')
            result['encoding_detected'] = 'utf-8'
            result['sample_content'] = content_utf8[:200]
            
            # Check for Cyrillic characters
            import re
            cyrillic_pattern = re.compile(r'[а-яё]', re.IGNORECASE)
            result['has_cyrillic'] = bool(cyrillic_pattern.search(content_utf8))
            
        except UnicodeDecodeError:
            try:
                # Try UTF-8 with BOM
                content_utf8_sig = response.content.decode('utf-8-sig')
                result['encoding_detected'] = 'utf-8-sig'
                result['sample_content'] = content_utf8_sig[:200]
            except UnicodeDecodeError:
                try:
                    # Try Latin-1 (might show mojibake)
                    content_latin1 = response.content.decode('latin-1')
                    result['encoding_detected'] = 'latin-1 (may cause mojibake)'
                    result['sample_content'] = content_latin1[:200]
                except UnicodeDecodeError:
                    result['encoding_detected'] = 'unknown'
                    result['sample_content'] = str(response.content[:200])
        
        # Add recommendations based on findings
        if result['has_cyrillic'] and result['encoding_detected'] == 'utf-8':
            result['recommendations'].append("✅ Кириллица корректно обнаружена в UTF-8")
        elif result['encoding_detected'] == 'latin-1 (may cause mojibake)':
            result['recommendations'].append("⚠️ Возможны проблемы с кодировкой. Убедитесь, что Google Sheets сохранен в UTF-8")
        elif not result['has_cyrillic']:
            result['recommendations'].append("ℹ️ Кириллические символы не обнаружены в первых 200 символах")
            
    except Exception as e:
        result['recommendations'].append(f"❌ Ошибка диагностики: {e}")
    
    return result 