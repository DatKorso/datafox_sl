"""
Data cleaning utilities for marketplace data import.

This module provides functions to clean and validate data before import,
handling type conversion errors gracefully and logging problematic records.
"""

import pandas as pd
import re
import streamlit as st
from typing import Tuple, List, Dict, Any


def clean_integer_field(
    series: pd.Series, 
    field_name: str, 
    identifier_series: pd.Series = None,
    identifier_name: str = "row"
) -> Tuple[pd.Series, List[Dict[str, Any]]]:
    """
    Clean a pandas series to convert values to integers, handling problematic values.
    
    Args:
        series: The pandas series to clean
        field_name: Name of the field being cleaned (for logging)
        identifier_series: Series with identifiers (like wb_sku) for logging
        identifier_name: Name of the identifier field
    
    Returns:
        Tuple of (cleaned_series, list_of_issues)
    """
    issues = []
    cleaned_series = series.copy()
    
    for idx, value in series.items():
        if pd.isna(value) or value == '' or value is None:
            cleaned_series.loc[idx] = None
            continue
            
        # Convert to string for processing
        str_value = str(value).strip()
        
        # Try direct conversion first
        try:
            cleaned_series.loc[idx] = int(float(str_value))
            continue
        except (ValueError, TypeError):
            pass
        
        # Try to extract first number from string like "рост 146" or "3998 - 4114"
        number_match = re.search(r'(\d+)', str_value)
        if number_match:
            try:
                extracted_number = int(number_match.group(1))
                cleaned_series.loc[idx] = extracted_number
                
                # Log the conversion
                identifier = identifier_series.loc[idx] if identifier_series is not None else f"row {idx}"
                issues.append({
                    'type': 'conversion_warning',
                    'field': field_name,
                    'identifier': identifier,
                    'identifier_name': identifier_name,
                    'original_value': str_value,
                    'converted_value': extracted_number,
                    'message': f"Extracted number {extracted_number} from '{str_value}'"
                })
                continue
            except (ValueError, TypeError):
                pass
        
        # If we can't convert, set to None and log the issue
        cleaned_series.loc[idx] = None
        identifier = identifier_series.loc[idx] if identifier_series is not None else f"row {idx}"
        issues.append({
            'type': 'conversion_error',
            'field': field_name,
            'identifier': identifier,
            'identifier_name': identifier_name,
            'original_value': str_value,
            'converted_value': None,
            'message': f"Could not convert '{str_value}' to integer, set to NULL"
        })
    
    return cleaned_series, issues


def clean_double_field(
    series: pd.Series, 
    field_name: str, 
    identifier_series: pd.Series = None,
    identifier_name: str = "row"
) -> Tuple[pd.Series, List[Dict[str, Any]]]:
    """
    Clean a pandas series to convert values to floats, handling problematic values.
    
    Args:
        series: The pandas series to clean
        field_name: Name of the field being cleaned (for logging)
        identifier_series: Series with identifiers (like wb_sku) for logging
        identifier_name: Name of the identifier field
    
    Returns:
        Tuple of (cleaned_series, list_of_issues)
    """
    issues = []
    cleaned_series = series.copy()
    
    for idx, value in series.items():
        if pd.isna(value) or value == '' or value is None:
            cleaned_series.loc[idx] = None
            continue
            
        # Convert to string for processing
        str_value = str(value).strip()
        
        # Try direct conversion first
        try:
            cleaned_series.loc[idx] = float(str_value)
            continue
        except (ValueError, TypeError):
            pass
        
        # Try to extract first number (including decimals) from string
        number_match = re.search(r'(\d+\.?\d*)', str_value)
        if number_match:
            try:
                extracted_number = float(number_match.group(1))
                cleaned_series.loc[idx] = extracted_number
                
                # Log the conversion
                identifier = identifier_series.loc[idx] if identifier_series is not None else f"row {idx}"
                issues.append({
                    'type': 'conversion_warning',
                    'field': field_name,
                    'identifier': identifier,
                    'identifier_name': identifier_name,
                    'original_value': str_value,
                    'converted_value': extracted_number,
                    'message': f"Extracted number {extracted_number} from '{str_value}'"
                })
                continue
            except (ValueError, TypeError):
                pass
        
        # If we can't convert, set to None and log the issue
        cleaned_series.loc[idx] = None
        identifier = identifier_series.loc[idx] if identifier_series is not None else f"row {idx}"
        issues.append({
            'type': 'conversion_error',
            'field': field_name,
            'identifier': identifier,
            'identifier_name': identifier_name,
            'original_value': str_value,
            'converted_value': None,
            'message': f"Could not convert '{str_value}' to float, set to NULL"
        })
    
    return cleaned_series, issues


def apply_data_cleaning(
    df: pd.DataFrame, 
    table_name: str, 
    schema_columns_info: List[Tuple[str, str, str, str]]
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Apply data cleaning to a DataFrame based on schema information.
    
    Args:
        df: The DataFrame to clean
        table_name: Name of the table (for identification)
        schema_columns_info: List of (target_col, sql_type, source_col, notes)
    
    Returns:
        Tuple of (cleaned_dataframe, list_of_all_issues)
    """
    all_issues = []
    cleaned_df = df.copy()
    
    # Try to find an identifier column for better logging
    identifier_series = None
    identifier_name = "row"
    
    # Look for common identifier columns
    for target_col, _, source_col, _ in schema_columns_info:
        if any(keyword in target_col.lower() for keyword in ['sku', 'id', 'vendor_code']):
            if source_col in cleaned_df.columns:
                identifier_series = cleaned_df[source_col]
                identifier_name = source_col
                break
    
    # Apply cleaning based on SQL type
    for target_col, sql_type, source_col, notes in schema_columns_info:
        if source_col not in cleaned_df.columns:
            continue
            
        if sql_type.upper() in ['INTEGER', 'BIGINT']:  # Handle both INTEGER and BIGINT
            cleaned_series, issues = clean_integer_field(
                cleaned_df[source_col], 
                source_col, 
                identifier_series, 
                identifier_name
            )
            cleaned_df[source_col] = cleaned_series
            all_issues.extend(issues)
            
        elif sql_type.upper() == 'DOUBLE':
            cleaned_series, issues = clean_double_field(
                cleaned_df[source_col], 
                source_col, 
                identifier_series, 
                identifier_name
            )
            cleaned_df[source_col] = cleaned_series
            all_issues.extend(issues)
    
    return cleaned_df, all_issues


def display_cleaning_report(issues: List[Dict[str, Any]], table_name: str):
    """
    Display a report of data cleaning issues in Streamlit.
    
    Args:
        issues: List of issue dictionaries from cleaning process
        table_name: Name of the table being processed
    """
    if not issues:
        st.success(f"✅ Все данные для таблицы '{table_name}' прошли проверку без проблем!")
        return
    
    errors = [issue for issue in issues if issue['type'] == 'conversion_error']
    warnings = [issue for issue in issues if issue['type'] == 'conversion_warning']
    
    if errors:
        st.error(f"❌ Найдено {len(errors)} ошибок конвертации для таблицы '{table_name}':")
        
        # Group errors by field
        errors_by_field = {}
        for error in errors:
            field = error['field']
            if field not in errors_by_field:
                errors_by_field[field] = []
            errors_by_field[field].append(error)
        
        for field, field_errors in errors_by_field.items():
            with st.expander(f"Ошибки в поле '{field}' ({len(field_errors)} записей)"):
                for error in field_errors[:10]:  # Show first 10 errors
                    st.write(f"• {error['identifier_name']}: **{error['identifier']}** - "
                            f"'{error['original_value']}' → NULL")
                if len(field_errors) > 10:
                    st.write(f"... и еще {len(field_errors) - 10} ошибок")
    
    if warnings:
        st.warning(f"⚠️ Найдено {len(warnings)} предупреждений для таблицы '{table_name}':")
        
        # Group warnings by field
        warnings_by_field = {}
        for warning in warnings:
            field = warning['field']
            if field not in warnings_by_field:
                warnings_by_field[field] = []
            warnings_by_field[field].append(warning)
        
        for field, field_warnings in warnings_by_field.items():
            with st.expander(f"Преобразования в поле '{field}' ({len(field_warnings)} записей)"):
                for warning in field_warnings[:10]:  # Show first 10 warnings
                    st.write(f"• {warning['identifier_name']}: **{warning['identifier']}** - "
                            f"'{warning['original_value']}' → {warning['converted_value']}")
                if len(field_warnings) > 10:
                    st.write(f"... и еще {len(field_warnings) - 10} преобразований")


def validate_required_fields(
    df: pd.DataFrame, 
    schema_columns_info: List[Tuple[str, str, str, str]]
) -> List[Dict[str, Any]]:
    """
    Validate that required fields are present and not empty.
    
    Args:
        df: The DataFrame to validate
        schema_columns_info: List of (target_col, sql_type, source_col, notes)
    
    Returns:
        List of validation issues
    """
    validation_issues = []
    
    for target_col, sql_type, source_col, notes in schema_columns_info:
        if source_col not in df.columns:
            validation_issues.append({
                'type': 'missing_column',
                'field': source_col,
                'message': f"Required column '{source_col}' not found in data"
            })
            continue
        
        # Check for completely empty columns
        non_null_count = df[source_col].notna().sum()
        if non_null_count == 0:
            validation_issues.append({
                'type': 'empty_column',
                'field': source_col,
                'message': f"Column '{source_col}' is completely empty"
            })
    
    return validation_issues 