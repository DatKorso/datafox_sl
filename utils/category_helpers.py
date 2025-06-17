"""
Helper functions for category comparison and management.

This module provides functionality to:
- Extract unique categories from marketplace data
- Suggest category mappings based on similarity
- Validate category mappings
- Generate category analytics reports
"""
import pandas as pd
import streamlit as st
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Optional

def get_unique_wb_categories(db_conn) -> List[str]:
    """
    Gets all unique WB categories from wb_products table.
    
    Args:
        db_conn: Database connection
        
    Returns:
        List of unique WB categories
    """
    try:
        query = """
        SELECT DISTINCT wb_category 
        FROM wb_products 
        WHERE wb_category IS NOT NULL 
            AND TRIM(wb_category) != ''
        ORDER BY wb_category
        """
        result_df = db_conn.execute(query).fetchdf()
        return result_df['wb_category'].tolist()
    except Exception as e:
        st.error(f"Ошибка при получении категорий WB: {e}")
        return []

def get_unique_oz_categories(db_conn) -> List[str]:
    """
    Gets all unique Ozon categories from oz_category_products table.
    
    Args:
        db_conn: Database connection
        
    Returns:
        List of unique Ozon categories
    """
    try:
        query = """
        SELECT DISTINCT type as oz_category 
        FROM oz_category_products 
        WHERE type IS NOT NULL 
            AND TRIM(type) != ''
        ORDER BY type
        """
        result_df = db_conn.execute(query).fetchdf()
        return result_df['oz_category'].tolist()
    except Exception as e:
        st.error(f"Ошибка при получении категорий Ozon: {e}")
        return []

def calculate_similarity(str1: str, str2: str) -> float:
    """
    Calculates similarity between two strings using SequenceMatcher.
    
    Args:
        str1: First string
        str2: Second string
        
    Returns:
        Similarity ratio (0.0 to 1.0)
    """
    return SequenceMatcher(None, str1.lower().strip(), str2.lower().strip()).ratio()

def suggest_category_mappings(db_conn, similarity_threshold: float = 0.7) -> List[Dict]:
    """
    Suggests category mappings based on string similarity.
    
    Args:
        db_conn: Database connection
        similarity_threshold: Minimum similarity threshold for suggestions
        
    Returns:
        List of suggested mappings with similarity scores
    """
    try:
        wb_categories = get_unique_wb_categories(db_conn)
        oz_categories = get_unique_oz_categories(db_conn)
        
        suggestions = []
        
        for wb_cat in wb_categories:
            best_matches = []
            
            for oz_cat in oz_categories:
                similarity = calculate_similarity(wb_cat, oz_cat)
                if similarity >= similarity_threshold:
                    best_matches.append({
                        'oz_category': oz_cat,
                        'similarity': similarity
                    })
            
            # Sort by similarity and take top matches
            best_matches.sort(key=lambda x: x['similarity'], reverse=True)
            
            for match in best_matches[:3]:  # Top 3 matches
                suggestions.append({
                    'wb_category': wb_cat,
                    'oz_category': match['oz_category'],
                    'similarity': match['similarity'],
                    'confidence': 'High' if match['similarity'] > 0.9 else 'Medium' if match['similarity'] > 0.8 else 'Low'
                })
        
        return suggestions
        
    except Exception as e:
        st.error(f"Ошибка при генерации предложений: {e}")
        return []

def get_unmapped_categories(db_conn) -> Dict[str, List[str]]:
    """
    Gets categories that don't have mappings yet.
    
    Args:
        db_conn: Database connection
        
    Returns:
        Dictionary with 'wb' and 'oz' keys containing unmapped categories
    """
    try:
        # Get all categories
        wb_categories = set(get_unique_wb_categories(db_conn))
        oz_categories = set(get_unique_oz_categories(db_conn))
        
        # Get mapped categories
        mapped_query = "SELECT DISTINCT wb_category, oz_category FROM category_mapping"
        mapped_df = db_conn.execute(mapped_query).fetchdf()
        
        if not mapped_df.empty:
            mapped_wb = set(mapped_df['wb_category'].tolist())
            mapped_oz = set(mapped_df['oz_category'].tolist())
        else:
            mapped_wb = set()
            mapped_oz = set()
        
        # Find unmapped
        unmapped_wb = list(wb_categories - mapped_wb)
        unmapped_oz = list(oz_categories - mapped_oz)
        
        return {
            'wb': sorted(unmapped_wb),
            'oz': sorted(unmapped_oz)
        }
        
    except Exception as e:
        st.error(f"Ошибка при поиске несопоставленных категорий: {e}")
        return {'wb': [], 'oz': []}

def get_category_usage_stats(db_conn) -> Dict[str, pd.DataFrame]:
    """
    Gets usage statistics for categories (how many products in each category).
    
    Args:
        db_conn: Database connection
        
    Returns:
        Dictionary with WB and Ozon category usage statistics
    """
    try:
        # WB category usage
        wb_query = """
        SELECT 
            wb_category, 
            COUNT(*) as product_count,
            COUNT(DISTINCT wb_sku) as unique_skus
        FROM wb_products 
        WHERE wb_category IS NOT NULL 
            AND TRIM(wb_category) != ''
        GROUP BY wb_category 
        ORDER BY product_count DESC
        """
        wb_stats = db_conn.execute(wb_query).fetchdf()
        
        # Ozon category usage
        oz_query = """
        SELECT 
            type as oz_category, 
            COUNT(*) as product_count,
            COUNT(DISTINCT oz_sku) as unique_skus
        FROM oz_category_products 
        WHERE type IS NOT NULL 
            AND TRIM(type) != ''
        GROUP BY type 
        ORDER BY product_count DESC
        """
        oz_stats = db_conn.execute(oz_query).fetchdf()
        
        return {
            'wb': wb_stats,
            'oz': oz_stats
        }
        
    except Exception as e:
        st.error(f"Ошибка при получении статистики категорий: {e}")
        return {'wb': pd.DataFrame(), 'oz': pd.DataFrame()}

def validate_category_mapping(db_conn, wb_category: str, oz_category: str) -> Dict[str, any]:
    """
    Validates a category mapping by checking if categories exist and have products.
    
    Args:
        db_conn: Database connection
        wb_category: WB category to validate
        oz_category: Ozon category to validate
        
    Returns:
        Dictionary with validation results
    """
    try:
        result = {
            'valid': True,
            'warnings': [],
            'wb_exists': False,
            'oz_exists': False,
            'wb_product_count': 0,
            'oz_product_count': 0
        }
        
        # Check WB category
        wb_check_query = """
        SELECT COUNT(*) as count, COUNT(DISTINCT wb_sku) as unique_skus
        FROM wb_products 
        WHERE wb_category = ?
        """
        wb_result = db_conn.execute(wb_check_query, [wb_category]).fetchone()
        
        if wb_result and wb_result[0] > 0:
            result['wb_exists'] = True
            result['wb_product_count'] = wb_result[1]
        else:
            result['warnings'].append(f"Категория WB '{wb_category}' не найдена в товарах")
        
        # Check Ozon category
        oz_check_query = """
        SELECT COUNT(*) as count, COUNT(DISTINCT oz_sku) as unique_skus
        FROM oz_category_products 
        WHERE type = ?
        """
        oz_result = db_conn.execute(oz_check_query, [oz_category]).fetchone()
        
        if oz_result and oz_result[0] > 0:
            result['oz_exists'] = True
            result['oz_product_count'] = oz_result[1]
        else:
            result['warnings'].append(f"Категория Ozon '{oz_category}' не найдена в товарах")
        
        # Overall validation
        if not result['wb_exists'] or not result['oz_exists']:
            result['valid'] = False
        
        return result
        
    except Exception as e:
        return {
            'valid': False,
            'warnings': [f"Ошибка валидации: {e}"],
            'wb_exists': False,
            'oz_exists': False,
            'wb_product_count': 0,
            'oz_product_count': 0
        }

def export_category_mappings_to_csv(db_conn, include_stats: bool = True) -> Optional[str]:
    """
    Exports category mappings to CSV format.
    
    Args:
        db_conn: Database connection
        include_stats: Whether to include usage statistics
        
    Returns:
        CSV content as string or None if error
    """
    try:
        if include_stats:
            query = """
            SELECT 
                cm.wb_category,
                cm.oz_category,
                cm.created_at,
                cm.notes,
                COALESCE(wb_stats.product_count, 0) as wb_products,
                COALESCE(oz_stats.product_count, 0) as oz_products
            FROM category_mapping cm
            LEFT JOIN (
                SELECT wb_category, COUNT(DISTINCT wb_sku) as product_count
                FROM wb_products 
                GROUP BY wb_category
            ) wb_stats ON cm.wb_category = wb_stats.wb_category
            LEFT JOIN (
                SELECT type as oz_category, COUNT(DISTINCT oz_sku) as product_count
                FROM oz_category_products 
                GROUP BY type
            ) oz_stats ON cm.oz_category = oz_stats.oz_category
            ORDER BY cm.wb_category
            """
        else:
            query = """
            SELECT wb_category, oz_category, created_at, notes
            FROM category_mapping 
            ORDER BY wb_category
            """
        
        df = db_conn.execute(query).fetchdf()
        return df.to_csv(index=False)
        
    except Exception as e:
        st.error(f"Ошибка при экспорте: {e}")
        return None

def import_category_mappings_from_csv(db_conn, csv_content: str) -> Dict[str, int]:
    """
    Imports category mappings from CSV content.
    
    Args:
        db_conn: Database connection
        csv_content: CSV content as string
        
    Returns:
        Dictionary with import statistics
    """
    try:
        from io import StringIO
        
        df = pd.read_csv(StringIO(csv_content))
        
        required_columns = ['wb_category', 'oz_category']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Отсутствуют обязательные колонки: {missing_columns}")
        
        stats = {
            'total_rows': len(df),
            'successful_imports': 0,
            'skipped_existing': 0,
            'errors': 0
        }
        
        for _, row in df.iterrows():
            wb_cat = str(row['wb_category']).strip()
            oz_cat = str(row['oz_category']).strip()
            notes = str(row.get('notes', '')).strip() if pd.notna(row.get('notes')) else ''
            
            if not wb_cat or not oz_cat:
                stats['errors'] += 1
                continue
            
            # Check if mapping already exists
            check_query = "SELECT id FROM category_mapping WHERE wb_category = ? AND oz_category = ?"
            existing = db_conn.execute(check_query, [wb_cat, oz_cat]).fetchone()
            
            if existing:
                stats['skipped_existing'] += 1
            else:
                # Insert new mapping
                insert_query = """
                INSERT INTO category_mapping (wb_category, oz_category, notes) 
                VALUES (?, ?, ?)
                """
                db_conn.execute(insert_query, [wb_cat, oz_cat, notes])
                stats['successful_imports'] += 1
        
        return stats
        
    except Exception as e:
        st.error(f"Ошибка при импорте: {e}")
        return {
            'total_rows': 0,
            'successful_imports': 0,
            'skipped_existing': 0,
            'errors': 1
        } 