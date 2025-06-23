"""
Helper functions for working with existing product groups.

This module provides utilities for:
- Analyzing existing groups from oz_category_products table
- Finding cross-marketplace connections for existing groups
- Managing and editing existing product groups
"""
import pandas as pd
import duckdb
from typing import List, Dict, Tuple, Optional
import streamlit as st


def get_existing_groups_statistics(conn) -> Dict:
    """
    Получает статистику по существующим группам из таблицы oz_category_products.
    
    Args:
        conn: соединение с БД
        
    Returns:
        Словарь со статистикой групп
    """
    try:
        # Получаем статистику по группам на основе merge_on_card
        stats_query = """
        SELECT 
            COUNT(*) as total_products,
            COUNT(DISTINCT merge_on_card) as total_groups,
            COUNT(DISTINCT oz_vendor_code) as unique_vendor_codes,
            COUNT(CASE WHEN merge_on_card IS NOT NULL THEN 1 END) as products_with_groups,
            COUNT(CASE WHEN oz_sku IS NOT NULL AND TRIM(CAST(oz_sku AS VARCHAR)) != '' THEN 1 END) as products_with_oz_sku
        FROM oz_category_products
        WHERE merge_on_card IS NOT NULL 
        AND TRIM(CAST(merge_on_card AS VARCHAR)) != ''
        """
        
        result = conn.execute(stats_query).fetchone()
        
        if result:
            # Получаем статистику по размерам групп
            group_sizes_query = """
            SELECT 
                merge_on_card,
                COUNT(*) as group_size
            FROM oz_category_products
            WHERE merge_on_card IS NOT NULL 
            AND TRIM(CAST(merge_on_card AS VARCHAR)) != ''
            GROUP BY merge_on_card
            HAVING COUNT(*) >= 2
            ORDER BY group_size DESC
            """
            
            group_sizes_df = conn.execute(group_sizes_query).fetchdf()
            
            # Анализ размеров групп
            if not group_sizes_df.empty:
                avg_group_size = group_sizes_df['group_size'].mean()
                max_group_size = group_sizes_df['group_size'].max()
                min_group_size = group_sizes_df['group_size'].min()
                groups_with_2_plus = len(group_sizes_df)
                
                # Категории групп по размеру
                small_groups = len(group_sizes_df[group_sizes_df['group_size'] <= 5])
                medium_groups = len(group_sizes_df[(group_sizes_df['group_size'] > 5) & (group_sizes_df['group_size'] <= 15)])
                large_groups = len(group_sizes_df[group_sizes_df['group_size'] > 15])
            else:
                avg_group_size = 0
                max_group_size = 0
                min_group_size = 0
                groups_with_2_plus = 0
                small_groups = 0
                medium_groups = 0
                large_groups = 0
            
            return {
                'total_products': result[0],
                'total_groups': result[1],
                'unique_vendor_codes': result[2],
                'products_with_groups': result[3],
                'products_with_oz_sku': result[4],
                'groups_with_2_plus': groups_with_2_plus,
                'avg_group_size': round(avg_group_size, 1) if avg_group_size > 0 else 0,
                'max_group_size': max_group_size,
                'min_group_size': min_group_size,
                'small_groups': small_groups,
                'medium_groups': medium_groups,
                'large_groups': large_groups
            }
        else:
            return {}
            
    except Exception as e:
        st.error(f"Ошибка получения статистики групп: {e}")
        return {}


def get_existing_groups_list(conn, min_group_size: int = 2) -> pd.DataFrame:
    """
    Получает список существующих групп с основной информацией.
    
    Args:
        conn: соединение с БД
        min_group_size: минимальный размер группы для отображения
        
    Returns:
        DataFrame со списком групп
    """
    try:
        query = """
        SELECT 
            merge_on_card,
            COUNT(*) as group_size,
            COUNT(DISTINCT oz_vendor_code) as unique_vendor_codes,
            COUNT(CASE WHEN oz_sku IS NOT NULL AND TRIM(CAST(oz_sku AS VARCHAR)) != '' THEN 1 END) as products_with_oz_sku,
            STRING_AGG(DISTINCT oz_vendor_code, '; ') as vendor_codes_sample,
            STRING_AGG(DISTINCT product_name, '; ') as product_names_sample
        FROM oz_category_products
        WHERE merge_on_card IS NOT NULL 
        AND TRIM(CAST(merge_on_card AS VARCHAR)) != ''
        GROUP BY merge_on_card
        HAVING COUNT(*) >= ?
        ORDER BY COUNT(*) DESC, merge_on_card
        """
        
        result_df = conn.execute(query, [min_group_size]).fetchdf()
        
        # Обрезаем длинные строки для лучшего отображения
        if not result_df.empty:
            result_df['vendor_codes_sample'] = result_df['vendor_codes_sample'].apply(
                lambda x: x[:100] + '...' if len(str(x)) > 100 else x
            )
            result_df['product_names_sample'] = result_df['product_names_sample'].apply(
                lambda x: x[:150] + '...' if len(str(x)) > 150 else x
            )
        
        return result_df
        
    except Exception as e:
        st.error(f"Ошибка получения списка групп: {e}")
        return pd.DataFrame()


def get_group_details_with_wb_connections(conn, merge_on_card: str) -> pd.DataFrame:
    """
    Получает детальную информацию о группе с попыткой найти связи с WB.
    
    ОБНОВЛЕНО: Теперь использует централизованный CrossMarketplaceLinker
    для исключения дублирования логики связывания.
    
    Args:
        conn: соединение с БД
        merge_on_card: идентификатор группы
        
    Returns:
        DataFrame с детальной информацией о группе
    """
    try:
        # Сначала получаем основные данные группы
        group_query = """
        SELECT 
            ocp.merge_on_card,
            ocp.oz_vendor_code,
            ocp.oz_sku,
            ocp.product_name,
            ocp.barcode,
            ocp.oz_actual_price,
            op.oz_fbo_stock
        FROM oz_category_products ocp
        LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
        WHERE ocp.merge_on_card = ?
        ORDER BY ocp.oz_vendor_code
        """
        
        group_df = conn.execute(group_query, [merge_on_card]).fetchdf()
        
        if group_df.empty:
            return pd.DataFrame()
        
        # Используем централизованный линкер для поиска связей с WB через штрихкоды
        from utils.cross_marketplace_linker import CrossMarketplaceLinker
        linker = CrossMarketplaceLinker(conn)
        
        # Обогащаем группу связями с WB
        group_df = linker.enrich_group_with_wb_connections(group_df, barcode_column='barcode')
        
        # Переупорядочиваем колонки для лучшего отображения
        desired_columns = ['merge_on_card', 'wb_sku', 'oz_vendor_code', 'oz_sku', 'oz_fbo_stock', 'product_name', 'barcode', 'oz_actual_price']
        available_columns = [col for col in desired_columns if col in group_df.columns]
        
        return group_df[available_columns]
        
    except Exception as e:
        st.error(f"Ошибка получения деталей группы: {e}")
        return pd.DataFrame()


def analyze_group_wb_connections(conn, merge_on_card: str) -> Dict:
    """
    Анализирует связи группы с товарами WB.
    
    Args:
        conn: соединение с БД
        merge_on_card: идентификатор группы
        
    Returns:
        Словарь с анализом связей
    """
    try:
        group_details = get_group_details_with_wb_connections(conn, merge_on_card)
        
        if group_details.empty:
            return {'error': 'Группа не найдена'}
        
        total_products = len(group_details)
        products_with_wb_sku = len(group_details[group_details['wb_sku'].notna()])
        products_with_oz_sku = len(group_details[group_details['oz_sku'].notna()])
        products_with_stock = len(group_details[group_details['oz_fbo_stock'].notna() & (group_details['oz_fbo_stock'] > 0)])
        
        # Уникальные wb_sku в группе
        unique_wb_skus = group_details['wb_sku'].dropna().nunique()
        
        # Средняя цена в группе
        avg_price = group_details['oz_actual_price'].mean() if group_details['oz_actual_price'].notna().any() else 0
        
        # Общий остаток
        total_stock = group_details['oz_fbo_stock'].sum() if group_details['oz_fbo_stock'].notna().any() else 0
        
        return {
            'total_products': total_products,
            'products_with_wb_sku': products_with_wb_sku,
            'products_with_oz_sku': products_with_oz_sku,
            'products_with_stock': products_with_stock,
            'unique_wb_skus': unique_wb_skus,
            'avg_price': round(avg_price, 2) if avg_price > 0 else 0,
            'total_stock': int(total_stock) if total_stock > 0 else 0,
            'wb_connection_rate': round((products_with_wb_sku / total_products) * 100, 1) if total_products > 0 else 0,
            'oz_sku_rate': round((products_with_oz_sku / total_products) * 100, 1) if total_products > 0 else 0
        }
        
    except Exception as e:
        return {'error': f'Ошибка анализа группы: {e}'}


def search_groups_by_criteria(conn, search_text: str = "", min_group_size: int = 2, max_group_size: int = 100) -> pd.DataFrame:
    """
    Поиск групп по различным критериям.
    
    Args:
        conn: соединение с БД
        search_text: текст для поиска в названиях товаров, vendor кодах и merge_on_card
        min_group_size: минимальный размер группы
        max_group_size: максимальный размер группы
        
    Returns:
        DataFrame с результатами поиска
    """
    try:
        # Базовый запрос
        base_query = """
        SELECT 
            merge_on_card,
            COUNT(*) as group_size,
            COUNT(DISTINCT oz_vendor_code) as unique_vendor_codes,
            STRING_AGG(DISTINCT oz_vendor_code, '; ') as vendor_codes_sample,
            STRING_AGG(DISTINCT product_name, '; ') as product_names_sample
        FROM oz_category_products
        WHERE merge_on_card IS NOT NULL 
        AND TRIM(CAST(merge_on_card AS VARCHAR)) != ''
        """
        
        params = []
        
        # Добавляем поиск по тексту если указан
        if search_text and search_text.strip():
            search_text = search_text.strip()
            base_query += """
            AND (
                LOWER(CAST(merge_on_card AS VARCHAR)) LIKE LOWER(?) 
                OR LOWER(CAST(oz_vendor_code AS VARCHAR)) LIKE LOWER(?)
                OR LOWER(CAST(product_name AS VARCHAR)) LIKE LOWER(?)
            )
            """
            search_pattern = f"%{search_text}%"
            params.extend([search_pattern, search_pattern, search_pattern])
        
        # Завершаем запрос
        base_query += """
        GROUP BY merge_on_card
        HAVING COUNT(*) >= ? AND COUNT(*) <= ?
        ORDER BY COUNT(*) DESC, merge_on_card
        """
        params.extend([min_group_size, max_group_size])
        
        result_df = conn.execute(base_query, params).fetchdf()
        
        # Обрезаем длинные строки
        if not result_df.empty:
            result_df['vendor_codes_sample'] = result_df['vendor_codes_sample'].apply(
                lambda x: x[:100] + '...' if len(str(x)) > 100 else x
            )
            result_df['product_names_sample'] = result_df['product_names_sample'].apply(
                lambda x: x[:150] + '...' if len(str(x)) > 150 else x
            )
        
        return result_df
        
    except Exception as e:
        st.error(f"Ошибка поиска групп: {e}")
        return pd.DataFrame()


def update_oz_sku_from_oz_products(conn) -> Dict:
    """
    Обновляет пустые значения oz_sku в таблице oz_category_products 
    на основе совпадений oz_vendor_code с таблицей oz_products.
    
    Args:
        conn: соединение с БД
        
    Returns:
        Словарь с результатами операции
    """
    try:
        # Сначала проверим, сколько записей нужно обновить
        check_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN ocp.oz_sku IS NULL OR TRIM(CAST(ocp.oz_sku AS VARCHAR)) = '' THEN 1 END) as empty_oz_sku,
            COUNT(CASE WHEN op.oz_sku IS NOT NULL AND TRIM(CAST(op.oz_sku AS VARCHAR)) != '' THEN 1 END) as potential_updates
        FROM oz_category_products ocp
        LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
        WHERE ocp.oz_vendor_code IS NOT NULL 
        AND TRIM(CAST(ocp.oz_vendor_code AS VARCHAR)) != ''
        """
        
        check_result = conn.execute(check_query).fetchone()
        
        if not check_result:
            return {'error': 'Не удалось получить информацию о данных'}
        
        total_records, empty_oz_sku, potential_updates = check_result
        
        if empty_oz_sku == 0:
            return {
                'success': True,
                'message': 'Все записи уже имеют заполненное поле oz_sku',
                'total_records': total_records,
                'empty_oz_sku': empty_oz_sku,
                'updated_count': 0
            }
        
        if potential_updates == 0:
            return {
                'success': True,
                'message': 'Нет совпадений с таблицей oz_products для обновления',
                'total_records': total_records,
                'empty_oz_sku': empty_oz_sku,
                'updated_count': 0
            }
        
        # Выполняем обновление
        update_query = """
        UPDATE oz_category_products 
        SET oz_sku = (
            SELECT op.oz_sku 
            FROM oz_products op 
            WHERE op.oz_vendor_code = oz_category_products.oz_vendor_code
            AND op.oz_sku IS NOT NULL 
            AND TRIM(CAST(op.oz_sku AS VARCHAR)) != ''
            LIMIT 1
        )
        WHERE (oz_category_products.oz_sku IS NULL OR TRIM(CAST(oz_category_products.oz_sku AS VARCHAR)) = '')
        AND oz_category_products.oz_vendor_code IS NOT NULL 
        AND TRIM(CAST(oz_category_products.oz_vendor_code AS VARCHAR)) != ''
        AND EXISTS (
            SELECT 1 FROM oz_products op 
            WHERE op.oz_vendor_code = oz_category_products.oz_vendor_code
            AND op.oz_sku IS NOT NULL 
            AND TRIM(CAST(op.oz_sku AS VARCHAR)) != ''
        )
        """
        
        # Выполняем обновление
        result = conn.execute(update_query)
        updated_count = result.rowcount if hasattr(result, 'rowcount') else 0
        
        # Коммитим изменения
        conn.commit()
        
        return {
            'success': True,
            'message': f'Успешно обновлено {updated_count} записей',
            'total_records': total_records,
            'empty_oz_sku': empty_oz_sku,
            'potential_updates': potential_updates,
            'updated_count': updated_count
        }
        
    except Exception as e:
        return {'error': f'Ошибка при обновлении oz_sku: {str(e)}'}


def get_oz_sku_update_statistics(conn) -> Dict:
    """
    Получает статистику для предварительного анализа обновления oz_sku.
    
    Args:
        conn: соединение с БД
        
    Returns:
        Словарь со статистикой
    """
    try:
        stats_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN ocp.oz_sku IS NULL OR TRIM(CAST(ocp.oz_sku AS VARCHAR)) = '' THEN 1 END) as empty_oz_sku,
            COUNT(CASE WHEN ocp.oz_sku IS NOT NULL AND TRIM(CAST(ocp.oz_sku AS VARCHAR)) != '' THEN 1 END) as filled_oz_sku,
            COUNT(CASE WHEN op.oz_sku IS NOT NULL AND TRIM(CAST(op.oz_sku AS VARCHAR)) != '' 
                       AND (ocp.oz_sku IS NULL OR TRIM(CAST(ocp.oz_sku AS VARCHAR)) = '') THEN 1 END) as can_be_updated,
            COUNT(DISTINCT ocp.oz_vendor_code) as unique_vendor_codes,
            COUNT(DISTINCT op.oz_vendor_code) as matching_vendor_codes_in_oz_products
        FROM oz_category_products ocp
        LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
        WHERE ocp.oz_vendor_code IS NOT NULL 
        AND TRIM(CAST(ocp.oz_vendor_code AS VARCHAR)) != ''
        """
        
        result = conn.execute(stats_query).fetchone()
        
        if result:
            return {
                'total_records': result[0],
                'empty_oz_sku': result[1],
                'filled_oz_sku': result[2],
                'can_be_updated': result[3],
                'unique_vendor_codes': result[4],
                'matching_vendor_codes_in_oz_products': result[5]
            }
        else:
            return {}
            
    except Exception as e:
        return {'error': f'Ошибка получения статистики: {str(e)}'}


def get_group_products_details(conn, merge_on_card: str) -> pd.DataFrame:
    """
    Получает список всех товаров в группе с детальной информацией.
    Отдельная функция для четкого отображения содержимого группы.
    
    Args:
        conn: соединение с БД
        merge_on_card: идентификатор группы
        
    Returns:
        DataFrame с товарами группы
    """
    try:
        # Получаем базовую информацию из oz_category_products
        query = """
        SELECT 
            ocp.merge_on_card,
            ocp.oz_vendor_code,
            ocp.oz_sku as ocp_oz_sku,
            CAST(op.oz_sku AS VARCHAR) as oz_sku,
            op.oz_fbo_stock,
            ocp.product_name,
            ocp.barcode,
            ocp.oz_actual_price
        FROM oz_category_products ocp
        LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
        WHERE ocp.merge_on_card = ?
        ORDER BY ocp.oz_vendor_code
        """
        
        result_df = conn.execute(query, [merge_on_card]).fetchdf()
        
        if result_df.empty:
            return result_df
        
        # Используем oz_sku из oz_products, если он есть, иначе из oz_category_products
        result_df['oz_sku'] = result_df['oz_sku'].fillna(result_df['ocp_oz_sku'])
        result_df = result_df.drop('ocp_oz_sku', axis=1)
        
        # Добавляем wb_sku используя полную логику поиска как на странице 5
        from utils.db_search_helpers import find_cross_marketplace_matches
        import pandas as pd
        
        result_df['wb_sku'] = None
        
        # Для каждого oz_sku в группе ищем соответствующие wb_sku
        oz_skus_to_search = result_df[result_df['oz_sku'].notna()]['oz_sku'].unique().tolist()
        
        if oz_skus_to_search:
            # Используем ту же логику поиска, что и на странице 5
            selected_fields_map = {
                'oz_sku_search': ('oz_products', 'oz_sku'),
                'wb_sku_found': ('wb_products', 'wb_sku'),
            }
            
            # Ищем wb_sku для всех oz_sku из группы
            wb_matches = find_cross_marketplace_matches(
                conn,
                search_criterion='oz_sku',
                search_values=[str(sku) for sku in oz_skus_to_search],
                selected_fields_map=selected_fields_map
            )
            
            if not wb_matches.empty:
                # Создаем словарь oz_sku -> наибольший wb_sku
                oz_to_wb_mapping = {}
                for _, match_row in wb_matches.iterrows():
                    oz_sku_val = str(match_row['oz_sku_search'])
                    wb_sku_val = str(match_row['wb_sku_found'])
                    
                    # Если для этого oz_sku уже есть wb_sku, выбираем наибольший
                    if oz_sku_val in oz_to_wb_mapping:
                        current_wb = oz_to_wb_mapping[oz_sku_val]
                        # Сравниваем как числа для выбора наибольшего
                        try:
                            if int(wb_sku_val) > int(current_wb):
                                oz_to_wb_mapping[oz_sku_val] = wb_sku_val
                        except ValueError:
                            # Если не удается сравнить как числа, оставляем текущий
                            pass
                    else:
                        oz_to_wb_mapping[oz_sku_val] = wb_sku_val
                
                # Обновляем result_df с найденными wb_sku
                for idx, row in result_df.iterrows():
                    if pd.notna(row['oz_sku']):
                        oz_sku_str = str(row['oz_sku'])
                        if oz_sku_str in oz_to_wb_mapping:
                            result_df.at[idx, 'wb_sku'] = oz_to_wb_mapping[oz_sku_str]
        
        # Переупорядочиваем колонки согласно требованиям пользователя
        desired_columns = ['merge_on_card', 'wb_sku', 'oz_vendor_code', 'oz_sku', 'oz_fbo_stock']
        available_columns = [col for col in desired_columns if col in result_df.columns]
        
        # Добавляем дополнительные информативные колонки
        additional_columns = ['product_name', 'barcode', 'oz_actual_price']
        for col in additional_columns:
            if col in result_df.columns:
                available_columns.append(col)
        
        return result_df[available_columns]
        
    except Exception as e:
        st.error(f"Ошибка получения товаров группы: {e}")
        return pd.DataFrame()
