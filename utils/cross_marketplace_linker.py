"""
Централизованный модуль для связывания артикулов между маркетплейсами через штрихкоды.

Основная цель: исключить дублирование логики связывания wb_sku <-> oz_sku,
которая используется в множестве мест проекта.

Автор: DataFox SL Project
Версия: 1.0.0
"""

import pandas as pd
import streamlit as st
import duckdb
from typing import List, Dict, Optional, Tuple, Any
from utils.db_search_helpers import get_normalized_wb_barcodes, get_ozon_barcodes_and_identifiers


class CrossMarketplaceLinker:
    """
    Класс для создания и управления связями между товарами разных маркетплейсов
    на основе общих штрихкодов.
    """
    
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        """
        Инициализация линкера.
        
        Args:
            connection: Активное соединение с базой данных DuckDB
        """
        self.connection = connection
        
    def _normalize_and_merge_barcodes(
        self, 
        wb_skus: Optional[List[str]] = None,
        oz_skus: Optional[List[str]] = None,
        oz_vendor_codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Центральная логика объединения штрихкодов WB и Ozon.
        
        Args:
            wb_skus: Список WB SKU для фильтрации (если None - все)
            oz_skus: Список Ozon SKU для фильтрации (если None - все)
            oz_vendor_codes: Список Ozon vendor codes для фильтрации
            
        Returns:
            DataFrame с колонками: wb_sku, oz_sku, oz_vendor_code, oz_product_id, barcode, barcode_position, is_primary_barcode
        """
        try:
            # Получаем нормализованные штрихкоды WB с позициями
            wb_barcodes_df = get_normalized_wb_barcodes(self.connection, wb_skus=wb_skus)
            if wb_barcodes_df.empty:
                return pd.DataFrame()
            
            # Получаем штрихкоды и идентификаторы Ozon  
            oz_barcodes_df = get_ozon_barcodes_and_identifiers(
                self.connection, 
                oz_skus=oz_skus,
                oz_vendor_codes=oz_vendor_codes
            )
            if oz_barcodes_df.empty:
                return pd.DataFrame()
            
            # Стандартизируем названия колонок для объединения
            wb_barcodes_df = wb_barcodes_df.rename(columns={'individual_barcode_wb': 'barcode'})
            oz_barcodes_df = oz_barcodes_df.rename(columns={'oz_barcode': 'barcode'})
            
            # Нормализуем данные для надежного объединения
            wb_barcodes_df['barcode'] = wb_barcodes_df['barcode'].astype(str).str.strip()
            oz_barcodes_df['barcode'] = oz_barcodes_df['barcode'].astype(str).str.strip()
            wb_barcodes_df['wb_sku'] = wb_barcodes_df['wb_sku'].astype(str)
            oz_barcodes_df['oz_sku'] = oz_barcodes_df['oz_sku'].astype(str)
            
            # Убираем пустые штрихкоды и дубликаты (используем централизованную утилиту)
            from utils.data_cleaning import DataCleaningUtils
            
            # Для поиска по oz_vendor_codes используем более мягкую очистку
            if oz_vendor_codes is not None:
                # Мягкая очистка - только пустые штрихкоды, но НЕ удаляем дубликаты по sku+barcode
                wb_barcodes_df = wb_barcodes_df[
                    wb_barcodes_df['barcode'].notna() & 
                    (wb_barcodes_df['barcode'].astype(str).str.strip() != '')
                ].drop_duplicates()  # Только полные дубликаты
                
                oz_barcodes_df = oz_barcodes_df[
                    oz_barcodes_df['barcode'].notna() & 
                    (oz_barcodes_df['barcode'].astype(str).str.strip() != '')
                ].drop_duplicates()  # Только полные дубликаты
            else:
                # Обычная агрессивная очистка для остальных случаев
                # Но сохраняем информацию о позиции
                barcode_position_backup = wb_barcodes_df[['wb_sku', 'barcode', 'barcode_position']].copy()
                wb_barcodes_df_clean, oz_barcodes_df = DataCleaningUtils.clean_marketplace_data(
                    wb_barcodes_df.drop('barcode_position', axis=1), oz_barcodes_df
                )
                # Восстанавливаем информацию о позиции
                wb_barcodes_df = wb_barcodes_df_clean.merge(
                    barcode_position_backup, 
                    on=['wb_sku', 'barcode'], 
                    how='left'
                )
            
            # Объединяем по общим штрихкодам
            linked_df = pd.merge(wb_barcodes_df, oz_barcodes_df, on='barcode', how='inner')
            
            # ИСПРАВЛЕННАЯ ЛОГИКА: Определяем актуальный штрихкод среди СОВПАДАЮЩИХ
            if not linked_df.empty:
                # Шаг 1: Для каждого oz_vendor_code среди СОВПАДАЮЩИХ штрихкодов находим максимальную позицию
                if 'oz_barcode_position' in linked_df.columns:
                    # Находим максимальную позицию среди совпадающих штрихкодов для каждого oz_vendor_code
                    max_matching_oz_positions = linked_df.groupby('oz_vendor_code')['oz_barcode_position'].max().reset_index()
                    max_matching_oz_positions.columns = ['oz_vendor_code', 'max_matching_oz_position']
                    
                    # Добавляем информацию о максимальной позиции среди совпадающих
                    linked_df = linked_df.merge(max_matching_oz_positions, on='oz_vendor_code', how='left')
                    
                    # Шаг 2: Помечаем штрихкоды с максимальной позицией среди совпадающих как актуальные для Ozon
                    linked_df['is_actual_oz_barcode'] = (
                        linked_df['oz_barcode_position'] == linked_df['max_matching_oz_position']
                    )
                else:
                    # Если нет информации о позиции Ozon, считаем все совпадающие штрихкоды актуальными
                    linked_df['is_actual_oz_barcode'] = True
                
                # Шаг 3: Определяем актуальность для каждой связи wb_sku-oz_vendor_code
                if 'barcode_position' in linked_df.columns:
                    # Для каждой комбинации wb_sku + oz_vendor_code определяем актуальность
                    wb_oz_combinations = linked_df[['wb_sku', 'oz_vendor_code']].drop_duplicates()
                    
                    linked_df['is_primary_barcode'] = False  # Инициализация
                    
                    for _, combo in wb_oz_combinations.iterrows():
                        wb_sku = combo['wb_sku']
                        oz_vendor_code = combo['oz_vendor_code']
                        
                        # Фильтруем связи для этой комбинации
                        combo_links = linked_df[
                            (linked_df['wb_sku'] == wb_sku) & 
                            (linked_df['oz_vendor_code'] == oz_vendor_code)
                        ].copy()
                        
                        # Проверяем, есть ли среди связей актуальный штрихкод Ozon
                        has_actual_oz = combo_links['is_actual_oz_barcode'].any()
                        
                        if has_actual_oz:
                            # Если есть актуальный штрихкод Ozon, находим первый в WB среди актуальных
                            actual_oz_links = combo_links[combo_links['is_actual_oz_barcode']]
                            min_wb_position = actual_oz_links['barcode_position'].min()
                            
                            # Помечаем как актуальные только связи с актуальным штрихкодом Ozon и минимальной позицией WB
                            mask = (
                                (linked_df['wb_sku'] == wb_sku) & 
                                (linked_df['oz_vendor_code'] == oz_vendor_code) &
                                (linked_df['is_actual_oz_barcode']) &
                                (linked_df['barcode_position'] == min_wb_position)
                            )
                            linked_df.loc[mask, 'is_primary_barcode'] = True
                else:
                    # Если нет информации о позиции WB, учитываем только актуальность Ozon
                    linked_df['is_primary_barcode'] = linked_df['is_actual_oz_barcode']
            else:
                # Если нет совпадений, возвращаем пустой DataFrame
                linked_df['is_primary_barcode'] = False
            
            return linked_df
            
        except Exception as e:
            st.error(f"Ошибка при объединении штрихкодов: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)  # Кэш на 5 минут
    def link_wb_to_oz(_self, wb_skus: List[str]) -> Dict[str, List[str]]:
        """
        Находит связанные Ozon SKU для заданных WB SKU.
        
        Args:
            wb_skus: Список WB SKU
            
        Returns:
            Словарь {wb_sku: [список связанных oz_sku]}
        """
        if not wb_skus:
            return {}
        
        # Валидируем и очищаем входные данные
        wb_skus_clean = [str(sku).strip() for sku in wb_skus if str(sku).strip()]
        if not wb_skus_clean:
            return {}
        
        linked_df = _self._normalize_and_merge_barcodes(wb_skus=wb_skus_clean)
        if linked_df.empty:
            return {}
        
        # Группируем по wb_sku и собираем уникальные oz_sku
        result = {}
        for wb_sku in wb_skus_clean:
            oz_skus = linked_df[linked_df['wb_sku'] == wb_sku]['oz_sku'].unique().tolist()
            if oz_skus:
                result[wb_sku] = oz_skus
                
        return result
    
    @st.cache_data(ttl=300)
    def link_oz_to_wb(_self, oz_skus: List[str]) -> Dict[str, List[str]]:
        """
        Находит связанные WB SKU для заданных Ozon SKU.
        
        Args:
            oz_skus: Список Ozon SKU
            
        Returns:
            Словарь {oz_sku: [список связанных wb_sku]}
        """
        if not oz_skus:
            return {}
        
        oz_skus_clean = [str(sku).strip() for sku in oz_skus if str(sku).strip()]
        if not oz_skus_clean:
            return {}
        
        linked_df = _self._normalize_and_merge_barcodes(oz_skus=oz_skus_clean)
        if linked_df.empty:
            return {}
        
        result = {}
        for oz_sku in oz_skus_clean:
            wb_skus = linked_df[linked_df['oz_sku'] == oz_sku]['wb_sku'].unique().tolist()
            if wb_skus:
                result[oz_sku] = wb_skus
                
        return result
    
    def get_bidirectional_links(
        self, 
        wb_skus: Optional[List[str]] = None, 
        oz_skus: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Возвращает полную таблицу связей между WB и Ozon SKU.
        
        Args:
            wb_skus: Опциональный фильтр по WB SKU
            oz_skus: Опциональный фильтр по Ozon SKU
            
        Returns:
            DataFrame с колонками: wb_sku, oz_sku, oz_vendor_code, oz_product_id, common_barcode
        """
        wb_skus_clean = None
        if wb_skus:
            wb_skus_clean = [str(sku).strip() for sku in wb_skus if str(sku).strip()]
            
        oz_skus_clean = None  
        if oz_skus:
            oz_skus_clean = [str(sku).strip() for sku in oz_skus if str(sku).strip()]
        
        linked_df = self._normalize_and_merge_barcodes(
            wb_skus=wb_skus_clean,
            oz_skus=oz_skus_clean
        )
        
        if linked_df.empty:
            return pd.DataFrame()
        
        # Переименовываем колонку для ясности
        result_df = linked_df.rename(columns={'barcode': 'common_barcode'})
        
        # ИСПРАВЛЕНИЕ: Более агрессивное удаление дубликатов 
        # Удаляем дубликаты сначала по основным связям wb_sku-oz_sku
        result_df = result_df.drop_duplicates(subset=['wb_sku', 'oz_sku'], keep='first')
        
        # Затем удаляем дубликаты по более широкому набору колонок  
        from utils.data_cleaning import DataCleaningUtils
        result_df = DataCleaningUtils.remove_duplicates_by_columns(
            result_df, 
            subset_columns=['wb_sku', 'oz_sku', 'oz_vendor_code'], 
            keep='first'
        )
        
        return result_df[['wb_sku', 'oz_sku', 'oz_vendor_code', 'oz_product_id', 'common_barcode']]
    
    def get_links_with_ozon_ratings(self, wb_skus: List[str]) -> pd.DataFrame:
        """
        Получает связанные Ozon SKU с их рейтингами для заданных WB SKU.
        
        ИСПРАВЛЕНИЕ: Теперь возвращает все wb_sku с связями Ozon, даже если нет рейтингов.
        Это устраняет проблему "товаров без связей с товарами Ozon" когда связи есть, но рейтингов нет.
        
        Args:
            wb_skus: Список WB SKU
            
        Returns:
            DataFrame с колонками: wb_sku, avg_rating, oz_sku_count, oz_skus_list,
                                 total_reviews, min_rating, max_rating
        """
        if not wb_skus:
            return pd.DataFrame()
        
        linked_df = self.get_bidirectional_links(wb_skus=wb_skus)
        if linked_df.empty:
            return pd.DataFrame()
        
        # Получаем рейтинги для найденных oz_sku, исключая товары с браком
        oz_skus_for_rating = linked_df['oz_sku'].unique().tolist()
        
        try:
            oz_skus_str = ', '.join([f"'{sku}'" for sku in oz_skus_for_rating])
            
            # НОВОЕ: Исключаем товары с браком (oz_vendor_code начинается с "БракSH") из расчета рейтингов
            rating_query = f"""
            SELECT 
                r.oz_sku,
                r.rating,
                r.rev_number
            FROM oz_card_rating r
            INNER JOIN oz_products p ON CAST(r.oz_sku AS VARCHAR) = CAST(p.oz_sku AS VARCHAR)
            WHERE r.oz_sku IN ({oz_skus_str})
            AND (p.oz_vendor_code IS NULL OR NOT p.oz_vendor_code LIKE 'БракSH%')
            """
            
            ratings_df = self.connection.execute(rating_query).fetchdf()
            
            # ИСПРАВЛЕНИЕ: Не возвращаем пустой DataFrame если нет рейтингов!
            # Вместо этого продолжаем с пустыми рейтингами для всех wb_sku с связями
            
            # Группируем по wb_sku и вычисляем агрегированные рейтинги или создаем записи без рейтингов
            result_list = []
            
            for wb_sku in linked_df['wb_sku'].unique():
                wb_links = linked_df[linked_df['wb_sku'] == wb_sku]
                oz_skus_list = wb_links['oz_sku'].unique().tolist()
                
                # Если есть рейтинги для этого wb_sku, используем их
                if not ratings_df.empty:
                    linked_df['oz_sku'] = linked_df['oz_sku'].astype(str)
                    ratings_df['oz_sku'] = ratings_df['oz_sku'].astype(str)
                    
                    wb_ratings = pd.merge(wb_links, ratings_df, on='oz_sku', how='left')
                    ratings = wb_ratings['rating'].dropna()
                    reviews = wb_ratings['rev_number'].dropna()
                    
                    result_list.append({
                        'wb_sku': wb_sku,
                        'avg_rating': ratings.mean() if not ratings.empty else None,
                        'oz_sku_count': len(oz_skus_list),
                        'oz_skus_list': ', '.join(oz_skus_list),
                        'total_reviews': reviews.sum() if not reviews.empty else 0,
                        'min_rating': ratings.min() if not ratings.empty else None,
                        'max_rating': ratings.max() if not ratings.empty else None
                    })
                else:
                    # Нет рейтингов, но есть связи - создаем запись с пустыми рейтингами
                    result_list.append({
                        'wb_sku': wb_sku,
                        'avg_rating': None,
                        'oz_sku_count': len(oz_skus_list),
                        'oz_skus_list': ', '.join(oz_skus_list),
                        'total_reviews': 0,
                        'min_rating': None,
                        'max_rating': None
                    })
            
            return pd.DataFrame(result_list)
            
        except Exception as e:
            st.error(f"Ошибка получения рейтингов: {e}")
            # В случае ошибки все равно возвращаем wb_sku с связями, но без рейтингов
            result_list = []
            for wb_sku in linked_df['wb_sku'].unique():
                wb_links = linked_df[linked_df['wb_sku'] == wb_sku]
                oz_skus_list = wb_links['oz_sku'].unique().tolist()
                
                result_list.append({
                    'wb_sku': wb_sku,
                    'avg_rating': None,
                    'oz_sku_count': len(oz_skus_list),
                    'oz_skus_list': ', '.join(oz_skus_list),
                    'total_reviews': 0,
                    'min_rating': None,
                    'max_rating': None
                })
            
            return pd.DataFrame(result_list)
    
    def get_links_with_categories(
        self, 
        input_skus: List[str], 
        search_by: str = "wb_sku"
    ) -> pd.DataFrame:
        """
        Находит связанные товары с информацией о категориях.
        
        Заменяет функцию find_linked_products_with_categories из pages/9_🔄_Сверка_Категорий_OZ.py
        
        Args:
            input_skus: Список SKU для поиска
            search_by: "wb_sku" или "oz_sku"
            
        Returns:
            DataFrame со связанными товарами и их категориями:
            - wb_sku, oz_sku, oz_vendor_code, barcode (общий штрихкод)
            - wb_category, wb_brand, oz_category
        """
        if not input_skus:
            return pd.DataFrame()
        
        try:
            if search_by == "wb_sku":
                # Получаем базовые связи
                linked_df = self.get_bidirectional_links(wb_skus=input_skus)
                if linked_df.empty:
                    return pd.DataFrame()
                
                # Добавляем категории и бренды WB
                wb_skus_list = linked_df['wb_sku'].unique().tolist()
                wb_categories_query = f"""
                SELECT wb_sku, wb_category, wb_brand 
                FROM wb_products 
                WHERE wb_sku IN ({', '.join(['?'] * len(wb_skus_list))})
                """
                wb_categories_df = self.connection.execute(wb_categories_query, wb_skus_list).fetchdf()
                wb_categories_df['wb_sku'] = wb_categories_df['wb_sku'].astype(str)
                
                # Добавляем категории Ozon через oz_category_products + oz_products
                oz_vendor_codes_list = linked_df['oz_vendor_code'].unique().tolist()
                oz_categories_query = f"""
                SELECT DISTINCT
                    cp.oz_vendor_code, 
                    cp.type as oz_category,
                    p.oz_sku
                FROM oz_category_products cp
                LEFT JOIN oz_products p ON cp.oz_vendor_code = p.oz_vendor_code
                WHERE cp.oz_vendor_code IN ({', '.join(['?'] * len(oz_vendor_codes_list))})
                """
                oz_categories_df = self.connection.execute(oz_categories_query, oz_vendor_codes_list).fetchdf()
                if 'oz_sku' in oz_categories_df.columns:
                    oz_categories_df['oz_sku'] = oz_categories_df['oz_sku'].astype(str)
                
                # Объединяем все данные
                result_df = linked_df[['wb_sku', 'oz_sku', 'oz_vendor_code', 'common_barcode']].drop_duplicates()
                result_df = result_df.rename(columns={'common_barcode': 'barcode'})
                result_df = pd.merge(result_df, wb_categories_df, on='wb_sku', how='left')
                result_df = pd.merge(result_df, oz_categories_df, on='oz_vendor_code', how='left')
                
                # Обновляем oz_sku из категорийной таблицы если он более точный
                if 'oz_sku_y' in result_df.columns:
                    result_df['oz_sku'] = result_df['oz_sku_y'].fillna(result_df['oz_sku_x'])
                    result_df = result_df.drop(columns=['oz_sku_x', 'oz_sku_y'], errors='ignore')
                
            else:  # search_by == "oz_sku"
                # Поиск от Ozon к WB
                oz_skus_for_query = list(set(input_skus))
                
                # Получаем Ozon товары с категориями и штрихкодами
                oz_query = f"""
                SELECT DISTINCT
                    p.oz_sku,
                    p.oz_vendor_code,
                    cp.type as oz_category,
                    b.oz_barcode as barcode
                FROM oz_products p
                LEFT JOIN (
                    SELECT DISTINCT oz_vendor_code, type 
                    FROM oz_category_products 
                    WHERE type IS NOT NULL
                ) cp ON p.oz_vendor_code = cp.oz_vendor_code
                LEFT JOIN oz_barcodes b ON p.oz_product_id = b.oz_product_id
                WHERE p.oz_sku IN ({', '.join(['?'] * len(oz_skus_for_query))})
                    AND NULLIF(TRIM(b.oz_barcode), '') IS NOT NULL
                """
                
                oz_data_df = self.connection.execute(oz_query, oz_skus_for_query).fetchdf()
                
                if oz_data_df.empty:
                    return pd.DataFrame()
                
                # Получаем WB товары с теми же штрихкодами
                # Используем нормализованные штрихкоды WB
                wb_barcodes_df = self._get_normalized_wb_barcodes_for_all()
                
                if wb_barcodes_df.empty:
                    return pd.DataFrame()
                
                # Подготавливаем данные для объединения
                oz_data_df['barcode'] = oz_data_df['barcode'].astype(str).str.strip()
                wb_barcodes_df['barcode'] = wb_barcodes_df['individual_barcode_wb'].astype(str).str.strip()
                oz_data_df['oz_sku'] = oz_data_df['oz_sku'].astype(str)
                wb_barcodes_df['wb_sku'] = wb_barcodes_df['wb_sku'].astype(str)
                
                # Убираем пустые штрихкоды
                oz_data_df = oz_data_df[oz_data_df['barcode'] != ''].drop_duplicates()
                wb_barcodes_df = wb_barcodes_df[wb_barcodes_df['barcode'] != ''].drop_duplicates()
                
                # Объединяем по общим штрихкодам
                result_df = pd.merge(
                    oz_data_df, 
                    wb_barcodes_df[['wb_sku', 'wb_category', 'wb_brand', 'barcode']], 
                    on='barcode', 
                    how='inner'
                )
                
            # Убираем дубликаты по уникальным комбинациям товаров
            if not result_df.empty:
                result_df = result_df.drop_duplicates(
                    subset=['wb_sku', 'oz_sku', 'oz_vendor_code'], 
                    keep='first'
                )
            
            return result_df
            
        except Exception as e:
            st.error(f"Ошибка получения категорий: {e}")
            return pd.DataFrame()

    def get_links_with_sizes(
        self, 
        wb_skus: List[str]
    ) -> Dict[str, Dict[int, List[str]]]:
        """
        Возвращает связи WB -> Ozon, сгруппированные по размерам WB.
        
        Заменяет функцию map_wb_to_ozon_by_size из utils/analytic_report_helpers.py
        
        Args:
            wb_skus: Список WB SKU
            
        Returns:
            Словарь вида: wb_sku -> размер -> [список oz_sku]
        """
        if not wb_skus:
            return {}
        
        try:
            # Получаем WB продукты с их размерами и штрихкодами
            wb_skus_for_query = list(set(wb_skus))  # Убираем дубликаты
            wb_query = f"""
            SELECT 
                p.wb_sku,
                p.wb_size,
                TRIM(b.barcode) AS individual_barcode
            FROM wb_products p,
            UNNEST(regexp_split_to_array(COALESCE(p.wb_barcodes, ''), E'[\\s;]+')) AS b(barcode)
            WHERE p.wb_sku IN ({', '.join(['?'] * len(wb_skus_for_query))})
                AND NULLIF(TRIM(b.barcode), '') IS NOT NULL
            """
            
            wb_data_df = self.connection.execute(wb_query, wb_skus_for_query).fetchdf()
            
            if wb_data_df.empty:
                st.warning("Не найдены WB продукты с валидными штрихкодами")
                return {}
            
            # Получаем все Ozon продукты и их штрихкоды
            oz_query = """
            SELECT DISTINCT
                p.oz_sku,
                b.oz_barcode
            FROM oz_products p
            JOIN oz_barcodes b ON p.oz_product_id = b.oz_product_id
            WHERE NULLIF(TRIM(b.oz_barcode), '') IS NOT NULL
            """
            
            oz_data_df = self.connection.execute(oz_query).fetchdf()
            
            if oz_data_df.empty:
                st.warning("Не найдены Ozon продукты с валидными штрихкодами")
                return {}
            
            # Объединяем WB и Ozon данные по общим штрихкодам
            wb_data_df = wb_data_df.rename(columns={'individual_barcode': 'barcode'})
            oz_data_df = oz_data_df.rename(columns={'oz_barcode': 'barcode'})
            
            # Обеспечиваем согласованность типов данных
            wb_data_df['barcode'] = wb_data_df['barcode'].astype(str).str.strip()
            oz_data_df['barcode'] = oz_data_df['barcode'].astype(str).str.strip()
            wb_data_df['wb_sku'] = wb_data_df['wb_sku'].astype(str)
            oz_data_df['oz_sku'] = oz_data_df['oz_sku'].astype(str)
            
            # Очищаем пустые штрихкоды
            wb_data_df = wb_data_df[wb_data_df['barcode'] != ''].drop_duplicates()
            oz_data_df = oz_data_df[oz_data_df['barcode'] != ''].drop_duplicates()
            
            # Объединяем по общим штрихкодам
            matched_df = pd.merge(wb_data_df, oz_data_df, on='barcode', how='inner')
            
            if matched_df.empty:
                st.info("Не найдено совпадений по штрихкодам между WB и Ozon")
                return {}
            
            # Группируем по wb_sku и wb_size для построения результирующего словаря
            result_map = {}
            
            for _, row in matched_df.iterrows():
                wb_sku = str(row['wb_sku'])
                wb_size = None
                
                # Пытаемся получить целочисленный размер из wb_size
                if pd.notna(row['wb_size']):
                    try:
                        wb_size = int(float(row['wb_size']))
                    except (ValueError, TypeError):
                        # Если wb_size не числовой, пропускаем эту запись
                        continue
                
                oz_sku = str(row['oz_sku'])
                
                if wb_sku not in result_map:
                    result_map[wb_sku] = {}
                
                if wb_size is not None:
                    if wb_size not in result_map[wb_sku]:
                        result_map[wb_sku][wb_size] = []
                    if oz_sku not in result_map[wb_sku][wb_size]:
                        result_map[wb_sku][wb_size].append(oz_sku)
            
            return result_map
            
        except Exception as e:
            st.error(f"Ошибка при сопоставлении WB и Ozon размеров: {e}")
            return {}

    def _get_normalized_wb_barcodes_for_all(self) -> pd.DataFrame:
        """Получает нормализованные штрихкоды WB для всех товаров."""
        try:
            # Получаем сырые штрихкоды WB
            wb_raw_query = """
            SELECT DISTINCT wb_sku, wb_barcodes, wb_category, wb_brand
            FROM wb_products 
            WHERE NULLIF(TRIM(wb_barcodes), '') IS NOT NULL
            """
            wb_raw_df = self.connection.execute(wb_raw_query).fetchdf()
            
            # Нормализуем штрихкоды путем разделения по точке с запятой
            wb_barcodes_data = []
            for _, row in wb_raw_df.iterrows():
                wb_sku = str(row['wb_sku'])
                wb_category = row['wb_category']
                wb_brand = row['wb_brand']
                barcodes_str = str(row['wb_barcodes'])
                # Разделяем по точке с запятой и очищаем каждый штрихкод
                individual_barcodes = [b.strip() for b in barcodes_str.split(';') if b.strip()]
                for barcode in individual_barcodes:
                    wb_barcodes_data.append({
                        'wb_sku': wb_sku, 
                        'individual_barcode_wb': barcode,
                        'wb_category': wb_category,
                        'wb_brand': wb_brand
                    })
            
            return pd.DataFrame(wb_barcodes_data)
            
        except Exception as e:
            st.error(f"Ошибка нормализации штрихкодов WB: {e}")
            return pd.DataFrame()
    
    def get_extended_links(
        self, 
        wb_skus: List[str], 
        include_product_details: bool = True
    ) -> pd.DataFrame:
        """
        Возвращает расширенные связи WB -> Ozon с дополнительными полями продуктов.
        
        Заменяет часть логики get_linked_ozon_skus_with_details из pages/7_🎯_Менеджер_Рекламы_OZ.py
        
        Args:
            wb_skus: Список WB SKU
            include_product_details: Включать ли дополнительную информацию о продуктах
            
        Returns:
            DataFrame с колонками: wb_sku, oz_sku, oz_vendor_code, oz_product_id, 
                                 common_barcode, и дополнительными полями если include_product_details=True
        """
        if not wb_skus:
            return pd.DataFrame()
        
        # Получаем базовые связи
        linked_df = self.get_bidirectional_links(wb_skus=wb_skus)
        if linked_df.empty:
            return pd.DataFrame()
        
        if not include_product_details:
            return linked_df
        
        try:
            # Добавляем информацию о продуктах Ozon
            oz_skus_for_query = linked_df['oz_sku'].unique().tolist()
            oz_skus_str = ', '.join([f"'{sku}'" for sku in oz_skus_for_query])
            
            product_details_query = f"""
            SELECT 
                oz_sku,
                oz_brand,
                oz_product_status,
                oz_actual_price,
                oz_fbo_stock
            FROM oz_products 
            WHERE oz_sku IN ({oz_skus_str})
            """
            
            product_details_df = self.connection.execute(product_details_query).fetchdf()
            if not product_details_df.empty:
                product_details_df['oz_sku'] = product_details_df['oz_sku'].astype(str)
                linked_df = pd.merge(linked_df, product_details_df, on='oz_sku', how='left')
            
            return linked_df
            
        except Exception as e:
            st.error(f"Ошибка получения расширенных связей: {e}")
            return linked_df  # Возвращаем базовые связи в случае ошибки

    def find_marketplace_matches(
        self,
        search_criterion: str,
        search_values: List[str], 
        selected_fields_map: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Универсальный поиск между маркетплейсами с выбираемыми полями.
        
        Заменяет функцию find_cross_marketplace_matches из utils/db_search_helpers.py
        
        Args:
            search_criterion: Критерий поиска ('wb_sku', 'oz_sku', 'oz_vendor_code', 'barcode')
            search_values: Список значений для поиска
            selected_fields_map: Словарь полей для отображения {label: (table, column) или special_id}
            
        Returns:
            DataFrame с результатами поиска, колонки названы по ключам selected_fields_map
        """
        if not search_values:
            st.info("Не указаны значения для поиска")
            return pd.DataFrame()
        
        if not selected_fields_map:
            st.warning("Не выбраны поля для отображения")
            return pd.DataFrame()
        
        try:
            # Получаем базовые связи в зависимости от критерия поиска
            if search_criterion == 'wb_sku':
                # ИСПРАВЛЕНИЕ: Используем только _normalize_and_merge_barcodes для wb_sku
                # Это избегает дублирования от двойного вызова методов
                linked_df = self._normalize_and_merge_barcodes(wb_skus=search_values)
                if 'barcode' in linked_df.columns:
                    linked_df = linked_df.rename(columns={'barcode': 'common_barcode'})
                search_column = 'wb_sku'
            elif search_criterion == 'oz_sku':
                linked_df = self.get_bidirectional_links(oz_skus=search_values)
                search_column = 'oz_sku'
                # Для Ozon SKU поиска также добавляем информацию об актуальном штрихкоде
                enhanced_df = self._normalize_and_merge_barcodes(oz_skus=search_values)
                if not enhanced_df.empty:
                    linked_df = linked_df.merge(
                        enhanced_df[['wb_sku', 'oz_sku', 'barcode', 'is_primary_barcode']],
                        on=['wb_sku', 'oz_sku'],
                        how='left',
                        suffixes=('', '_enhanced')
                    )
                    if 'barcode_enhanced' in linked_df.columns:
                        linked_df['common_barcode'] = linked_df['barcode_enhanced']
                        linked_df = linked_df.drop('barcode_enhanced', axis=1)
            elif search_criterion == 'oz_vendor_code':
                linked_df = self._normalize_and_merge_barcodes(oz_vendor_codes=search_values)
                # Переименовываем колонку для единообразия с другими методами
                if 'barcode' in linked_df.columns:
                    linked_df = linked_df.rename(columns={'barcode': 'common_barcode'})
                search_column = 'oz_vendor_code'
            elif search_criterion == 'barcode':
                # Специальная обработка для поиска по штрихкоду
                return self._search_by_barcode(search_values, selected_fields_map)
            else:
                st.error(f"Неподдерживаемый критерий поиска: {search_criterion}")
                return pd.DataFrame()
            
            if linked_df.empty:
                return pd.DataFrame()
            
            # Создаем результирующий DataFrame
            result_data = []
            
            # Для поиска по oz_vendor_code убеждаемся, что включены все введенные значения
            if search_criterion == 'oz_vendor_code':
                # Фильтруем только те строки, которые соответствуют введенным vendor_codes
                # Это гарантирует, что каждый введенный код поставщика будет в результатах
                linked_df = linked_df[linked_df['oz_vendor_code'].isin(search_values)]
            
            
            for _, row in linked_df.iterrows():
                result_row = {}
                
                # Добавляем значение поиска как первую колонку
                result_row["Search_Value"] = row[search_column]
                
                # Обрабатываем каждое выбранное поле
                for field_label, field_detail in selected_fields_map.items():
                    if field_label == "Search_Value":
                        continue
                    
                    if field_detail == "common_matched_barcode":
                        result_row[field_label] = row.get('common_barcode', row.get('barcode', ''))
                    elif field_detail == "is_primary_barcode":
                        # Новое поле для отображения актуального штрихкода
                        is_primary = row.get('is_primary_barcode', False)
                        result_row[field_label] = "Да" if is_primary else "Нет"
                    elif isinstance(field_detail, tuple):
                        table_alias, column_name = field_detail
                        
                        # Получаем значение из уже имеющихся данных или делаем дополнительный запрос
                        if table_alias == 'oz_products' and column_name in ['oz_sku', 'oz_vendor_code', 'oz_product_id']:
                            result_row[field_label] = row.get(column_name, '')
                        elif table_alias == 'wb_products' and column_name == 'wb_sku':
                            result_row[field_label] = row.get('wb_sku', '')
                        elif table_alias == 'oz_barcodes' and column_name == 'oz_barcode':
                            result_row[field_label] = row.get('common_barcode', row.get('barcode', ''))
                        elif table_alias == 'oz_category_products':
                            # Дополнительный запрос для получения данных из oz_category_products
                            try:
                                query = f"SELECT {column_name} FROM oz_category_products WHERE oz_vendor_code = ?"
                                cat_result = self.connection.execute(query, [row.get('oz_vendor_code')]).fetchdf()
                                result_row[field_label] = str(cat_result.iloc[0, 0]) if not cat_result.empty else ''
                            except Exception:
                                result_row[field_label] = ''
                        else:
                            result_row[field_label] = self._get_additional_field_data(
                                row, table_alias, column_name
                            )
                    else:
                        result_row[field_label] = ''
                result_data.append(result_row)
            
            results_df = pd.DataFrame(result_data)
            
            # ИСПРАВЛЕНИЕ: Удаление дубликатов в результатах поиска
            if not results_df.empty:
                if search_criterion == 'oz_vendor_code':
                    # При поиске по vendor_code НЕ удаляем дубликаты по wb_sku,
                    # так как пользователь хочет видеть каждый введенный vendor_code
                    # Удаляем только полные дубликаты строк
                    results_df = results_df.drop_duplicates()
                else:
                    # ИСПРАВЛЕННАЯ ЛОГИКА для wb_sku: сохраняем все уникальные oz_vendor_code
                    if search_criterion == 'wb_sku':
                        # Найти название колонки актуального штрихкода из selected_fields_map
                        primary_barcode_column = None
                        for field_label, field_detail in selected_fields_map.items():
                            if field_detail == "is_primary_barcode":
                                primary_barcode_column = field_label
                                break
                        
                        # Найти название колонки oz_vendor_code
                        oz_vendor_col = None
                        for field_label, field_detail in selected_fields_map.items():
                            if isinstance(field_detail, tuple) and field_detail == ("oz_products", "oz_vendor_code"):
                                oz_vendor_col = field_label
                                break
                        
                        if primary_barcode_column and primary_barcode_column in results_df.columns and oz_vendor_col and oz_vendor_col in results_df.columns:
                            # ИСПРАВЛЕНИЕ: Показываем ВСЕ уникальные oz_vendor_code для каждого wb_sku
                            # Но для каждой комбинации wb_sku + oz_vendor_code берем лучшую запись
                            
                            def select_best_record_per_group(group):
                                """Выбирает лучшую запись в группе: приоритет "Да", иначе первую"""
                                yes_records = group[group[primary_barcode_column] == 'Да']
                                if not yes_records.empty:
                                    return yes_records.iloc[0:1]  # Берем первую "Да" запись
                                else:
                                    return group.iloc[0:1]  # Если нет "Да", берем первую
                            
                            # Группируем по wb_sku + oz_vendor_code и выбираем лучшую запись для каждой комбинации
                            # Это сохранит все уникальные oz_vendor_code для каждого wb_sku
                            grouped_results = []
                            for (search_value, oz_vendor_code), group in results_df.groupby(['Search_Value', oz_vendor_col]):
                                best_record = select_best_record_per_group(group)
                                grouped_results.append(best_record)
                            
                            # Объединяем все лучшие записи
                            if grouped_results:
                                results_df = pd.concat(grouped_results, ignore_index=True)
                            else:
                                results_df = pd.DataFrame()
                        elif primary_barcode_column and primary_barcode_column in results_df.columns:
                            # Если есть колонка актуального штрихкода, но нет oz_vendor_code
                            # Сортируем по актуальности и удаляем дубликаты только по Search_Value
                            results_df = (results_df
                                        .sort_values(primary_barcode_column, ascending=False)  # "Да" перед "Нет"
                                        .drop_duplicates(subset=['Search_Value'], keep='first'))
                        else:
                            # Если нет колонки актуального штрихкода, просто удаляем полные дубликаты
                            results_df = results_df.drop_duplicates()
                    else:
                        # Для других случаев просто удаляем полные дубликаты
                        results_df = results_df.drop_duplicates()
            
            return results_df
            
        except Exception as e:
            st.error(f"Ошибка поиска между маркетплейсами: {e}")
            return pd.DataFrame()
    
    def _search_by_barcode(self, barcodes: List[str], selected_fields_map: Dict[str, Any]) -> pd.DataFrame:
        """Специализированный поиск по штрихкодам."""
        try:
            clean_barcodes = [str(barcode).strip() for barcode in barcodes if str(barcode).strip()]
            if not clean_barcodes:
                return pd.DataFrame()

            result_data = []

            for barcode in clean_barcodes:
                # Ищем по штрихкоду в обеих системах
                # ИСПРАВЛЕНО: Добавляем все поля wb_products для поиска по штрихкоду
                wb_query = f"""
                WITH split_barcodes AS (
                    SELECT 
                        p.wb_sku,
                        p.wb_barcodes,
                        p.wb_category,
                        p.wb_brand,
                        UNNEST(string_split(p.wb_barcodes, ';')) AS individual_barcode
                    FROM wb_products p
                )
                SELECT wb_sku, wb_barcodes, wb_category, wb_brand, TRIM(individual_barcode) as barcode
                FROM split_barcodes
                WHERE TRIM(individual_barcode) = ?
                """

                # ОБНОВЛЕНО: Используем новую функцию для получения позиций штрихкодов Ozon
                oz_query = """
                SELECT DISTINCT
                    b.oz_barcode,
                    p.oz_sku, 
                    p.oz_vendor_code, 
                    p.oz_product_id,
                    ROW_NUMBER() OVER (PARTITION BY p.oz_vendor_code ORDER BY b.oz_barcode) AS oz_barcode_position
                FROM oz_barcodes b
                LEFT JOIN oz_products p ON b.oz_product_id = p.oz_product_id
                WHERE TRIM(b.oz_barcode) = ? AND p.oz_vendor_code IS NOT NULL
                """

                wb_matches = self.connection.execute(wb_query, [barcode]).fetchdf()
                oz_matches = self.connection.execute(oz_query, [barcode]).fetchdf()

                # Добавляем позицию штрихкода WB на уровне Python
                if not wb_matches.empty:
                    wb_matches['barcode_position'] = wb_matches.apply(
                        lambda row: row['wb_barcodes'].split(';').index(row['barcode']) + 1 
                        if row['barcode'] in row['wb_barcodes'].split(';') else 1, 
                        axis=1
                    )
                    wb_matches = wb_matches.drop('wb_barcodes', axis=1)

                # ИСПРАВЛЕННАЯ ЛОГИКА: Определяем актуальный штрихкод среди СОВПАДАЮЩИХ
                if not oz_matches.empty:
                    # Для поиска по штрихкоду: проверяем является ли найденный штрихкод актуальным для Ozon
                    # Получаем максимальную позицию штрихкода для каждого oz_vendor_code среди всех его штрихкодов
                    # ИСПРАВЛЕНО: Разделяем оконную функцию и агрегацию через CTE
                    all_oz_positions_query = """
                    WITH barcode_positions AS (
                        SELECT 
                            p.oz_vendor_code,
                            ROW_NUMBER() OVER (PARTITION BY p.oz_vendor_code ORDER BY b.oz_barcode) AS position
                        FROM oz_barcodes b
                        LEFT JOIN oz_products p ON b.oz_product_id = p.oz_product_id
                        WHERE p.oz_vendor_code IN ({})
                    )
                    SELECT 
                        oz_vendor_code,
                        MAX(position) AS max_position
                    FROM barcode_positions
                    GROUP BY oz_vendor_code
                    """.format(','.join(['?' for _ in oz_matches['oz_vendor_code'].unique()]))
                    
                    max_positions_all = self.connection.execute(all_oz_positions_query, 
                                                              list(oz_matches['oz_vendor_code'].unique())).fetchdf()
                    
                    if not max_positions_all.empty:
                        # Добавляем информацию о максимальной позиции среди всех штрихкодов
                        oz_matches = oz_matches.merge(max_positions_all, on='oz_vendor_code', how='left')
                        
                        # Помечаем как актуальные только те штрихкоды, которые имеют максимальную позицию
                        oz_matches['is_actual_oz_barcode'] = (
                            oz_matches['oz_barcode_position'] == oz_matches['max_position']
                        )
                    else:
                        oz_matches['is_actual_oz_barcode'] = True  # fallback

                # Формируем результат для данного штрихкода
                if not wb_matches.empty and not oz_matches.empty:
                    # Определяем актуальность для каждого WB товара
                    wb_matches['is_primary_barcode'] = False  # Инициализация
                    
                    for _, oz_row in oz_matches.iterrows():
                        oz_vendor_code = oz_row['oz_vendor_code']
                        is_actual_oz = oz_row.get('is_actual_oz_barcode', True)
                        
                        # Если штрихкод актуальный для Ozon, помечаем все связанные WB товары как актуальные
                        if is_actual_oz:
                            wb_matches['is_primary_barcode'] = True

                    # Формируем результат для каждой комбинации WB-Ozon
                    for _, wb_row in wb_matches.iterrows():
                        for _, oz_row in oz_matches.iterrows():
                            result_row = {"Search_Value": barcode}

                            for field_label, field_detail in selected_fields_map.items():
                                if field_label == "Search_Value":
                                    continue

                                if field_detail == "common_matched_barcode":
                                    result_row[field_label] = barcode
                                elif field_detail == "is_primary_barcode":
                                    # Актуальный штрихкод: если штрихкод актуальный для Ozon
                                    is_primary_wb = wb_row.get('is_primary_barcode', False)
                                    is_actual_oz = oz_row.get('is_actual_oz_barcode', True)
                                    is_primary = is_primary_wb and is_actual_oz
                                    result_row[field_label] = "Да" if is_primary else "Нет"
                                elif isinstance(field_detail, tuple):
                                    table_alias, column_name = field_detail

                                    if table_alias == 'wb_products':
                                        # ИСПРАВЛЕНО: Используем данные из wb_row, если они есть, иначе дополнительный запрос
                                        if column_name in wb_row:
                                            result_row[field_label] = wb_row[column_name]
                                        else:
                                            # Для полей, которых нет в wb_row, делаем дополнительный запрос
                                            result_row[field_label] = self._get_additional_field_data(
                                                pd.Series({'wb_sku': wb_row['wb_sku']}), table_alias, column_name
                                            )
                                    elif table_alias == 'oz_products':
                                        result_row[field_label] = oz_row.get(column_name, '')
                                    elif table_alias == 'oz_barcodes' and column_name == 'oz_barcode':
                                        result_row[field_label] = barcode
                                    elif table_alias == 'oz_category_products':
                                        # Дополнительный запрос для получения данных из oz_category_products
                                        try:
                                            query = f"SELECT {column_name} FROM oz_category_products WHERE oz_vendor_code = ?"
                                            cat_result = self.connection.execute(query, [oz_row.get('oz_vendor_code')]).fetchdf()
                                            result_row[field_label] = str(cat_result.iloc[0, 0]) if not cat_result.empty else ''
                                        except Exception:
                                            result_row[field_label] = ''
                                    else:
                                        result_row[field_label] = ''
                                else:
                                    result_row[field_label] = ''

                            result_data.append(result_row)

            return pd.DataFrame(result_data)

        except Exception as e:
            st.error(f"Ошибка поиска по штрихкоду: {e}")
            return pd.DataFrame()
    
    def _get_additional_field_data(self, row: pd.Series, table_alias: str, column_name: str) -> str:
        """Получает дополнительные данные для полей, которых нет в базовых связях."""
        try:
            if table_alias == 'oz_products':
                query = f"SELECT {column_name} FROM oz_products WHERE oz_product_id = ?"
                result = self.connection.execute(query, [row.get('oz_product_id')]).fetchdf()
                return str(result.iloc[0, 0]) if not result.empty else ''
                
            elif table_alias == 'wb_products':
                query = f"SELECT {column_name} FROM wb_products WHERE wb_sku = ?"
                result = self.connection.execute(query, [row.get('wb_sku')]).fetchdf()
                return str(result.iloc[0, 0]) if not result.empty else ''
                
            elif table_alias == 'wb_prices':
                query = f"SELECT {column_name} FROM wb_prices WHERE wb_sku = ?"
                result = self.connection.execute(query, [row.get('wb_sku')]).fetchdf()
                return str(result.iloc[0, 0]) if not result.empty else ''
                
            elif table_alias == 'oz_category_products':
                # Связывается через oz_vendor_code
                query = f"SELECT {column_name} FROM oz_category_products WHERE oz_vendor_code = ?"
                result = self.connection.execute(query, [row.get('oz_vendor_code')]).fetchdf()
                return str(result.iloc[0, 0]) if not result.empty else ''
                
            return ''
            
        except Exception:
            return ''
    
    def enrich_group_with_wb_connections(self, group_df: pd.DataFrame, barcode_column: str = 'barcode') -> pd.DataFrame:
        """
        Обогащает DataFrame группы товаров связями с WB через штрихкоды.
        
        Заменяет дублированную логику из existing_groups_helpers.py get_group_details_with_wb_connections()
        
        Args:
            group_df: DataFrame с товарами группы (должен содержать колонку со штрихкодами)
            barcode_column: название колонки со штрихкодами (по умолчанию 'barcode')
            
        Returns:
            DataFrame с добавленной колонкой 'wb_sku'
        """
        if group_df.empty or barcode_column not in group_df.columns:
            return group_df
        
        try:
            # Создаем копию для работы
            result_df = group_df.copy()
            
            # Получаем все штрихкоды из группы (исключая пустые)
            group_barcodes = []
            for _, row in result_df.iterrows():
                if pd.notna(row[barcode_column]) and str(row[barcode_column]).strip():
                    group_barcodes.append(str(row[barcode_column]).strip())
            
            # Инициализируем колонку wb_sku
            result_df['wb_sku'] = None
            
            if group_barcodes:
                # Используем нормализованные штрихкоды WB для поиска совпадений
                wb_barcodes_df = self._get_normalized_wb_barcodes_for_all()
                
                if not wb_barcodes_df.empty:
                    # Создаем словарь для быстрого поиска: barcode -> wb_sku
                    barcode_to_wb_sku = {}
                    for _, wb_row in wb_barcodes_df.iterrows():
                        barcode = str(wb_row['individual_barcode_wb']).strip()
                        wb_sku = str(wb_row['wb_sku'])
                        if barcode and barcode not in barcode_to_wb_sku:
                            barcode_to_wb_sku[barcode] = wb_sku
                    
                    # Обновляем result_df с найденными wb_sku
                    for idx, row in result_df.iterrows():
                        if pd.notna(row[barcode_column]) and str(row[barcode_column]).strip():
                            barcode = str(row[barcode_column]).strip()
                            if barcode in barcode_to_wb_sku:
                                result_df.at[idx, 'wb_sku'] = barcode_to_wb_sku[barcode]
            
            return result_df
            
        except Exception as e:
            st.error(f"Ошибка обогащения группы связями с WB: {e}")
            return group_df

    def clear_cache(self):
        """Очищает кэш для принудительного обновления данных."""
        try:
            self.link_wb_to_oz.clear()
            self.link_oz_to_wb.clear()
            st.success("Кэш связей между маркетплейсами очищен")
        except Exception as e:
            st.warning(f"Не удалось очистить кэш: {e}")


# Удобные функции для быстрого использования
def get_wb_to_oz_links(connection: duckdb.DuckDBPyConnection, wb_skus: List[str]) -> Dict[str, List[str]]:
    """
    Быстрый способ получить связи WB -> Ozon.
    
    Args:
        connection: Соединение с БД
        wb_skus: Список WB SKU
        
    Returns:
        Словарь {wb_sku: [oz_sku]}
    """
    linker = CrossMarketplaceLinker(connection)
    return linker.link_wb_to_oz(wb_skus)


def get_oz_to_wb_links(connection: duckdb.DuckDBPyConnection, oz_skus: List[str]) -> Dict[str, List[str]]:
    """
    Быстрый способ получить связи Ozon -> WB.
    
    Args:
        connection: Соединение с БД  
        oz_skus: Список Ozon SKU
        
    Returns:
        Словарь {oz_sku: [wb_sku]}
    """
    linker = CrossMarketplaceLinker(connection)
    return linker.link_oz_to_wb(oz_skus) 