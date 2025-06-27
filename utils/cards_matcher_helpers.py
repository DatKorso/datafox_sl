"""
Helper functions for Cards Matcher functionality.

This module provides utilities for:
- Rating analysis and statistics
- Product grouping recommendations
- Card optimization suggestions
"""
import pandas as pd
import duckdb
from typing import List, Dict, Tuple, Optional
import streamlit as st


def get_rating_statistics(conn: duckdb.DuckDBPyConnection) -> Dict:
    """
    Get comprehensive rating statistics from oz_card_rating table.
    
    Args:
        conn: Database connection
        
    Returns:
        Dictionary with rating statistics
    """
    try:
        stats_query = """
        SELECT 
            COUNT(*) as total_products,
            COUNT(DISTINCT oz_sku) as unique_skus,
            AVG(rating) as avg_rating,
            MIN(rating) as min_rating,
            MAX(rating) as max_rating,
            SUM(rev_number) as total_reviews,
            COUNT(CASE WHEN rating >= 4 THEN 1 END) as high_rated_count,
            COUNT(CASE WHEN rating <= 2 THEN 1 END) as low_rated_count,
            COUNT(CASE WHEN rating = 3 THEN 1 END) as medium_rated_count
        FROM oz_card_rating
        WHERE rating IS NOT NULL
        """
        
        result = conn.execute(stats_query).fetchone()
        
        if result:
            return {
                'total_products': result[0],
                'unique_skus': result[1],
                'avg_rating': round(result[2], 2) if result[2] else 0,
                'min_rating': result[3],
                'max_rating': result[4],
                'total_reviews': result[5] if result[5] else 0,
                'high_rated_count': result[6],
                'low_rated_count': result[7],
                'medium_rated_count': result[8]
            }
        else:
            return {}
            
    except Exception as e:
        st.error(f"Ошибка получения статистики рейтингов: {e}")
        return {}


def get_rating_distribution(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Get rating distribution data for visualization.
    
    Args:
        conn: Database connection
        
    Returns:
        DataFrame with rating distribution
    """
    try:
        query = """
        SELECT 
            rating,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM oz_card_rating
        WHERE rating IS NOT NULL
        GROUP BY rating
        ORDER BY rating
        """
        
        return conn.execute(query).fetchdf()
        
    except Exception as e:
        st.error(f"Ошибка получения распределения рейтингов: {e}")
        return pd.DataFrame()


def get_low_rated_products(conn: duckdb.DuckDBPyConnection, 
                          rating_threshold: float = 3.0,
                          min_reviews: int = 5) -> pd.DataFrame:
    """
    Get products with low ratings that might need attention.
    
    Args:
        conn: Database connection
        rating_threshold: Maximum rating to consider as "low"
        min_reviews: Minimum number of reviews to consider
        
    Returns:
        DataFrame with low-rated products
    """
    try:
        query = """
        SELECT 
            r.oz_sku,
            r.oz_vendor_code,
            r.rating,
            r.rev_number,
            p.oz_brand,
            p.oz_product_status,
            p.oz_actual_price
        FROM oz_card_rating r
        LEFT JOIN oz_products p ON r.oz_sku = p.oz_sku
        WHERE r.rating <= ? 
        AND r.rev_number >= ?
        ORDER BY r.rating ASC, r.rev_number DESC
        """
        
        return conn.execute(query, [rating_threshold, min_reviews]).fetchdf()
        
    except Exception as e:
        st.error(f"Ошибка получения товаров с низким рейтингом: {e}")
        return pd.DataFrame()


def get_high_rated_products(conn: duckdb.DuckDBPyConnection, 
                           rating_threshold: float = 4.0,
                           min_reviews: int = 10) -> pd.DataFrame:
    """
    Get products with high ratings that could be used for grouping.
    
    Args:
        conn: Database connection
        rating_threshold: Minimum rating to consider as "high"
        min_reviews: Minimum number of reviews to consider
        
    Returns:
        DataFrame with high-rated products
    """
    try:
        query = """
        SELECT 
            r.oz_sku,
            r.oz_vendor_code,
            r.rating,
            r.rev_number,
            p.oz_brand,
            p.oz_product_status,
            p.oz_actual_price
        FROM oz_card_rating r
        LEFT JOIN oz_products p ON r.oz_sku = p.oz_sku
        WHERE r.rating >= ? 
        AND r.rev_number >= ?
        ORDER BY r.rating DESC, r.rev_number DESC
        """
        
        return conn.execute(query, [rating_threshold, min_reviews]).fetchdf()
        
    except Exception as e:
        st.error(f"Ошибка получения товаров с высоким рейтингом: {e}")
        return pd.DataFrame()


def analyze_brand_ratings(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Analyze rating performance by brand.
    
    Args:
        conn: Database connection
        
    Returns:
        DataFrame with brand rating analysis
    """
    try:
        query = """
        SELECT 
            p.oz_brand,
            COUNT(r.oz_sku) as product_count,
            AVG(r.rating) as avg_rating,
            SUM(r.rev_number) as total_reviews,
            COUNT(CASE WHEN r.rating >= 4 THEN 1 END) as high_rated_count,
            COUNT(CASE WHEN r.rating <= 2 THEN 1 END) as low_rated_count
        FROM oz_card_rating r
        LEFT JOIN oz_products p ON r.oz_sku = p.oz_sku
        WHERE p.oz_brand IS NOT NULL 
        AND r.rating IS NOT NULL
        GROUP BY p.oz_brand
        HAVING COUNT(r.oz_sku) >= 3  -- Only brands with at least 3 products
        ORDER BY avg_rating DESC, total_reviews DESC
        """
        
        df = conn.execute(query).fetchdf()
        
        # Add calculated metrics
        if not df.empty:
            df['avg_rating'] = df['avg_rating'].round(2)
            df['high_rated_pct'] = (df['high_rated_count'] / df['product_count'] * 100).round(1)
            df['low_rated_pct'] = (df['low_rated_count'] / df['product_count'] * 100).round(1)
        
        return df
        
    except Exception as e:
        st.error(f"Ошибка анализа рейтингов по брендам: {e}")
        return pd.DataFrame()


def find_potential_groups_by_brand_and_rating(conn: duckdb.DuckDBPyConnection,
                                             min_rating: float = 3.5,
                                             min_products: int = 2) -> pd.DataFrame:
    """
    Find potential product groups based on brand and rating criteria.
    
    Args:
        conn: Database connection
        min_rating: Minimum average rating for group consideration
        min_products: Minimum number of products to form a group
        
    Returns:
        DataFrame with potential grouping recommendations
    """
    try:
        query = """
        WITH brand_groups AS (
            SELECT 
                p.oz_brand,
                COUNT(r.oz_sku) as product_count,
                AVG(r.rating) as avg_rating,
                MIN(r.rating) as min_rating,
                MAX(r.rating) as max_rating,
                STRING_AGG(r.oz_vendor_code, ', ') as vendor_codes,
                STRING_AGG(CAST(r.oz_sku AS VARCHAR), ', ') as skus
            FROM oz_card_rating r
            LEFT JOIN oz_products p ON r.oz_sku = p.oz_sku
            WHERE p.oz_brand IS NOT NULL 
            AND r.rating IS NOT NULL
            GROUP BY p.oz_brand
            HAVING COUNT(r.oz_sku) >= ?
            AND AVG(r.rating) >= ?
        )
        SELECT 
            oz_brand,
            product_count,
            ROUND(avg_rating, 2) as avg_rating,
            min_rating,
            max_rating,
            vendor_codes,
            skus,
            CASE 
                WHEN max_rating - min_rating <= 0.5 THEN 'Отличные кандидаты для группировки'
                WHEN max_rating - min_rating <= 1.0 THEN 'Хорошие кандидаты для группировки'
                ELSE 'Требуется анализ - большой разброс рейтингов'
            END as recommendation
        FROM brand_groups
        ORDER BY avg_rating DESC, product_count DESC
        """
        
        return conn.execute(query, [min_products, min_rating]).fetchdf()
        
    except Exception as e:
        st.error(f"Ошибка поиска потенциальных групп: {e}")
        return pd.DataFrame()


def identify_problematic_products(conn: duckdb.DuckDBPyConnection,
                                 rating_threshold: float = 2.5,
                                 review_threshold: int = 10) -> pd.DataFrame:
    """
    Identify products that should potentially be separated from groups due to poor performance.
    
    Args:
        conn: Database connection
        rating_threshold: Maximum rating to consider problematic
        review_threshold: Minimum reviews to consider for separation
        
    Returns:
        DataFrame with problematic products and separation recommendations
    """
    try:
        query = """
        SELECT 
            r.oz_sku,
            r.oz_vendor_code,
            r.rating,
            r.rev_number,
            p.oz_brand,
            p.oz_product_status,
            p.oz_actual_price,
            CASE 
                WHEN r.rating <= 2.0 AND r.rev_number >= 20 THEN 'Срочно убрать из группы'
                WHEN r.rating <= 2.5 AND r.rev_number >= 10 THEN 'Рекомендуется убрать из группы' 
                WHEN r.rating <= 3.0 AND r.rev_number >= 50 THEN 'Требует анализа отзывов'
                ELSE 'Мониторинг'
            END as separation_recommendation,
            CASE 
                WHEN r.rating <= 2.0 THEN 'Высокий'
                WHEN r.rating <= 2.5 THEN 'Средний'
                ELSE 'Низкий'
            END as risk_level
        FROM oz_card_rating r
        LEFT JOIN oz_products p ON r.oz_sku = p.oz_sku
        WHERE r.rating <= ? 
        AND r.rev_number >= ?
        ORDER BY r.rating ASC, r.rev_number DESC
        """
        
        return conn.execute(query, [rating_threshold, review_threshold]).fetchdf()
        
    except Exception as e:
        st.error(f"Ошибка выявления проблемных товаров: {e}")
        return pd.DataFrame()


def get_rating_trends_by_review_volume(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Analyze rating trends based on review volume to identify reliability patterns.
    
    Args:
        conn: Database connection
        
    Returns:
        DataFrame with rating trends by review volume segments
    """
    try:
        query = """
        SELECT 
            CASE 
                WHEN rev_number = 0 THEN 'Без отзывов'
                WHEN rev_number <= 5 THEN '1-5 отзывов'
                WHEN rev_number <= 20 THEN '6-20 отзывов'
                WHEN rev_number <= 50 THEN '21-50 отзывов'
                WHEN rev_number <= 100 THEN '51-100 отзывов'
                ELSE '100+ отзывов'
            END as review_segment,
            COUNT(*) as product_count,
            AVG(rating) as avg_rating,
            MIN(rating) as min_rating,
            MAX(rating) as max_rating,
            COUNT(CASE WHEN rating >= 4 THEN 1 END) as high_rated_count,
            COUNT(CASE WHEN rating <= 2 THEN 1 END) as low_rated_count
        FROM oz_card_rating
        WHERE rating IS NOT NULL
        GROUP BY 
            CASE 
                WHEN rev_number = 0 THEN 'Без отзывов'
                WHEN rev_number <= 5 THEN '1-5 отзывов'
                WHEN rev_number <= 20 THEN '6-20 отзывов'
                WHEN rev_number <= 50 THEN '21-50 отзывов'
                WHEN rev_number <= 100 THEN '51-100 отзывов'
                ELSE '100+ отзывов'
            END
        ORDER BY 
            CASE 
                WHEN review_segment = 'Без отзывов' THEN 0
                WHEN review_segment = '1-5 отзывов' THEN 1
                WHEN review_segment = '6-20 отзывов' THEN 2
                WHEN review_segment = '21-50 отзывов' THEN 3
                WHEN review_segment = '51-100 отзывов' THEN 4
                ELSE 5
            END
        """
        
        df = conn.execute(query).fetchdf()
        
        # Add calculated metrics
        if not df.empty:
            df['avg_rating'] = df['avg_rating'].round(2)
            df['high_rated_pct'] = (df['high_rated_count'] / df['product_count'] * 100).round(1)
            df['low_rated_pct'] = (df['low_rated_count'] / df['product_count'] * 100).round(1)
        
        return df
        
    except Exception as e:
        st.error(f"Ошибка анализа трендов по объему отзывов: {e}")
        return pd.DataFrame()


def format_rating_display(rating: float) -> str:
    """
    Format rating for display with appropriate emoji.
    
    Args:
        rating: Rating value
        
    Returns:
        Formatted rating string with emoji
    """
    if pd.isna(rating):
        return "N/A"
    
    if rating >= 4.5:
        return f"⭐⭐⭐⭐⭐ {rating:.1f}"
    elif rating >= 3.5:
        return f"⭐⭐⭐⭐ {rating:.1f}"
    elif rating >= 2.5:
        return f"⭐⭐⭐ {rating:.1f}"
    elif rating >= 1.5:
        return f"⭐⭐ {rating:.1f}"
    else:
        return f"⭐ {rating:.1f}"


def get_rating_color(rating: float) -> str:
    """
    Get color code for rating display.
    
    Args:
        rating: Rating value
        
    Returns:
        Color code (green, yellow, red)
    """
    if pd.isna(rating):
        return "gray"
    elif rating >= 4.0:
        return "green"
    elif rating >= 3.0:
        return "orange"
    else:
        return "red"


# Функция get_wb_sku_ratings_with_oz_data удалена - заменена на 
# CrossMarketplaceLinker.get_links_with_ozon_ratings() из utils/cross_marketplace_linker.py


def get_punta_table_data_for_wb_skus(conn, wb_skus: list[str], selected_columns: list[str] = None) -> pd.DataFrame:
    """
    Получает данные из punta_table для указанных wb_sku.
    
    Args:
        conn: соединение с БД
        wb_skus: список wb_sku
        selected_columns: список колонок для выборки (если None, выбираются все)
        
    Returns:
        DataFrame с данными из punta_table
    """
    if not wb_skus:
        return pd.DataFrame()
    
    try:
        # Получаем доступные колонки punta_table
        from utils.db_crud import get_punta_table_columns
        available_columns = get_punta_table_columns(conn)
        
        if not available_columns or 'wb_sku' not in available_columns:
            return pd.DataFrame()
        
        # Определяем колонки для выборки
        if selected_columns:
            columns_to_select = ['wb_sku'] + [col for col in selected_columns if col in available_columns and col != 'wb_sku']
            # Всегда добавляем поле sort если оно доступно (для приоритизации)
            if 'sort' in available_columns and 'sort' not in columns_to_select:
                columns_to_select.append('sort')
        else:
            columns_to_select = available_columns
        
        if len(columns_to_select) <= 1:  # Только wb_sku
            return pd.DataFrame()
        
        # Формируем запрос
        wb_skus_str = ', '.join([f"'{sku}'" for sku in wb_skus])
        columns_str = ', '.join(columns_to_select)
        
        query = f"""
        SELECT {columns_str}
        FROM punta_table 
        WHERE wb_sku IN ({wb_skus_str})
        """
        
        result_df = conn.execute(query).fetchdf()
        
        # Приводим wb_sku к строковому типу
        if not result_df.empty:
            result_df['wb_sku'] = result_df['wb_sku'].astype(str)
        
        # Удаляем дубликаты wb_sku, оставляя первую запись для каждого
        if not result_df.empty and result_df['wb_sku'].duplicated().any():
            result_df = result_df.drop_duplicates(subset=['wb_sku'], keep='first')
        
        return result_df
        
    except Exception as e:
        print(f"Ошибка при получении данных punta_table: {e}")
        return pd.DataFrame()


def create_product_groups(
    conn, 
    wb_skus: list[str], 
    grouping_columns: list[str] = None,
    min_group_rating: float = 0.0,
    max_wb_sku_per_group: int = 20,
    enable_sort_priority: bool = True
) -> pd.DataFrame:
    """
    Создает группы товаров на основе wb_sku с учетом рейтингов и дополнительных параметров.
    
    Алгоритм:
    1. Сначала формируются группы на основе общих параметров из punta_table
    2. Применяется приоритизация по полю sort (если включена)
    3. Затем из групп исключаются wb_sku с низким рейтингом для достижения min_group_rating
    4. Группы разделяются если превышено максимальное количество wb_sku (max_wb_sku_per_group)
    5. Все oz_sku, принадлежащие одному wb_sku, рассматриваются как единое целое
    
    Args:
        conn: соединение с БД
        wb_skus: список wb_sku для группировки
        grouping_columns: список колонок из punta_table для группировки
        min_group_rating: минимальный рейтинг группы (исключаются wb_sku для достижения этого значения)
        max_wb_sku_per_group: максимальное количество wb_sku в одной группе (по умолчанию 20)
        enable_sort_priority: включить приоритизацию по полю sort из punta_table (по умолчанию True)
        
    Returns:
        DataFrame с группами товаров
    """
    if not wb_skus:
        return pd.DataFrame()
    
    try:
        # Получаем рейтинги wb_sku через централизованный линкер
        from utils.cross_marketplace_linker import CrossMarketplaceLinker
        linker = CrossMarketplaceLinker(conn)
        wb_ratings_df = linker.get_links_with_ozon_ratings(wb_skus)
        
        if wb_ratings_df.empty:
            return pd.DataFrame()
        
        # Все товары участвуют в первоначальной группировке
        all_ratings = wb_ratings_df.copy()
        
        # Получаем данные из punta_table если указаны колонки группировки
        if grouping_columns:
            punta_data = get_punta_table_data_for_wb_skus(
                conn, 
                all_ratings['wb_sku'].tolist(), 
                grouping_columns
            )
            
            if not punta_data.empty:
                # Приводим wb_sku к одному типу данных перед соединением
                all_ratings['wb_sku'] = all_ratings['wb_sku'].astype(str)
                punta_data['wb_sku'] = punta_data['wb_sku'].astype(str)
                
                # Соединяем рейтинги с данными punta_table
                result_df = pd.merge(all_ratings, punta_data, on='wb_sku', how='left')
                
                # Группируем по указанным колонкам
                group_columns = [col for col in grouping_columns if col in result_df.columns]
                
                if group_columns:
                    # Создаем группы на основе одинаковых значений в group_columns
                    result_df['group_key'] = result_df[group_columns].apply(
                        lambda row: '|'.join([str(row[col]) if pd.notna(row[col]) else 'NULL' for col in group_columns]), 
                        axis=1
                    )
                    
                    # Добавляем предварительные номера групп
                    unique_groups = result_df['group_key'].unique()
                    group_mapping = {key: i+1 for i, key in enumerate(unique_groups)}
                    result_df['preliminary_group_id'] = result_df['group_key'].map(group_mapping)
                    
                    # Фильтруем группы по минимальному рейтингу
                    def filter_group_by_min_rating(group_data, min_rating):
                        """Удаляет из группы wb_sku с низким рейтингом для достижения min_rating СРЕДНЕГО рейтинга группы"""
                        if min_rating <= 0:
                            return group_data
                        
                        # Проверяем, нужна ли фильтрация
                        current_avg = group_data['avg_rating'].mean()
                        if current_avg >= min_rating:
                            return group_data
                        
                        # Сортируем с учетом приоритизации
                        if enable_sort_priority and 'sort' in group_data.columns:
                            # Приоритизация: сначала по sort (убыванию), затем по рейтингу (убыванию)
                            # Товары с высоким sort исключаются в последнюю очередь
                            group_data['sort_filled'] = group_data['sort'].fillna(0)  # Заполняем NaN нулями
                            sorted_group = group_data.sort_values(['sort_filled', 'avg_rating'], ascending=[False, False])
                        else:
                            # Обычная сортировка по рейтингу по убыванию
                            sorted_group = group_data.sort_values('avg_rating', ascending=False)
                        
                        # Пошагово удаляем товары с самым низким приоритетом/рейтингом
                        # пока средний рейтинг группы не станет >= min_rating
                        for i in range(1, len(sorted_group) + 1):
                            current_subset = sorted_group.iloc[:i]
                            avg_rating_in_subset = current_subset['avg_rating'].mean()
                            
                            if avg_rating_in_subset >= min_rating:
                                # Эта подгруппа удовлетворяет требованию по среднему рейтингу
                                return current_subset
                        
                        # Если даже один лучший товар не дает нужный средний рейтинг
                        return pd.DataFrame()
                    
                    # Применяем фильтрацию к каждой группе
                    filtered_groups = []
                    excluded_items = []
                    
                    for group_id in result_df['preliminary_group_id'].unique():
                        group_data = result_df[result_df['preliminary_group_id'] == group_id].copy()
                        
                        if len(group_data) == 1:
                            # Одиночные товары остаются как есть
                            filtered_group = group_data
                        else:
                            # Фильтруем группу по минимальному рейтингу
                            filtered_group = filter_group_by_min_rating(group_data, min_group_rating)
                            
                            # Исключенные товары
                            if not filtered_group.empty:
                                excluded_in_group = group_data[~group_data.index.isin(filtered_group.index)]
                                if not excluded_in_group.empty:
                                    excluded_items.append(excluded_in_group)
                        
                        if not filtered_group.empty:
                            filtered_groups.append(filtered_group)
                    
                    # Объединяем отфильтрованные группы
                    if filtered_groups:
                        result_df = pd.concat(filtered_groups, ignore_index=True)
                        
                        # Проверяем дубликаты после объединения отфильтрованных групп
                        if result_df['wb_sku'].duplicated().any():
                            print(f"ВНИМАНИЕ: Дубликаты после объединения отфильтрованных групп")
                            print(f"Дубликаты: {result_df[result_df['wb_sku'].duplicated()]['wb_sku'].tolist()}")
                            result_df = result_df.drop_duplicates(subset=['wb_sku'], keep='first')
                        
                        # Разделяем группы, превышающие максимальное количество wb_sku
                        if max_wb_sku_per_group > 0:
                            def split_large_groups(df):
                                """Разделяет группы, превышающие максимальное количество wb_sku"""
                                split_groups = []
                                
                                for group_id in df['preliminary_group_id'].unique():
                                    group_data = df[df['preliminary_group_id'] == group_id].copy()
                                    
                                    if len(group_data) <= max_wb_sku_per_group:
                                        # Группа не превышает лимит - оставляем как есть
                                        split_groups.append(group_data)
                                    else:
                                        # Группа превышает лимит - разделяем
                                        # Сортируем с учетом приоритизации (лучшие товары в первых подгруппах)
                                        if enable_sort_priority and 'sort' in group_data.columns:
                                            # Приоритизация: сначала по sort (убыванию), затем по рейтингу (убыванию)
                                            group_data['sort_filled'] = group_data['sort'].fillna(0)  # Заполняем NaN нулями
                                            sorted_group = group_data.sort_values(['sort_filled', 'avg_rating'], ascending=[False, False])
                                        else:
                                            # Обычная сортировка по рейтингу (лучшие товары в первых подгруппах)
                                            sorted_group = group_data.sort_values('avg_rating', ascending=False)
                                        
                                        # Разделяем на подгруппы
                                        for i in range(0, len(sorted_group), max_wb_sku_per_group):
                                            subgroup = sorted_group.iloc[i:i+max_wb_sku_per_group].copy()
                                            
                                            # Обновляем ключ группы для подгруппы
                                            subgroup_suffix = f"_SPLIT_{i//max_wb_sku_per_group + 1}"
                                            subgroup['group_key'] = subgroup['group_key'] + subgroup_suffix
                                            
                                            split_groups.append(subgroup)
                                
                                return pd.concat(split_groups, ignore_index=True) if split_groups else pd.DataFrame()
                            
                            result_df = split_large_groups(result_df)
                    else:
                        result_df = pd.DataFrame()
                    
                    # Добавляем исключенные товары как индивидуальные
                    if excluded_items:
                        excluded_df = pd.concat(excluded_items, ignore_index=True)
                        
                        # Проверяем дубликаты в исключенных товарах
                        if excluded_df['wb_sku'].duplicated().any():
                            print(f"ВНИМАНИЕ: Дубликаты в исключенных товарах")
                            excluded_df = excluded_df.drop_duplicates(subset=['wb_sku'], keep='first')
                        
                        excluded_df['group_key'] = 'EXCLUDED_' + excluded_df['wb_sku'].astype(str)
                        excluded_df['preliminary_group_id'] = range(
                            result_df['preliminary_group_id'].max() + 1 if not result_df.empty else 1,
                            result_df['preliminary_group_id'].max() + 1 + len(excluded_df) if not result_df.empty else 1 + len(excluded_df)
                        )
                        result_df = pd.concat([result_df, excluded_df], ignore_index=True)
                    
                    # Перенумеровываем группы
                    unique_groups = result_df['group_key'].unique()
                    group_mapping = {key: i+1 for i, key in enumerate(unique_groups)}
                    result_df['group_id'] = result_df['group_key'].map(group_mapping)
                    
                    # Вычисляем статистики по группам
                    # Для правильного подсчета oz_sku и отзывов нужно посчитать уникальные значения
                    group_stats_data = []
                    
                    for group_id in result_df['group_id'].unique():
                        group_data = result_df[result_df['group_id'] == group_id]
                        
                        # Подсчет уникальных oz_sku в группе (объединяем все списки)
                        all_oz_skus = []
                        total_reviews_in_group = 0
                        
                        for _, row in group_data.iterrows():
                            # Разбираем строку oz_skus_list
                            oz_skus_in_row = [sku.strip() for sku in str(row['oz_skus_list']).split(';') if sku.strip()]
                            all_oz_skus.extend(oz_skus_in_row)
                            total_reviews_in_group += row['total_reviews']
                        
                        # Уникальные oz_sku в группе
                        unique_oz_skus = len(set(all_oz_skus))
                        
                        group_stats_data.append({
                            'group_id': group_id,
                            'wb_count': len(group_data),
                            'group_avg_rating': group_data['avg_rating'].mean(),
                            'group_min_rating': group_data['avg_rating'].min(),
                            'group_max_rating': group_data['avg_rating'].max(),
                            'total_oz_sku_count': unique_oz_skus,
                            'group_total_reviews': total_reviews_in_group
                        })
                    
                    group_stats = pd.DataFrame(group_stats_data).round(2)
                    
                    # Приводим group_id к одному типу данных перед соединением
                    result_df['group_id'] = result_df['group_id'].astype(int)
                    group_stats['group_id'] = group_stats['group_id'].astype(int)
                    
                    # Добавляем статистики групп к основным данным
                    result_df = pd.merge(result_df, group_stats, on='group_id', how='left')
                    
                    # Определяем рекомендации по группам
                    def get_group_recommendation(row):
                        rating_spread = row['group_max_rating'] - row['group_min_rating']
                        avg_rating = row['group_avg_rating']
                        min_rating = row['group_min_rating']
                        wb_count = row['wb_count']
                        is_excluded = row['group_key'].startswith('EXCLUDED_') if 'group_key' in result_df.columns else False
                        is_split = '_SPLIT_' in str(row['group_key']) if 'group_key' in result_df.columns else False
                        
                        if is_excluded:
                            return "Исключен из группы - низкий рейтинг"
                        elif wb_count == 1:
                            if min_rating >= min_group_rating:
                                return "Единственный товар в группе"
                            else:
                                return "Одиночный товар - не соответствует требованиям группы"
                        elif is_split:
                            if rating_spread <= 0.5 and avg_rating >= 4.5:
                                return f"Отличная подгруппа (разделена, макс. {max_wb_sku_per_group} товаров)"
                            elif rating_spread <= 1.0 and avg_rating >= 4.0:
                                return f"Хорошая подгруппа (разделена, макс. {max_wb_sku_per_group} товаров)"
                            elif rating_spread <= 1.5 and avg_rating >= 3.5:
                                return f"Удовлетворительная подгруппа (разделена, макс. {max_wb_sku_per_group} товаров)"
                            else:
                                return f"Подгруппа требует проверки (разделена, макс. {max_wb_sku_per_group} товаров)"
                        elif wb_count >= max_wb_sku_per_group * 0.8:  # Близко к максимуму
                            if rating_spread <= 0.5 and avg_rating >= 4.5:
                                return f"Отличная большая группа ({wb_count} товаров)"
                            elif rating_spread <= 1.0 and avg_rating >= 4.0:
                                return f"Хорошая большая группа ({wb_count} товаров)"
                            else:
                                return f"Большая группа требует проверки ({wb_count} товаров)"
                        elif rating_spread <= 0.5 and avg_rating >= 4.5:
                            return "Отличная группа - объединить"
                        elif rating_spread <= 1.0 and avg_rating >= 4.0:
                            return "Хорошая группа - рекомендуется"
                        elif rating_spread <= 1.5 and avg_rating >= 3.5:
                            return "Удовлетворительная группа"
                        else:
                            return "Требует проверки"
                    
                    result_df['group_recommendation'] = result_df.apply(get_group_recommendation, axis=1)
                    
                    # Сортируем по группам и приоритету/рейтингу
                    if enable_sort_priority and 'sort' in result_df.columns:
                        # Заполняем NaN в sort нулями для корректной сортировки
                        result_df['sort_filled'] = result_df['sort'].fillna(0)
                        result_df = result_df.sort_values(['group_id', 'sort_filled', 'avg_rating'], ascending=[True, False, False])
                    else:
                        result_df = result_df.sort_values(['group_id', 'avg_rating'], ascending=[True, False])
                    
                    # Финальная проверка на дубликаты wb_sku (защита от ошибок)
                    if result_df['wb_sku'].duplicated().any():
                        print(f"ВНИМАНИЕ: Найдены дубликаты wb_sku в результате create_product_groups")
                        result_df = result_df.drop_duplicates(subset=['wb_sku'], keep='first')
                    
                    return result_df
            
        # Если нет колонок группировки или данных punta_table, возвращаем все товары с пометками
        all_ratings['group_id'] = range(1, len(all_ratings) + 1)  # Каждый товар в отдельной группе
        all_ratings['wb_count'] = 1
        all_ratings['group_avg_rating'] = all_ratings['avg_rating']
        all_ratings['group_min_rating'] = all_ratings['avg_rating']
        all_ratings['group_max_rating'] = all_ratings['avg_rating']
        all_ratings['total_oz_sku_count'] = all_ratings['oz_sku_count']
        all_ratings['group_total_reviews'] = all_ratings['total_reviews']
        
        # Рекомендации для отдельных товаров
        def get_simple_recommendation(rating):
            if rating < min_group_rating:
                return "Одиночный товар - не соответствует требованиям группы"
            elif rating >= 4.5:
                return "Отличный товар"
            elif rating >= 4.0:
                return "Хороший товар"
            elif rating >= 3.5:
                return "Удовлетворительный товар"
            else:
                return "Требует анализа"
        
        all_ratings['group_recommendation'] = all_ratings['avg_rating'].apply(get_simple_recommendation)
        
        # Финальная проверка на дубликаты wb_sku (защита от ошибок)
        if all_ratings['wb_sku'].duplicated().any():
            print(f"ВНИМАНИЕ: Найдены дубликаты wb_sku в результате create_product_groups (без группировки)")
            all_ratings = all_ratings.drop_duplicates(subset=['wb_sku'], keep='first')
        
        return all_ratings
        
    except Exception as e:
        print(f"Ошибка при создании групп товаров: {e}")
        return pd.DataFrame()


def get_available_grouping_columns(conn) -> list[str]:
    """
    Получает список доступных колонок из punta_table для группировки.
    
    Args:
        conn: соединение с БД
        
    Returns:
        Список названий колонок (исключая wb_sku)
    """
    try:
        from utils.db_crud import get_punta_table_columns
        all_columns = get_punta_table_columns(conn)
        
        # Исключаем wb_sku и системные колонки
        grouping_columns = [
            col for col in all_columns 
            if col not in ['wb_sku', 'id', 'created_at', 'updated_at']
        ]
        
        return grouping_columns
        
    except Exception as e:
        print(f"Ошибка при получении колонок группировки: {e}")
        return []


def test_wb_sku_connections(conn, wb_sku: str, show_debug: bool = True) -> dict:
    """
    Детально тестирует связи для конкретного wb_sku и возвращает отладочную информацию.
    
    Args:
        conn: соединение с БД
        wb_sku: wb_sku для тестирования
        show_debug: показывать ли отладочную информацию в Streamlit
        
    Returns:
        dict с результатами тестирования
    """
    import streamlit as st
    
    results = {
        'wb_sku': wb_sku,
        'wb_barcodes_count': 0,
        'linked_products_count': 0,
        'unique_oz_sku_count': 0,
        'rated_oz_sku_count': 0,
        'final_rating_data': None,
        'debug_info': []
    }
    
    try:
        # Шаг 1: Получаем сырые штрихкоды WB
        wb_raw_query = """
        SELECT DISTINCT wb_sku, wb_barcodes
        FROM wb_products 
        WHERE wb_sku = ?
            AND NULLIF(TRIM(wb_barcodes), '') IS NOT NULL
        """
        wb_raw_df = conn.execute(wb_raw_query, [wb_sku]).fetchdf()
        
        if wb_raw_df.empty:
            results['debug_info'].append("❌ WB SKU не найден или у него нет штрихкодов")
            return results
        
        # Шаг 2: Нормализуем штрихкоды
        wb_barcodes_data = []
        for _, row in wb_raw_df.iterrows():
            barcodes_str = str(row['wb_barcodes'])
            individual_barcodes = [b.strip() for b in barcodes_str.split(';') if b.strip()]
            for barcode in individual_barcodes:
                wb_barcodes_data.append({'wb_sku': wb_sku, 'barcode': barcode})
        
        results['wb_barcodes_count'] = len(wb_barcodes_data)
        results['debug_info'].append(f"✅ Найдено штрихкодов WB: {len(wb_barcodes_data)}")
        
        if show_debug:
            st.info(f"Штрихкоды WB: {[item['barcode'] for item in wb_barcodes_data[:10]]}")
        
        wb_barcodes_df = pd.DataFrame(wb_barcodes_data)
        
        # Шаг 3: Получаем штрихкоды Ozon
        from utils.db_search_helpers import get_ozon_barcodes_and_identifiers
        oz_barcodes_df = get_ozon_barcodes_and_identifiers(conn)
        
        if oz_barcodes_df.empty:
            results['debug_info'].append("❌ Штрихкоды Ozon не найдены в базе данных")
            return results
        
        # Шаг 4: Находим связанные товары
        oz_barcodes_df = oz_barcodes_df.rename(columns={'oz_barcode': 'barcode'})
        
        wb_barcodes_df['barcode'] = wb_barcodes_df['barcode'].astype(str).str.strip()
        oz_barcodes_df['barcode'] = oz_barcodes_df['barcode'].astype(str).str.strip()
        
        # Используем централизованную утилиту для очистки данных
        from utils.data_cleaning import DataCleaningUtils
        wb_barcodes_df, oz_barcodes_df = DataCleaningUtils.clean_marketplace_data(
            wb_barcodes_df, oz_barcodes_df
        )
        
        linked_df = pd.merge(wb_barcodes_df, oz_barcodes_df, on='barcode', how='inner')
        
        results['linked_products_count'] = len(linked_df)
        results['unique_oz_sku_count'] = linked_df['oz_sku'].nunique() if not linked_df.empty else 0
        
        results['debug_info'].append(f"✅ Найдено связанных записей: {len(linked_df)}")
        results['debug_info'].append(f"✅ Уникальных oz_sku: {results['unique_oz_sku_count']}")
        
        if show_debug and not linked_df.empty:
            st.info(f"Связанные oz_sku: {sorted(linked_df['oz_sku'].unique().tolist())}")
        
        # Шаг 5: Проверяем рейтинги
        if not linked_df.empty:
            oz_skus_for_rating = linked_df['oz_sku'].astype(str).unique().tolist()
            oz_skus_str = ', '.join([f"'{sku}'" for sku in oz_skus_for_rating])
            
            rating_query = f"""
            SELECT oz_sku, rating, rev_number
            FROM oz_card_rating 
            WHERE oz_sku IN ({oz_skus_str})
            """
            
            ratings_df = conn.execute(rating_query).fetchdf()
            results['rated_oz_sku_count'] = len(ratings_df)
            
            results['debug_info'].append(f"✅ Найдено рейтингов: {len(ratings_df)}")
            
            if show_debug and not ratings_df.empty:
                st.info(f"OZ SKU с рейтингами: {sorted(ratings_df['oz_sku'].astype(str).unique().tolist())}")
            
            if not ratings_df.empty:
                # Финальные результаты через централизованный линкер
                from utils.cross_marketplace_linker import CrossMarketplaceLinker
                linker = CrossMarketplaceLinker(conn)
                final_result = linker.get_links_with_ozon_ratings([wb_sku])
                results['final_rating_data'] = final_result
                
                if not final_result.empty:
                    results['debug_info'].append("✅ Итоговые данные успешно сформированы")
                else:
                    results['debug_info'].append("❌ Ошибка при формировании итоговых данных")
            else:
                results['debug_info'].append("❌ Рейтинги для связанных oz_sku не найдены")
        
        return results
        
    except Exception as e:
        results['debug_info'].append(f"❌ Ошибка: {e}")
        return results


def analyze_group_quality(groups_df: pd.DataFrame) -> dict:
    """
    Анализирует качество созданных групп товаров.
    
    Args:
        groups_df: DataFrame с группами товаров
        
    Returns:
        Словарь с метриками качества групп
    """
    if groups_df.empty:
        return {}
    
    try:
        total_groups = groups_df['group_id'].nunique()
        total_products = len(groups_df)
        
        # Анализ размеров групп
        group_sizes = groups_df.groupby('group_id').size()
        avg_group_size = group_sizes.mean()
        max_group_size = group_sizes.max()
        min_group_size = group_sizes.min()
        
        # Анализ рейтингов
        avg_group_rating = groups_df['group_avg_rating'].mean()
        
        # Рекомендации
        recommendations = groups_df['group_recommendation'].value_counts()
        
        # Группы с хорошими рейтингами
        good_groups = groups_df[groups_df['group_avg_rating'] >= 4.0]['group_id'].nunique()
        excellent_groups = groups_df[groups_df['group_avg_rating'] >= 4.5]['group_id'].nunique()
        
        # Анализ разделенных групп
        split_groups = groups_df[groups_df['group_recommendation'].str.contains('разделена', case=False, na=False)]['group_id'].nunique()
        large_groups = groups_df[groups_df['group_recommendation'].str.contains('большая группа', case=False, na=False)]['group_id'].nunique()
        
        # Статистика по размерам групп
        single_product_groups = (group_sizes == 1).sum()
        small_groups = ((group_sizes > 1) & (group_sizes <= 5)).sum()
        medium_groups = ((group_sizes > 5) & (group_sizes <= 15)).sum()
        large_groups_count = (group_sizes > 15).sum()
        
        # Анализ приоритизации (если включена)
        priority_stats = {}
        if 'sort' in groups_df.columns:
            # Статистика по полю sort
            sort_data = groups_df['sort'].dropna()
            if not sort_data.empty:
                priority_stats = {
                    'has_priority_data': True,
                    'products_with_priority': len(sort_data),
                    'avg_sort_value': round(sort_data.mean(), 2),
                    'max_sort_value': sort_data.max(),
                    'min_sort_value': sort_data.min(),
                    'high_priority_products': len(sort_data[sort_data > sort_data.median()]) if len(sort_data) > 0 else 0
                }
            else:
                priority_stats = {'has_priority_data': False}
        else:
            priority_stats = {'has_priority_data': False}
        
        result = {
            'total_groups': total_groups,
            'total_products': total_products,
            'avg_group_size': round(avg_group_size, 1),
            'max_group_size': max_group_size,
            'min_group_size': min_group_size,
            'avg_group_rating': round(avg_group_rating, 2),
            'good_groups_count': good_groups,
            'excellent_groups_count': excellent_groups,
            'split_groups_count': split_groups,
            'large_groups_count': large_groups,
            'single_product_groups': single_product_groups,
            'small_groups': small_groups,
            'medium_groups': medium_groups,
            'large_groups_size_count': large_groups_count,
            'recommendations': recommendations.to_dict(),
            'priority_stats': priority_stats
        }
        
        return result
        
    except Exception as e:
        print(f"Ошибка при анализе качества групп: {e}")
        return {}


def create_advanced_product_groups(
    conn,
    wb_skus: list[str],
    grouping_columns: list[str] = None,
    min_group_rating: float = 0.0,
    max_wb_sku_per_group: int = 20,
    enable_sort_priority: bool = True,
    wb_category_filter: str = None,
    progress_callback = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Создает расширенные группы товаров с умной приоритизацией и компенсацией рейтинга.
    
    Args:
        conn: соединение с БД
        wb_skus: список wb_sku для группировки
        grouping_columns: колонки для группировки (gender добавляется автоматически)
        min_group_rating: минимальный рейтинг группы
        max_wb_sku_per_group: максимальный размер группы
        enable_sort_priority: использовать приоритизацию по sort
        wb_category_filter: фильтр по категории wb_category
        progress_callback: функция для обновления прогресса progress_callback(progress, status)
        
    Returns:
        tuple: (groups_df, no_links_df, low_rating_df)
    """
    from utils.cross_marketplace_linker import CrossMarketplaceLinker
    
    def update_progress(progress: float, status: str):
        """Вспомогательная функция для обновления прогресса"""
        if progress_callback:
            # Конвертируем локальный прогресс (0.85-1.0) в нужный диапазон
            adjusted_progress = 0.85 + (progress * 0.15)
            progress_callback(adjusted_progress, status)
    
    # Step 1: Получаем данные о товарах и их рейтингах
    update_progress(0.1, "🔍 Поиск связанных товаров Ozon...")
    linker = CrossMarketplaceLinker(conn)
    wb_ratings_df = linker.get_links_with_ozon_ratings(wb_skus)
    
    update_progress(0.2, "📊 Анализ рейтингов товаров...")
    
    # Отделяем товары без связей
    linked_wb_skus = wb_ratings_df['wb_sku'].unique().tolist() if not wb_ratings_df.empty else []
    no_links_wb_skus = [sku for sku in wb_skus if sku not in linked_wb_skus]
    
    no_links_df = pd.DataFrame({
        'wb_sku': no_links_wb_skus,
        'issue': 'Не найдены связанные товары Ozon'
    })
    
    if wb_ratings_df.empty:
        update_progress(1.0, "❌ Завершено: товары без связей")
        return pd.DataFrame(), no_links_df, pd.DataFrame()
    
    # Step 2: Получаем данные из punta_table включая поле sort
    update_progress(0.35, "📋 Получение данных punta_table...")
    punta_columns = ['gender']  # gender обязательно
    if grouping_columns:
        punta_columns.extend([col for col in grouping_columns if col != 'gender'])
    if enable_sort_priority:
        punta_columns.append('sort')
    
    punta_data = get_punta_table_data_for_wb_skus(conn, linked_wb_skus, punta_columns)
    
    # Step 3: Получаем остатки товаров Ozon с группировкой по wb_sku
    update_progress(0.5, "📦 Анализ остатков товаров...")
    stock_data = _get_wb_sku_stock_summary(conn, linked_wb_skus)
    
    # Step 4: Объединяем все данные
    update_progress(0.65, "🔗 Объединение данных...")
    # Приводим wb_sku к строковому типу перед соединением
    wb_ratings_df['wb_sku'] = wb_ratings_df['wb_sku'].astype(str)
    if not punta_data.empty:
        punta_data['wb_sku'] = punta_data['wb_sku'].astype(str)
    if not stock_data.empty:
        stock_data['wb_sku'] = stock_data['wb_sku'].astype(str)
    
    merged_df = pd.merge(wb_ratings_df, punta_data, on='wb_sku', how='left')
    merged_df = pd.merge(merged_df, stock_data, on='wb_sku', how='left')
    
    # ВАЖНО: ВСЕГДА добавляем данные о категориях wb_category для критериев группировки
    update_progress(0.65, "📦 Получение категорий товаров...")
    if 'wb_category' not in merged_df.columns:
        # Получаем данные о категориях из wb_products для всех товаров
        category_data = _get_wb_category_data(conn, linked_wb_skus)
        if not category_data.empty:
            category_data['wb_sku'] = category_data['wb_sku'].astype(str)
            merged_df = pd.merge(merged_df, category_data, on='wb_sku', how='left')
            print(f"DEBUG: Добавлены категории для {len(category_data)} товаров")
        else:
            print(f"DEBUG: Категории не найдены для товаров")
    
    # Фильтруем по категории если указана
    if wb_category_filter:
        update_progress(0.7, f"🎯 Фильтрация по категории: {wb_category_filter}...")
        if 'wb_category' in merged_df.columns:
            merged_df = merged_df[merged_df['wb_category'] == wb_category_filter]
        else:
            print(f"DEBUG: Колонка wb_category отсутствует, фильтрация невозможна")
    
    if merged_df.empty:
        update_progress(1.0, "❌ Завершено: нет данных после фильтрации")
        return pd.DataFrame(), no_links_df, pd.DataFrame()
    
    # Step 5: Приоритизация товаров
    update_progress(0.75, "🏆 Приоритизация товаров...")
    priority_df = _prioritize_wb_skus(merged_df, enable_sort_priority)
    
    # Отладочная информация
    total_input = len(wb_skus)
    found_punta_data = len(punta_data)
    found_stock_data = len(stock_data) 
    merged_data_count = len(merged_df)
    prioritized_count = len(priority_df)
    
    print(f"DEBUG: Входные данные: {total_input} wb_sku")
    print(f"DEBUG: Найдено punta данных: {found_punta_data}")
    print(f"DEBUG: Найдено данных об остатках: {found_stock_data}")
    print(f"DEBUG: После объединения: {merged_data_count}")
    print(f"DEBUG: После приоритизации: {prioritized_count}")
    
    update_progress(0.85, "🔄 Создание групп и компенсация рейтинга...")
    groups_df, low_rating_df = _create_groups_with_compensation(
        conn, priority_df, grouping_columns, min_group_rating, max_wb_sku_per_group, progress_callback
    )
    
    # Дополнительная отладочная информация после группировки
    if not groups_df.empty:
        # Правильный подсчет: считаем уникальные группы по merge_on_card
        unique_groups_count = groups_df['merge_on_card'].nunique()
        total_items_in_groups = len(groups_df)
    else:
        unique_groups_count = 0
        total_items_in_groups = 0
    
    low_rating_count = len(low_rating_df) if not low_rating_df.empty else 0
    
    print(f"DEBUG: Создано уникальных групп: {unique_groups_count} (общее количество товаров в группах: {total_items_in_groups})")
    print(f"DEBUG: Товаров с низким рейтингом: {low_rating_count}")
    
    # Расширяем группы с oz_vendor_code для экспорта
    update_progress(0.95, "🔗 Добавление oz_vendor_code...")
    if not groups_df.empty:
        groups_df = _expand_groups_with_oz_vendor_codes(conn, groups_df)
    
    update_progress(1.0, "✅ Группировка завершена успешно!")
    return groups_df, no_links_df, low_rating_df


def _create_single_item_group(item: pd.Series, recommendation: str) -> pd.DataFrame:
    """
    Создает группу из одного товара для товаров с высоким рейтингом
    """
    wb_sku = item['wb_sku']
    
    group_data = {
        'wb_sku': [wb_sku],
        'merge_on_card': [wb_sku],  # Используем собственный wb_sku как код объединения
        'avg_rating': [item.get('avg_rating', 0)],
        'group_avg_rating': [item.get('avg_rating', 0)],
        'wb_count': [1],
        'is_defective': [False],
        'group_recommendation': [recommendation]
    }
    
    # Добавляем дополнительные поля если они есть
    for key in ['gender', 'season', 'material', 'sort', 'total_stock', 'has_stock']:
        if key in item:
            group_data[key] = [item[key]]
    
    return pd.DataFrame(group_data)


def _find_similar_low_rating_items(priority_df: pd.DataFrame, group_criteria: dict, used_wb_skus: set, max_items: int, min_rating_threshold: float) -> list:
    """
    Находит похожие товары для группы по критериям группировки.
    ВАЖНО: Исключает товары с рейтингом выше минимального порога!
    """
    print(f"DEBUG: Поиск похожих товаров (критерии: {group_criteria}, макс: {max_items}, порог рейтинга: {min_rating_threshold})")
    
    similar_items = []
    
    # Фильтруем по критериям группировки
    filtered_df = priority_df.copy()
    original_count = len(filtered_df)
    
    for col, value in group_criteria.items():
        if col in filtered_df.columns:
            before_count = len(filtered_df)
            if pd.notna(value):
                filtered_df = filtered_df[filtered_df[col] == value]
            else:
                # Если значение NaN, ищем товары с NaN в этом поле
                filtered_df = filtered_df[filtered_df[col].isna()]
            after_count = len(filtered_df)
            print(f"DEBUG: Фильтрация по {col}={value}: {before_count} -> {after_count}")
    
    # ВАЖНО: Исключаем товары с высоким рейтингом (они должны быть в отдельных группах)
    before_rating_filter = len(filtered_df)
    filtered_df = filtered_df[
        (filtered_df['avg_rating'].isna()) | 
        (filtered_df['avg_rating'] < min_rating_threshold)
    ]
    after_rating_filter = len(filtered_df)
    print(f"DEBUG: Исключение высокорейтинговых товаров: {before_rating_filter} -> {after_rating_filter}")
    
    # Исключаем уже использованные товары
    before_used_filter = len(filtered_df)
    filtered_df = filtered_df[~filtered_df['wb_sku'].isin(used_wb_skus)]
    after_used_filter = len(filtered_df)
    print(f"DEBUG: Исключение использованных товаров: {before_used_filter} -> {after_used_filter}")
    
    # Берем топ товаров по рейтингу
    selected_count = min(max_items, len(filtered_df))
    for _, row in filtered_df.head(selected_count).iterrows():
        similar_items.append(row)
    
    print(f"DEBUG: Итого найдено {len(similar_items)} похожих товаров из {original_count} исходных")
    return similar_items


def _is_defective_item(conn, wb_sku: str) -> bool:
    """
    Проверяет является ли товар бракованным (oz_vendor_code начинается с "БракSH")
    """
    print(f"DEBUG: Проверяем на брак wb_sku {wb_sku}")
    
    try:
        from utils.cross_marketplace_linker import CrossMarketplaceLinker
        
        linker = CrossMarketplaceLinker(conn)
        extended_links = linker.get_extended_links([wb_sku], include_product_details=False)
        
        if extended_links.empty:
            print(f"DEBUG: Нет связей для wb_sku {wb_sku}")
            return False
        
        # Проверяем есть ли oz_vendor_code начинающиеся с "БракSH"
        defective_count = 0
        for _, link in extended_links.iterrows():
            oz_vendor_code = link.get('oz_vendor_code', '')
            if isinstance(oz_vendor_code, str) and oz_vendor_code.startswith('БракSH'):
                defective_count += 1
        
        is_defective = defective_count > 0
        print(f"DEBUG: wb_sku {wb_sku} {'является' if is_defective else 'не является'} бракованным (найдено {defective_count} бракованных кодов)")
        
        return is_defective
        
    except Exception as e:
        print(f"Ошибка проверки брака для {wb_sku}: {e}")
        return False


def _create_defective_group(priority_item: pd.Series) -> pd.DataFrame:
    """
    Создает уникальную группу для бракованного товара
    """
    wb_sku = priority_item['wb_sku']
    unique_merge_code = f"БракSH_{wb_sku}"
    
    group_data = {
        'wb_sku': [wb_sku],
        'merge_on_card': [unique_merge_code],
        'avg_rating': [priority_item.get('avg_rating', 0)],
        'group_avg_rating': [priority_item.get('avg_rating', 0)],
        'wb_count': [1],
        'is_defective': [True],
        'group_recommendation': ['Бракованный товар - отдельная карточка']
    }
    
    return pd.DataFrame(group_data)


def _find_rating_compensators(
    conn, 
    group_criteria: dict, 
    target_rating: float, 
    current_rating: float, 
    current_group_size: int,
    used_wb_skus: set,
    max_compensators: int = 10
) -> list:
    """
    Ищет товары с высоким рейтингом для компенсации низкого рейтинга группы.
    
    Приоритизация поиска:
    1. Сначала ищет среди товаров БЕЗ остатка (stock = 0)
    2. Если недостаточно, ищет среди товаров С остатком (stock > 0)
    """
    from utils.cross_marketplace_linker import CrossMarketplaceLinker
    
    print(f"DEBUG: Поиск компенсаторов для группы (текущий рейтинг: {current_rating}, целевой: {target_rating})")
    
    try:
        # Создаем базовые WHERE условия на основе критериев группировки
        base_conditions = []
        base_params = []
        
        for key, value in group_criteria.items():
            if value is not None and str(value).strip():
                # wb_category находится в таблице wb_products, а не в punta_table
                if key == 'wb_category':
                    base_conditions.append(f"wp.{key} = ?")
                else:
                    base_conditions.append(f"p.{key} = ?")
                base_params.append(str(value))
        
        # Исключаем уже используемые wb_sku
        if used_wb_skus:
            used_wb_skus_list = list(used_wb_skus)
            placeholders = ', '.join(['?'] * len(used_wb_skus_list))
            base_conditions.append(f"p.wb_sku NOT IN ({placeholders})")
            base_params.extend(used_wb_skus_list)
        
        def search_compensators_with_stock_filter(stock_condition, stock_description):
            """Поиск компенсаторов с определенным условием по остаткам"""
            where_conditions = base_conditions.copy()
            params = base_params.copy()
            
            # Добавляем условие по остаткам
            where_conditions.append(stock_condition)
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            print(f"DEBUG: Ищем компенсаторов среди товаров {stock_description}")
            print(f"DEBUG: SQL критерии: {where_clause}")
            
            # Базовый запрос с JOIN к таблице остатков
            query = f"""
            WITH wb_with_ratings_and_stock AS (
                SELECT 
                    p.wb_sku,
                    AVG(r.rating) as avg_rating,
                    p.gender,
                    wp.wb_category,
                    COUNT(r.rating) as rating_count,
                    COALESCE(stock_summary.total_stock, 0) as total_stock
                FROM punta_table p
                INNER JOIN wb_products wp ON CAST(p.wb_sku AS VARCHAR) = CAST(wp.wb_sku AS VARCHAR)
                LEFT JOIN (
                    -- Получаем общий остаток по wb_sku через связи с Ozon
                    SELECT 
                        wp_stock.wb_sku,
                        SUM(COALESCE(oz_products_stock.oz_fbo_stock, 0)) as total_stock
                    FROM wb_products wp_stock
                    INNER JOIN (
                        SELECT wb_sku, unnest(string_split(wb_barcodes, ';')) as barcode
                        FROM wb_products
                        WHERE wb_barcodes IS NOT NULL AND wb_barcodes != ''
                    ) wb_normalized_stock ON wp_stock.wb_sku = wb_normalized_stock.wb_sku
                    INNER JOIN oz_barcodes ob_stock ON TRIM(wb_normalized_stock.barcode) = TRIM(ob_stock.oz_barcode)
                    INNER JOIN oz_products oz_products_stock ON ob_stock.oz_product_id = oz_products_stock.oz_product_id
                    WHERE oz_products_stock.oz_fbo_stock IS NOT NULL 
                      AND oz_products_stock.oz_fbo_stock > 0
                    GROUP BY wp_stock.wb_sku
                ) stock_summary ON p.wb_sku = stock_summary.wb_sku
                INNER JOIN (
                    -- Получаем связи wb_sku -> oz_sku через штрихкоды
                    SELECT DISTINCT
                        wp2.wb_sku,
                        op.oz_sku
                    FROM wb_products wp2
                    INNER JOIN (
                        SELECT wb_sku, unnest(string_split(wb_barcodes, ';')) as barcode
                        FROM wb_products
                        WHERE wb_barcodes IS NOT NULL AND wb_barcodes != ''
                    ) wb_normalized ON wp2.wb_sku = wb_normalized.wb_sku
                    INNER JOIN oz_barcodes ob ON TRIM(wb_normalized.barcode) = TRIM(ob.oz_barcode)
                    INNER JOIN oz_products op ON ob.oz_product_id = op.oz_product_id
                    WHERE wp2.wb_sku = p.wb_sku
                ) links ON p.wb_sku = links.wb_sku
                INNER JOIN oz_card_rating r ON CAST(links.oz_sku AS VARCHAR) = CAST(r.oz_sku AS VARCHAR)
                WHERE {where_clause}
                GROUP BY p.wb_sku, p.gender, wp.wb_category, stock_summary.total_stock
                HAVING AVG(r.rating) >= ?
            )
            SELECT DISTINCT wb_sku, avg_rating, gender, wb_category, total_stock
            FROM wb_with_ratings_and_stock
            ORDER BY avg_rating DESC
            LIMIT ?
            """
            
            # Добавляем параметр для минимального рейтинга
            params.append(target_rating)
            
            return conn.execute(query, params + [max_compensators]).fetchdf()
        
        # ЭТАП 1: Ищем среди товаров БЕЗ остатка
        print(f"DEBUG: ЭТАП 1 - Поиск среди товаров без остатка")
        no_stock_df = search_compensators_with_stock_filter(
            "COALESCE(stock_summary.total_stock, 0) = 0", 
            "без остатка (stock = 0)"
        )
        
        compensators = []
        
        if not no_stock_df.empty:
            print(f"DEBUG: Найдено {len(no_stock_df)} компенсаторов без остатка")
            compensators.extend(no_stock_df['wb_sku'].tolist())
        else:
            print(f"DEBUG: Компенсаторов без остатка не найдено")
        
        # ЭТАП 2: Если нужно больше компенсаторов, ищем среди товаров С остатком
        remaining_slots = max_compensators - len(compensators)
        if remaining_slots > 0:
            print(f"DEBUG: ЭТАП 2 - Поиск среди товаров с остатком (нужно еще {remaining_slots})")
            
            # Исключаем уже найденных компенсаторов
            if compensators:
                current_used = used_wb_skus.copy()
                current_used.update(compensators)
            else:
                current_used = used_wb_skus
            
            # Обновляем базовые условия для исключения уже найденных компенсаторов
            updated_base_conditions = []
            updated_base_params = []
            
            for key, value in group_criteria.items():
                if value is not None and str(value).strip():
                    # wb_category находится в таблице wb_products, а не в punta_table
                    if key == 'wb_category':
                        updated_base_conditions.append(f"wp.{key} = ?")
                    else:
                        updated_base_conditions.append(f"p.{key} = ?")
                    updated_base_params.append(str(value))
            
            # Исключаем уже используемые wb_sku (включая найденных компенсаторов)
            if current_used:
                current_used_list = list(current_used)
                placeholders = ', '.join(['?'] * len(current_used_list))
                updated_base_conditions.append(f"p.wb_sku NOT IN ({placeholders})")
                updated_base_params.extend(current_used_list)
            
            # Временно обновляем base условия
            original_base_conditions = base_conditions
            original_base_params = base_params
            base_conditions = updated_base_conditions
            base_params = updated_base_params
            
            with_stock_df = search_compensators_with_stock_filter(
                "COALESCE(stock_summary.total_stock, 0) > 0", 
                "с остатком (stock > 0)"
            )
            
            # Восстанавливаем оригинальные base условия
            base_conditions = original_base_conditions
            base_params = original_base_params
            
            if not with_stock_df.empty:
                print(f"DEBUG: Найдено {len(with_stock_df)} компенсаторов с остатком")
                # Берем только необходимое количество
                additional_compensators = with_stock_df['wb_sku'].tolist()[:remaining_slots]
                compensators.extend(additional_compensators)
            else:
                print(f"DEBUG: Компенсаторов с остатком не найдено")
        
        if not compensators:
            print(f"DEBUG: Компенсаторы не найдены, пробуем расширенный поиск без gender...")
            
            # Пробуем найти компенсаторы без учета gender (расширенный поиск)
            fallback_criteria = {k: v for k, v in group_criteria.items() if k != 'gender'}
            fallback_conditions = []
            fallback_params = []
            
            for key, value in fallback_criteria.items():
                if value is not None and str(value).strip():
                    # wb_category находится в таблице wb_products, а не в punta_table
                    if key == 'wb_category':
                        fallback_conditions.append(f"wp.{key} = ?")
                    else:
                        fallback_conditions.append(f"p.{key} = ?")
                    fallback_params.append(str(value))
            
            # Исключаем уже используемые wb_sku
            if used_wb_skus:
                used_wb_skus_list = list(used_wb_skus)
                placeholders = ', '.join(['?'] * len(used_wb_skus_list))
                fallback_conditions.append(f"p.wb_sku NOT IN ({placeholders})")
                fallback_params.extend(used_wb_skus_list)
            
            # Сначала пробуем без остатка
            fallback_conditions_no_stock = fallback_conditions.copy()
            fallback_conditions_no_stock.append("COALESCE(stock_summary.total_stock, 0) = 0")
            
            fallback_where = " AND ".join(fallback_conditions_no_stock) if fallback_conditions_no_stock else "1=1"
            fallback_params_copy = fallback_params.copy()
            fallback_params_copy.append(target_rating)
            
            fallback_query = f"""
            WITH wb_with_ratings_and_stock AS (
                SELECT 
                    p.wb_sku,
                    AVG(r.rating) as avg_rating,
                    p.gender,
                    wp.wb_category,
                    COUNT(r.rating) as rating_count,
                    COALESCE(stock_summary.total_stock, 0) as total_stock
                FROM punta_table p
                INNER JOIN wb_products wp ON CAST(p.wb_sku AS VARCHAR) = CAST(wp.wb_sku AS VARCHAR)
                LEFT JOIN (
                    -- Получаем общий остаток по wb_sku через связи с Ozon
                    SELECT 
                        wp_stock.wb_sku,
                        SUM(COALESCE(oz_products_stock.oz_fbo_stock, 0)) as total_stock
                    FROM wb_products wp_stock
                    INNER JOIN (
                        SELECT wb_sku, unnest(string_split(wb_barcodes, ';')) as barcode
                        FROM wb_products
                        WHERE wb_barcodes IS NOT NULL AND wb_barcodes != ''
                    ) wb_normalized_stock ON wp_stock.wb_sku = wb_normalized_stock.wb_sku
                    INNER JOIN oz_barcodes ob_stock ON TRIM(wb_normalized_stock.barcode) = TRIM(ob_stock.oz_barcode)
                    INNER JOIN oz_products oz_products_stock ON ob_stock.oz_product_id = oz_products_stock.oz_product_id
                    WHERE oz_products_stock.oz_fbo_stock IS NOT NULL 
                      AND oz_products_stock.oz_fbo_stock > 0
                    GROUP BY wp_stock.wb_sku
                ) stock_summary ON p.wb_sku = stock_summary.wb_sku
                INNER JOIN (
                    SELECT DISTINCT
                        wp2.wb_sku,
                        op.oz_sku
                    FROM wb_products wp2
                    INNER JOIN (
                        SELECT wb_sku, unnest(string_split(wb_barcodes, ';')) as barcode
                        FROM wb_products
                        WHERE wb_barcodes IS NOT NULL AND wb_barcodes != ''
                    ) wb_normalized ON wp2.wb_sku = wb_normalized.wb_sku
                    INNER JOIN oz_barcodes ob ON TRIM(wb_normalized.barcode) = TRIM(ob.oz_barcode)
                    INNER JOIN oz_products op ON ob.oz_product_id = op.oz_product_id
                    WHERE wp2.wb_sku = p.wb_sku
                ) links ON p.wb_sku = links.wb_sku
                INNER JOIN oz_card_rating r ON CAST(links.oz_sku AS VARCHAR) = CAST(r.oz_sku AS VARCHAR)
                WHERE {fallback_where}
                GROUP BY p.wb_sku, p.gender, wp.wb_category, stock_summary.total_stock
                HAVING AVG(r.rating) >= ?
            )
            SELECT DISTINCT wb_sku, avg_rating, gender, wb_category, total_stock
            FROM wb_with_ratings_and_stock
            ORDER BY avg_rating DESC
            LIMIT ?
            """
            
            compensators_df = conn.execute(fallback_query, fallback_params_copy + [max_compensators]).fetchdf()
            
            if not compensators_df.empty:
                print(f"DEBUG: Найдено {len(compensators_df)} компенсаторов в расширенном поиске (без остатка)")
                compensators.extend(compensators_df['wb_sku'].tolist())
            else:
                print(f"DEBUG: Компенсаторы не найдены даже в расширенном поиске")
        
        print(f"DEBUG: Всего найдено {len(compensators)} компенсаторов")
        for i, comp_sku in enumerate(compensators[:5]):  # Показываем первые 5
            print(f"DEBUG: Компенсатор {i+1}: {comp_sku}")
        
        return compensators
        
    except Exception as e:
        print(f"Ошибка поиска компенсаторов: {e}")
        
        # Добавляем диагностическую информацию о базе данных
        try:
            print(f"DEBUG: Диагностика базы данных...")
            
            # Проверяем количество товаров в punta_table
            total_punta = conn.execute("SELECT COUNT(*) FROM punta_table").fetchone()[0]
            print(f"DEBUG: Всего товаров в punta_table: {total_punta}")
            
            # Проверяем количество рейтингов
            total_ratings = conn.execute("SELECT COUNT(*) FROM oz_card_rating").fetchone()[0]
            print(f"DEBUG: Всего рейтингов в oz_card_rating: {total_ratings}")
            
            # Проверяем диапазон рейтингов
            rating_stats = conn.execute("SELECT MIN(rating), MAX(rating), AVG(rating) FROM oz_card_rating WHERE rating IS NOT NULL").fetchone()
            print(f"DEBUG: Рейтинги - мин: {rating_stats[0]}, макс: {rating_stats[1]}, средний: {rating_stats[2]}")
            
            # Проверяем высокорейтинговые товары
            high_rating_count = conn.execute(f"SELECT COUNT(*) FROM oz_card_rating WHERE rating >= {target_rating}").fetchone()[0]
            print(f"DEBUG: Товаров с рейтингом >= {target_rating}: {high_rating_count}")
            
            # Проверяем товары без остатка (через связи с Ozon)
            no_stock_query = """
            SELECT COUNT(DISTINCT p.wb_sku) 
            FROM punta_table p
            LEFT JOIN (
                SELECT 
                    wp_stock.wb_sku,
                    SUM(COALESCE(oz_products_stock.oz_fbo_stock, 0)) as total_stock
                FROM wb_products wp_stock
                INNER JOIN (
                    SELECT wb_sku, unnest(string_split(wb_barcodes, ';')) as barcode
                    FROM wb_products
                    WHERE wb_barcodes IS NOT NULL AND wb_barcodes != ''
                ) wb_normalized_stock ON wp_stock.wb_sku = wb_normalized_stock.wb_sku
                INNER JOIN oz_barcodes ob_stock ON TRIM(wb_normalized_stock.barcode) = TRIM(ob_stock.oz_barcode)
                INNER JOIN oz_products oz_products_stock ON ob_stock.oz_product_id = oz_products_stock.oz_product_id
                GROUP BY wp_stock.wb_sku
            ) stock_summary ON p.wb_sku = stock_summary.wb_sku
            WHERE COALESCE(stock_summary.total_stock, 0) = 0
            """
            no_stock_count = conn.execute(no_stock_query).fetchone()[0]
            print(f"DEBUG: Товаров без остатка: {no_stock_count}")
            
            # Проверяем товары с остатком (через связи с Ozon)
            with_stock_query = """
            SELECT COUNT(DISTINCT p.wb_sku) 
            FROM punta_table p
            INNER JOIN (
                SELECT 
                    wp_stock.wb_sku,
                    SUM(COALESCE(oz_products_stock.oz_fbo_stock, 0)) as total_stock
                FROM wb_products wp_stock
                INNER JOIN (
                    SELECT wb_sku, unnest(string_split(wb_barcodes, ';')) as barcode
                    FROM wb_products
                    WHERE wb_barcodes IS NOT NULL AND wb_barcodes != ''
                ) wb_normalized_stock ON wp_stock.wb_sku = wb_normalized_stock.wb_sku
                INNER JOIN oz_barcodes ob_stock ON TRIM(wb_normalized_stock.barcode) = TRIM(ob_stock.oz_barcode)
                INNER JOIN oz_products oz_products_stock ON ob_stock.oz_product_id = oz_products_stock.oz_product_id
                GROUP BY wp_stock.wb_sku
            ) stock_summary ON p.wb_sku = stock_summary.wb_sku
            WHERE stock_summary.total_stock > 0
            """
            with_stock_count = conn.execute(with_stock_query).fetchone()[0]
            print(f"DEBUG: Товаров с остатком: {with_stock_count}")
            
        except Exception as diag_error:
            print(f"DEBUG: Ошибка диагностики: {diag_error}")
        
        return []


def _create_group_dataframe(group_items: list, primary_wb_sku: str, group_rating: float) -> pd.DataFrame:
    """
    Создает DataFrame для группы товаров
    """
    group_data = []
    
    for item in group_items:
        # Обрабатываем как Series (из DataFrame), так и dict объекты
        if isinstance(item, pd.Series):
            row_data = {
                'wb_sku': item.get('wb_sku', ''),
                'merge_on_card': primary_wb_sku,  # Используем wb_sku приоритетного товара
                'avg_rating': item.get('avg_rating', 0),
                'group_avg_rating': group_rating,
                'wb_count': len(group_items),
                'is_defective': False
            }
            
            # Добавляем дополнительные поля если они есть
            for key in ['gender', 'season', 'material', 'sort', 'total_stock', 'has_stock']:
                if key in item:
                    row_data[key] = item[key]
        else:
            # Обрабатываем dict объект
            row_data = {
                'wb_sku': item.get('wb_sku', ''),
                'merge_on_card': primary_wb_sku,
                'avg_rating': item.get('avg_rating', 0),
                'group_avg_rating': group_rating,
                'wb_count': len(group_items),
                'is_defective': False
            }
            
            # Добавляем дополнительные поля
            for key in ['gender', 'season', 'material', 'sort', 'total_stock', 'has_stock']:
                if key in item:
                    row_data[key] = item[key]
        
        group_data.append(row_data)
    
    group_df = pd.DataFrame(group_data)
    
    # Проверяем, есть ли в группе товары без рейтингов
    has_no_rating_items = any(
        pd.isna(item.get('avg_rating')) or item.get('avg_rating') == 0 
        for item in group_items
    )
    
    # Добавляем рекомендации с учетом товаров без рейтингов
    if has_no_rating_items and group_rating > 0:
        # Группа содержит товары без рейтингов, но компенсаторы дали рейтинг
        if group_rating >= 4.5:
            recommendation = "Товары без рейтингов компенсированы (отличный рейтинг)"
        elif group_rating >= 4.0:
            recommendation = "Товары без рейтингов компенсированы (хороший рейтинг)"
        elif group_rating >= 3.5:
            recommendation = "Товары без рейтингов компенсированы (удовлетворительный рейтинг)"
        else:
            recommendation = "Товары без рейтингов частично компенсированы"
    elif has_no_rating_items and group_rating == 0:
        # Товары без рейтингов и нет компенсаторов
        recommendation = "Товары без рейтингов - требуются компенсаторы"
    else:
        # Обычная группа с рейтингами
        if group_rating >= 4.5:
            recommendation = "Отличная группа"
        elif group_rating >= 4.0:
            recommendation = "Хорошая группа"
        elif group_rating >= 3.5:
            recommendation = "Удовлетворительная группа"
        else:
            recommendation = "Требует внимания"
    
    group_df['group_recommendation'] = recommendation
    
    return group_df


def _expand_groups_with_oz_vendor_codes(conn, groups_df: pd.DataFrame) -> pd.DataFrame:
    """
    Расширяет группы, добавляя oz_vendor_code для каждого wb_sku.
    Создает отдельную строку для каждого oz_vendor_code.
    """
    if groups_df.empty:
        return groups_df
        
    print(f"DEBUG: Расширение групп с oz_vendor_code для {len(groups_df)} строк")
    
    try:
        from utils.cross_marketplace_linker import CrossMarketplaceLinker
        
        # Получаем уникальные wb_sku из групп
        unique_wb_skus = groups_df['wb_sku'].unique().tolist()
        
        linker = CrossMarketplaceLinker(conn)
        extended_links = linker.get_extended_links(unique_wb_skus, include_product_details=False)
        
        if extended_links.empty:
            print(f"DEBUG: Нет связей с Ozon для wb_sku из групп")
            # Добавляем пустую колонку oz_vendor_code
            groups_df['oz_vendor_code'] = ''
            return groups_df
        
        # Приводим wb_sku к строковому типу для корректного merge
        groups_df['wb_sku'] = groups_df['wb_sku'].astype(str)
        extended_links['wb_sku'] = extended_links['wb_sku'].astype(str)
        
        # Объединяем группы с oz_vendor_code
        # Каждый wb_sku может иметь несколько oz_vendor_code
        expanded_groups = pd.merge(
            groups_df,
            extended_links[['wb_sku', 'oz_vendor_code']].drop_duplicates(),
            on='wb_sku',
            how='left'
        )
        
        # Заполняем пустые oz_vendor_code
        expanded_groups['oz_vendor_code'] = expanded_groups['oz_vendor_code'].fillna('')
        
        # Переупорядочиваем колонки: oz_vendor_code на вторую позицию после wb_sku
        columns = list(expanded_groups.columns)
        if 'oz_vendor_code' in columns and 'wb_sku' in columns:
            # Удаляем oz_vendor_code из текущей позиции
            columns.remove('oz_vendor_code')
            # Находим позицию wb_sku и вставляем oz_vendor_code после него
            wb_sku_index = columns.index('wb_sku')
            columns.insert(wb_sku_index + 1, 'oz_vendor_code')
            # Переупорядочиваем DataFrame
            expanded_groups = expanded_groups[columns]
        
        print(f"DEBUG: Расширение завершено: {len(groups_df)} -> {len(expanded_groups)} строк")
        print(f"DEBUG: Порядок колонок: {list(expanded_groups.columns)}")
        
        return expanded_groups
        
    except Exception as e:
        print(f"DEBUG: Ошибка расширения групп с oz_vendor_code: {e}")
        # В случае ошибки добавляем пустую колонку
        groups_df['oz_vendor_code'] = ''
        return groups_df 


def format_rating_color(rating: float) -> str:
    """
    Возвращает цвет для отображения рейтинга в UI
    """
    if pd.isna(rating) or rating == 0:
        return "gray"
    elif rating >= 4.5:
        return "green"
    elif rating >= 4.0:
        return "lightgreen"
    elif rating >= 3.5:
        return "yellow"
    elif rating >= 3.0:
        return "orange"
    else:
        return "red"


def _ensure_wb_sku_string_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Утилита для приведения wb_sku к строковому типу во избежание ошибок merge
    """
    if df.empty or 'wb_sku' not in df.columns:
        return df
    
    df = df.copy()
    df['wb_sku'] = df['wb_sku'].astype(str)
    return df


def _get_wb_sku_stock_summary(conn, wb_skus: list[str]) -> pd.DataFrame:
    """
    Получает суммарные остатки по wb_sku из всех связанных oz_sku
    """
    if not wb_skus:
        return pd.DataFrame()
    
    try:
        from utils.cross_marketplace_linker import CrossMarketplaceLinker
        
        linker = CrossMarketplaceLinker(conn)
        extended_links = linker.get_extended_links(wb_skus, include_product_details=True)
        
        if extended_links.empty:
            result_df = pd.DataFrame({'wb_sku': wb_skus, 'total_stock': 0, 'has_stock': False})
            result_df['wb_sku'] = result_df['wb_sku'].astype(str)
            return result_df
        
        # Приводим wb_sku к строковому типу
        extended_links['wb_sku'] = extended_links['wb_sku'].astype(str)
        
        # Группируем остатки по wb_sku
        stock_summary = extended_links.groupby('wb_sku')['oz_fbo_stock'].sum().reset_index()
        stock_summary = stock_summary.rename(columns={'oz_fbo_stock': 'total_stock'})
        stock_summary['has_stock'] = stock_summary['total_stock'] > 0
        stock_summary['wb_sku'] = stock_summary['wb_sku'].astype(str)
        
        return stock_summary
        
    except Exception as e:
        print(f"Ошибка получения остатков: {e}")
        result_df = pd.DataFrame({'wb_sku': wb_skus, 'total_stock': 0, 'has_stock': False})
        result_df['wb_sku'] = result_df['wb_sku'].astype(str)
        return result_df


def _get_wb_category_data(conn, wb_skus: list[str]) -> pd.DataFrame:
    """
    Получает данные о категориях wb_category из таблицы wb_products
    """
    if not wb_skus:
        return pd.DataFrame()
    
    try:
        wb_skus_str = ', '.join([f"'{sku}'" for sku in wb_skus])
        
        query = f"""
        SELECT DISTINCT wb_sku, wb_category
        FROM wb_products 
        WHERE CAST(wb_sku AS VARCHAR) IN ({wb_skus_str})
        """
        
        category_df = conn.execute(query).fetchdf()
        category_df['wb_sku'] = category_df['wb_sku'].astype(str)
        
        return category_df
        
    except Exception as e:
        print(f"Ошибка получения категорий: {e}")
        return pd.DataFrame()


def _prioritize_wb_skus(df: pd.DataFrame, enable_sort_priority: bool) -> pd.DataFrame:
    """
    Приоритизация wb_sku:
    1. Товары с максимальным sort (независимо от остатков)
    2. Товары с положительными остатками
    3. Остальные товары
    """
    df = df.copy()
    
    if enable_sort_priority and 'sort' in df.columns:
        # Находим максимальное значение sort
        max_sort = df['sort'].max()
        
        # Создаем приоритеты
        df['priority'] = 3  # Низкий приоритет по умолчанию
        df.loc[df['has_stock'] == True, 'priority'] = 2  # Средний приоритет для товаров с остатками
        df.loc[df['sort'] == max_sort, 'priority'] = 1  # Высший приоритет для максимального sort
    else:
        # Приоритизация только по остаткам
        df['priority'] = df['has_stock'].apply(lambda x: 1 if x else 2)
    
    # Сортируем по приоритету, затем по рейтингу (descending)
    df = df.sort_values(['priority', 'avg_rating'], ascending=[True, False])
    
    return df


def _create_groups_with_compensation(
    conn, 
    priority_df: pd.DataFrame, 
    grouping_columns: list[str], 
    min_group_rating: float,
    max_wb_sku_per_group: int,
    progress_callback = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Создает группы с компенсацией низкого рейтинга.
    ВАЖНО: Товары с рейтингом выше минимального порога остаются в отдельных группах!
    """
    if priority_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    def update_progress(progress: float, status: str):
        """Вспомогательная функция для обновления прогресса"""
        if progress_callback:
            # Конвертируем локальный прогресс (0.85-1.0) в нужный диапазон
            adjusted_progress = 0.85 + (progress * 0.15)
            progress_callback(adjusted_progress, status)
    
    update_progress(0.0, "🔄 Подготовка группировки...")
    
    # Обязательно включаем gender и wb_category в группировку
    final_grouping_columns = ['gender', 'wb_category']
    if grouping_columns:
        final_grouping_columns.extend([col for col in grouping_columns if col not in ['gender', 'wb_category'] and col in priority_df.columns])
    
    # Удаляем дубликаты колонок
    final_grouping_columns = list(dict.fromkeys(final_grouping_columns))
    
    groups = []
    low_rating_items = []
    used_wb_skus = set()
    
    # ИСПРАВЛЕНИЕ: Обрабатываем ВСЕ товары в порядке приоритета, а не только priority=1
    all_items = priority_df.copy()
    total_items = len(all_items)
    
    update_progress(0.1, f"🎯 Обработка {total_items} товаров...")
    
    for idx, (_, item) in enumerate(all_items.iterrows()):
        # Обновляем прогресс каждые 10 товаров или на последнем
        if idx % 10 == 0 or idx == total_items - 1:
            progress = 0.1 + (idx / total_items) * 0.7  # 0.1 - 0.8
            update_progress(progress, f"🔍 Анализ товара {idx + 1}/{total_items}...")
        
        if item['wb_sku'] in used_wb_skus:
            print(f"DEBUG: wb_sku {item['wb_sku']} уже обработан, пропускаем")
            continue
        
        wb_sku = item['wb_sku']
        current_rating = item['avg_rating']
        
        print(f"DEBUG: Обрабатываем товар {idx + 1}/{total_items}: wb_sku={wb_sku}, рейтинг={current_rating}")
        
        # Обрабатываем бракованные товары отдельно
        if _is_defective_item(conn, wb_sku):
            print(f"DEBUG: Товар {wb_sku} является бракованным")
            defective_group = _create_defective_group(item)
            groups.append(defective_group)
            used_wb_skus.add(wb_sku)
            continue
        
        # НОВАЯ ЛОГИКА: Если товар уже имеет высокий рейтинг, создаем для него отдельную группу
        if pd.notna(current_rating) and current_rating >= min_group_rating:
            print(f"DEBUG: Товар {wb_sku} имеет высокий рейтинг ({current_rating} >= {min_group_rating}), создаем отдельную группу")
            # Создаем отдельную группу для товара с высоким рейтингом
            high_rating_group = _create_single_item_group(item, "Высокий рейтинг - отдельная карточка")
            groups.append(high_rating_group)
            used_wb_skus.add(wb_sku)
            continue
        
        # ИСПРАВЛЕНИЕ: Товары без рейтингов требуют компенсаторов для получения рейтинга группы
        if pd.isna(current_rating) or current_rating is None:
            print(f"DEBUG: Товар {wb_sku} имеет связи с Ozon, но нет рейтингов, требуется компенсация")
            # Товары без рейтингов обрабатываются как товары с рейтингом 0 для поиска компенсаторов
            current_rating = 0.0
        
        print(f"DEBUG: Товар {wb_sku} имеет низкий рейтинг ({current_rating} < {min_group_rating}), создаем группу с компенсацией")
        
        # Создаем группу для товара с низким рейтингом
        group_items = [item]
        
        # ВАЖНО: Добавляем основной товар в used_wb_skus ДО поиска похожих товаров
        # чтобы избежать дублирования
        used_wb_skus.add(wb_sku)
        
        # Ищем товары для той же группы (по grouping_columns)
        group_criteria = {}
        for col in final_grouping_columns:
            if col in item and pd.notna(item[col]):
                group_criteria[col] = item[col]
        
        print(f"DEBUG: Критерии группировки для {wb_sku}: {group_criteria}")
        
        # Добавляем похожие товары в группу (только с рейтингом ниже минимального!)
        similar_items = _find_similar_low_rating_items(priority_df, group_criteria, used_wb_skus, max_wb_sku_per_group - 1, min_group_rating)
        
        print(f"DEBUG: Найдено {len(similar_items)} похожих товаров для группы {wb_sku}")
        
        for similar_item in similar_items:
            group_items.append(similar_item)
            used_wb_skus.add(similar_item['wb_sku'])
        
        # Вычисляем средний рейтинг группы (товары без рейтингов не участвуют в расчете)
        valid_ratings = []
        for group_item in group_items:
            if isinstance(group_item, pd.Series):
                rating = group_item.get('avg_rating', 0)
            else:
                rating = group_item['avg_rating']
            # ИСПРАВЛЕНИЕ: Исключаем товары без рейтингов (None, NaN) из расчета среднего
            # Товары с rating=0.0 (которые мы назначили выше) тоже не участвуют в среднем
            if pd.notna(rating) and rating > 0:
                valid_ratings.append(rating)
        
        # Если нет товаров с рейтингами, group_rating = 0 (требуются компенсаторы)
        group_rating = sum(valid_ratings) / len(valid_ratings) if valid_ratings else 0
        
        print(f"DEBUG: Начальный рейтинг группы {wb_sku}: {group_rating} (из {len(valid_ratings)} товаров)")
        
        # Если рейтинг недостаточен, ищем компенсаторы
        if group_rating < min_group_rating:
            print(f"DEBUG: Ищем компенсаторы для группы {wb_sku} (нужно {min_group_rating}, есть {group_rating})")
            
            # Ограничиваем количество компенсаторов с учетом уже существующих товаров
            max_compensators = min(10, max_wb_sku_per_group - len(group_items))
            
            if max_compensators > 0:
                # Получаем список уже использованных wb_sku в текущей группе
                current_group_wb_skus = set()
                for group_item in group_items:
                    if isinstance(group_item, pd.Series):
                        current_group_wb_skus.add(str(group_item.get('wb_sku', '')))
                    else:
                        current_group_wb_skus.add(str(group_item.get('wb_sku', '')))
                
                # Добавляем к уже использованным глобально
                all_used_wb_skus = used_wb_skus.union(current_group_wb_skus)
                
                compensator_wb_skus = _find_rating_compensators(
                    conn, group_criteria, min_group_rating, group_rating, 
                    len(group_items), all_used_wb_skus, max_compensators
                )
                
                if compensator_wb_skus:
                    print(f"DEBUG: Найдено {len(compensator_wb_skus)} компенсаторов для группы {wb_sku}")
                    
                    # Получаем данные компенсаторов с рейтингами
                    from utils.cross_marketplace_linker import CrossMarketplaceLinker
                    linker = CrossMarketplaceLinker(conn)
                    compensators_rating_data = linker.get_links_with_ozon_ratings(compensator_wb_skus)
                    
                    if not compensators_rating_data.empty:
                        # Получаем данные о поле (gender) и категории (wb_category) для компенсаторов из базы данных
                        compensators_wb_skus_str = ', '.join([f"'{sku}'" for sku in compensator_wb_skus])
                        additional_data_query = f"""
                        SELECT 
                            p.wb_sku, 
                            p.gender,
                            wp.wb_category
                        FROM punta_table p
                        INNER JOIN wb_products wp ON CAST(p.wb_sku AS VARCHAR) = CAST(wp.wb_sku AS VARCHAR)
                        WHERE CAST(p.wb_sku AS VARCHAR) IN ({compensators_wb_skus_str})
                        """
                        
                        try:
                            additional_data = conn.execute(additional_data_query).fetchdf()
                            additional_data['wb_sku'] = additional_data['wb_sku'].astype(str)
                            
                            # Объединяем данные рейтингов с данными о поле и категории
                            compensators_rating_data['wb_sku'] = compensators_rating_data['wb_sku'].astype(str)
                            compensators_full_data = pd.merge(
                                compensators_rating_data, 
                                additional_data, 
                                on='wb_sku', 
                                how='left'
                            )
                        except Exception as e:
                            print(f"DEBUG: Ошибка получения gender и wb_category для компенсаторов: {e}")
                            compensators_full_data = compensators_rating_data.copy()
                            compensators_full_data['gender'] = group_criteria.get('gender', '')
                            compensators_full_data['wb_category'] = group_criteria.get('wb_category', '')
                        
                        # Добавляем компенсаторы в группу ПОШТУЧНО (проверяем на дублирование)
                        added_compensators = 0
                        for _, compensator_row in compensators_full_data.iterrows():
                            compensator_wb_sku = str(compensator_row['wb_sku'])
                            
                            # Проверяем, что этот wb_sku еще не в группе
                            if compensator_wb_sku not in current_group_wb_skus:
                                # Добавляем компенсатор
                                group_items.append(compensator_row)
                                used_wb_skus.add(compensator_wb_sku)
                                current_group_wb_skus.add(compensator_wb_sku)
                                added_compensators += 1
                                
                                # Пересчитываем рейтинг группы после добавления каждого компенсатора
                                # ИСПРАВЛЕНИЕ: Товары без рейтингов не участвуют в расчете среднего
                                current_ratings = []
                                for group_item in group_items:
                                    if isinstance(group_item, pd.Series):
                                        rating = group_item.get('avg_rating', 0)
                                    else:
                                        rating = group_item['avg_rating']
                                    # Исключаем товары без рейтингов и с рейтингом 0 из расчета
                                    if pd.notna(rating) and rating > 0:
                                        current_ratings.append(rating)
                                
                                current_group_rating = sum(current_ratings) / len(current_ratings) if current_ratings else 0
                                print(f"DEBUG: После добавления {compensator_wb_sku}: рейтинг группы = {current_group_rating} (товаров в группе: {len(group_items)})")
                                
                                # ГЛАВНОЕ: останавливаемся, как только достигли целевого рейтинга
                                if current_group_rating >= min_group_rating:
                                    print(f"DEBUG: Целевой рейтинг {min_group_rating} достигнут! Останавливаем добавление компенсаторов")
                                    group_rating = current_group_rating  # Обновляем группу рейтинг
                                    break
                                
                                # Проверяем максимальный размер группы
                                if len(group_items) >= max_wb_sku_per_group:
                                    print(f"DEBUG: Достигнут максимальный размер группы ({max_wb_sku_per_group})")
                                    group_rating = current_group_rating  # Обновляем группу рейтинг
                                    break
                            else:
                                print(f"DEBUG: wb_sku {compensator_wb_sku} уже есть в группе, пропускаем")
                        
                        print(f"DEBUG: Добавлено {added_compensators} компенсаторов. Финальный рейтинг группы: {group_rating}")
                        
                        # Убираем старую логику пересчета рейтинга, так как он уже пересчитан выше
                        # if added_compensators > 0:
                        #     ...
                    else:
                        print(f"DEBUG: Не удалось получить данные для компенсаторов группы {wb_sku}")
                else:
                    print(f"DEBUG: Нет места для компенсаторов в группе {wb_sku} (размер группы: {len(group_items)}, макс: {max_wb_sku_per_group})")
        
        # Проверяем финальный рейтинг
        if group_rating >= min_group_rating:
            print(f"DEBUG: Группа {wb_sku} создана успешно (рейтинг {group_rating} >= {min_group_rating})")
            # Создаем группу
            group_df = _create_group_dataframe(group_items, wb_sku, group_rating)
            groups.append(group_df)
        else:
            print(f"DEBUG: Группа {wb_sku} добавлена в список низкого рейтинга (рейтинг {group_rating} < {min_group_rating})")
            # Добавляем в список товаров с низким рейтингом
            low_rating_items.append({
                'wb_sku': wb_sku,
                'avg_rating': item['avg_rating'],
                'issue': f'Не удалось компенсировать рейтинг до {min_group_rating}'
            })
    
    update_progress(0.9, "📊 Финализация групп...")
    
    # Дополнительная отладочная информация
    total_processed = len(used_wb_skus)
    remaining_items = len(all_items) - total_processed
    
    # Если остались необработанные товары, добавляем их в список низкого рейтинга
    if remaining_items > 0:
        remaining_wb_skus = set(all_items['wb_sku']) - used_wb_skus
        for wb_sku in remaining_wb_skus:
            item_data = all_items[all_items['wb_sku'] == wb_sku].iloc[0]
            low_rating_items.append({
                'wb_sku': wb_sku,
                'avg_rating': item_data.get('avg_rating', 0),
                'issue': f'Товар не был обработан в основном цикле'
            })
    
    # Объединяем все группы
    if groups:
        final_groups_df = pd.concat(groups, ignore_index=True)
        # Добавляем номера групп
        final_groups_df['group_id'] = final_groups_df.groupby('merge_on_card').ngroup() + 1
    else:
        final_groups_df = pd.DataFrame()
    
    # Создаем DataFrame для товаров с низким рейтингом
    low_rating_df = pd.DataFrame(low_rating_items) if low_rating_items else pd.DataFrame()
    
    update_progress(1.0, "✅ Группировка завершена!")
    return final_groups_df, low_rating_df