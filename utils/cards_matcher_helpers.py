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