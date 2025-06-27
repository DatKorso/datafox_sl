"""
Модуль для сбора wb_sku по текущему ассортименту товаров Озон.

Основная цель: 
- Получить список всех wb_sku, которые принадлежат карточкам из oz_products
- Избежать дублирования wb_sku при пересечении штрихкодов
- Определить oz_sku без связей с wb_sku
- Использовать алгоритм поиска последнего актуального штрихкода

Алгоритм актуального штрихкода:
- WB: последний штрихкод в списке wb_products.barcodes (через ';')
- OZ: последний штрихкод в алфавитном порядке из oz_barcodes для данного товара

Автор: DataFox SL Project
Версия: 1.0.0
"""

import pandas as pd
import streamlit as st
import duckdb
from typing import List, Dict, Set, Tuple, Optional, Any
from dataclasses import dataclass
from utils.cross_marketplace_linker import CrossMarketplaceLinker
from utils.db_search_helpers import get_normalized_wb_barcodes, get_ozon_barcodes_and_identifiers
import time


@dataclass
class WbSkuCollectionResult:
    """Результат сбора wb_sku для товаров Озон"""
    wb_skus: List[str]  # Найденные wb_sku
    no_links_oz_skus: List[str]  # oz_sku без связей с wb
    duplicate_mappings: List[Dict]  # Дублирующиеся привязки oz_sku -> wb_sku
    stats: Dict[str, Any]  # Статистика выполнения


class OzToWbCollector:
    """
    Класс для сбора wb_sku на основе товаров Озон.
    
    Алгоритм сопоставления:
    1. Для каждого OZ SKU определяется актуальный штрихкод:
       - Последний штрихкод по порядку вставки в таблицу oz_barcodes (ORDER BY rowid DESC)
    2. Актуальный штрихкод OZ ищется среди ВСЕХ штрихкодов WB товаров (не только актуальных)
    3. При найденном совпадении wb_sku добавляется в результат
    
    Этот подход позволяет найти связи даже если актуальные штрихкоды на разных маркетплейсах 
    не совпадают, но один товар представлен в обеих системах.
    """
    
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        """
        Инициализация коллектора.
        
        Args:
            connection: Активное соединение с базой данных DuckDB
        """
        self.connection = connection
        self.linker = CrossMarketplaceLinker(connection)
        
    def get_oz_actual_barcodes(self, oz_skus: List[str]) -> pd.DataFrame:
        """
        Получает актуальные штрихкоды для списка OZ SKU.
        Актуальный штрихкод = последний по порядку вставки в таблицу oz_barcodes.
        
        Args:
            oz_skus: Список OZ SKU
            
        Returns:
            DataFrame с колонками oz_sku, actual_barcode
        """
        try:
            if not oz_skus:
                return pd.DataFrame()
            
            # Преобразуем oz_skus в строки и фильтруем
            oz_skus_clean = [str(sku).strip() for sku in oz_skus if str(sku).strip()]
            if not oz_skus_clean:
                return pd.DataFrame()
            
            # Получаем актуальные штрихкоды (последние по ROWID для каждого товара)
            # ИСПРАВЛЕНИЕ: используем ORDER BY rowid вместо ORDER BY oz_barcode
            actual_barcodes_query = """
            WITH oz_barcodes_with_products AS (
                SELECT 
                    p.oz_sku,
                    b.oz_barcode,
                    b.rowid,
                    ROW_NUMBER() OVER (PARTITION BY p.oz_sku ORDER BY b.rowid DESC) as rn
                FROM oz_products p
                INNER JOIN oz_barcodes b ON p.oz_product_id = b.oz_product_id
                WHERE p.oz_sku IN ({})
            )
            SELECT 
                oz_sku,
                oz_barcode as actual_barcode
            FROM oz_barcodes_with_products
            WHERE rn = 1
            """.format(', '.join(['?' for _ in oz_skus_clean]))
            
            # Выполняем запрос с параметрами
            result = self.connection.execute(
                actual_barcodes_query, 
                [int(sku) for sku in oz_skus_clean]
            ).fetchdf()
            
            return result
            
        except Exception as e:
            st.error(f"Ошибка при получении актуальных штрихкодов OZ: {e}")
            return pd.DataFrame()
    
    def get_wb_actual_barcodes(self, wb_skus: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Получает актуальные (последние) штрихкоды для товаров WB.
        
        Алгоритм: последний штрихкод в списке wb_products.barcodes (через ';').
        
        Args:
            wb_skus: Список WB SKU для фильтрации (если None - все товары)
            
        Returns:
            DataFrame с колонками: wb_sku, actual_barcode
        """
        try:
            base_query = """
            WITH wb_products_filtered AS (
                SELECT wb_sku, wb_barcodes
                FROM wb_products 
                WHERE wb_barcodes IS NOT NULL 
                  AND TRIM(wb_barcodes) != ''
                {wb_filter}
            ),
            wb_barcodes_split AS (
                SELECT 
                    wb_sku,
                    string_split(wb_barcodes, ';') as barcodes_array
                FROM wb_products_filtered
            ),
            wb_actual_barcodes AS (
                SELECT 
                    wb_sku,
                    TRIM(barcodes_array[length(barcodes_array)]) as actual_barcode
                FROM wb_barcodes_split
                WHERE length(barcodes_array) > 0
            )
            SELECT wb_sku, actual_barcode
            FROM wb_actual_barcodes
            WHERE actual_barcode IS NOT NULL 
              AND TRIM(actual_barcode) != ''
            """
            
            # Добавляем фильтр по wb_skus если указан
            if wb_skus:
                wb_skus_clean = [str(sku).strip() for sku in wb_skus if str(sku).strip()]
                if wb_skus_clean:
                    wb_filter = f"AND wb_sku IN ({', '.join(['?' for _ in wb_skus_clean])})"
                    query = base_query.format(wb_filter=wb_filter)
                    result = self.connection.execute(query, wb_skus_clean).fetchdf()
                else:
                    return pd.DataFrame()
            else:
                query = base_query.format(wb_filter="")
                result = self.connection.execute(query).fetchdf()
            
            return result
            
        except Exception as e:
            st.error(f"Ошибка при получении актуальных штрихкодов WB: {e}")
            return pd.DataFrame()
    
    def collect_wb_skus_for_oz_assortment(self, oz_skus: List[str]) -> WbSkuCollectionResult:
        """
        Собирает wb_sku для списка oz_sku на основе совпадения актуальных штрихкодов
        
        Args:
            oz_skus: Список OZ SKU для поиска
            
        Returns:
            WbSkuCollectionResult: Результат сбора с данными и статистикой
        """
        start_time = time.time()
        
        # Получаем актуальные штрихкоды для OZ товаров
        oz_actual_barcodes = self.get_oz_actual_barcodes(oz_skus)
        
        if oz_actual_barcodes.empty:
            return WbSkuCollectionResult(
                wb_skus=[],
                no_links_oz_skus=oz_skus.copy(),
                duplicate_mappings=[],
                stats={
                    'total_oz_skus_processed': len(oz_skus),
                    'oz_skus_with_barcodes': 0,
                    'unique_wb_skus_found': 0,
                    'total_barcode_matches': 0,
                    'processing_time_seconds': time.time() - start_time
                }
            )
        
        # ИСПРАВЛЕНИЕ: Ищем wb_sku, где актуальные штрихкоды OZ встречаются среди ЛЮБЫХ штрихкодов WB
        # Не только актуальных штрихкодов WB
        barcode_matching_query = """
        WITH oz_barcodes_list AS (
            SELECT DISTINCT actual_barcode
            FROM UNNEST(?) AS t(actual_barcode)
            WHERE actual_barcode IS NOT NULL AND TRIM(actual_barcode) != ''
        )
        SELECT DISTINCT 
            wb.wb_sku,
            ozb.actual_barcode as matching_barcode
        FROM wb_products wb
        CROSS JOIN oz_barcodes_list ozb
        WHERE wb.wb_barcodes IS NOT NULL 
          AND TRIM(wb.wb_barcodes) != ''
          AND wb.wb_barcodes LIKE '%' || ozb.actual_barcode || '%'
        """
        
        # Подготавливаем список уникальных штрихкодов
        unique_barcodes = oz_actual_barcodes['actual_barcode'].unique().tolist()
        
        # Выполняем поиск совпадений
        matches_result = self.connection.execute(barcode_matching_query, [unique_barcodes]).fetchdf()
        
        # Группируем результаты
        wb_skus = matches_result['wb_sku'].unique().tolist() if not matches_result.empty else []
        total_matches = len(matches_result) if not matches_result.empty else 0
        
        # Создаем mapping oz_sku -> wb_sku для анализа дублей
        oz_to_wb_mapping = {}
        duplicate_mappings = []
        
        if not matches_result.empty:
            # Объединяем с исходными данными OZ
            detailed_matches = matches_result.merge(
                oz_actual_barcodes[['oz_sku', 'actual_barcode']],
                left_on='matching_barcode',
                right_on='actual_barcode',
                how='left'
            )
            
            # Группируем по oz_sku
            for oz_sku, group in detailed_matches.groupby('oz_sku'):
                wb_skus_for_oz = group['wb_sku'].unique().tolist()
                oz_to_wb_mapping[oz_sku] = wb_skus_for_oz
                
                # Если oz_sku связан с несколькими wb_sku - это дублирование
                if len(wb_skus_for_oz) > 1:
                    duplicate_mappings.append({
                        'oz_sku': oz_sku,
                        'wb_skus': wb_skus_for_oz,
                        'matching_barcode': group['matching_barcode'].iloc[0]
                    })
        
        # Определяем oz_sku без связей
        oz_skus_with_matches = set(oz_to_wb_mapping.keys())
        no_links_oz_skus = [sku for sku in oz_skus if sku not in oz_skus_with_matches]
        
        return WbSkuCollectionResult(
            wb_skus=wb_skus,
            no_links_oz_skus=no_links_oz_skus,
            duplicate_mappings=duplicate_mappings,
            stats={
                'total_oz_skus_processed': len(oz_skus),
                'oz_skus_with_barcodes': len(oz_actual_barcodes),
                'unique_wb_skus_found': len(wb_skus),
                'total_barcode_matches': total_matches,
                'processing_time_seconds': time.time() - start_time
            }
        )
    
    def _get_wb_sku_details(self, wb_skus: List[str]) -> pd.DataFrame:
        """
        Получает детальную информацию по списку wb_sku
        
        Args:
            wb_skus: Список wb_sku для получения информации
            
        Returns:
            DataFrame с детальной информацией
        """
        try:
            if not wb_skus:
                return pd.DataFrame()
                
            wb_skus_str = "', '".join(map(str, wb_skus))
            
            details_query = f"""
            SELECT 
                wb_sku,
                wb_brand,
                wb_category,
                wb_barcodes,
                wb_price
            FROM wb_products 
            WHERE wb_sku IN ('{wb_skus_str}')
            ORDER BY wb_sku
            """
            
            return self.connection.execute(details_query).fetchdf()
            
        except Exception as e:
            st.error(f"Ошибка при получении деталей wb_sku: {e}")
            return pd.DataFrame()
    
    def export_results_to_excel(
        self, 
        result: WbSkuCollectionResult, 
        filename: str = "wb_skus_collection_results.xlsx"
    ) -> str:
        """
        Экспортирует результаты сбора wb_sku в Excel файл
        
        Args:
            result: Результат сбора wb_sku
            filename: Имя файла для сохранения
            
        Returns:
            Путь к созданному файлу
        """
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                
                # Лист 1: Найденные wb_sku
                if result.wb_skus:
                    wb_skus_df = pd.DataFrame({
                        'wb_sku': result.wb_skus
                    })
                    wb_skus_df.to_excel(
                        writer, 
                        sheet_name='Found_WB_SKUs', 
                        index=False
                    )
                
                # Лист 2: oz_sku без связей
                if result.no_links_oz_skus:
                    no_links_df = pd.DataFrame({
                        'oz_sku_without_wb_links': result.no_links_oz_skus
                    })
                    no_links_df.to_excel(
                        writer, 
                        sheet_name='OZ_No_Links', 
                        index=False
                    )
                
                # Лист 3: Дубликаты связей
                if result.duplicate_mappings:
                    duplicates_df = pd.DataFrame(result.duplicate_mappings)
                    duplicates_df.to_excel(
                        writer, 
                        sheet_name='Duplicate_Mappings', 
                        index=False
                    )
                
                # Лист 4: Статистика
                stats_df = pd.DataFrame([result.stats]).T
                stats_df.columns = ['Value']
                stats_df.index.name = 'Metric'
                stats_df.to_excel(
                    writer, 
                    sheet_name='Statistics'
                )
            
            return filename
            
        except Exception as e:
            st.error(f"Ошибка при экспорте в Excel: {e}")
            return ""


# Удобные функции для быстрого использования
def collect_wb_skus_for_all_oz_products(connection: duckdb.DuckDBPyConnection) -> WbSkuCollectionResult:
    """
    Быстрая функция для сбора wb_sku для всего ассортимента Озон.
    
    Args:
        connection: Соединение с базой данных
        
    Returns:
        WbSkuCollectionResult с результатами сбора
    """
    collector = OzToWbCollector(connection)
    return collector.collect_wb_skus_for_oz_assortment()


def collect_wb_skus_for_oz_list(
    connection: duckdb.DuckDBPyConnection, 
    oz_skus: List[str]
) -> WbSkuCollectionResult:
    """
    Быстрая функция для сбора wb_sku для указанного списка oz_sku.
    
    Args:
        connection: Соединение с базой данных
        oz_skus: Список OZ SKU
        
    Returns:
        WbSkuCollectionResult с результатами сбора
    """
    collector = OzToWbCollector(connection)
    return collector.collect_wb_skus_for_oz_assortment(oz_skus) 