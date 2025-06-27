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
    
    def __init__(self, connection: duckdb.DuckDBPyConnection, progress_callback=None):
        """
        Инициализация коллектора.
        
        Args:
            connection: Активное соединение с базой данных DuckDB
            progress_callback: Функция для обновления прогресса (опционально)
        """
        self.connection = connection
        self.linker = CrossMarketplaceLinker(connection)
        self.progress_callback = progress_callback
    
    def _update_progress(self, current: int, total: int, message: str = ""):
        """Обновляет прогресс если задан callback"""
        if self.progress_callback:
            progress = current / total if total > 0 else 0
            self.progress_callback(progress, f"{message} ({current}/{total})")
        
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
        debug_info = {}
        
        # Этап 1: Получение актуальных штрихкодов для OZ товаров
        step1_start = time.time()
        self._update_progress(1, 8, f"Получение актуальных штрихкодов для {len(oz_skus)} OZ товаров")
        oz_actual_barcodes = self.get_oz_actual_barcodes(oz_skus)
        step1_time = time.time() - step1_start
        debug_info['step1_time'] = step1_time
        debug_info['oz_skus_input'] = len(oz_skus)
        debug_info['oz_skus_with_barcodes'] = len(oz_actual_barcodes)
        
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
        
        # ОПТИМИЗИРОВАННЫЙ АЛГОРИТМ: Заменяем медленные LIKE операции на быстрый JOIN
        # 1. Разделяем все WB штрихкоды на отдельные записи
        # 2. Делаем JOIN с OZ штрихкодами
        barcode_matching_query = """
        WITH oz_barcodes_list AS (
            SELECT DISTINCT actual_barcode
            FROM UNNEST(?) AS t(actual_barcode)
            WHERE actual_barcode IS NOT NULL AND TRIM(actual_barcode) != ''
        ),
        wb_barcodes_split AS (
            SELECT 
                wb_sku,
                TRIM(barcode) as individual_barcode
            FROM wb_products wb,
            UNNEST(string_split(wb.wb_barcodes, ';')) AS t(barcode)
            WHERE wb.wb_barcodes IS NOT NULL 
              AND TRIM(wb.wb_barcodes) != ''
              AND TRIM(barcode) != ''
        )
        SELECT DISTINCT 
            wbs.wb_sku,
            ozb.actual_barcode as matching_barcode
        FROM wb_barcodes_split wbs
        INNER JOIN oz_barcodes_list ozb 
            ON wbs.individual_barcode = ozb.actual_barcode
        """
        
        # Этап 2: Подготовка поиска совпадений
        step2_start = time.time()
        self._update_progress(2, 8, "Подготовка списка уникальных штрихкодов")
        unique_barcodes = oz_actual_barcodes['actual_barcode'].unique().tolist()
        step2_time = time.time() - step2_start
        debug_info['step2_time'] = step2_time
        debug_info['unique_barcodes_count'] = len(unique_barcodes)
        
        # Этап 3: Получение информации о базе WB
        step3_start = time.time()
        self._update_progress(3, 8, "Подсчет записей в базе WB")
        wb_count_query = "SELECT COUNT(*) as wb_count FROM wb_products WHERE wb_barcodes IS NOT NULL AND TRIM(wb_barcodes) != ''"
        wb_count = self.connection.execute(wb_count_query).fetchone()[0]
        step3_time = time.time() - step3_start
        debug_info['step3_time'] = step3_time
        debug_info['wb_products_count'] = wb_count
        
        # Этап 4: Поиск совпадений штрихкодов в WB (оптимизированный алгоритм)
        step4_start = time.time()
        self._update_progress(4, 8, f"Оптимизированный поиск: разделение {wb_count:,} WB товаров на штрихкоды")
        
        # Сначала получаем количество индивидуальных WB штрихкодов для лучшего понимания сложности
        wb_individual_count_query = """
        SELECT COUNT(*) as individual_count
        FROM wb_products wb,
        UNNEST(string_split(wb.wb_barcodes, ';')) AS t(barcode)
        WHERE wb.wb_barcodes IS NOT NULL 
          AND TRIM(wb.wb_barcodes) != ''
          AND TRIM(barcode) != ''
        """
        wb_individual_count = self.connection.execute(wb_individual_count_query).fetchone()[0]
        debug_info['wb_individual_barcodes_count'] = wb_individual_count
        
        step4_split_time = time.time() - step4_start
        self._update_progress(4, 8, f"JOIN {len(unique_barcodes):,} OZ штрихкодов с {wb_individual_count:,} WB штрихкодами")
        
        matches_result = self.connection.execute(barcode_matching_query, [unique_barcodes]).fetchdf()
        step4_time = time.time() - step4_start
        debug_info['step4_time'] = step4_time
        debug_info['sql_execution_time'] = step4_time
        debug_info['wb_split_time'] = step4_split_time
        
        # Этап 5: Обработка результатов поиска
        step5_start = time.time()
        self._update_progress(5, 8, f"Обработка {len(matches_result)} найденных совпадений")
        wb_skus = matches_result['wb_sku'].unique().tolist() if not matches_result.empty else []
        total_matches = len(matches_result) if not matches_result.empty else 0
        step5_time = time.time() - step5_start
        debug_info['step5_time'] = step5_time
        debug_info['raw_matches_found'] = total_matches
        debug_info['unique_wb_skus_found'] = len(wb_skus)
        
        # Этап 6: Создание маппинга oz_sku -> wb_sku
        step6_start = time.time()
        self._update_progress(6, 8, "Создание маппинга oz_sku -> wb_sku")
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
        step6_time = time.time() - step6_start
        debug_info['step6_time'] = step6_time
        debug_info['duplicate_mappings_count'] = len(duplicate_mappings)
        
        # Этап 7: Определение oz_sku без связей
        step7_start = time.time()
        self._update_progress(7, 8, "Определение oz_sku без связей с WB")
        # ИСПРАВЛЕНИЕ: Приводим типы данных к строкам для корректного сравнения
        oz_skus_with_matches = set(str(sku) for sku in oz_to_wb_mapping.keys())
        no_links_oz_skus = [sku for sku in oz_skus if str(sku) not in oz_skus_with_matches]
        step7_time = time.time() - step7_start
        debug_info['step7_time'] = step7_time
        debug_info['oz_skus_without_links'] = len(no_links_oz_skus)
        
        # Этап 8: Финализация результатов
        step8_start = time.time()
        self._update_progress(8, 8, "Финализация и подготовка отчета")
        total_time = time.time() - start_time
        debug_info['step8_time'] = time.time() - step8_start
        debug_info['total_processing_time'] = total_time
        
        return WbSkuCollectionResult(
            wb_skus=wb_skus,
            no_links_oz_skus=no_links_oz_skus,
            duplicate_mappings=duplicate_mappings,
            stats={
                'total_oz_skus_processed': len(oz_skus),
                'oz_skus_with_barcodes': len(oz_actual_barcodes),
                'unique_wb_skus_found': len(wb_skus),
                'total_barcode_matches': total_matches,
                'processing_time_seconds': total_time,
                # Отладочная информация
                'debug_info': debug_info
            }
        )
    
    def collect_wb_skus_for_oz_assortment_batched(self, oz_skus: List[str], batch_size: int = 1000) -> WbSkuCollectionResult:
        """
        Батчевая версия сбора wb_sku для очень больших объемов данных.
        Обрабатывает OZ SKU порциями для снижения нагрузки на базу данных.
        
        Args:
            oz_skus: Список OZ SKU для поиска
            batch_size: Размер батча для обработки
            
        Returns:
            WbSkuCollectionResult: Результат сбора с данными и статистикой
        """
        start_time = time.time()
        debug_info = {'batched_processing': True, 'batch_size': batch_size}
        
        # Разбиваем на батчи
        oz_skus_batches = [oz_skus[i:i + batch_size] for i in range(0, len(oz_skus), batch_size)]
        total_batches = len(oz_skus_batches)
        debug_info['total_batches'] = total_batches
        
        # Аккумуляторы результатов
        all_wb_skus = set()
        all_no_links = []
        all_duplicates = []
        total_matches = 0
        total_oz_with_barcodes = 0
        
        self._update_progress(0, total_batches, f"Начинаем батчевую обработку: {total_batches} батчей по {batch_size}")
        
        for batch_idx, batch_oz_skus in enumerate(oz_skus_batches):
            batch_start = time.time()
            self._update_progress(
                batch_idx, 
                total_batches, 
                f"Обработка батча {batch_idx + 1}/{total_batches} ({len(batch_oz_skus)} oz_sku)"
            )
            
            # Обрабатываем батч без прогресса (чтобы не конфликтовать)
            batch_collector = OzToWbCollector(self.connection, progress_callback=None)
            batch_result = batch_collector.collect_wb_skus_for_oz_assortment(batch_oz_skus)
            
            # Аккумулируем результаты
            all_wb_skus.update(batch_result.wb_skus)
            all_no_links.extend(batch_result.no_links_oz_skus)
            all_duplicates.extend(batch_result.duplicate_mappings)
            total_matches += batch_result.stats['total_barcode_matches']
            total_oz_with_barcodes += batch_result.stats['oz_skus_with_barcodes']
            
            batch_time = time.time() - batch_start
            debug_info[f'batch_{batch_idx}_time'] = batch_time
        
        self._update_progress(total_batches, total_batches, "Батчевая обработка завершена")
        
        total_time = time.time() - start_time
        debug_info['total_processing_time'] = total_time
        
        return WbSkuCollectionResult(
            wb_skus=list(all_wb_skus),
            no_links_oz_skus=all_no_links,
            duplicate_mappings=all_duplicates,
            stats={
                'total_oz_skus_processed': len(oz_skus),
                'oz_skus_with_barcodes': total_oz_with_barcodes,
                'unique_wb_skus_found': len(all_wb_skus),
                'total_barcode_matches': total_matches,
                'processing_time_seconds': total_time,
                # Отладочная информация
                'debug_info': debug_info
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
def collect_wb_skus_for_all_oz_products(connection: duckdb.DuckDBPyConnection, progress_callback=None) -> WbSkuCollectionResult:
    """
    Быстрая функция для сбора wb_sku для всего ассортимента Озон.
    Применяет фильтр по брендам из настроек проекта.
    
    Args:
        connection: Соединение с базой данных
        progress_callback: Функция для отображения прогресса
        
    Returns:
        WbSkuCollectionResult с результатами сбора
    """
    try:
        # Импорт утилит конфигурации
        from . import config_utils
        
        # Получаем фильтр брендов из настроек
        brands_filter = config_utils.get_data_filter("oz_category_products_brands")
        
        if brands_filter and brands_filter.strip():
            # Разбираем список брендов
            allowed_brands = [brand.strip() for brand in brands_filter.split(";") if brand.strip()]
            
            if allowed_brands:
                # Создаем условие для фильтрации по брендам
                brands_condition = ", ".join([f"'{brand}'" for brand in allowed_brands])
                all_oz_query = f"""
                SELECT DISTINCT CAST(oz_sku AS VARCHAR) as oz_sku 
                FROM oz_products 
                WHERE oz_sku IS NOT NULL 
                AND oz_brand IN ({brands_condition})
                """
                
                if progress_callback:
                    progress_callback(0.0, f"Фильтрация по брендам: {', '.join(allowed_brands[:3])}{'...' if len(allowed_brands) > 3 else ''}")
            else:
                # Если фильтр пустой - берем все
                all_oz_query = "SELECT DISTINCT CAST(oz_sku AS VARCHAR) as oz_sku FROM oz_products WHERE oz_sku IS NOT NULL"
        else:
            # Если фильтр не настроен - берем все
            all_oz_query = "SELECT DISTINCT CAST(oz_sku AS VARCHAR) as oz_sku FROM oz_products WHERE oz_sku IS NOT NULL"
        
        all_oz_result = connection.execute(all_oz_query).fetchdf()
        
        if all_oz_result.empty:
            return WbSkuCollectionResult(
                wb_skus=[],
                no_links_oz_skus=[],
                duplicate_mappings=[],
                stats={
                    'total_oz_skus_processed': 0,
                    'oz_skus_with_barcodes': 0,
                    'unique_wb_skus_found': 0,
                    'total_barcode_matches': 0,
                    'processing_time_seconds': 0.0
                }
            )
        
        oz_skus = all_oz_result['oz_sku'].tolist()
        collector = OzToWbCollector(connection, progress_callback)
        
        # Автоматически используем батчинг для больших объемов (>50,000 SKU)
        if len(oz_skus) > 50000:
            return collector.collect_wb_skus_for_oz_assortment_batched(oz_skus, batch_size=1000)
        else:
            return collector.collect_wb_skus_for_oz_assortment(oz_skus)
        
    except Exception as e:
        st.error(f"Ошибка при сборе wb_sku для всего ассортимента: {e}")
        return WbSkuCollectionResult(
            wb_skus=[],
            no_links_oz_skus=[],
            duplicate_mappings=[],
            stats={
                'total_oz_skus_processed': 0,
                'oz_skus_with_barcodes': 0,
                'unique_wb_skus_found': 0,
                'total_barcode_matches': 0,
                'processing_time_seconds': 0.0
            }
        )


def collect_wb_skus_for_oz_list(
    connection: duckdb.DuckDBPyConnection, 
    oz_skus: List[str],
    progress_callback=None
) -> WbSkuCollectionResult:
    """
    Быстрая функция для сбора wb_sku для указанного списка oz_sku.
    
    Args:
        connection: Соединение с базой данных
        oz_skus: Список OZ SKU
        
    Returns:
        WbSkuCollectionResult с результатами сбора
    """
    try:
        if not oz_skus:
            return WbSkuCollectionResult(
                wb_skus=[],
                no_links_oz_skus=[],
                duplicate_mappings=[],
                stats={
                    'total_oz_skus_processed': 0,
                    'oz_skus_with_barcodes': 0,
                    'unique_wb_skus_found': 0,
                    'total_barcode_matches': 0,
                    'processing_time_seconds': 0.0
                }
            )
        
        collector = OzToWbCollector(connection, progress_callback)
        
        # Автоматически используем батчинг для больших объемов (>10,000 SKU)
        if len(oz_skus) > 10000:
            return collector.collect_wb_skus_for_oz_assortment_batched(oz_skus, batch_size=1000)
        else:
            return collector.collect_wb_skus_for_oz_assortment(oz_skus)
        
    except Exception as e:
        st.error(f"Ошибка при сборе wb_sku для списка oz_sku: {e}")
        return WbSkuCollectionResult(
            wb_skus=[],
            no_links_oz_skus=[],
            duplicate_mappings=[],
            stats={
                'total_oz_skus_processed': 0,
                'oz_skus_with_barcodes': 0,
                'unique_wb_skus_found': 0,
                'total_barcode_matches': 0,
                'processing_time_seconds': 0.0
            }
        ) 