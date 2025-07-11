"""Улучшенный модуль для группировки товаров с корректной компенсацией рейтингов.

Этот модуль решает проблемы оригинального алгоритма группировки:
- Исправляет SQL запросы для поиска компенсаторов
- Реализует справедливое распределение компенсаторов
- Добавляет детальное логирование процесса
- Разделяет бизнес-логику от UI

Автор: DataFox SL Project
Версия: 2.0.0
"""

import pandas as pd
import streamlit as st
import duckdb
from typing import List, Dict, Optional, Tuple, Any, Set
from dataclasses import dataclass
from collections import defaultdict
import logging
from utils.cross_marketplace_linker import CrossMarketplaceLinker
from utils.config_utils import get_data_filter


@dataclass
class GroupingConfig:
    """Конфигурация для группировки товаров."""
    grouping_columns: List[str]
    min_group_rating: float
    max_wb_sku_per_group: int
    enable_sort_priority: bool = True
    wb_category: Optional[str] = None
    

@dataclass
class GroupingResult:
    """Результат группировки товаров."""
    groups: List[Dict[str, Any]]
    low_rating_items: List[Dict[str, Any]]
    defective_items: List[Dict[str, Any]]
    statistics: Dict[str, Any]
    logs: List[str]


class AdvancedProductGrouper:
    """Улучшенный класс для группировки товаров с корректной компенсацией рейтингов."""
    
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        """Инициализация группировщика.
        
        Args:
            connection: Активное соединение с базой данных DuckDB
        """
        self.connection = connection
        self.linker = CrossMarketplaceLinker(connection)
        self.logs = []
        
        # Настройка логирования
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _log(self, message: str, level: str = "info"):
        """Добавляет сообщение в лог."""
        self.logs.append(f"[{level.upper()}] {message}")
        if level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
    
    def create_advanced_product_groups(
        self, 
        wb_skus: List[str], 
        config: GroupingConfig
    ) -> GroupingResult:
        """Создает продвинутые группы товаров с улучшенной компенсацией рейтингов.
        
        Args:
            wb_skus: Список WB SKU для группировки
            config: Конфигурация группировки
            
        Returns:
            GroupingResult с результатами группировки
        """
        self.logs = []  # Очищаем логи
        self._log(f"Начинаем группировку {len(wb_skus)} товаров")
        
        try:
            # 1. Получаем рейтинги Ozon
            self._log("Получаем рейтинги Ozon")
            ratings_df = self.linker.get_links_with_ozon_ratings(wb_skus)
            
            # 2. Получаем данные Punta
            self._log("Получаем данные Punta")
            punta_data = self._get_punta_data(wb_skus)
            
            # 3. Получаем данные по остаткам
            self._log("Получаем данные по остаткам")
            stock_data = self._get_stock_summary(wb_skus)
            
            # 4. Объединяем все данные
            merged_data = self._merge_all_data(ratings_df, punta_data, stock_data)
            
            # 5. Фильтруем по категории если указана
            if config.wb_category:
                merged_data = merged_data[merged_data['wb_category'] == config.wb_category]
                self._log(f"Отфильтровано по категории {config.wb_category}: {len(merged_data)} товаров")
            
            # 6. Приоритизируем товары
            prioritized_data = self._prioritize_items(merged_data, config.enable_sort_priority)
            
            # 7. Создаем группы с улучшенной компенсацией
            result = self._create_groups_with_improved_compensation(
                prioritized_data, config
            )
            
            self._log(f"Группировка завершена. Создано {len(result.groups)} групп")
            return result
            
        except Exception as e:
            self._log(f"Ошибка при группировке: {str(e)}", "error")
            raise
    
    def _get_punta_data(self, wb_skus: List[str]) -> pd.DataFrame:
        """Получает данные из punta_table для указанных WB SKU."""
        try:
            from utils.cards_matcher_helpers import get_punta_table_data_for_wb_skus
            
            # Получаем данные из punta_table (без wb_category, так как её там нет)
            punta_data = get_punta_table_data_for_wb_skus(
                self.connection, 
                wb_skus, 
                ['gender', 'sort']
            )
            
            if punta_data.empty:
                self._log("Данные punta_table не найдены", "warning")
                return pd.DataFrame()
            
            return punta_data
            
        except Exception as e:
            self._log(f"Ошибка получения данных Punta: {str(e)}", "error")
            return pd.DataFrame()
    
    def _get_wb_category_data(self, wb_skus: List[str]) -> pd.DataFrame:
        """Получает данные wb_category из таблицы wb_products."""
        try:
            wb_skus_str = "', '".join(wb_skus)
            query = f"""
            SELECT DISTINCT wb_sku, wb_category
            FROM wb_products 
            WHERE wb_sku IN ('{wb_skus_str}')
            AND wb_category IS NOT NULL
            """
            
            result = self.connection.execute(query).fetchdf()
            result['wb_sku'] = result['wb_sku'].astype(str)
            
            self._log(f"Получено wb_category для {len(result)} товаров")
            return result
            
        except Exception as e:
            self._log(f"Ошибка получения wb_category: {str(e)}", "error")
            return pd.DataFrame()
    
    def _get_stock_summary(self, wb_skus: List[str]) -> pd.DataFrame:
        """Получает сводку по остаткам товаров через CrossMarketplaceLinker."""
        try:
            from utils.cross_marketplace_linker import CrossMarketplaceLinker
            from utils.cards_matcher_helpers import _get_wb_sku_stock_summary
            
            # Используем проверенную логику из cards_matcher_helpers
            result = _get_wb_sku_stock_summary(self.connection, wb_skus)
            return result
            
        except Exception as e:
            self._log(f"Ошибка получения данных по остаткам: {str(e)}", "error")
            return pd.DataFrame()
    
    def _merge_all_data(
        self,
        ratings_df: pd.DataFrame,
        punta_data: pd.DataFrame,
        stock_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Объединяет все данные в единый DataFrame."""
        # Начинаем с рейтингов
        merged = ratings_df.copy()
        
        # Приводим wb_sku к строковому типу для корректного слияния
        merged['wb_sku'] = merged['wb_sku'].astype(str)
        
        # Получаем wb_skus для дополнительных данных
        wb_skus = merged['wb_sku'].tolist()
        
        # Добавляем данные Punta
        if not punta_data.empty:
            punta_data['wb_sku'] = punta_data['wb_sku'].astype(str)
            self._log(f"Punta данные содержат колонки: {list(punta_data.columns)}")
            merged = merged.merge(punta_data, on='wb_sku', how='left')
            self._log(f"После объединения с punta: gender в merged: {'gender' in merged.columns}")
        else:
            self._log("Punta данные пусты, устанавливаем gender и sort = None")
            merged['gender'] = None
            merged['sort'] = None
        
        # Получаем данные wb_category из wb_products
        wb_category_data = self._get_wb_category_data(wb_skus)
        if not wb_category_data.empty:
            wb_category_data['wb_sku'] = wb_category_data['wb_sku'].astype(str)
            self._log(f"wb_category данные содержат колонки: {list(wb_category_data.columns)}")
            merged = merged.merge(wb_category_data, on='wb_sku', how='left')
            self._log(f"После объединения с wb_category: wb_category в merged: {'wb_category' in merged.columns}")
        else:
            self._log("wb_category данные пусты, устанавливаем wb_category = None")
            merged['wb_category'] = None
        
        # Добавляем данные по остаткам
        if not stock_data.empty:
            stock_data['wb_sku'] = stock_data['wb_sku'].astype(str)
            merged = merged.merge(stock_data, on='wb_sku', how='left')
        else:
            merged['total_stock'] = 0
        
        # Заполняем пропуски
        merged['total_stock'] = merged['total_stock'].fillna(0)
        
        self._log(f"Финальные колонки merged: {list(merged.columns)}")
        self._log(f"wb_category в финальном merged: {'wb_category' in merged.columns}")
        
        return merged
    
    def _prioritize_items(self, data: pd.DataFrame, enable_sort_priority: bool) -> pd.DataFrame:
        """Приоритизирует товары для обработки согласно сезонности и остаткам."""
        data = data.copy()
        
        # Определяем приоритетные товары:
        # 1. Товары из актуального сезона (13)
        # 2. Товары из прошлых сезонов с положительным остатком
        # 3. Товары с низким рейтингом (требующие группировки)
        current_season = 13
        data['is_current_season'] = (data['sort'] == current_season)
        data['has_positive_stock'] = (data.get('total_stock', 0) > 0)
        data['has_low_rating'] = (data['avg_rating'].notna()) & (data['avg_rating'] < 4.5)
        
        # Расширяем определение приоритетных товаров
        data['is_priority_item'] = (
            data['is_current_season'] | 
            (data['has_positive_stock'] & (data['sort'] < current_season)) |
            data['has_low_rating']  # Добавляем товары с низким рейтингом
        )
        
        # Базовый приоритет: приоритетные товары идут первыми
        data['has_rating'] = data['avg_rating'].notna() & (data['avg_rating'] > 0)
        
        # Дополнительный приоритет по полю sort если включен
        if enable_sort_priority and 'sort' in data.columns:
            data['sort_priority'] = data['sort'].fillna(999999)
        else:
            data['sort_priority'] = 0
        
        # Сортируем: сначала приоритетные товары, потом по sort (актуальный сезон первым), потом по wb_sku
        data = data.sort_values([
            'is_priority_item',
            'sort_priority', 
            'wb_sku'
        ], ascending=[False, True, True])
        
        self._log(f"Приоритетных товаров: {data['is_priority_item'].sum()}, неприоритетных: {(~data['is_priority_item']).sum()}")
        
        return data
    
    # В функции _create_groups_with_improved_compensation (около строки 264)
    def _create_groups_with_improved_compensation(
        self, 
        data: pd.DataFrame, 
        config: GroupingConfig
    ) -> GroupingResult:
        """Создает группы с улучшенной стратегией компенсации."""
        groups = []
        low_rating_items = []
        defective_items = []
        
        # Создаем пулы компенсаторов по категориям
        compensator_pools = self._create_compensator_pools(data, config)
        
        # Обрабатываем ВСЕ товары для формирования групп
        processed_items = set()
        priority_items = data[data.get('is_priority_item', False)]
        non_priority_items = data[~data.get('is_priority_item', False)]
        
        self._log(f"Обрабатываем {len(priority_items)} приоритетных товаров для создания групп")
        
        # Сначала обрабатываем приоритетные товары
        for _, item in priority_items.iterrows():
            wb_sku = item['wb_sku']
            
            if wb_sku in processed_items:
                continue
            
            # Проверяем на дефектность
            if self._is_defective_item(item):
                defective_items.append(item.to_dict())
                processed_items.add(wb_sku)
                continue
            
            # Создаем группу для приоритетного товара (одиночную или с компенсаторами)
            group = self._create_single_group(item, data, config, compensator_pools, processed_items)
            
            if group:
                groups.append(group)
                # Отмечаем все товары группы как обработанные
                for group_item in group['items']:
                    processed_items.add(group_item['wb_sku'])
            else:
                # Если группа не была создана, добавляем товар в отдельные
                low_rating_items.append(item.to_dict())
                processed_items.add(wb_sku)
        
        # Теперь обрабатываем неприоритетные товары
        self._log(f"Обрабатываем {len(non_priority_items)} неприоритетных товаров")
        
        for _, item in non_priority_items.iterrows():
            wb_sku = item['wb_sku']
            
            if wb_sku in processed_items:
                continue
            
            # Проверяем на дефектность
            if self._is_defective_item(item):
                defective_items.append(item.to_dict())
                processed_items.add(wb_sku)
                continue
            
            item_rating = item.get('avg_rating', 0) or 0
            
            if item_rating >= config.min_group_rating:
                # Товар с достаточным рейтингом создаем как отдельную группу
                single_item_group = {
                    'group_id': len(groups) + 1,
                    'items': [item.to_dict()],
                    'group_rating': item_rating,
                    'item_count': 1,
                    'main_wb_sku': wb_sku
                }
                groups.append(single_item_group)
                processed_items.add(wb_sku)
                self._log(f"Неприоритетный товар {wb_sku} имеет достаточный рейтинг {item_rating:.2f}, создана отдельная группа")
            else:
                # Товары с низким рейтингом добавляем в проблемные
                low_rating_items.append(item.to_dict())
                processed_items.add(wb_sku)
        
        # Собираем статистику
        statistics = self._calculate_statistics(groups, low_rating_items, defective_items)
        
        return GroupingResult(
            groups=groups,
            low_rating_items=low_rating_items,
            defective_items=defective_items,
            statistics=statistics,
            logs=self.logs
        )
    
    def _create_compensator_pools(
        self, 
        data: pd.DataFrame, 
        config: GroupingConfig
    ) -> Dict[str, List[Dict]]:
        """Создает пулы компенсаторов по категориям для справедливого распределения.
        
        Ищет компенсаторы как в предоставленных данных, так и в базе данных.
        """
        pools = defaultdict(list)
        
        # 1. Сначала добавляем компенсаторов из предоставленных данных
        compensator_candidates = data[
            (~data.get('is_priority_item', False)) &  # Только неприоритетные товары
            (data['avg_rating'].notna()) & 
            (data['avg_rating'] >= config.min_group_rating)
        ]
        
        for _, item in compensator_candidates.iterrows():
            # НОВОЕ: Исключаем товары с браком из компенсаторов
            if not self._is_defective_item(item):
                pool_key = self._get_pool_key(item, config.grouping_columns)
                pools[pool_key].append(item.to_dict())
            else:
                self._log(f"Исключен товар с браком {item['wb_sku']} из компенсаторов")
        
        # 2. Дополняем компенсаторами из базы данных
        self._add_database_compensators(pools, config)
        
        self._log(f"Создано {len(pools)} пулов компенсаторов")
        for pool_key, items in pools.items():
            self._log(f"Пул {pool_key}: {len(items)} компенсаторов")
        
        return dict(pools)
    
    def _add_database_compensators(
        self, 
        pools: Dict[str, List[Dict]], 
        config: GroupingConfig
    ) -> None:
        """Добавляет компенсаторов из базы данных для каждого пула."""
        try:
            # Получаем все уникальные комбинации группировочных колонок из существующих пулов
            unique_combinations = set(pools.keys())
            
            # Если пулов нет, создаем базовые комбинации
            if not unique_combinations:
                self._log("Нет существующих пулов, пропускаем поиск в БД")
                return
            
            # Для каждой комбинации ищем дополнительных компенсаторов в БД
            for pool_key in unique_combinations:
                additional_compensators = self._find_database_compensators(pool_key, config)
                
                # Добавляем найденных компенсаторов в пул
                if additional_compensators:
                    existing_wb_skus = {item['wb_sku'] for item in pools[pool_key]}
                    new_compensators = [
                        comp for comp in additional_compensators 
                        if comp['wb_sku'] not in existing_wb_skus
                    ]
                    pools[pool_key].extend(new_compensators)
                    self._log(f"Добавлено {len(new_compensators)} компенсаторов из БД для пула {pool_key}")
                    
        except Exception as e:
            self._log(f"Ошибка при поиске компенсаторов в БД: {str(e)}", "warning")
    
    def _find_database_compensators(
        self, 
        pool_key: str, 
        config: GroupingConfig
    ) -> List[Dict]:
        """Находит компенсаторов в базе данных для конкретного пула."""
        try:
            # Парсим ключ пула для получения условий поиска
            conditions = self._parse_pool_key(pool_key)
            self._log(f"Ищем компенсаторы для пула {pool_key} с условиями: {conditions}")
            
            # Строим SQL запрос для поиска компенсаторов БЕЗ остатка
            query = f"""
            SELECT DISTINCT 
                p.wb_sku,
                p.wb_category,
                pt.gender,
                pt.sort,
                r.avg_rating,
                COALESCE(stock_summary.total_stock, 0) as total_stock
            FROM wb_products p
            LEFT JOIN punta_table pt ON p.wb_sku = pt.wb_sku
            LEFT JOIN (
                SELECT 
                    p2.wb_sku, 
                    AVG(ocr.rating) as avg_rating
                FROM wb_products p2
                INNER JOIN (
                    SELECT wb_sku, TRIM(value) as barcode 
                    FROM (
                        SELECT wb_sku, UNNEST(string_split(wb_barcodes, ';')) as value
                        FROM wb_products
                        WHERE wb_barcodes IS NOT NULL AND wb_barcodes != ''
                    )
                    WHERE TRIM(value) != ''
                ) wb_normalized ON p2.wb_sku = wb_normalized.wb_sku
                INNER JOIN oz_barcodes ob ON TRIM(wb_normalized.barcode) = TRIM(ob.oz_barcode)
                INNER JOIN oz_products op_link ON ob.oz_product_id = op_link.oz_product_id
                INNER JOIN oz_card_rating ocr ON op_link.oz_sku = ocr.oz_sku
                WHERE ocr.rating IS NOT NULL AND ocr.rating > 0
                GROUP BY p2.wb_sku
            ) r ON p.wb_sku = r.wb_sku
            LEFT JOIN (
                SELECT 
                    p3.wb_sku,
                    SUM(COALESCE(op.oz_fbo_stock, 0)) as total_stock
                FROM wb_products p3
                INNER JOIN (
                    SELECT wb_sku, TRIM(value) as barcode 
                    FROM (
                        SELECT wb_sku, UNNEST(string_split(wb_barcodes, ';')) as value
                        FROM wb_products
                        WHERE wb_barcodes IS NOT NULL AND wb_barcodes != ''
                    )
                    WHERE TRIM(value) != ''
                ) wb_normalized_stock ON p3.wb_sku = wb_normalized_stock.wb_sku
                INNER JOIN oz_barcodes ob_stock ON TRIM(wb_normalized_stock.barcode) = TRIM(ob_stock.oz_barcode)
                INNER JOIN oz_products op ON ob_stock.oz_product_id = op.oz_product_id
                GROUP BY p3.wb_sku
            ) stock_summary ON p.wb_sku = stock_summary.wb_sku
            WHERE r.avg_rating >= {config.min_group_rating}
            AND r.avg_rating IS NOT NULL
            AND COALESCE(stock_summary.total_stock, 0) = 0
            AND NOT EXISTS (
                SELECT 1 FROM wb_products wb_defective
                INNER JOIN (
                    SELECT wb_sku, TRIM(value) as barcode 
                    FROM (
                        SELECT wb_sku, UNNEST(string_split(wb_barcodes, ';')) as value
                        FROM wb_products
                        WHERE wb_barcodes IS NOT NULL AND wb_barcodes != ''
                    )
                    WHERE TRIM(value) != ''
                ) wb_norm_def ON wb_defective.wb_sku = wb_norm_def.wb_sku
                INNER JOIN oz_barcodes ob_def ON TRIM(wb_norm_def.barcode) = TRIM(ob_def.oz_barcode)
                INNER JOIN oz_products op_def ON ob_def.oz_product_id = op_def.oz_product_id
                WHERE wb_defective.wb_sku = p.wb_sku
                AND op_def.oz_vendor_code LIKE 'БракSH%'
            )
            ORDER BY r.avg_rating DESC
            LIMIT 100
            """
            
            # Добавляем условия по категориям если они есть
            if conditions:
                category_conditions = []
                for col, value in conditions.items():
                    if value is not None and value != 'None':
                        if col == 'wb_category':
                            category_conditions.append(f"p.wb_category = '{value}'")
                        elif col == 'gender':
                            category_conditions.append(f"pt.gender = '{value}'")
                        elif col == 'sort':
                            category_conditions.append(f"pt.sort = '{value}'")
                
                if category_conditions:
                    # Заменяем WHERE на AND в запросе
                    query = query.replace(
                        f"WHERE r.avg_rating >= {config.min_group_rating}",
                        f"WHERE {' AND '.join(category_conditions)} AND r.avg_rating >= {config.min_group_rating}"
                    )
            
            result = self.connection.execute(query).fetchdf()
            
            self._log(f"SQL запрос выполнен успешно, найдено {len(result)} записей")
            
            if result.empty:
                self._log(f"Не найдено компенсаторов без остатка для пула {pool_key}")
                return []
            
            # Преобразуем в список словарей
            compensators = []
            for _, row in result.iterrows():
                compensator = {
                    'wb_sku': str(row['wb_sku']),
                    'wb_category': row.get('wb_category'),
                    'gender': row.get('gender'),
                    'sort': row.get('sort'),
                    'avg_rating': float(row['avg_rating']) if pd.notna(row['avg_rating']) else 0,
                    'total_stock': int(row.get('total_stock', 0)),
                    'is_priority_item': False
                }
                compensators.append(compensator)
            
            self._log(f"Найдено {len(compensators)} компенсаторов без остатка для пула {pool_key}")
            return compensators
            
        except Exception as e:
            self._log(f"Ошибка при поиске компенсаторов в БД для пула {pool_key}: {str(e)}", "warning")
            return []
    
    def _parse_pool_key(self, pool_key: str) -> Dict[str, str]:
        """Парсит ключ пула обратно в условия поиска."""
        conditions = {}
        if not pool_key:
            return conditions
            
        parts = pool_key.split('|')  # Исправлено: используем '|' вместо '_'
        for part in parts:
            if ':' in part:
                col, value = part.split(':', 1)
                conditions[col] = value
        
        return conditions
    
    def _get_pool_key(self, item: pd.Series, grouping_columns: List[str]) -> str:
        """Создает ключ пула на основе группировочных колонок."""
        key_parts = []
        for col in grouping_columns:
            if col in item and pd.notna(item[col]):
                key_parts.append(f"{col}:{item[col]}")
            else:
                key_parts.append(f"{col}:None")
        return "|".join(key_parts)
    
    def _create_single_group(
        self, 
        main_item: pd.Series, 
        all_data: pd.DataFrame, 
        config: GroupingConfig,
        compensator_pools: Dict[str, List[Dict]],
        processed_items: Set[str]
    ) -> Optional[Dict[str, Any]]:
        """Создает одну группу товаров с оптимальной стратегией."""
        # Проверяем рейтинг основного товара
        main_item_rating = main_item.get('avg_rating', 0) or 0
        main_item_dict = main_item.to_dict()
        
        # Если у приоритетного товара достаточный рейтинг, создаем группу из одного товара
        if main_item_rating >= config.min_group_rating:
            group = {
                'group_id': len(processed_items) + 1,
                'items': [main_item_dict],
                'group_rating': main_item_rating,
                'item_count': 1,
                'main_wb_sku': main_item['wb_sku']
            }
            self._log(f"Приоритетный товар {main_item['wb_sku']} имеет достаточный рейтинг {main_item_rating:.2f}, создана одиночная группа")
            return group
            
        # Отладочная информация
        self._log(f"Колонки в main_item: {list(main_item.index)}")
        self._log(f"wb_category в main_item: {'wb_category' in main_item.index}")
        if 'wb_category' in main_item.index:
            self._log(f"Значение wb_category в main_item: {main_item['wb_category']}")
        
        main_item_dict = main_item.to_dict()
        self._log(f"Ключи в main_item_dict: {list(main_item_dict.keys())}")
        self._log(f"wb_category в main_item_dict: {'wb_category' in main_item_dict}")
        
        # Для приоритетного товара с низким рейтингом создаем группу только с компенсаторами
        group_items = [main_item_dict]
        
        # Вычисляем текущий рейтинг группы
        group_rating = self._calculate_group_rating(group_items)
        
        # Если рейтинг все еще низкий, добавляем минимальное количество компенсаторов
        if group_rating < config.min_group_rating:
            # Сначала пытаемся найти компенсаторы в существующих пулах
            group_items = self._add_minimal_compensators(
                group_items, main_item, config, compensator_pools, processed_items
            )
            group_rating = self._calculate_group_rating(group_items)
            
            # Если рейтинг все еще низкий, ищем компенсаторы в БД
            if group_rating < config.min_group_rating:
                pool_key = self._get_pool_key(main_item, config.grouping_columns)
                db_compensators = self._find_database_compensators(pool_key, config)
                
                if db_compensators:
                    # Фильтруем уже использованные товары
                    available_db_compensators = [
                        comp for comp in db_compensators 
                        if comp['wb_sku'] not in processed_items and 
                           comp['wb_sku'] not in [item['wb_sku'] for item in group_items]
                    ]
                    
                    # Находим минимальное количество компенсаторов из БД
                    best_db_combination = self._find_minimal_compensator_combination(
                        group_items, available_db_compensators, config.min_group_rating, config.max_wb_sku_per_group
                    )
                    
                    if best_db_combination:
                        group_items.extend(best_db_combination)
                        group_rating = self._calculate_group_rating(group_items)
                        self._log(f"Добавлено {len(best_db_combination)} компенсаторов из БД для товара {main_item['wb_sku']}")
                    else:
                        self._log(f"Не найдено подходящих компенсаторов в БД для товара {main_item['wb_sku']}", "warning")
                else:
                    self._log(f"Компенсаторы в БД не найдены для товара {main_item['wb_sku']}", "warning")
        
        # Создаем финальную группу
        group = {
            'group_id': len(processed_items) + 1,
            'items': group_items,
            'group_rating': group_rating,
            'item_count': len(group_items),
            'main_wb_sku': main_item['wb_sku']
        }
        
        self._log(f"Создана группа {group['group_id']}: {len(group_items)} товаров, рейтинг {group_rating:.2f}")
        
        return group
    

    
    def _add_minimal_compensators(
        self,
        group_items: List[Dict],
        main_item: pd.Series,
        config: GroupingConfig,
        compensator_pools: Dict[str, List[Dict]],
        processed_items: Set[str]
    ) -> List[Dict]:
        """Добавляет минимальное количество компенсаторов для достижения целевого рейтинга."""
        pool_key = self._get_pool_key(main_item, config.grouping_columns)
        
        if pool_key not in compensator_pools:
            self._log(f"Пул компенсаторов для ключа {pool_key} не найден", "warning")
            return group_items
        
        available_compensators = [
            comp for comp in compensator_pools[pool_key] 
            if comp['wb_sku'] not in processed_items and 
               comp['wb_sku'] not in [item['wb_sku'] for item in group_items] and
               comp.get('total_stock', 0) == 0  # Только товары без остатка
        ]
        
        # Сортируем компенсаторы по рейтингу (лучшие первыми)
        available_compensators.sort(key=lambda x: x.get('avg_rating', 0), reverse=True)
        
        # Находим минимальное количество компенсаторов для достижения целевого рейтинга
        best_combination = self._find_minimal_compensator_combination(
            group_items, available_compensators, config.min_group_rating, config.max_wb_sku_per_group
        )
        
        if best_combination:
            group_items.extend(best_combination)
            self._log(f"Добавлено {len(best_combination)} компенсаторов для достижения рейтинга {config.min_group_rating}")
        else:
            self._log("Не удалось найти подходящие компенсаторы для достижения целевого рейтинга", "warning")
        
        return group_items
    
    def _find_minimal_compensator_combination(
        self,
        base_items: List[Dict],
        compensators: List[Dict],
        target_rating: float,
        max_items: int
    ) -> List[Dict]:
        """Находит минимальную комбинацию компенсаторов для достижения целевого рейтинга."""
        current_rating = self._calculate_group_rating(base_items)
        
        if current_rating >= target_rating:
            return []
        
        # Пробуем добавлять компенсаторы по одному
        for i in range(1, min(len(compensators) + 1, max_items - len(base_items) + 1)):
            # Берем i лучших компенсаторов
            test_combination = compensators[:i]
            test_group = base_items + test_combination
            test_rating = self._calculate_group_rating(test_group)
            
            if test_rating >= target_rating:
                return test_combination
        
        # Если не удалось достичь целевого рейтинга, возвращаем максимально возможное количество
        max_possible = min(len(compensators), max_items - len(base_items))
        if max_possible > 0:
            return compensators[:max_possible]
        
        return []
    
    def _add_compensators_from_pools(
        self,
        group_items: List[Dict],
        main_item: pd.Series,
        config: GroupingConfig,
        compensator_pools: Dict[str, List[Dict]],
        processed_items: Set[str]
    ) -> List[Dict]:
        """Добавляет компенсаторы из пулов (старый метод для совместимости)."""
        return self._add_minimal_compensators(group_items, main_item, config, compensator_pools, processed_items)
    
    def _calculate_group_rating(self, group_items: List[Dict]) -> float:
        """Вычисляет средний рейтинг группы.
        
        Товары с рейтингом 0 (без отзывов) не учитываются в расчете среднего рейтинга.
        Это важно для новинок из актуального сезона.
        """
        ratings = []
        zero_rating_count = 0
        
        for item in group_items:
            rating = item.get('avg_rating')
            if rating is not None:
                if rating > 0:
                    ratings.append(rating)
                elif rating == 0:
                    zero_rating_count += 1
        
        if not ratings:
            return 0.0
        
        # Логируем информацию о товарах с нулевым рейтингом
        if zero_rating_count > 0:
            self._log(f"В группе {zero_rating_count} товаров с рейтингом 0 (не учитываются в среднем)")
        
        return sum(ratings) / len(ratings)
    
    def _is_defective_item(self, item: pd.Series) -> bool:
        """Проверяет, является ли товар дефектным (oz_vendor_code начинается с "БракSH")."""
        try:
            wb_sku = str(item.get('wb_sku', ''))
            if not wb_sku:
                return False
            
            # Получаем связанные oz_vendor_code для проверки на брак
            extended_links = self.linker.get_extended_links([wb_sku], include_product_details=False)
            
            if extended_links.empty:
                return False
            
            # Проверяем есть ли oz_vendor_code начинающиеся с "БракSH"
            for _, link in extended_links.iterrows():
                oz_vendor_code = link.get('oz_vendor_code', '')
                if isinstance(oz_vendor_code, str) and oz_vendor_code.startswith('БракSH'):
                    self._log(f"Товар {wb_sku} является бракованным (oz_vendor_code: {oz_vendor_code})", "info")
                    return True
            
            return False
            
        except Exception as e:
            self._log(f"Ошибка проверки брака для товара {item.get('wb_sku', 'Unknown')}: {str(e)}", "error")
            return False
    
    def _calculate_statistics(
        self, 
        groups: List[Dict], 
        low_rating_items: List[Dict], 
        defective_items: List[Dict]
    ) -> Dict[str, Any]:
        """Вычисляет статистику группировки."""
        total_items = sum(len(group['items']) for group in groups)
        total_items += len(low_rating_items) + len(defective_items)
        
        avg_group_size = sum(len(group['items']) for group in groups) / len(groups) if groups else 0
        avg_group_rating = sum(group['group_rating'] for group in groups) / len(groups) if groups else 0
        
        return {
            'total_groups': len(groups),
            'total_items_processed': total_items,
            'avg_group_size': avg_group_size,
            'avg_group_rating': avg_group_rating,
            'low_rating_items_count': len(low_rating_items),
            'defective_items_count': len(defective_items)
        }