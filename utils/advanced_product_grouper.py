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
        """Получает данные из таблицы punta_table."""
        try:
            from utils.cards_matcher_helpers import get_punta_table_data_for_wb_skus
            
            # Получаем данные из punta_table
            punta_data = get_punta_table_data_for_wb_skus(
                self.connection, 
                wb_skus, 
                ['gender', 'sort', 'wb_category']
            )
            
            if punta_data.empty:
                self._log("Данные punta_table не найдены", "warning")
                return pd.DataFrame()
            
            return punta_data
            
        except Exception as e:
            self._log(f"Ошибка получения данных Punta: {str(e)}", "error")
            return pd.DataFrame()
    
    def _get_stock_summary(self, wb_skus: List[str]) -> pd.DataFrame:
        """Получает сводку по остаткам товаров."""
        try:
            wb_skus_str = "', '".join(wb_skus)
            query = f"""
            SELECT 
                wp.wb_sku,
                SUM(COALESCE(wpr.wb_fbo_stock, 0)) as total_stock
            FROM wb_products wp
            LEFT JOIN wb_prices wpr ON wp.wb_sku = wpr.wb_sku
            WHERE wp.wb_sku IN ('{wb_skus_str}')
            GROUP BY wp.wb_sku
            """
            
            result = self.connection.execute(query).fetchdf()
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
        
        # Добавляем данные Punta
        if not punta_data.empty:
            punta_data['wb_sku'] = punta_data['wb_sku'].astype(str)
            merged = merged.merge(punta_data, on='wb_sku', how='left')
        else:
            merged['gender'] = None
            merged['sort'] = None
            merged['wb_category'] = None
        
        # Добавляем данные по остаткам
        if not stock_data.empty:
            stock_data['wb_sku'] = stock_data['wb_sku'].astype(str)
            merged = merged.merge(stock_data, on='wb_sku', how='left')
        else:
            merged['total_stock'] = 0
        
        # Заполняем пропуски
        merged['total_stock'] = merged['total_stock'].fillna(0)
        
        return merged
    
    def _prioritize_items(self, data: pd.DataFrame, enable_sort_priority: bool) -> pd.DataFrame:
        """Приоритизирует товары для обработки согласно сезонности и остаткам."""
        data = data.copy()
        
        # Определяем приоритетные товары:
        # 1. Товары из актуального сезона (13)
        # 2. Товары из прошлых сезонов с положительным остатком
        current_season = 13
        data['is_current_season'] = (data['sort'] == current_season)
        data['has_positive_stock'] = (data.get('total_stock', 0) > 0)
        data['is_priority_item'] = (
            data['is_current_season'] | 
            (data['has_positive_stock'] & (data['sort'] < current_season))
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
        
        # Обрабатываем только приоритетные товары для формирования основы групп
        processed_items = set()
        priority_items = data[data.get('is_priority_item', False)]
        non_priority_items = data[~data.get('is_priority_item', False)]
        
        self._log(f"Обрабатываем {len(priority_items)} приоритетных товаров для создания групп")
        
        for _, item in priority_items.iterrows():
            wb_sku = item['wb_sku']
            
            if wb_sku in processed_items:
                continue
            
            # Проверяем на дефектность
            if self._is_defective_item(item):
                defective_items.append(item.to_dict())
                processed_items.add(wb_sku)
                continue
            
            # Проверяем, нужна ли группа для этого товара
            item_rating = item.get('avg_rating', 0) or 0
            if item_rating >= config.min_group_rating:
                # Товар с достаточным рейтингом не нуждается в группировке
                low_rating_items.append(item.to_dict())
                processed_items.add(wb_sku)
                self._log(f"Приоритетный товар {wb_sku} имеет достаточный рейтинг {item_rating:.2f}, добавлен в отдельные товары")
                continue
            
            # Создаем новую группу на основе приоритетного товара
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
        
        # Неприоритетные товары, которые не были использованы как компенсаторы, добавляем в low_rating_items
        for _, item in non_priority_items.iterrows():
            if item['wb_sku'] not in processed_items:
                low_rating_items.append(item.to_dict())
        
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
        
        В компенсаторы попадают только неприоритетные товары с высоким рейтингом.
        Приоритетные товары используются для формирования основы групп.
        """
        pools = defaultdict(list)
        
        # Находим неприоритетные высокорейтинговые товары для компенсации
        compensator_candidates = data[
            (~data.get('is_priority_item', False)) &  # Только неприоритетные товары
            (data['avg_rating'].notna()) & 
            (data['avg_rating'] >= config.min_group_rating)
        ]
        
        for _, item in compensator_candidates.iterrows():
            # Создаем ключ пула на основе группировочных колонок
            pool_key = self._get_pool_key(item, config.grouping_columns)
            pools[pool_key].append(item.to_dict())
        
        self._log(f"Создано {len(pools)} пулов компенсаторов из неприоритетных товаров")
        for pool_key, items in pools.items():
            self._log(f"Пул {pool_key}: {len(items)} компенсаторов")
        
        return dict(pools)
    
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
        # Проверяем, нужна ли группа для этого товара
        main_item_rating = main_item.get('avg_rating', 0) or 0
        if main_item_rating >= config.min_group_rating:
            self._log(f"Товар {main_item['wb_sku']} имеет рейтинг {main_item_rating:.2f} >= {config.min_group_rating}, группа не нужна")
            return None
            
        group_items = [main_item.to_dict()]
        
        # Ищем похожие товары для группы
        similar_items = self._find_similar_items(
            main_item, all_data, config.grouping_columns, processed_items
        )
        
        # Добавляем похожие товары по одному, проверяя рейтинг
        for similar_item in similar_items:
            if len(group_items) >= config.max_wb_sku_per_group:
                break
            
            # Временно добавляем товар для проверки рейтинга
            temp_group = group_items + [similar_item.to_dict()]
            temp_rating = self._calculate_group_rating(temp_group)
            
            # Если рейтинг уже достаточный, останавливаемся
            if temp_rating >= config.min_group_rating:
                group_items = temp_group
                break
            else:
                # Иначе добавляем товар и продолжаем
                group_items.append(similar_item.to_dict())
        
        # Вычисляем текущий рейтинг группы
        group_rating = self._calculate_group_rating(group_items)
        
        # Если рейтинг все еще низкий, добавляем минимальное количество компенсаторов
        if group_rating < config.min_group_rating:
            group_items = self._add_minimal_compensators(
                group_items, main_item, config, compensator_pools, processed_items
            )
            group_rating = self._calculate_group_rating(group_items)
        
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
    
    def _find_similar_items(
        self, 
        main_item: pd.Series, 
        all_data: pd.DataFrame, 
        grouping_columns: List[str],
        processed_items: Set[str]
    ) -> List[pd.Series]:
        """Находит похожие приоритетные товары для группировки.
        
        Ищет только среди приоритетных товаров для формирования основы группы.
        """
        similar = []
        
        # Фильтруем только приоритетные товары
        priority_data = all_data[all_data.get('is_priority_item', False)]
        
        for _, item in priority_data.iterrows():
            if item['wb_sku'] in processed_items or item['wb_sku'] == main_item['wb_sku']:
                continue
            
            # Проверяем совпадение по группировочным колонкам
            is_similar = True
            for col in grouping_columns:
                if col in main_item and col in item:
                    if pd.isna(main_item[col]) and pd.isna(item[col]):
                        continue
                    if main_item[col] != item[col]:
                        is_similar = False
                        break
            
            if is_similar:
                similar.append(item)
        
        return similar
    
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
               comp['wb_sku'] not in [item['wb_sku'] for item in group_items]
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
        """Проверяет, является ли товар дефектным."""
        # Логика определения дефектных товаров
        # Можно расширить по необходимости
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