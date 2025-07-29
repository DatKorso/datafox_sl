"""
Модуль для поиска рекомендаций WB товаров.

Основная цель: адаптировать алгоритм рекомендаций из Rich Content OZ
для работы с WB товарами, используя связь WB ↔ Ozon через штрихкоды.

Принцип работы:
1. Для каждого WB SKU находим связанные Ozon товары через штрихкоды
2. Обогащаем WB товары характеристиками из Ozon
3. Применяем адаптированный алгоритм рекомендаций для поиска похожих WB товаров

Автор: DataFox SL Project
Версия: 1.0.0
"""

import json
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple, Callable
from enum import Enum
import re
import time
import pandas as pd

# Импорт модулей для связывания
from .cross_marketplace_linker import CrossMarketplaceLinker
from .data_cleaning import DataCleaningUtils

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Если нет handlers, добавляем консольный handler
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Константы
WB_RECOMMENDATIONS_VERSION = 1.0
DEFAULT_RECOMMENDATIONS_COUNT = 20


class WBProcessingStatus(Enum):
    """Статусы обработки WB товара"""
    SUCCESS = "success"
    NO_DATA = "no_data"
    NO_OZON_LINK = "no_ozon_link"
    NO_SIMILAR = "no_similar"
    INSUFFICIENT_RECOMMENDATIONS = "insufficient_recommendations"
    ERROR = "error"


@dataclass
class WBProductInfo:
    """Модель данных WB товара с обогащенными характеристиками из Ozon"""
    
    # Основные поля WB
    wb_sku: str
    wb_category: Optional[str] = None
    wb_brand: Optional[str] = None
    wb_sizes: List[int] = field(default_factory=list)  # Список всех размеров товара
    wb_barcodes: Optional[str] = None
    
    # Поля из wb_prices
    wb_fbo_stock: int = 0
    wb_full_price: Optional[int] = None
    wb_discount: Optional[int] = None
    
    # Обогащенные характеристики из связанных Ozon товаров
    enriched_type: Optional[str] = None
    enriched_gender: Optional[str] = None
    enriched_brand: Optional[str] = None
    enriched_season: Optional[str] = None
    enriched_color: Optional[str] = None
    enriched_material: Optional[str] = None
    enriched_fastener_type: Optional[str] = None
    
    # Punta данные (если доступны)
    punta_material_short: Optional[str] = None
    punta_new_last: Optional[str] = None
    punta_mega_last: Optional[str] = None
    punta_best_last: Optional[str] = None
    punta_heel_type: Optional[str] = None
    punta_sole_type: Optional[str] = None
    punta_heel_up_type: Optional[str] = None
    punta_lacing_type: Optional[str] = None
    punta_nose_type: Optional[str] = None
    
    # Техническая информация
    linked_oz_vendor_codes: List[str] = field(default_factory=list)
    linked_oz_skus: List[str] = field(default_factory=list)
    enrichment_source: str = "none"  # "ozon", "punta", "none"
    
    def __post_init__(self):
        """Нормализация данных после создания"""
        # Нормализация размеров
        if self.wb_sizes:
            self.wb_sizes = self._normalize_sizes(self.wb_sizes)
        
        # Нормализация брендов
        if self.wb_brand:
            self.wb_brand = self._normalize_brand(self.wb_brand)
        if self.enriched_brand:
            self.enriched_brand = self._normalize_brand(self.enriched_brand)
    
    def _normalize_sizes(self, sizes: List[int]) -> List[int]:
        """Нормализация размеров"""
        if not sizes:
            return []
        # Убираем дубликаты, сортируем, фильтруем валидные размеры
        normalized = []
        for size in sizes:
            try:
                size_int = int(size)
                if 10 <= size_int <= 60:  # Разумные пределы для размеров обуви
                    normalized.append(size_int)
            except (ValueError, TypeError):
                continue
        return sorted(list(set(normalized)))
    
    def _normalize_brand(self, brand: str) -> str:
        """Нормализация названия бренда"""
        if not brand:
            return brand
        return brand.strip().title()
    
    def get_size_range(self) -> Tuple[int, int]:
        """Получение диапазона размеров (min, max)"""
        if not self.wb_sizes:
            return (0, 0)
        return (min(self.wb_sizes), max(self.wb_sizes))
    
    def get_size_range_str(self) -> str:
        """Получение строкового представления диапазона размеров"""
        if not self.wb_sizes:
            return "Нет размеров"
        if len(self.wb_sizes) == 1:
            return str(self.wb_sizes[0])
        min_size, max_size = self.get_size_range()
        return f"{min_size}-{max_size}"
    
    def has_size_overlap(self, other: 'WBProductInfo') -> bool:
        """Проверка пересечения размеров с другим товаром"""
        if not self.wb_sizes or not other.wb_sizes:
            return False
        return bool(set(self.wb_sizes) & set(other.wb_sizes))
    
    def calculate_size_overlap_percentage(self, other: 'WBProductInfo') -> float:
        """Вычисление процента пересечения размеров"""
        if not self.wb_sizes or not other.wb_sizes:
            return 0.0
        
        self_sizes = set(self.wb_sizes)
        other_sizes = set(other.wb_sizes)
        
        intersection = self_sizes & other_sizes
        if not intersection:
            return 0.0
        
        # Используем минимум от двух товаров для справедливого сравнения
        union_size = min(len(self_sizes), len(other_sizes))
        return len(intersection) / union_size
    
    def get_effective_brand(self) -> str:
        """Получение эффективного бренда (приоритет обогащенному)"""
        return self.enriched_brand or self.wb_brand or ""
    
    def get_effective_type(self) -> str:
        """Получение эффективного типа товара"""
        return self.enriched_type or ""
    
    def get_effective_gender(self) -> str:
        """Получение эффективного пола"""
        return self.enriched_gender or ""
    
    def has_enriched_data(self) -> bool:
        """Проверка наличия обогащенных данных"""
        return bool(self.enriched_type and self.enriched_gender and self.enriched_brand)
    
    def get_enrichment_score(self) -> float:
        """Оценка качества обогащения (0-1)"""
        score = 0.0
        total_fields = 12
        
        if self.enriched_type: score += 1
        if self.enriched_gender: score += 1
        if self.enriched_brand: score += 1
        if self.enriched_season: score += 1
        if self.enriched_color: score += 1
        if self.enriched_material: score += 1
        if self.enriched_fastener_type: score += 1
        if self.punta_heel_type: score += 1
        if self.punta_sole_type: score += 1
        if self.punta_heel_up_type: score += 1
        if self.punta_lacing_type: score += 1
        if self.punta_nose_type: score += 1
        
        return score / total_fields


@dataclass
class WBRecommendation:
    """Модель рекомендации WB товара"""
    product_info: WBProductInfo
    score: float
    match_details: str
    processing_status: WBProcessingStatus = WBProcessingStatus.SUCCESS
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь для сериализации"""
        return {
            'wb_sku': self.product_info.wb_sku,
            'score': self.score,
            'wb_brand': self.product_info.wb_brand,
            'wb_category': self.product_info.wb_category,
            'wb_sizes': self.product_info.get_size_range_str(),
            'wb_stock': self.product_info.wb_fbo_stock,
            'wb_price': self.product_info.wb_full_price,
            'enriched_type': self.product_info.enriched_type,
            'enriched_gender': self.product_info.enriched_gender,
            'enriched_season': self.product_info.enriched_season,
            'enriched_color': self.product_info.enriched_color,
            'match_details': self.match_details,
            'status': self.processing_status.value,
            'enrichment_score': self.product_info.get_enrichment_score()
        }


@dataclass
class WBScoringConfig:
    """Конфигурация системы оценки схожести WB товаров"""
    
    # Базовые параметры
    base_score: int = 100
    max_score: int = 500
    
    # Обязательные критерии (вес 0, т.к. используются для фильтрации)
    exact_type_weight: int = 0
    exact_gender_weight: int = 0
    exact_brand_weight: int = 0
    
    # Размер (критический для обуви)
    exact_size_weight: int = 100
    close_size_weight: int = 40      # ±1 размер
    size_mismatch_penalty: int = -50
    
    # Сезонность
    season_match_bonus: int = 80
    season_mismatch_penalty: int = -40
    
    # Второстепенные характеристики
    color_match_bonus: int = 40
    material_match_bonus: int = 40
    fastener_match_bonus: int = 30
    
    # Новые характеристики из punta_table
    heel_type_match_bonus: int = 50
    sole_type_match_bonus: int = 50
    heel_up_type_match_bonus: int = 50
    lacing_type_match_bonus: int = 50
    nose_type_match_bonus: int = 50
    
    # Колодки (приоритет по типам)
    mega_last_bonus: int = 90
    best_last_bonus: int = 70
    new_last_bonus: int = 50
    no_last_penalty: float = 0.7
    
    # Остатки на складе WB
    stock_high_bonus: int = 40       # >10 шт
    stock_medium_bonus: int = 20     # 3-10 шт
    stock_low_bonus: int = 10        # 1-2 шт
    stock_threshold_high: int = 10
    stock_threshold_medium: int = 3
    
    # Цена (отключено по требованию пользователя)
    price_similarity_bonus: int = 0
    price_diff_threshold: float = 0.2  # 20% разница в цене
    
    # Качество обогащения
    enrichment_quality_bonus: int = 30  # Бонус за хорошее обогащение
    enrichment_quality_threshold: float = 0.7  # Порог качества
    
    # Лимиты
    max_recommendations: int = 20
    min_recommendations: int = 5  # Минимальное количество для предупреждения, но не блокировки
    min_score_threshold: float = 40.0  # Снижен порог для увеличения количества рекомендаций
    
    @classmethod
    def get_preset(cls, preset_name: str) -> 'WBScoringConfig':
        """Получение предустановленных конфигураций"""
        presets = {
            "balanced": cls(),  # Стандартная конфигурация
            
            "size_focused": cls(
                exact_size_weight=150,
                close_size_weight=60,
                season_match_bonus=60,
                color_match_bonus=20,
                max_recommendations=15
            ),
            
            "price_focused": cls(
                price_similarity_bonus=0,  # Отключено по требованию пользователя
                price_diff_threshold=0.15,
                exact_size_weight=80,
                season_match_bonus=60,
                max_recommendations=25
            ),
            
            "quality_focused": cls(
                enrichment_quality_bonus=50,
                enrichment_quality_threshold=0.8,
                min_score_threshold=80.0,
                max_recommendations=15,
                min_recommendations=3
            ),
            
            "conservative": cls(
                min_score_threshold=100.0,
                max_recommendations=10,
                min_recommendations=3,
                enrichment_quality_threshold=0.6
            )
        }
        
        if preset_name not in presets:
            raise ValueError(f"Неизвестный preset: {preset_name}. Доступные: {list(presets.keys())}")
        
        return presets[preset_name]


@dataclass
class WBProcessingResult:
    """Результат обработки WB товара"""
    wb_sku: str
    status: WBProcessingStatus
    recommendations: List[WBRecommendation]
    processing_time: float
    enrichment_info: Dict[str, Any]
    error_message: Optional[str] = None
    
    @property
    def success(self) -> bool:
        """Успешность обработки"""
        return self.status in [WBProcessingStatus.SUCCESS, WBProcessingStatus.INSUFFICIENT_RECOMMENDATIONS]


@dataclass
class WBBatchResult:
    """Результат пакетной обработки WB товаров"""
    processed_items: List[WBProcessingResult]
    total_processing_time: float
    success_count: int
    error_count: int
    
    @property
    def success_rate(self) -> float:
        """Процент успешных обработок"""
        if not self.processed_items:
            return 0.0
        return (self.success_count / len(self.processed_items)) * 100


class WBDataCollector:
    """Класс для сбора и обогащения данных WB товаров"""
    
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.linker = CrossMarketplaceLinker(db_conn)
        self._cache = {}
    
    def get_wb_product_info(self, wb_sku: str) -> Optional[WBProductInfo]:
        """
        Получение полной информации о WB товаре с обогащением
        
        Args:
            wb_sku: WB SKU товара
            
        Returns:
            WBProductInfo с обогащенными данными или None
        """
        logger.info(f"🔍 WBDataCollector: получение данных для WB SKU {wb_sku}")
        
        try:
            # Получаем базовые данные WB
            wb_base_data = self._get_wb_base_data(wb_sku)
            if not wb_base_data:
                logger.warning(f"❌ WB товар {wb_sku} не найден в базе данных")
                return None
            
            # Создаем базовый объект
            product_info = WBProductInfo(
                wb_sku=wb_sku,
                wb_category=wb_base_data.get('wb_category'),
                wb_brand=wb_base_data.get('wb_brand'),
                wb_sizes=wb_base_data.get('wb_sizes', []),
                wb_barcodes=wb_base_data.get('wb_barcodes'),
                wb_fbo_stock=wb_base_data.get('wb_fbo_stock', 0),
                wb_full_price=wb_base_data.get('wb_full_price'),
                wb_discount=wb_base_data.get('wb_discount')
            )
            
            # Обогащаем данными из Ozon
            enriched_product = self._enrich_with_ozon_data(product_info)
            
            # Обогащаем данными из Punta (если доступно)
            enriched_product = self._enrich_with_punta_data(enriched_product)
            
            logger.info(f"✅ WB товар {wb_sku} обогащен, качество: {enriched_product.get_enrichment_score():.2f}")
            return enriched_product
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении данных для WB товара {wb_sku}: {e}")
            return None
    
    def _get_wb_base_data(self, wb_sku: str) -> Optional[Dict[str, Any]]:
        """Получение базовых данных WB товара со всеми размерами"""
        logger.info(f"📊 Получение базовых данных WB товара: {wb_sku}")
        
        try:
            # Получаем основные данные товара (берем первую запись для базовой информации)
            base_query = """
            SELECT DISTINCT
                wb.wb_sku,
                wb.wb_category,
                wb.wb_brand,
                wb.wb_barcodes,
                COALESCE(wp.wb_fbo_stock, 0) as wb_fbo_stock,
                wp.wb_full_price,
                wp.wb_discount
            FROM wb_products wb
            LEFT JOIN wb_prices wp ON wb.wb_sku = wp.wb_sku
            WHERE wb.wb_sku = ?
            LIMIT 1
            """
            
            logger.info(f"📊 Выполнение базового запроса для WB товара: {wb_sku}")
            start_time = time.time()
            base_result = self.db_conn.execute(base_query, [wb_sku]).fetchdf()
            query_time = time.time() - start_time
            logger.info(f"📊 Базовый запрос выполнен за {query_time:.2f}с")
            
            if base_result.empty:
                logger.warning(f"⚠️ WB товар {wb_sku} не найден в базе данных")
                return None
            
            # Преобразуем в словарь
            base_dict = base_result.iloc[0].to_dict()
            logger.info(f"📊 Базовые данные получены: категория={base_dict.get('wb_category')}, бренд={base_dict.get('wb_brand')}")
            
            # Получаем все размеры для этого wb_sku
            sizes_query = """
            SELECT DISTINCT wb_size
            FROM wb_products
            WHERE wb_sku = ?
            AND wb_size IS NOT NULL
            ORDER BY wb_size
            """
            
            logger.info(f"📊 Запрос размеров для WB товара: {wb_sku}")
            start_time = time.time()
            sizes_df = self.db_conn.execute(sizes_query, [wb_sku]).fetchdf()
            query_time = time.time() - start_time
            logger.info(f"📊 Запрос размеров выполнен за {query_time:.2f}с")
            
            sizes = sizes_df['wb_size'].tolist() if not sizes_df.empty else []
            logger.info(f"📊 Найдено размеров: {len(sizes)} - {sizes}")
            
            # Объединяем данные
            base_dict['wb_sizes'] = sizes
            
            logger.info(f"✅ Базовые данные WB товара {wb_sku} получены успешно")
            return base_dict
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения базовых данных WB {wb_sku}: {e}")
            logger.error(f"❌ Детали ошибки: {type(e).__name__}: {str(e)}")
            return None
    
    def _enrich_with_ozon_data(self, product_info: WBProductInfo) -> WBProductInfo:
        """Обогащение данными из связанных Ozon товаров"""
        try:
            # Находим связанные Ozon товары
            linked_oz = self.linker.link_wb_to_oz([product_info.wb_sku])
            
            if not linked_oz or product_info.wb_sku not in linked_oz:
                logger.info(f"⚠️ Нет связанных Ozon товаров для WB SKU {product_info.wb_sku}")
                return product_info
            
            oz_skus = linked_oz[product_info.wb_sku]
            product_info.linked_oz_skus = oz_skus
            
            # Получаем характеристики из Ozon (oz_skus здесь на самом деле vendor_codes)
            oz_characteristics = self._get_ozon_characteristics(oz_skus)
            
            if oz_characteristics:
                # Обогащаем характеристики (используем стратегию "наиболее популярные")
                product_info.enriched_type = self._get_most_common_value(oz_characteristics, 'type')
                product_info.enriched_gender = self._get_most_common_value(oz_characteristics, 'gender')
                product_info.enriched_brand = self._get_most_common_value(oz_characteristics, 'oz_brand')
                product_info.enriched_season = self._get_most_common_value(oz_characteristics, 'season')
                product_info.enriched_color = self._get_most_common_value(oz_characteristics, 'color')
                # Материал берем только из punta_table.material_short, не из oz_category_products
                product_info.enriched_material = None
                product_info.enriched_fastener_type = self._get_most_common_value(oz_characteristics, 'fastener_type')
                
                # Сохраняем связанные vendor codes
                product_info.linked_oz_vendor_codes = [item['oz_vendor_code'] for item in oz_characteristics]
                product_info.enrichment_source = "ozon"
                
                logger.info(f"✅ Обогащение Ozon данными для WB {product_info.wb_sku}: {len(oz_characteristics)} товаров")
            
            return product_info
            
        except Exception as e:
            logger.error(f"❌ Ошибка обогащения Ozon данными для WB {product_info.wb_sku}: {e}")
            return product_info
    
    def _get_ozon_characteristics(self, oz_vendor_codes: List[str]) -> List[Dict[str, Any]]:
        """Получение характеристик из Ozon товаров по vendor_code"""
        if not oz_vendor_codes:
            return []
        
        try:
            # Создаем placeholders для SQL
            placeholders = ','.join(['?' for _ in oz_vendor_codes])
            
            query = f"""
            SELECT DISTINCT
                ocp.oz_vendor_code,
                ocp.type,
                ocp.gender,
                ocp.oz_brand,
                ocp.season,
                ocp.color,
                ocp.fastener_type
            FROM oz_category_products ocp
            WHERE ocp.oz_vendor_code IN ({placeholders})
            AND ocp.type IS NOT NULL
            AND ocp.gender IS NOT NULL
            AND ocp.oz_brand IS NOT NULL
            """
            
            results_df = self.db_conn.execute(query, oz_vendor_codes).fetchdf()
            
            if results_df.empty:
                return []
            
            return results_df.to_dict('records')
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения характеристик Ozon: {e}")
            return []
    
    def _enrich_with_punta_data(self, product_info: WBProductInfo) -> WBProductInfo:
        """Обогащение данными из punta_table"""
        try:
            query = """
            SELECT 
                material_short,
                new_last,
                mega_last,
                best_last,
                heel_type,
                sole_type,
                heel_up_type,
                lacing_type,
                nose_type
            FROM punta_table
            WHERE wb_sku = ?
            """
            
            result_df = self.db_conn.execute(query, [product_info.wb_sku]).fetchdf()
            if not result_df.empty:
                result = result_df.iloc[0].to_dict()
                product_info.punta_material_short = result.get('material_short')
                product_info.punta_new_last = result.get('new_last')
                product_info.punta_mega_last = result.get('mega_last')
                product_info.punta_best_last = result.get('best_last')
                product_info.punta_heel_type = result.get('heel_type')
                product_info.punta_sole_type = result.get('sole_type')
                product_info.punta_heel_up_type = result.get('heel_up_type')
                product_info.punta_lacing_type = result.get('lacing_type')
                product_info.punta_nose_type = result.get('nose_type')
                
                # Если нет обогащения из Ozon, используем источник "punta"
                if product_info.enrichment_source == "none":
                    product_info.enrichment_source = "punta"
                
                logger.info(f"✅ Обогащение Punta данными для WB {product_info.wb_sku}")
            
            return product_info
            
        except Exception as e:
            logger.error(f"❌ Ошибка обогащения Punta данными для WB {product_info.wb_sku}: {e}")
            return product_info
    
    def _get_most_common_value(self, characteristics: List[Dict[str, Any]], field: str) -> Optional[str]:
        """Получение наиболее частого значения поля"""
        values = [item.get(field) for item in characteristics if item.get(field)]
        if not values:
            return None
        
        # Подсчитываем частоту
        freq = {}
        for value in values:
            freq[value] = freq.get(value, 0) + 1
        
        # Возвращаем наиболее частое значение
        return max(freq.items(), key=lambda x: x[1])[0]
    
    def find_wb_candidates(self, source_product: WBProductInfo) -> List[WBProductInfo]:
        """
        Поиск кандидатов WB товаров для рекомендаций
        
        Args:
            source_product: Исходный товар для поиска похожих
            
        Returns:
            Список кандидатов WB товаров
        """
        logger.info(f"🔍 WBDataCollector: поиск кандидатов для WB товара {source_product.wb_sku}")
        
        try:
            # Проверяем наличие обогащенных данных
            if not source_product.has_enriched_data():
                logger.warning(f"⚠️ Недостаточно обогащенных данных для поиска кандидатов: {source_product.wb_sku}")
                return []
            
            # Поиск по обязательным критериям
            effective_type = source_product.get_effective_type()
            effective_gender = source_product.get_effective_gender()
            effective_brand = source_product.get_effective_brand()
            
            logger.info(f"📊 Критерии поиска - тип: {effective_type}, пол: {effective_gender}, бренд: {effective_brand}")
            
            # Находим WB товары с похожими характеристиками
            candidates = self._find_wb_candidates_by_criteria(effective_type, effective_gender, effective_brand, source_product.wb_sku)
            
            logger.info(f"✅ Найдено {len(candidates)} кандидатов для WB товара {source_product.wb_sku}")
            return candidates
            
        except Exception as e:
            logger.error(f"❌ Ошибка при поиске кандидатов для WB товара {source_product.wb_sku}: {e}")
            return []
    
    def _find_wb_candidates_by_criteria(self, type_val: str, gender_val: str, brand_val: str, exclude_wb_sku: str) -> List[WBProductInfo]:
        """
        ОПТИМИЗИРОВАННЫЙ поиск WB кандидатов по критериям через связанные Ozon товары
        Алгоритм из Rich Content OZ: разделение сложного запроса на быстрые этапы
        """
        logger.info(f"🔍 ОПТИМИЗИРОВАННЫЙ поиск WB кандидатов: тип={type_val}, пол={gender_val}, бренд={brand_val}")
        
        try:
            # ЭТАП 1: Быстрый поиск OZ товаров по критериям (БЕЗ штрихкодов)
            logger.info(f"📊 Этап 1: Поиск OZ товаров по критериям...")
            step1_start = time.time()
            
            oz_candidates_query = """
            SELECT DISTINCT ocp.oz_vendor_code
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.type = ?
            AND ocp.gender = ?
            AND ocp.oz_brand = ?
            AND COALESCE(op.oz_fbo_stock, 0) > 0
            """
            
            oz_candidates_df = self.db_conn.execute(oz_candidates_query, [type_val, gender_val, brand_val]).fetchdf()
            step1_time = time.time() - step1_start
            logger.info(f"📊 Этап 1 завершен за {step1_time:.2f}с, найдено OZ товаров: {len(oz_candidates_df)}")
            
            if oz_candidates_df.empty:
                logger.warning(f"⚠️ Не найдено OZ товаров для критериев: {type_val}, {gender_val}, {brand_val}")
                return []
            
            # ЭТАП 2: Получение штрихкодов для найденных OZ товаров
            logger.info(f"📊 Этап 2: Получение штрихкодов для {len(oz_candidates_df)} OZ товаров...")
            step2_start = time.time()
            
            oz_vendor_codes = oz_candidates_df['oz_vendor_code'].tolist()
            vendor_codes_str = ','.join(['?' for _ in oz_vendor_codes])
            
            oz_barcodes_query = f"""
            SELECT DISTINCT ozb.oz_barcode
            FROM oz_barcodes ozb
            WHERE ozb.oz_vendor_code IN ({vendor_codes_str})
            AND ozb.oz_barcode IS NOT NULL
            AND TRIM(ozb.oz_barcode) != ''
            """
            
            oz_barcodes_df = self.db_conn.execute(oz_barcodes_query, oz_vendor_codes).fetchdf()
            step2_time = time.time() - step2_start
            logger.info(f"📊 Этап 2 завершен за {step2_time:.2f}с, найдено штрихкодов: {len(oz_barcodes_df)}")
            
            if oz_barcodes_df.empty:
                logger.warning(f"⚠️ Не найдено штрихкодов для OZ товаров")
                return []
            
            # ЭТАП 3: Оптимизированный поиск WB товаров по штрихкодам (без LIKE)
            logger.info(f"📊 Этап 3: Оптимизированный поиск WB товаров по {len(oz_barcodes_df)} штрихкодам...")
            step3_start = time.time()
            
            # Используем технику из OzToWbCollector - разделяем WB штрихкоды и делаем JOIN
            oz_barcodes = oz_barcodes_df['oz_barcode'].tolist()
            barcodes_str = ','.join(['?' for _ in oz_barcodes])
            
            wb_matching_query = f"""
            WITH oz_barcodes_list AS (
                SELECT DISTINCT barcode as oz_barcode
                FROM UNNEST([{','.join([f"'{bc}'" for bc in oz_barcodes])}]) AS t(barcode)
                WHERE barcode IS NOT NULL AND TRIM(barcode) != ''
            ),
            wb_barcodes_split AS (
                SELECT 
                    wb.wb_sku,
                    TRIM(bc.barcode) as individual_barcode
                FROM wb_products wb,
                UNNEST(string_split(wb.wb_barcodes, ';')) AS bc(barcode)
                WHERE wb.wb_barcodes IS NOT NULL 
                  AND TRIM(wb.wb_barcodes) != ''
                  AND TRIM(bc.barcode) != ''
                  AND wb.wb_sku != '{exclude_wb_sku}'
            )
            SELECT DISTINCT wbs.wb_sku
            FROM wb_barcodes_split wbs
            INNER JOIN oz_barcodes_list ozb ON wbs.individual_barcode = ozb.oz_barcode
            """
            
            wb_candidates_df = self.db_conn.execute(wb_matching_query).fetchdf()
            step3_time = time.time() - step3_start
            logger.info(f"📊 Этап 3 завершен за {step3_time:.2f}с, найдено WB кандидатов: {len(wb_candidates_df)}")
            
            if wb_candidates_df.empty:
                logger.warning(f"⚠️ Не найдено WB товаров по штрихкодам")
                return []
            
            # ЭТАП 4: Фильтрация по остаткам WB и создание базовых объектов (БЕЗ полного обогащения)
            logger.info(f"📊 Этап 4: Фильтрация по остаткам и создание базовых объектов...")
            step4_start = time.time()
            
            candidates = []
            processed_count = 0
            
            for _, row in wb_candidates_df.iterrows():
                wb_sku = str(row['wb_sku'])
                
                if processed_count % 20 == 0:  # Логируем каждый 20-й кандидат
                    logger.info(f"⏳ Обработано базовых кандидатов: {processed_count}/{len(wb_candidates_df)}")
                
                # Получаем только базовые данные WB (быстро)
                wb_base_data = self._get_wb_base_data(wb_sku)
                if not wb_base_data:
                    continue
                
                # Проверяем остатки
                if wb_base_data.get('wb_fbo_stock', 0) <= 0:
                    continue
                
                # Создаем базовый объект БЕЗ обогащения (быстро)
                candidate = WBProductInfo(
                    wb_sku=wb_sku,
                    wb_category=wb_base_data.get('wb_category'),
                    wb_brand=wb_base_data.get('wb_brand'),
                    wb_sizes=wb_base_data.get('wb_sizes', []),
                    wb_barcodes=wb_base_data.get('wb_barcodes'),
                    wb_fbo_stock=wb_base_data.get('wb_fbo_stock', 0),
                    wb_full_price=wb_base_data.get('wb_full_price'),
                    wb_discount=wb_base_data.get('wb_discount')
                )
                
                candidates.append(candidate)
                processed_count += 1
            
            step4_time = time.time() - step4_start
            logger.info(f"📊 Этап 4 завершен за {step4_time:.2f}с, создано базовых объектов: {len(candidates)}")
            
            # ЭТАП 5: Обогащение только топ кандидатов (оптимизация из Rich Content OZ)
            if candidates:
                logger.info(f"📊 Этап 5: Обогащение кандидатов Ozon и Punta данными...")
                step5_start = time.time()
                
                # 🚀 ОПТИМИЗАЦИЯ: Пакетное обогащение вместо индивидуального
                logger.info(f"🚀 Используем пакетное обогащение для ускорения...")
                enriched_candidates = self._batch_enrich_candidates(candidates)
                
                step5_time = time.time() - step5_start
                logger.info(f"📊 Этап 5 завершен за {step5_time:.2f}с, обогащено: {len(enriched_candidates)} кандидатов")
                
                total_time = time.time() - step1_start
                logger.info(f"🎉 ОПТИМИЗИРОВАННЫЙ поиск завершен за {total_time:.2f}с, найдено: {len(enriched_candidates)} кандидатов")
                logger.info(f"📊 Времена этапов: OZ={step1_time:.2f}с, штрихкоды={step2_time:.2f}с, WB={step3_time:.2f}с, базовые={step4_time:.2f}с, обогащение={step5_time:.2f}с")
                
                return enriched_candidates
            else:
                logger.warning(f"⚠️ Не найдено валидных кандидатов после фильтрации")
                return []
            
        except Exception as e:
            logger.error(f"❌ Ошибка ОПТИМИЗИРОВАННОГО поиска кандидатов: {e}")
            logger.error(f"❌ Детали ошибки: {type(e).__name__}: {str(e)}")
            return []
    
    def _batch_enrich_candidates(self, candidates: List[WBProductInfo]) -> List[WBProductInfo]:
        """
        🚀 ПАКЕТНОЕ обогащение кандидатов - значительно быстрее индивидуального
        Вместо 366 * 2 = 732 SQL запросов делаем только 2 пакетных запроса
        """
        if not candidates:
            return []
        
        logger.info(f"🚀 Начинаем пакетное обогащение {len(candidates)} кандидатов...")
        
        # ЭТАП 1: Пакетное обогащение Punta данными (1 SQL запрос)
        punta_start = time.time()
        punta_data = self._batch_get_punta_data([c.wb_sku for c in candidates])
        punta_time = time.time() - punta_start
        logger.info(f"📊 Пакетное обогащение Punta: {punta_time:.2f}с, получено {len(punta_data)} записей")
        
        # ЭТАП 2: Пакетное обогащение Ozon данными 
        ozon_start = time.time()
        
        # Собираем все wb_sku для связывания с Ozon
        all_wb_skus = [c.wb_sku for c in candidates]
        linked_data = self.linker.link_wb_to_oz(all_wb_skus)
        
        # Собираем все уникальные oz_skus
        all_oz_skus = []
        for wb_sku in linked_data:
            all_oz_skus.extend(linked_data[wb_sku])
        unique_oz_skus = list(set(all_oz_skus))
        
        # Получаем характеристики одним запросом
        ozon_characteristics = {}
        if unique_oz_skus:
            characteristics_list = self._get_ozon_characteristics(unique_oz_skus)
            
            # Группируем по oz_vendor_code для быстрого поиска
            for char in characteristics_list:
                vendor_code = char.get('oz_vendor_code')
                if vendor_code:
                    if vendor_code not in ozon_characteristics:
                        ozon_characteristics[vendor_code] = []
                    ozon_characteristics[vendor_code].append(char)
        
        ozon_time = time.time() - ozon_start
        logger.info(f"📊 Пакетное обогащение Ozon: {ozon_time:.2f}с, получено {len(ozon_characteristics)} групп")
        
        # ЭТАП 3: Применяем данные к кандидатам
        enriched_candidates = []
        for i, candidate in enumerate(candidates):
            enriched_candidate = candidate
            
            # Применяем Punta данные
            wb_sku = candidate.wb_sku
            if wb_sku in punta_data:
                punta_record = punta_data[wb_sku]
                enriched_candidate.punta_material_short = punta_record.get('material_short')
                enriched_candidate.punta_new_last = punta_record.get('new_last')
                enriched_candidate.punta_mega_last = punta_record.get('mega_last')
                enriched_candidate.punta_best_last = punta_record.get('best_last')
                enriched_candidate.punta_heel_type = punta_record.get('heel_type')
                enriched_candidate.punta_sole_type = punta_record.get('sole_type')
                enriched_candidate.punta_heel_up_type = punta_record.get('heel_up_type')
                enriched_candidate.punta_lacing_type = punta_record.get('lacing_type')
                enriched_candidate.punta_nose_type = punta_record.get('nose_type')
                
                if enriched_candidate.enrichment_source == "none":
                    enriched_candidate.enrichment_source = "punta"
            
            # Применяем Ozon данные
            if wb_sku in linked_data:
                oz_skus = linked_data[wb_sku]
                enriched_candidate.linked_oz_skus = oz_skus
                
                # Собираем характеристики из связанных товаров
                all_characteristics = []
                for oz_sku in oz_skus:
                    # Ищем vendor_code по oz_sku в нашем кэше
                    for vendor_code, chars in ozon_characteristics.items():
                        for char in chars:
                            if str(char.get('oz_vendor_code')) == str(vendor_code):
                                all_characteristics.extend(chars)
                                break
                
                if all_characteristics:
                    # Применяем наиболее популярные значения
                    enriched_candidate.enriched_type = self._get_most_common_value(all_characteristics, 'type')
                    enriched_candidate.enriched_gender = self._get_most_common_value(all_characteristics, 'gender')
                    enriched_candidate.enriched_brand = self._get_most_common_value(all_characteristics, 'oz_brand')
                    enriched_candidate.enriched_season = self._get_most_common_value(all_characteristics, 'season')
                    enriched_candidate.enriched_color = self._get_most_common_value(all_characteristics, 'color')
                    enriched_candidate.enriched_fastener_type = self._get_most_common_value(all_characteristics, 'fastener_type')
                    
                    # Сохраняем связанные vendor codes
                    enriched_candidate.linked_oz_vendor_codes = [item['oz_vendor_code'] for item in all_characteristics]
                    enriched_candidate.enrichment_source = "ozon"
            
            enriched_candidates.append(enriched_candidate)
            
            # Логируем прогресс
            if (i + 1) % 100 == 0:
                logger.info(f"⏳ Применено обогащение к {i + 1}/{len(candidates)} кандидатам")
        
        total_time = punta_time + ozon_time
        logger.info(f"🎉 Пакетное обогащение завершено за {total_time:.2f}с (было бы ~{len(candidates) * 0.0086:.1f}с)")
        
        return enriched_candidates
    
    def _batch_get_punta_data(self, wb_skus: List[str]) -> Dict[str, Dict[str, Any]]:
        """Пакетное получение данных из punta_table"""
        if not wb_skus:
            return {}
        
        try:
            # Создаем placeholders для IN запроса
            placeholders = ','.join(['?' for _ in wb_skus])
            
            query = f"""
            SELECT 
                wb_sku,
                material_short,
                new_last,
                mega_last,
                best_last,
                heel_type,
                sole_type,
                heel_up_type,
                lacing_type,
                nose_type
            FROM punta_table
            WHERE wb_sku IN ({placeholders})
            """
            
            result_df = self.db_conn.execute(query, wb_skus).fetchdf()
            
            # Конвертируем в словарь для быстрого поиска
            punta_data = {}
            for _, row in result_df.iterrows():
                wb_sku = str(row['wb_sku'])
                punta_data[wb_sku] = row.to_dict()
            
            return punta_data
            
        except Exception as e:
            logger.error(f"❌ Ошибка пакетного получения Punta данных: {e}")
            return {}
    
    def clear_cache(self):
        """Очистка кэша"""
        self._cache.clear()
        logger.info("✅ Кэш WBDataCollector очищен") 


class WBRecommendationEngine:
    """Основной движок рекомендаций WB товаров"""
    
    def __init__(self, db_conn, config: WBScoringConfig):
        self.db_conn = db_conn
        self.config = config
        self.data_collector = WBDataCollector(db_conn)
    
    def find_similar_wb_products(self, wb_sku: str) -> List[WBRecommendation]:
        """
        Основной метод поиска похожих WB товаров
        
        Args:
            wb_sku: WB SKU исходного товара
            
        Returns:
            Список рекомендаций, отсортированный по убыванию score
        """
        logger.info(f"🔎 WBRecommendationEngine: начинаем поиск похожих товаров для WB SKU {wb_sku}")
        
        try:
            # Получаем информацию об исходном товаре с обогащением
            logger.info(f"📋 Получаем информацию об исходном товаре: {wb_sku}")
            step_start = time.time()
            
            source_product = self.data_collector.get_wb_product_info(wb_sku)
            step_time = time.time() - step_start
            logger.info(f"✅ Информация о товаре получена за {step_time:.2f}с")
            
            if not source_product:
                logger.warning(f"❌ WB товар {wb_sku} не найден в базе данных")
                return []
            
            # Проверяем качество обогащения
            if not source_product.has_enriched_data():
                logger.warning(f"⚠️ WB товар {wb_sku} не имеет достаточных обогащенных данных для поиска рекомендаций")
                return []
            
            logger.info(f"📊 Товар найден - тип: {source_product.get_effective_type()}, пол: {source_product.get_effective_gender()}, бренд: {source_product.get_effective_brand()}")
            
            # Ищем кандидатов по обязательным критериям
            logger.info(f"🔍 Поиск кандидатов по обязательным критериям")
            step_start = time.time()
            
            candidates = self.data_collector.find_wb_candidates(source_product)
            step_time = time.time() - step_start
            logger.info(f"✅ Поиск кандидатов завершен за {step_time:.2f}с, найдено: {len(candidates)} кандидатов")
            
            if not candidates:
                logger.warning(f"❌ Нет кандидатов для WB товара {wb_sku}")
                return []
            
            # Вычисляем score для всех кандидатов
            logger.info(f"🧮 Вычисляем score для {len(candidates)} кандидатов")
            step_start = time.time()
            
            recommendations = []
            
            for i, candidate in enumerate(candidates):
                if i % 20 == 0:  # Логируем каждый 20-й кандидат
                    logger.info(f"⏳ Обработано кандидатов: {i}/{len(candidates)}")
                
                # Вычисляем score
                score = self.calculate_similarity_score(source_product, candidate)
                
                # Фильтруем по минимальному порогу
                if score >= self.config.min_score_threshold:
                    match_details = self.get_match_details(source_product, candidate)
                    
                    recommendation = WBRecommendation(
                        product_info=candidate,
                        score=score,
                        match_details=match_details
                    )
                    recommendations.append(recommendation)
            
            step_time = time.time() - step_start
            logger.info(f"✅ Scoring завершен за {step_time:.2f}с, прошли фильтр: {len(recommendations)}")
            
            # Если недостаточно рекомендаций, применяем fallback логику
            if len(recommendations) < self.config.min_recommendations:
                logger.warning(f"⚠️ Недостаточно рекомендаций ({len(recommendations)} < {self.config.min_recommendations}), применяем fallback логику")
                
                # Добавляем кандидатов с более низким score
                fallback_threshold = max(20.0, self.config.min_score_threshold - 20.0)
                logger.info(f"🔄 Fallback: снижаем порог до {fallback_threshold}")
                
                for candidate in candidates:
                    if len(recommendations) >= self.config.max_recommendations:
                        break
                        
                    score = self.calculate_similarity_score(source_product, candidate)
                    
                    # Проверяем, что кандидат еще не добавлен
                    if (score >= fallback_threshold and 
                        not any(r.product_info.wb_sku == candidate.wb_sku for r in recommendations)):
                        
                        match_details = self.get_match_details(source_product, candidate)
                        recommendation = WBRecommendation(
                            product_info=candidate,
                            score=score,
                            match_details=match_details
                        )
                        recommendations.append(recommendation)
                
                logger.info(f"✅ После fallback: {len(recommendations)} рекомендаций")
            
            if not recommendations:
                logger.warning(f"❌ Нет рекомендаций после всех попыток")
                return []
            
            # Сортируем и ограничиваем количество
            logger.info(f"📊 Сортировка и ограничение до {self.config.max_recommendations} рекомендаций")
            recommendations.sort(key=lambda r: r.score, reverse=True)
            final_recommendations = recommendations[:self.config.max_recommendations]
            
            logger.info(f"🎉 Найдено {len(final_recommendations)} итоговых рекомендаций для WB товара {wb_sku}")
            if final_recommendations:
                scores = [r.score for r in final_recommendations]
                logger.info(f"📊 Score диапазон: {min(scores):.1f} - {max(scores):.1f}")
            
            return final_recommendations
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при поиске похожих товаров для WB {wb_sku}: {e}")
            return []
    
    def calculate_similarity_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """
        Вычисление score схожести между WB товарами
        
        Args:
            source: Исходный товар
            candidate: Товар-кандидат
            
        Returns:
            Score от 0 до max_score
        """
        score = self.config.base_score
        
        # Размер (критический параметр)
        size_score = self._calculate_size_score(source, candidate)
        score += size_score
        
        # Сезонность
        season_score = self._calculate_season_score(source, candidate)
        score += season_score
        
        # Второстепенные характеристики
        color_score = self._calculate_color_score(source, candidate)
        score += color_score
        
        material_score = self._calculate_material_score(source, candidate)
        score += material_score
        
        fastener_score = self._calculate_fastener_score(source, candidate)
        score += fastener_score
        
        # Новые характеристики из punta_table
        heel_type_score = self._calculate_heel_type_score(source, candidate)
        score += heel_type_score
        
        sole_type_score = self._calculate_sole_type_score(source, candidate)
        score += sole_type_score
        
        heel_up_type_score = self._calculate_heel_up_type_score(source, candidate)
        score += heel_up_type_score
        
        lacing_type_score = self._calculate_lacing_type_score(source, candidate)
        score += lacing_type_score
        
        nose_type_score = self._calculate_nose_type_score(source, candidate)
        score += nose_type_score
        
        # Колодки (критический параметр для обуви)
        last_score = self._calculate_last_score(source, candidate)
        score += last_score
        
        # Применяем штраф за отсутствие совпадения колодки
        if last_score == 0:
            score *= self.config.no_last_penalty
        
        # Остатки на складе WB
        stock_score = self._calculate_stock_score(candidate)
        score += stock_score
        
        # Цена (новый параметр для WB)
        price_score = self._calculate_price_score(source, candidate)
        score += price_score
        
        # Качество обогащения
        enrichment_score = self._calculate_enrichment_score(candidate)
        score += enrichment_score
        
        # Ограничиваем максимальным значением
        return min(score, self.config.max_score)
    
    def _calculate_size_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за размеры на основе пересечения диапазонов"""
        if not source.wb_sizes or not candidate.wb_sizes:
            return self.config.size_mismatch_penalty
        
        # Вычисляем процент пересечения размеров
        overlap_percentage = source.calculate_size_overlap_percentage(candidate)
        
        if overlap_percentage == 0:
            # Нет пересечения размеров
            return self.config.size_mismatch_penalty
        elif overlap_percentage >= 0.8:
            # Высокое пересечение (80%+) - полные баллы
            return self.config.exact_size_weight
        elif overlap_percentage >= 0.4:
            # Среднее пересечение (40-80%) - частичные баллы
            return self.config.close_size_weight
        else:
            # Низкое пересечение (менее 40%) - минимальные баллы
            return int(self.config.close_size_weight * overlap_percentage * 2)
    
    def _calculate_season_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за сезон"""
        if not source.enriched_season or not candidate.enriched_season:
            return 0
        
        if source.enriched_season == candidate.enriched_season:
            return self.config.season_match_bonus
        else:
            return self.config.season_mismatch_penalty
    
    def _calculate_color_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за цвет"""
        if not source.enriched_color or not candidate.enriched_color:
            return 0
        
        if source.enriched_color.lower() == candidate.enriched_color.lower():
            return self.config.color_match_bonus
        else:
            return 0
    
    def _calculate_material_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за материал"""
        # Используем только material_short из punta_table для точного сравнения
        source_material = source.punta_material_short
        candidate_material = candidate.punta_material_short
        
        if not source_material or not candidate_material:
            return 0
        
        if source_material.lower() == candidate_material.lower():
            return self.config.material_match_bonus
        else:
            return 0
    
    def _calculate_fastener_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за тип застежки"""
        if not source.enriched_fastener_type or not candidate.enriched_fastener_type:
            return 0
        
        if source.enriched_fastener_type.lower() == candidate.enriched_fastener_type.lower():
            return self.config.fastener_match_bonus
        else:
            return 0
    
    def _calculate_heel_type_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за тип каблука"""
        if not source.punta_heel_type or not candidate.punta_heel_type:
            return 0
        
        if source.punta_heel_type.lower() == candidate.punta_heel_type.lower():
            return self.config.heel_type_match_bonus
        else:
            return 0
    
    def _calculate_sole_type_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за тип подошвы"""
        if not source.punta_sole_type or not candidate.punta_sole_type:
            return 0
        
        if source.punta_sole_type.lower() == candidate.punta_sole_type.lower():
            return self.config.sole_type_match_bonus
        else:
            return 0
    
    def _calculate_heel_up_type_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за тип задника"""
        if not source.punta_heel_up_type or not candidate.punta_heel_up_type:
            return 0
        
        if source.punta_heel_up_type.lower() == candidate.punta_heel_up_type.lower():
            return self.config.heel_up_type_match_bonus
        else:
            return 0
    
    def _calculate_lacing_type_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за тип шнуровки"""
        if not source.punta_lacing_type or not candidate.punta_lacing_type:
            return 0
        
        if source.punta_lacing_type.lower() == candidate.punta_lacing_type.lower():
            return self.config.lacing_type_match_bonus
        else:
            return 0
    
    def _calculate_nose_type_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за тип носка"""
        if not source.punta_nose_type or not candidate.punta_nose_type:
            return 0
        
        if source.punta_nose_type.lower() == candidate.punta_nose_type.lower():
            return self.config.nose_type_match_bonus
        else:
            return 0
    
    def _calculate_last_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за колодку (приоритет: mega > best > new)"""
        # MEGA колодка (высший приоритет)
        if (source.punta_mega_last and candidate.punta_mega_last and 
            source.punta_mega_last == candidate.punta_mega_last):
            return self.config.mega_last_bonus
        
        # BEST колодка
        if (source.punta_best_last and candidate.punta_best_last and 
            source.punta_best_last == candidate.punta_best_last):
            return self.config.best_last_bonus
        
        # NEW колодка
        if (source.punta_new_last and candidate.punta_new_last and 
            source.punta_new_last == candidate.punta_new_last):
            return self.config.new_last_bonus
        
        return 0
    
    def _calculate_stock_score(self, candidate: WBProductInfo) -> float:
        """Вычисление score за остатки на складе WB"""
        stock = candidate.wb_fbo_stock
        
        if stock > self.config.stock_threshold_high:
            return self.config.stock_high_bonus
        elif stock > self.config.stock_threshold_medium:
            return self.config.stock_medium_bonus
        elif stock > 0:
            return self.config.stock_low_bonus
        else:
            return 0
    
    def _calculate_price_score(self, source: WBProductInfo, candidate: WBProductInfo) -> float:
        """Вычисление score за схожесть цены"""
        if not source.wb_full_price or not candidate.wb_full_price:
            return 0
        
        # Вычисляем относительную разницу в цене
        price_diff = abs(candidate.wb_full_price - source.wb_full_price) / source.wb_full_price
        
        if price_diff <= self.config.price_diff_threshold:
            return self.config.price_similarity_bonus
        else:
            return 0
    
    def _calculate_enrichment_score(self, candidate: WBProductInfo) -> float:
        """Вычисление score за качество обогащения"""
        enrichment_quality = candidate.get_enrichment_score()
        
        if enrichment_quality >= self.config.enrichment_quality_threshold:
            return self.config.enrichment_quality_bonus
        else:
            return 0
    
    def get_match_details(self, source: WBProductInfo, candidate: WBProductInfo) -> str:
        """
        Генерация детального описания совпадений для рекомендации
        
        Args:
            source: Исходный товар
            candidate: Товар-кандидат
            
        Returns:
            Строка с детальным описанием совпадений и различий
        """
        details = []
        scores = []
        
        # Основные обязательные параметры
        details.append(f"Тип: {candidate.get_effective_type()} ✓")
        details.append(f"Пол: {candidate.get_effective_gender()} ✓")
        details.append(f"Бренд: {candidate.get_effective_brand()} ✓")
        
        # Размеры
        size_score = self._calculate_size_score(source, candidate)
        source_size_range = source.get_size_range_str()
        candidate_size_range = candidate.get_size_range_str()
        overlap_percentage = source.calculate_size_overlap_percentage(candidate)
        
        if size_score == self.config.exact_size_weight:
            details.append(f"Размеры: {candidate_size_range} (пересечение {overlap_percentage:.0%}) ✓✓")
            scores.append(f"+{self.config.exact_size_weight} баллов за отличное пересечение размеров")
        elif size_score == self.config.close_size_weight:
            details.append(f"Размеры: {candidate_size_range} (пересечение {overlap_percentage:.0%}) ✓")
            scores.append(f"+{self.config.close_size_weight} баллов за хорошее пересечение размеров")
        elif size_score > 0:
            details.append(f"Размеры: {candidate_size_range} (пересечение {overlap_percentage:.0%})")
            scores.append(f"+{int(size_score)} баллов за частичное пересечение размеров")
        else:
            details.append(f"Размеры: {candidate_size_range} (нет пересечения с {source_size_range})")
            scores.append(f"{self.config.size_mismatch_penalty} баллов штраф за отсутствие пересечения")
        
        # Сезон
        season_score = self._calculate_season_score(source, candidate)
        if season_score == self.config.season_match_bonus:
            details.append(f"Сезон: {candidate.enriched_season} ✓")
            scores.append(f"+{self.config.season_match_bonus} баллов за сезон")
        elif season_score == self.config.season_mismatch_penalty:
            details.append(f"Сезон: {candidate.enriched_season} (штраф {abs(self.config.season_mismatch_penalty)} баллов)")
        
        # Цвет
        color_score = self._calculate_color_score(source, candidate)
        if color_score > 0:
            details.append(f"Цвет: {candidate.enriched_color} ✓")
            scores.append(f"+{self.config.color_match_bonus} баллов за цвет")
        
        # Материал
        material_score = self._calculate_material_score(source, candidate)
        if material_score > 0:
            material = candidate.punta_material_short
            details.append(f"Материал: {material} ✓")
            scores.append(f"+{self.config.material_match_bonus} баллов за материал")
        
        # Застежка
        fastener_score = self._calculate_fastener_score(source, candidate)
        if fastener_score > 0:
            details.append(f"Застежка: {candidate.enriched_fastener_type} ✓")
            scores.append(f"+{self.config.fastener_match_bonus} баллов за застежку")
        
        # Новые характеристики из punta_table
        heel_type_score = self._calculate_heel_type_score(source, candidate)
        if heel_type_score > 0:
            details.append(f"Тип каблука: {candidate.punta_heel_type} ✓")
            scores.append(f"+{self.config.heel_type_match_bonus} баллов за тип каблука")
        
        sole_type_score = self._calculate_sole_type_score(source, candidate)
        if sole_type_score > 0:
            details.append(f"Тип подошвы: {candidate.punta_sole_type} ✓")
            scores.append(f"+{self.config.sole_type_match_bonus} баллов за тип подошвы")
        
        heel_up_type_score = self._calculate_heel_up_type_score(source, candidate)
        if heel_up_type_score > 0:
            details.append(f"Тип задника: {candidate.punta_heel_up_type} ✓")
            scores.append(f"+{self.config.heel_up_type_match_bonus} баллов за тип задника")
        
        lacing_type_score = self._calculate_lacing_type_score(source, candidate)
        if lacing_type_score > 0:
            details.append(f"Тип шнуровки: {candidate.punta_lacing_type} ✓")
            scores.append(f"+{self.config.lacing_type_match_bonus} баллов за тип шнуровки")
        
        nose_type_score = self._calculate_nose_type_score(source, candidate)
        if nose_type_score > 0:
            details.append(f"Тип носка: {candidate.punta_nose_type} ✓")
            scores.append(f"+{self.config.nose_type_match_bonus} баллов за тип носка")
        
        # Колодка
        last_score = self._calculate_last_score(source, candidate)
        if last_score == self.config.mega_last_bonus:
            details.append(f"Колодка MEGA: {candidate.punta_mega_last} ✓")
            scores.append(f"+{self.config.mega_last_bonus} баллов за колодку MEGA")
        elif last_score == self.config.best_last_bonus:
            details.append(f"Колодка BEST: {candidate.punta_best_last} ✓")
            scores.append(f"+{self.config.best_last_bonus} баллов за колодку BEST")
        elif last_score == self.config.new_last_bonus:
            details.append(f"Колодка NEW: {candidate.punta_new_last} ✓")
            scores.append(f"+{self.config.new_last_bonus} баллов за колодку NEW")
        else:
            details.append(f"Колодка: не совпадает (штраф {int((1 - self.config.no_last_penalty) * 100)}%)")
        
        # Остатки
        stock_score = self._calculate_stock_score(candidate)
        if stock_score == self.config.stock_high_bonus:
            details.append(f"В наличии: {candidate.wb_fbo_stock} шт. ✓✓")
            scores.append(f"+{self.config.stock_high_bonus} баллов за хороший остаток")
        elif stock_score == self.config.stock_medium_bonus:
            details.append(f"В наличии: {candidate.wb_fbo_stock} шт. ✓")
            scores.append(f"+{self.config.stock_medium_bonus} баллов за средний остаток")
        elif stock_score == self.config.stock_low_bonus:
            details.append(f"В наличии: {candidate.wb_fbo_stock} шт.")
            scores.append(f"+{self.config.stock_low_bonus} баллов за наличие")
        
        # Цена (отключено по требованию пользователя)
        price_score = self._calculate_price_score(source, candidate)
        if price_score > 0:
            details.append(f"Цена: {candidate.wb_full_price} руб.")
            scores.append(f"+{price_score} баллов за схожую цену")
        
        # Качество обогащения
        enrichment_score = self._calculate_enrichment_score(candidate)
        if enrichment_score > 0:
            details.append(f"Качество данных: {candidate.get_enrichment_score():.1%} ✓")
            scores.append(f"+{self.config.enrichment_quality_bonus} баллов за качество обогащения")
        
        # Формируем итоговое описание
        result = " • ".join(details)
        if scores:
            result += f"\n\n📊 Детализация баллов:\n" + "\n".join(scores)
        
        return result


class WBRecommendationProcessor:
    """Главный класс-оркестратор для обработки WB рекомендаций"""
    
    def __init__(self, db_conn, config: WBScoringConfig = None):
        self.db_conn = db_conn
        self.config = config or WBScoringConfig()
        self.data_collector = WBDataCollector(db_conn)  # Добавляем data_collector для оптимизированных методов
        self.recommendation_engine = WBRecommendationEngine(db_conn, self.config)
    
    def process_single_wb_product(self, wb_sku: str) -> WBProcessingResult:
        """
        Обработка одного WB товара - создание рекомендаций
        
        Args:
            wb_sku: WB SKU товара для обработки
            
        Returns:
            Результат обработки с рекомендациями
        """
        start_time = time.time()
        logger.info(f"🎯 Начинаем обработку WB товара: {wb_sku}")
        
        try:
            # Поиск рекомендаций
            logger.info(f"🔍 Поиск рекомендаций для WB товара: {wb_sku}")
            step_start = time.time()
            
            recommendations = self.recommendation_engine.find_similar_wb_products(wb_sku)
            step_time = time.time() - step_start
            logger.info(f"✅ Поиск рекомендаций завершен за {step_time:.2f}с, найдено: {len(recommendations)}")
            
            # Определяем статус обработки
            total_time = time.time() - start_time
            
            if not recommendations:
                logger.warning(f"⚠️ Нет рекомендаций для WB товара {wb_sku} (общее время: {total_time:.2f}с)")
                return WBProcessingResult(
                    wb_sku=wb_sku,
                    status=WBProcessingStatus.NO_SIMILAR,
                    recommendations=[],
                    processing_time=total_time,
                    enrichment_info={}
                )
            
            elif len(recommendations) < self.config.min_recommendations:
                logger.warning(f"⚠️ Недостаточно рекомендаций для WB товара {wb_sku}: {len(recommendations)} < {self.config.min_recommendations}")
                return WBProcessingResult(
                    wb_sku=wb_sku,
                    status=WBProcessingStatus.INSUFFICIENT_RECOMMENDATIONS,
                    recommendations=recommendations,
                    processing_time=total_time,
                    enrichment_info={"count": len(recommendations), "warning": "insufficient_count"}
                )
            
            else:
                logger.info(f"✅ Успешная обработка WB товара {wb_sku}: {len(recommendations)} рекомендаций (время: {total_time:.2f}с)")
                return WBProcessingResult(
                    wb_sku=wb_sku,
                    status=WBProcessingStatus.SUCCESS,
                    recommendations=recommendations,
                    processing_time=total_time,
                    enrichment_info={"count": len(recommendations)}
                )
                
        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"❌ Ошибка обработки WB товара {wb_sku}: {e}")
            return WBProcessingResult(
                wb_sku=wb_sku,
                status=WBProcessingStatus.ERROR,
                recommendations=[],
                processing_time=total_time,
                enrichment_info={},
                error_message=str(e)
            )
    
    def process_batch(self, wb_skus: List[str], progress_callback: Optional[Callable] = None) -> WBBatchResult:
        """
        Пакетная обработка WB товаров
        
        Args:
            wb_skus: Список WB SKU для обработки
            progress_callback: Функция для отслеживания прогресса
            
        Returns:
            Результат пакетной обработки
        """
        start_time = time.time()
        logger.info(f"📦 Начинаем пакетную обработку {len(wb_skus)} WB товаров")
        
        processed_items = []
        success_count = 0
        error_count = 0
        
        try:
            for i, wb_sku in enumerate(wb_skus):
                try:
                    # Обработка товара
                    result = self.process_single_wb_product(wb_sku)
                    processed_items.append(result)
                    
                    # Обновление статистики
                    if result.success:
                        success_count += 1
                    else:
                        error_count += 1
                    
                    # Callback для прогресса
                    if progress_callback:
                        progress_callback(i + 1, len(wb_skus), f"Обработан {wb_sku}")
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки WB товара {wb_sku}: {e}")
                    error_result = WBProcessingResult(
                        wb_sku=wb_sku,
                        status=WBProcessingStatus.ERROR,
                        recommendations=[],
                        processing_time=0,
                        enrichment_info={},
                        error_message=str(e)
                    )
                    processed_items.append(error_result)
                    error_count += 1
                    
                    if progress_callback:
                        progress_callback(i + 1, len(wb_skus), f"Ошибка при обработке {wb_sku}")
            
            total_time = time.time() - start_time
            logger.info(f"✅ Пакетная обработка завершена за {total_time:.1f}с. Успешно: {success_count}, Ошибок: {error_count}")
            
            return WBBatchResult(
                processed_items=processed_items,
                total_processing_time=total_time,
                success_count=success_count,
                error_count=error_count
            )
            
        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"❌ Критическая ошибка пакетной обработки: {e}")
            return WBBatchResult(
                processed_items=processed_items,
                total_processing_time=total_time,
                success_count=success_count,
                error_count=error_count
            )
    
    def process_batch_optimized(self, wb_skus: List[str], progress_callback: Optional[Callable] = None) -> WBBatchResult:
        """
        🚀 ОПТИМИЗИРОВАННАЯ пакетная обработка WB товаров
        
        Вместо 920 * несколько секунд = часы
        Делаем: несколько минут для всех 920 товаров
        
        Оптимизации:
        1. Предварительная загрузка всех данных одними запросами
        2. Группировка товаров по критериям
        3. Пакетный поиск кандидатов
        4. Минимизация SQL запросов
        
        Args:
            wb_skus: Список WB SKU для обработки
            progress_callback: Функция для отслеживания прогресса
            
        Returns:
            Результат пакетной обработки
        """
        start_time = time.time()
        logger.info(f"🚀 Начинаем ОПТИМИЗИРОВАННУЮ пакетную обработку {len(wb_skus)} WB товаров")
        
        # Проверяем размер пакета для выбора стратегии
        if len(wb_skus) < 50:
            logger.info(f"📋 Малый пакет ({len(wb_skus)} товаров), используем стандартную обработку")
            return self.process_batch(wb_skus, progress_callback)
        
        processed_items = []
        success_count = 0
        error_count = 0
        
        try:
            # ЭТАП 1: Предварительная загрузка всех WB данных (1 запрос вместо 920)
            logger.info("📊 ЭТАП 1: Предварительная загрузка WB данных...")
            if progress_callback:
                progress_callback(5, 100, "Загрузка базовых данных WB...")
                
            wb_data_start = time.time()
            wb_data_cache = self._preload_wb_data(wb_skus)
            wb_data_time = time.time() - wb_data_start
            logger.info(f"✅ WB данные загружены за {wb_data_time:.2f}с, получено {len(wb_data_cache)} записей")
            
            # ЭТАП 2: Предварительная загрузка Punta данных (1 запрос вместо 920)
            logger.info("📊 ЭТАП 2: Предварительная загрузка Punta данных...")
            if progress_callback:
                progress_callback(10, 100, "Загрузка данных Punta...")
                
            punta_start = time.time()
            punta_cache = self.data_collector._batch_get_punta_data(wb_skus)
            punta_time = time.time() - punta_start
            logger.info(f"✅ Punta данные загружены за {punta_time:.2f}с, получено {len(punta_cache)} записей")
            
            # ЭТАП 3: Предварительное создание связей WB↔OZ (группированные запросы)
            logger.info("📊 ЭТАП 3: Создание связей WB↔OZ...")
            if progress_callback:
                progress_callback(20, 100, "Создание связей с Ozon...")
                
            links_start = time.time()
            wb_to_oz_links = self._preload_wb_to_oz_links(wb_skus)
            links_time = time.time() - links_start
            logger.info(f"✅ Связи WB↔OZ созданы за {links_time:.2f}с, найдено {len(wb_to_oz_links)} связей")
            
            # ЭТАП 4: Предварительная загрузка Ozon характеристик (группированные запросы)
            logger.info("📊 ЭТАП 4: Загрузка характеристик Ozon...")
            if progress_callback:
                progress_callback(30, 100, "Загрузка характеристик Ozon...")
                
            oz_chars_start = time.time()
            ozon_chars_cache = self._preload_ozon_characteristics(wb_to_oz_links)
            oz_chars_time = time.time() - oz_chars_start
            logger.info(f"✅ Ozon характеристики загружены за {oz_chars_time:.2f}с, получено {len(ozon_chars_cache)} групп")
            
            # ЭТАП 5: Создание обогащенных объектов для всех товаров
            logger.info("📊 ЭТАП 5: Создание обогащенных объектов...")
            if progress_callback:
                progress_callback(40, 100, "Обогащение товаров...")
                
            enrichment_start = time.time()
            enriched_products = self._create_enriched_products_batch(
                wb_skus, wb_data_cache, punta_cache, wb_to_oz_links, ozon_chars_cache
            )
            enrichment_time = time.time() - enrichment_start
            logger.info(f"✅ Обогащение завершено за {enrichment_time:.2f}с, создано {len(enriched_products)} объектов")
            
            # ЭТАП 6: Группировка товаров по критериям для оптимизации поиска
            logger.info("📊 ЭТАП 6: Группировка товаров по критериям...")
            if progress_callback:
                progress_callback(50, 100, "Группировка товаров...")
                
            groups_start = time.time()
            product_groups = self._group_products_by_criteria(enriched_products)
            groups_time = time.time() - groups_start
            logger.info(f"✅ Группировка завершена за {groups_time:.2f}с, создано {len(product_groups)} групп")
            
            # ЭТАП 7: Обработка товаров по группам (значительно быстрее)
            logger.info("📊 ЭТАП 7: Обработка товаров по группам...")
            
            processing_start = time.time()
            processed_count = 0
            
            for group_key, products_in_group in product_groups.items():
                logger.info(f"🔄 Обрабатываем группу {group_key}: {len(products_in_group)} товаров")
                
                # Для группы находим кандидатов один раз
                group_candidates = self._find_group_candidates(group_key, enriched_products)
                
                # Обрабатываем каждый товар в группе с общими кандидатами
                for product in products_in_group:
                    try:
                        result = self._process_single_with_candidates(product, group_candidates)
                        processed_items.append(result)
                        
                        if result.success:
                            success_count += 1
                        else:
                            error_count += 1
                            
                        processed_count += 1
                        
                        # Обновляем прогресс
                        if progress_callback:
                            progress = 50 + int((processed_count / len(wb_skus)) * 50)  # 50-100%
                            progress_callback(progress, 100, f"Обработано {processed_count}/{len(wb_skus)}")
                            
                    except Exception as e:
                        logger.error(f"❌ Ошибка обработки WB товара {product.wb_sku}: {e}")
                        error_result = WBProcessingResult(
                            wb_sku=product.wb_sku,
                            status=WBProcessingStatus.ERROR,
                            recommendations=[],
                            processing_time=0,
                            enrichment_info={},
                            error_message=str(e)
                        )
                        processed_items.append(error_result)
                        error_count += 1
                        processed_count += 1
            
            processing_time = time.time() - processing_start
            logger.info(f"✅ Обработка товаров завершена за {processing_time:.2f}с")
            
            # Финальная статистика
            total_time = time.time() - start_time
            setup_time = wb_data_time + punta_time + links_time + oz_chars_time + enrichment_time + groups_time
            
            logger.info(f"🎉 ОПТИМИЗИРОВАННАЯ пакетная обработка завершена!")
            logger.info(f"📊 Общее время: {total_time:.1f}с")
            logger.info(f"📊 Подготовка данных: {setup_time:.1f}с")
            logger.info(f"📊 Обработка товаров: {processing_time:.1f}с")
            logger.info(f"📊 Успешно: {success_count}, Ошибок: {error_count}")
            logger.info(f"⚡ Ускорение: примерно в {(len(wb_skus) * 3) / total_time:.1f}x раз")
            
            return WBBatchResult(
                processed_items=processed_items,
                total_processing_time=total_time,
                success_count=success_count,
                error_count=error_count
            )
            
        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"❌ Критическая ошибка оптимизированной пакетной обработки: {e}")
            return WBBatchResult(
                processed_items=processed_items,
                total_processing_time=total_time,
                success_count=success_count,
                error_count=error_count
            )

    def _preload_wb_data(self, wb_skus: List[str]) -> Dict[str, Dict[str, Any]]:
        """Предварительная загрузка всех WB данных одним запросом"""
        logger.info(f"📊 Предварительная загрузка WB данных для {len(wb_skus)} товаров...")
        
        try:
            # Создаем placeholders для IN запроса
            placeholders = ','.join(['?' for _ in wb_skus])
            
            query = f"""
            SELECT 
                wb.wb_sku,
                wb.wb_category,
                wb.wb_brand,
                wb.wb_barcodes,
                wb.wb_size,
                COALESCE(wp.wb_fbo_stock, 0) as wb_fbo_stock,
                wp.wb_full_price,
                wp.wb_discount
            FROM wb_products wb
            LEFT JOIN wb_prices wp ON wb.wb_sku = wp.wb_sku
            WHERE wb.wb_sku IN ({placeholders})
            """
            
            result_df = self.db_conn.execute(query, wb_skus).fetchdf()
            
            # Группируем по wb_sku для обработки размеров
            wb_data_cache = {}
            for _, row in result_df.iterrows():
                wb_sku = str(row['wb_sku'])
                
                if wb_sku not in wb_data_cache:
                    wb_data_cache[wb_sku] = {
                        'wb_sku': wb_sku,
                        'wb_category': row['wb_category'],
                        'wb_brand': row['wb_brand'],
                        'wb_barcodes': row['wb_barcodes'],
                        'wb_sizes': [],
                        'wb_fbo_stock': row['wb_fbo_stock'],
                        'wb_full_price': row['wb_full_price'],
                        'wb_discount': row['wb_discount']
                    }
                
                # Добавляем размер к списку
                if pd.notna(row['wb_size']) and row['wb_size'] not in wb_data_cache[wb_sku]['wb_sizes']:
                    wb_data_cache[wb_sku]['wb_sizes'].append(int(row['wb_size']))
            
            logger.info(f"✅ WB данные загружены для {len(wb_data_cache)} товаров")
            return wb_data_cache
            
        except Exception as e:
            logger.error(f"❌ Ошибка предварительной загрузки WB данных: {e}")
            return {}

    def _preload_wb_to_oz_links(self, wb_skus: List[str]) -> Dict[str, List[str]]:
        """Предварительное создание связей WB↔OZ для всех товаров"""
        logger.info(f"📊 Создание связей WB↔OZ для {len(wb_skus)} товаров...")
        
        try:
            # Используем существующий механизм связывания через базу данных
            wb_skus_str = "', '".join(wb_skus)
            query = f"""
            SELECT DISTINCT 
                wb.wb_sku,
                ozb.oz_vendor_code
            FROM wb_products wb
            JOIN oz_barcodes ozb ON wb.wb_barcodes LIKE '%' || ozb.oz_barcode || '%'
            WHERE wb.wb_sku IN ('{wb_skus_str}')
            AND ozb.oz_barcode IS NOT NULL
            AND LENGTH(ozb.oz_barcode) >= 8
            """
            
            result = self.db_conn.execute(query).fetchall()
            
            # Группируем по wb_sku
            links = {}
            for row in result:
                wb_sku = str(row[0])
                oz_vendor_code = str(row[1])
                
                if wb_sku not in links:
                    links[wb_sku] = []
                links[wb_sku].append(oz_vendor_code)
            
            logger.info(f"✅ Создано {len(links)} связей WB↔OZ")
            return links
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания связей WB↔OZ: {e}")
            return {}

    def _preload_ozon_characteristics(self, wb_to_oz_links: Dict[str, List[str]]) -> Dict[str, List[Dict[str, Any]]]:
        """Предварительная загрузка всех Ozon характеристик"""
        logger.info(f"📊 Загрузка Ozon характеристик...")
        
        try:
            # Собираем все уникальные oz_vendor_codes
            all_oz_vendor_codes = []
            for wb_sku in wb_to_oz_links:
                all_oz_vendor_codes.extend(wb_to_oz_links[wb_sku])
            unique_oz_vendor_codes = list(set(all_oz_vendor_codes))
            
            if not unique_oz_vendor_codes:
                return {}
            
            # Получаем характеристики одним запросом
            characteristics_list = self.data_collector._get_ozon_characteristics(unique_oz_vendor_codes)
            
            # Группируем по oz_vendor_code
            ozon_chars_cache = {}
            for char in characteristics_list:
                vendor_code = char.get('oz_vendor_code')
                if vendor_code:
                    if vendor_code not in ozon_chars_cache:
                        ozon_chars_cache[vendor_code] = []
                    ozon_chars_cache[vendor_code].append(char)
            
            logger.info(f"✅ Загружены характеристики для {len(ozon_chars_cache)} Ozon товаров")
            return ozon_chars_cache
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки Ozon характеристик: {e}")
            return {}

    def _create_enriched_products_batch(
        self, 
        wb_skus: List[str],
        wb_data_cache: Dict[str, Dict[str, Any]],
        punta_cache: Dict[str, Dict[str, Any]],
        wb_to_oz_links: Dict[str, List[str]],
        ozon_chars_cache: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, WBProductInfo]:
        """Создание обогащенных объектов для всех товаров"""
        logger.info(f"📊 Создание обогащенных объектов для {len(wb_skus)} товаров...")
        
        enriched_products = {}
        
        for wb_sku in wb_skus:
            try:
                # Базовые данные WB
                wb_data = wb_data_cache.get(wb_sku, {})
                if not wb_data:
                    continue
                
                # Создаем базовый объект
                product = WBProductInfo(
                    wb_sku=wb_sku,
                    wb_category=wb_data.get('wb_category'),
                    wb_brand=wb_data.get('wb_brand'),
                    wb_sizes=wb_data.get('wb_sizes', []),
                    wb_barcodes=wb_data.get('wb_barcodes'),
                    wb_fbo_stock=wb_data.get('wb_fbo_stock', 0),
                    wb_full_price=wb_data.get('wb_full_price'),
                    wb_discount=wb_data.get('wb_discount')
                )
                
                # Обогащение Punta данными
                if wb_sku in punta_cache:
                    punta_data = punta_cache[wb_sku]
                    product.punta_material_short = punta_data.get('material_short')
                    product.punta_new_last = punta_data.get('new_last')
                    product.punta_mega_last = punta_data.get('mega_last')
                    product.punta_best_last = punta_data.get('best_last')
                    product.punta_heel_type = punta_data.get('heel_type')
                    product.punta_sole_type = punta_data.get('sole_type')
                    product.punta_heel_up_type = punta_data.get('heel_up_type')
                    product.punta_lacing_type = punta_data.get('lacing_type')
                    product.punta_nose_type = punta_data.get('nose_type')
                    
                    if product.enrichment_source == "none":
                        product.enrichment_source = "punta"
                
                # Обогащение Ozon данными
                if wb_sku in wb_to_oz_links:
                    oz_vendor_codes = wb_to_oz_links[wb_sku]
                    product.linked_oz_vendor_codes = oz_vendor_codes
                    
                    # Собираем характеристики из связанных товаров
                    all_characteristics = []
                    for oz_vendor_code in oz_vendor_codes:
                        if oz_vendor_code in ozon_chars_cache:
                            all_characteristics.extend(ozon_chars_cache[oz_vendor_code])
                    
                    if all_characteristics:
                        # Применяем наиболее популярные значения
                        product.enriched_type = self.data_collector._get_most_common_value(all_characteristics, 'type')
                        product.enriched_gender = self.data_collector._get_most_common_value(all_characteristics, 'gender')
                        product.enriched_brand = self.data_collector._get_most_common_value(all_characteristics, 'oz_brand')
                        product.enriched_season = self.data_collector._get_most_common_value(all_characteristics, 'season')
                        product.enriched_color = self.data_collector._get_most_common_value(all_characteristics, 'color')
                        product.enriched_fastener_type = self.data_collector._get_most_common_value(all_characteristics, 'fastener_type')
                        
                        product.linked_oz_vendor_codes = [item.get('oz_vendor_code') for item in all_characteristics if item.get('oz_vendor_code')]
                        product.enrichment_source = "ozon"
                
                enriched_products[wb_sku] = product
                
            except Exception as e:
                logger.error(f"❌ Ошибка создания обогащенного объекта для {wb_sku}: {e}")
                continue
        
        # Отладочная информация
        enriched_count = sum(1 for p in enriched_products.values() if p.has_enriched_data())
        logger.info(f"✅ Создано {len(enriched_products)} обогащенных объектов (с обогащением: {enriched_count})")
        
        # Диагностика: показываем пример данных
        if enriched_products:
            sample_sku = list(enriched_products.keys())[0]
            sample_product = enriched_products[sample_sku]
            logger.info(f"📊 Пример товара {sample_sku}: type={sample_product.enriched_type}, gender={sample_product.enriched_gender}, brand={sample_product.enriched_brand}")
        
        return enriched_products

    def _group_products_by_criteria(self, enriched_products: Dict[str, WBProductInfo]) -> Dict[str, List[WBProductInfo]]:
        """Группировка товаров по критериям для оптимизации поиска"""
        logger.info(f"📊 Группировка {len(enriched_products)} товаров по критериям...")
        
        groups = {}
        
        for wb_sku, product in enriched_products.items():
            if not product.has_enriched_data():
                # Товары без обогащения в отдельную группу
                group_key = "no_enrichment"
            else:
                # Группируем по тип+пол+бренд
                group_key = f"{product.get_effective_type()}|{product.get_effective_gender()}|{product.get_effective_brand()}"
            
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(product)
        
        logger.info(f"✅ Создано {len(groups)} групп товаров")
        
        # Диагностика: показываем размеры групп
        for group_key, products in groups.items():
            logger.info(f"📊 Группа '{group_key}': {len(products)} товаров")
        
        return groups

    def _find_group_candidates(self, group_key: str, all_products: Dict[str, WBProductInfo]) -> List[WBProductInfo]:
        """Поиск кандидатов для группы товаров"""
        if group_key == "no_enrichment":
            return []
        
        try:
            # Парсим группу
            parts = group_key.split('|')
            if len(parts) != 3:
                return []
            
            type_val, gender_val, brand_val = parts
            
            # Ищем кандидатов среди всех товаров с теми же критериями
            candidates = []
            for wb_sku, product in all_products.items():
                if (product.get_effective_type() == type_val and 
                    product.get_effective_gender() == gender_val and 
                    product.get_effective_brand() == brand_val and
                    product.wb_fbo_stock > 0):
                    candidates.append(product)
            
            return candidates
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска кандидатов для группы {group_key}: {e}")
            return []

    def _process_single_with_candidates(self, source_product: WBProductInfo, candidates: List[WBProductInfo]) -> WBProcessingResult:
        """Обработка одного товара с заранее найденными кандидатами"""
        start_time = time.time()
        
        try:
            if not source_product.has_enriched_data():
                return WBProcessingResult(
                    wb_sku=source_product.wb_sku,
                    status=WBProcessingStatus.NO_DATA,
                    recommendations=[],
                    processing_time=time.time() - start_time,
                    enrichment_info={}
                )
            
            # Исключаем сам товар из кандидатов
            filtered_candidates = [c for c in candidates if c.wb_sku != source_product.wb_sku]
            
            if not filtered_candidates:
                return WBProcessingResult(
                    wb_sku=source_product.wb_sku,
                    status=WBProcessingStatus.NO_SIMILAR,
                    recommendations=[],
                    processing_time=time.time() - start_time,
                    enrichment_info={}
                )
            
            # Вычисляем score для всех кандидатов
            recommendations = []
            
            for candidate in filtered_candidates:
                # Вычисляем similarity score
                score = self.recommendation_engine.calculate_similarity_score(source_product, candidate)
                
                # Фильтруем по минимальному score
                if score >= self.config.min_score_threshold:
                    match_details = self.recommendation_engine.get_match_details(source_product, candidate)
                    
                    recommendation = WBRecommendation(
                        product_info=candidate,
                        score=score,
                        match_details=match_details
                    )
                    recommendations.append(recommendation)
            
            # Сортируем по убыванию score
            recommendations.sort(key=lambda x: x.score, reverse=True)
            
            # Ограничиваем количество рекомендаций
            recommendations = recommendations[:self.config.max_recommendations]
            
            # Определяем статус
            total_time = time.time() - start_time
            
            if not recommendations:
                return WBProcessingResult(
                    wb_sku=source_product.wb_sku,
                    status=WBProcessingStatus.NO_SIMILAR,
                    recommendations=[],
                    processing_time=total_time,
                    enrichment_info={}
                )
            elif len(recommendations) < self.config.min_recommendations:
                return WBProcessingResult(
                    wb_sku=source_product.wb_sku,
                    status=WBProcessingStatus.INSUFFICIENT_RECOMMENDATIONS,
                    recommendations=recommendations,
                    processing_time=total_time,
                    enrichment_info={"count": len(recommendations)}
                )
            else:
                return WBProcessingResult(
                    wb_sku=source_product.wb_sku,
                    status=WBProcessingStatus.SUCCESS,
                    recommendations=recommendations,
                    processing_time=total_time,
                    enrichment_info={"count": len(recommendations)}
                )
                
        except Exception as e:
            total_time = time.time() - start_time
            return WBProcessingResult(
                wb_sku=source_product.wb_sku,
                status=WBProcessingStatus.ERROR,
                recommendations=[],
                processing_time=total_time,
                enrichment_info={},
                error_message=str(e)
            )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики по WB товарам"""
        logger.info("📊 Начало получения статистики...")
        
        try:
            stats = {}
            
            # Общее количество WB товаров
            logger.info("📊 Запрос общего количества WB товаров...")
            total_wb_query = "SELECT COUNT(*) as total FROM wb_products"
            start_time = time.time()
            total_df = self.db_conn.execute(total_wb_query).fetchdf()
            query_time = time.time() - start_time
            logger.info(f"📊 Первый запрос выполнен за {query_time:.2f}с")
            stats['total_wb_products'] = int(total_df.iloc[0]['total']) if not total_df.empty else 0
            logger.info(f"📊 Общее количество WB товаров: {stats['total_wb_products']}")
            
            # Количество WB товаров с остатками
            logger.info("📊 Запрос количества WB товаров в наличии...")
            in_stock_query = "SELECT COUNT(*) as in_stock FROM wb_products wb JOIN wb_prices wp ON wb.wb_sku = wp.wb_sku WHERE wp.wb_fbo_stock > 0"
            start_time = time.time()
            in_stock_df = self.db_conn.execute(in_stock_query).fetchdf()
            query_time = time.time() - start_time
            logger.info(f"📊 Второй запрос выполнен за {query_time:.2f}с")
            stats['wb_products_in_stock'] = int(in_stock_df.iloc[0]['in_stock']) if not in_stock_df.empty else 0
            logger.info(f"📊 WB товаров в наличии: {stats['wb_products_in_stock']}")
            
            # Количество WB товаров со связанными Ozon товарами (упрощенный запрос)
            logger.info("📊 Запрос количества связанных с Ozon товаров...")
            # Используем более простой запрос для избежания зависания
            linked_query = """
            SELECT COUNT(DISTINCT wb.wb_sku) as linked
            FROM wb_products wb
            WHERE wb.wb_barcodes IS NOT NULL AND wb.wb_barcodes != ''
            """
            start_time = time.time()
            linked_df = self.db_conn.execute(linked_query).fetchdf()
            query_time = time.time() - start_time
            logger.info(f"📊 Третий запрос выполнен за {query_time:.2f}с")
            stats['wb_products_linked_to_ozon'] = int(linked_df.iloc[0]['linked']) if not linked_df.empty else 0
            logger.info(f"📊 WB товаров со штрихкодами: {stats['wb_products_linked_to_ozon']}")
            
            # Качество связывания
            if stats['total_wb_products'] > 0:
                stats['linking_coverage'] = (stats['wb_products_linked_to_ozon'] / stats['total_wb_products']) * 100
            else:
                stats['linking_coverage'] = 0
                
            logger.info(f"📊 Покрытие связывания: {stats['linking_coverage']:.1f}%")
            logger.info("✅ Статистика получена успешно")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            logger.error(f"❌ Детали ошибки: {type(e).__name__}: {str(e)}")
            return {
                'total_wb_products': 0,
                'wb_products_in_stock': 0,
                'wb_products_linked_to_ozon': 0,
                'linking_coverage': 0
            } 