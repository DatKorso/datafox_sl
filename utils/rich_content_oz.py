"""
Основной модуль обработки Rich-контента для Ozon.
Содержит все алгоритмы, расчеты и бизнес-логику для генерации рекомендаций.

Архитектура:
- ProductInfo: модель данных товара
- ScoringConfig: настройки алгоритма оценки схожести
- ProductDataCollector: сбор данных из различных таблиц БД
- RecommendationEngine: основной движок рекомендаций
- RichContentGenerator: генерация JSON структуры
- RichContentProcessor: главный класс-оркестратор
"""

import json
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple, Callable
from enum import Enum
import re
import time

# Импорт оптимизированного модуля связывания
from .cross_marketplace_linker import CrossMarketplaceLinker

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
RICH_CONTENT_VERSION = 0.3
OZON_PRODUCT_BASE_URL = "https://www.ozon.ru/product/"


class ProcessingStatus(Enum):
    """Статусы обработки товара"""
    SUCCESS = "success"
    NO_DATA = "no_data"
    NO_SIMILAR = "no_similar"
    INSUFFICIENT_RECOMMENDATIONS = "insufficient_recommendations"
    ERROR = "error"
    FILTERED = "filtered"


@dataclass
class ProductInfo:
    """Модель данных товара с полной информацией для алгоритма рекомендаций"""
    
    # Основные поля из oz_category_products
    oz_vendor_code: str
    product_name: Optional[str] = None
    type: Optional[str] = None
    gender: Optional[str] = None
    oz_brand: Optional[str] = None
    russian_size: Optional[str] = None
    season: Optional[str] = None
    color: Optional[str] = None
    fastener_type: Optional[str] = None
    
    # Поля из oz_products
    oz_fbo_stock: int = 0
    main_photo_url: Optional[str] = None
    
    # Поля из punta_table (через cross-platform связи)
    material_short: Optional[str] = None
    new_last: Optional[str] = None
    mega_last: Optional[str] = None
    best_last: Optional[str] = None
    
    # Дополнительные поля
    wb_sku: Optional[int] = None
    has_punta_data: bool = False
    
    def __post_init__(self):
        """Нормализация данных после создания"""
        # Нормализация размера
        if self.russian_size:
            self.russian_size = self._normalize_size(self.russian_size)
    
    def _normalize_size(self, size: str) -> str:
        """Нормализация размера к единому формату"""
        if not size:
            return size
        
        # Удаляем лишние пробелы и приводим к строке
        size_str = str(size).strip()
        
        # Заменяем запятую на точку для десятичных размеров
        size_str = size_str.replace(',', '.')
        
        # Пытаемся преобразовать к числу и обратно для стандартизации
        try:
            size_float = float(size_str)
            # Если размер целый, возвращаем без дробной части
            if size_float.is_integer():
                return str(int(size_float))
            else:
                return str(size_float)
        except ValueError:
            # Если не число, возвращаем как есть
            return size_str
    
    def copy(self) -> 'ProductInfo':
        """Создание копии объекта"""
        return ProductInfo(
            oz_vendor_code=self.oz_vendor_code,
            product_name=self.product_name,
            type=self.type,
            gender=self.gender,
            oz_brand=self.oz_brand,
            russian_size=self.russian_size,
            season=self.season,
            color=self.color,
            fastener_type=self.fastener_type,
            oz_fbo_stock=self.oz_fbo_stock,
            main_photo_url=self.main_photo_url,
            material_short=self.material_short,
            new_last=self.new_last,
            mega_last=self.mega_last,
            best_last=self.best_last,
            wb_sku=self.wb_sku,
            has_punta_data=self.has_punta_data
        )


@dataclass
class Recommendation:
    """Модель рекомендации товара"""
    product_info: ProductInfo
    score: float
    match_details: str
    processing_status: ProcessingStatus = ProcessingStatus.SUCCESS
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь для сериализации"""
        return {
            'oz_vendor_code': self.product_info.oz_vendor_code,
            'score': self.score,
            'type': self.product_info.type,
            'brand': self.product_info.oz_brand,
            'size': self.product_info.russian_size,
            'season': self.product_info.season,
            'color': self.product_info.color,
            'stock': self.product_info.oz_fbo_stock,
            'match_details': self.match_details,
            'status': self.processing_status.value
        }


@dataclass
class ScoringConfig:
    """Конфигурация системы оценки схожести товаров"""
    
    # Базовые параметры
    base_score: int = 100
    max_score: int = 500
    
    # Веса для точных совпадений (обязательные критерии имеют вес 0, т.к. фильтруются отдельно)
    exact_type_weight: int = 0      # Обязательный критерий
    exact_gender_weight: int = 0    # Обязательный критерий  
    exact_brand_weight: int = 0     # Обязательный критерий
    
    # Размер (критический параметр)
    exact_size_weight: int = 100
    close_size_weight: int = 40     # ±1 размер
    size_mismatch_penalty: int = -50
    
    # Сезонность
    season_match_bonus: int = 80
    season_mismatch_penalty: int = -40
    
    # Второстепенные характеристики
    color_match_bonus: int = 40
    material_match_bonus: int = 40
    fastener_match_bonus: int = 30
    
    # Колодки (приоритет по типам)
    mega_last_bonus: int = 90
    best_last_bonus: int = 70
    new_last_bonus: int = 50
    no_last_penalty: float = 0.7    # Множитель при отсутствии совпадения колодки
    
    # Остатки на складе
    stock_high_bonus: int = 40      # >5 шт
    stock_medium_bonus: int = 20    # 2-5 шт
    stock_low_bonus: int = 10       # 1 шт
    stock_threshold_high: int = 5
    stock_threshold_medium: int = 2
    
    # Лимиты
    max_recommendations: int = 8
    min_recommendations: int = 8
    min_score_threshold: float = 50.0
    
    def __post_init__(self):
        """Валидация параметров конфигурации"""
        if self.base_score < 0:
            raise ValueError("base_score не может быть отрицательным")
        if self.max_score < self.base_score:
            raise ValueError("max_score должен быть больше base_score")
        if self.max_recommendations < 1:
            raise ValueError("max_recommendations должен быть больше 0")
        if self.min_recommendations < 1:
            raise ValueError("min_recommendations должен быть больше 0")
        if self.min_recommendations > self.max_recommendations:
            raise ValueError("min_recommendations не может быть больше max_recommendations")
    
    @classmethod
    def get_preset(cls, preset_name: str) -> 'ScoringConfig':
        """Получение предустановленных конфигураций"""
        presets = {
            "balanced": cls(),  # Стандартная конфигурация
            
            "size_focused": cls(
                exact_size_weight=150,
                close_size_weight=60,
                season_match_bonus=60,
                color_match_bonus=20
            ),
            
            "seasonal": cls(
                season_match_bonus=120,
                season_mismatch_penalty=-60,
                exact_size_weight=80,
                color_match_bonus=60
            ),
            
            "material_focused": cls(
                material_match_bonus=80,
                fastener_match_bonus=60,
                mega_last_bonus=120,
                best_last_bonus=90,
                new_last_bonus=70
            ),
            
            "conservative": cls(
                min_score_threshold=100.0,
                season_match_bonus=60,
                color_match_bonus=20,
                max_recommendations=5,
                min_recommendations=3
            )
        }
        
        if preset_name not in presets:
            raise ValueError(f"Неизвестный preset: {preset_name}. Доступные: {list(presets.keys())}")
        
        return presets[preset_name]


@dataclass 
class ProcessingResult:
    """Результат обработки одного товара"""
    oz_vendor_code: str
    status: ProcessingStatus
    recommendations: List[Recommendation] = field(default_factory=list)
    rich_content_json: Optional[str] = None
    error_message: Optional[str] = None
    processing_time: float = 0.0
    
    @property
    def success(self) -> bool:
        """Проверка успешности обработки"""
        return self.status == ProcessingStatus.SUCCESS


@dataclass
class BatchResult:
    """Результат пакетной обработки"""
    total_items: int
    processed_items: List[ProcessingResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Проверка, что хотя бы один товар обработан успешно"""
        return any(item.status == ProcessingStatus.SUCCESS for item in self.processed_items)
    
    @property  
    def all_successful(self) -> bool:
        """Проверка, что все товары обработаны успешно"""
        return all(item.status == ProcessingStatus.SUCCESS for item in self.processed_items)
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Статистика обработки пакета"""
        if not self.processed_items:
            return {
                'successful': 0,
                'no_similar': 0,
                'insufficient_recommendations': 0,
                'errors': 0,
                'success_rate': 0.0
            }
        
        successful = sum(1 for item in self.processed_items if item.status == ProcessingStatus.SUCCESS)
        no_similar = sum(1 for item in self.processed_items if item.status == ProcessingStatus.NO_SIMILAR)
        insufficient = sum(1 for item in self.processed_items if item.status == ProcessingStatus.INSUFFICIENT_RECOMMENDATIONS)
        errors = sum(1 for item in self.processed_items if item.status == ProcessingStatus.ERROR)
        
        success_rate = (successful / len(self.processed_items) * 100) if self.processed_items else 0.0
        
        return {
            'successful': successful,
            'no_similar': no_similar,
            'insufficient_recommendations': insufficient,
            'errors': errors,
            'success_rate': round(success_rate, 2)
        }


class ProductDataCollector:
    """Класс для сбора данных о товарах из различных таблиц БД"""
    
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self._cache = {}  # Простой кэш для оптимизации
        self.marketplace_linker = CrossMarketplaceLinker(db_conn)  # Оптимизированный линкер
    
    def get_full_product_info(self, oz_vendor_code: str) -> Optional[ProductInfo]:
        """
        Получение полной информации о товаре из всех связанных таблиц
        ОПТИМИЗИРОВАНО: разделен на быстрый основной запрос + отдельные punta данные
        
        Args:
            oz_vendor_code: Артикул товара в системе Ozon
            
        Returns:
            ProductInfo с полными данными или None если товар не найден
        """
        logger.info(f"🗄️ ProductDataCollector: получаем полную информацию о товаре {oz_vendor_code}")
        
        try:
            # Проверяем кэш
            if oz_vendor_code in self._cache:
                logger.info(f"⚡ Товар {oz_vendor_code} найден в кэше")
                return self._cache[oz_vendor_code]
            
            logger.info(f"🔍 Товар {oz_vendor_code} не в кэше, выполняем оптимизированный запрос")
            
            # ОПТИМИЗИРОВАННЫЙ запрос - без сложных JOIN по штрихкодам
            query = """
            SELECT 
                ocp.oz_vendor_code,
                ocp.product_name,
                ocp.type,
                ocp.gender, 
                ocp.oz_brand,
                ocp.russian_size,
                ocp.season,
                ocp.color,
                ocp.fastener_type,
                ocp.main_photo_url,
                COALESCE(op.oz_fbo_stock, 0) as oz_fbo_stock
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.oz_vendor_code = ?
            """
            
            logger.info(f"⏳ Выполняем быстрый SQL запрос для товара {oz_vendor_code}")
            query_start = time.time()
            
            result = self.db_conn.execute(query, [oz_vendor_code]).fetchone()
            
            query_time = time.time() - query_start
            logger.info(f"✅ Быстрый SQL запрос выполнен за {query_time:.2f}с")
            
            if not result:
                logger.warning(f"❌ Товар {oz_vendor_code} не найден в oz_category_products")
                return None
            
            logger.info(f"📊 Создаем базовый ProductInfo объект для товара {oz_vendor_code}")
            creation_start = time.time()
            
            # Создаем базовый ProductInfo без punta данных
            product_info = ProductInfo(
                oz_vendor_code=result[0],
                product_name=result[1],
                type=result[2],
                gender=result[3],
                oz_brand=result[4], 
                russian_size=result[5],
                season=result[6],
                color=result[7],
                fastener_type=result[8],
                main_photo_url=result[9],
                oz_fbo_stock=result[10],
                # Punta данные пока не заполнены
                material_short=None,
                new_last=None,
                mega_last=None,
                best_last=None,
                wb_sku=None,
                has_punta_data=False
            )
            
            creation_time = time.time() - creation_start
            logger.info(f"✅ Базовый ProductInfo создан за {creation_time:.4f}с")
            
            # Получаем punta данные отдельно через оптимизированный линкер
            logger.info(f"🔗 Получаем punta данные через CrossMarketplaceLinker")
            punta_start = time.time()
            
            try:
                # Используем готовый оптимизированный метод с кэшированием
                punta_data = self._get_punta_data_for_vendor_code(oz_vendor_code)
                if punta_data:
                    product_info.material_short = punta_data.get('material_short')
                    product_info.new_last = punta_data.get('new_last')
                    product_info.mega_last = punta_data.get('mega_last')
                    product_info.best_last = punta_data.get('best_last')
                    product_info.wb_sku = punta_data.get('wb_sku')
                    product_info.has_punta_data = True
                    
                punta_time = time.time() - punta_start
                logger.info(f"✅ Punta данные получены за {punta_time:.4f}с")
                
            except Exception as e:
                logger.warning(f"⚠️ Не удалось получить punta данные для {oz_vendor_code}: {e}")
                # Продолжаем работу без punta данных
            
            # Кэшируем результат
            logger.info(f"💾 Сохраняем товар {oz_vendor_code} в кэш")
            self._cache[oz_vendor_code] = product_info
            
            logger.info(f"🎉 Полная информация о товаре {oz_vendor_code} получена успешно")
            return product_info
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при получении данных товара {oz_vendor_code}: {e}")
            return None
    
    def _get_punta_data_for_vendor_code(self, oz_vendor_code: str) -> Optional[Dict[str, Any]]:
        """
        Получение punta данных для конкретного oz_vendor_code через оптимизированный линкер
        """
        try:
            # Получаем связи через готовый оптимизированный метод
            linked_df = self.marketplace_linker._normalize_and_merge_barcodes(
                oz_vendor_codes=[oz_vendor_code]
            )
            
            if linked_df.empty:
                return None
            
            # Берем первую найденную связь с wb_sku
            first_link = linked_df.iloc[0]
            wb_sku = first_link.get('wb_sku')
            
            if not wb_sku:
                return None
            
            # Получаем punta данные для найденного wb_sku
            punta_query = """
            SELECT material_short, new_last, mega_last, best_last
            FROM punta_table 
            WHERE wb_sku = ?
            """
            
            punta_result = self.db_conn.execute(punta_query, [wb_sku]).fetchone()
            
            if punta_result:
                return {
                    'material_short': punta_result[0],
                    'new_last': punta_result[1],
                    'mega_last': punta_result[2],
                    'best_last': punta_result[3],
                    'wb_sku': int(wb_sku)
                }
                
            return None
            
        except Exception as e:
            logger.warning(f"Ошибка получения punta данных для {oz_vendor_code}: {e}")
            return None
    
    def find_similar_products_candidates(self, source_product: ProductInfo) -> List[ProductInfo]:
        """
        Поиск кандидатов для рекомендаций по обязательным критериям
        ОПТИМИЗИРОВАНО: быстрый поиск без punta данных, которые добавляются потом
        
        Args:
            source_product: Исходный товар для поиска похожих
            
        Returns:
            Список кандидатов, прошедших базовую фильтрацию
        """
        logger.info(f"🔍 ProductDataCollector: поиск кандидатов для товара {source_product.oz_vendor_code}")
        
        try:
            # Обязательные критерии для фильтрации
            if not all([source_product.type, source_product.gender, source_product.oz_brand]):
                logger.warning(f"❌ Недостаточно данных для поиска похожих товаров: {source_product.oz_vendor_code}")
                logger.warning(f"   type: {source_product.type}, gender: {source_product.gender}, brand: {source_product.oz_brand}")
                return []
            
            logger.info(f"📊 Критерии поиска - тип: {source_product.type}, пол: {source_product.gender}, бренд: {source_product.oz_brand}")
            
            # ОПТИМИЗИРОВАННЫЙ запрос - без сложных JOIN по штрихкодам
            query = """
            SELECT 
                ocp.oz_vendor_code,
                ocp.product_name,
                ocp.type,
                ocp.gender,
                ocp.oz_brand,
                ocp.russian_size,
                ocp.season,
                ocp.color,
                ocp.fastener_type,
                ocp.main_photo_url,
                COALESCE(op.oz_fbo_stock, 0) as oz_fbo_stock
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.type = ?
            AND ocp.gender = ?
            AND ocp.oz_brand = ?
            AND ocp.oz_vendor_code != ?
            AND COALESCE(op.oz_fbo_stock, 0) > 0
            """
            
            logger.info(f"⏳ Выполняем оптимизированный SQL запрос поиска кандидатов")
            query_start = time.time()
            
            results = self.db_conn.execute(query, [
                source_product.type,
                source_product.gender, 
                source_product.oz_brand,
                source_product.oz_vendor_code
            ]).fetchall()
            
            query_time = time.time() - query_start
            logger.info(f"✅ Оптимизированный SQL запрос выполнен за {query_time:.2f}с, получено {len(results)} строк")
            
            if not results:
                logger.warning(f"❌ Кандидаты не найдены в базе данных")
                return []
            
            logger.info(f"📊 Создаем базовые ProductInfo объекты для {len(results)} кандидатов")
            creation_start = time.time()
            
            candidates = []
            for i, row in enumerate(results):
                if i % 50 == 0:  # Логируем каждый 50-й кандидат
                    logger.info(f"⏳ Создано базовых объектов: {i}/{len(results)}")
                
                # Создаем базовые ProductInfo без punta данных
                candidate = ProductInfo(
                    oz_vendor_code=row[0],
                    product_name=row[1],
                    type=row[2],
                    gender=row[3],
                    oz_brand=row[4],
                    russian_size=row[5], 
                    season=row[6],
                    color=row[7],
                    fastener_type=row[8],
                    main_photo_url=row[9],
                    oz_fbo_stock=row[10],
                    # Punta данные будут добавлены позже только для финальных рекомендаций
                    material_short=None,
                    new_last=None,
                    mega_last=None,
                    best_last=None,
                    wb_sku=None,
                    has_punta_data=False
                )
                candidates.append(candidate)
            
            creation_time = time.time() - creation_start
            logger.info(f"✅ Все базовые ProductInfo объекты созданы за {creation_time:.2f}с")
            
            logger.info(f"🎉 Найдено {len(candidates)} кандидатов для товара {source_product.oz_vendor_code}")
            
            # Логируем статистику по остаткам
            stock_stats = {}
            for candidate in candidates:
                stock_range = "0" if candidate.oz_fbo_stock == 0 else \
                             "1-5" if candidate.oz_fbo_stock <= 5 else \
                             "6-20" if candidate.oz_fbo_stock <= 20 else "20+"
                stock_stats[stock_range] = stock_stats.get(stock_range, 0) + 1
            
            logger.info(f"📊 Статистика остатков кандидатов: {stock_stats}")
            
            return candidates
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при поиске кандидатов для {source_product.oz_vendor_code}: {e}")
            return []
    
    def enrich_with_punta_data(self, products: List[ProductInfo]) -> List[ProductInfo]:
        """
        Обогащение списка товаров punta данными через оптимизированный линкер
        Вызывается только для финальных рекомендаций
        
        Args:
            products: Список товаров для обогащения
            
        Returns:
            Обогащенный список товаров
        """
        if not products:
            return products
        
        logger.info(f"🔗 Обогащение {len(products)} товаров punta данными")
        start_time = time.time()
        
        try:
            # Получаем vendor codes всех товаров
            vendor_codes = [p.oz_vendor_code for p in products]
            
            # Получаем связи batch запросом через оптимизированный линкер
            linked_df = self.marketplace_linker._normalize_and_merge_barcodes(
                oz_vendor_codes=vendor_codes
            )
            
            if linked_df.empty:
                logger.info(f"⚠️ Не найдено связей для обогащения punta данными")
                return products
            
            # Получаем wb_sku для найденных связей
            wb_skus = linked_df['wb_sku'].unique().tolist()
            
            if not wb_skus:
                logger.info(f"⚠️ Не найдено wb_sku для получения punta данных")
                return products
            
            # Получаем punta данные batch запросом
            wb_skus_str = ','.join([str(sku) for sku in wb_skus])
            punta_query = f"""
            SELECT wb_sku, material_short, new_last, mega_last, best_last
            FROM punta_table 
            WHERE wb_sku IN ({wb_skus_str})
            """
            
            punta_results = self.db_conn.execute(punta_query).fetchall()
            
            # Создаем маппинг wb_sku -> punta данные
            punta_map = {}
            for row in punta_results:
                punta_map[int(row[0])] = {
                    'material_short': row[1],
                    'new_last': row[2],
                    'mega_last': row[3],
                    'best_last': row[4]
                }
            
            # Создаем маппинг oz_vendor_code -> wb_sku
            vendor_to_wb = {}
            for _, row in linked_df.iterrows():
                vendor_code = row['oz_vendor_code']
                wb_sku = int(row['wb_sku'])
                if vendor_code not in vendor_to_wb:
                    vendor_to_wb[vendor_code] = wb_sku
            
            # Обогащаем товары
            enriched_count = 0
            for product in products:
                if product.oz_vendor_code in vendor_to_wb:
                    wb_sku = vendor_to_wb[product.oz_vendor_code]
                    if wb_sku in punta_map:
                        punta_data = punta_map[wb_sku]
                        product.material_short = punta_data['material_short']
                        product.new_last = punta_data['new_last']
                        product.mega_last = punta_data['mega_last']
                        product.best_last = punta_data['best_last']
                        product.wb_sku = wb_sku
                        product.has_punta_data = True
                        enriched_count += 1
            
            processing_time = time.time() - start_time
            logger.info(f"✅ Обогащение завершено за {processing_time:.2f}с, обогащено {enriched_count}/{len(products)} товаров")
            
            return products
            
        except Exception as e:
            logger.error(f"❌ Ошибка обогащения punta данными: {e}")
            return products
    
    def clear_cache(self):
        """Очистка кэша"""
        self._cache.clear()
        if hasattr(self.marketplace_linker, 'clear_cache'):
            self.marketplace_linker.clear_cache()

class RecommendationEngine:
    """Основной движок рекомендаций товаров"""
    
    def __init__(self, db_conn, config: ScoringConfig):
        self.db_conn = db_conn
        self.config = config
        self.data_collector = ProductDataCollector(db_conn)
    
    def find_similar_products(self, oz_vendor_code: str) -> List[Recommendation]:
        """
        Основной метод поиска похожих товаров
        ОПТИМИЗИРОВАНО: быстрый поиск кандидатов + обогащение только финальных рекомендаций
        
        Args:
            oz_vendor_code: Артикул исходного товара
            
        Returns:
            Список рекомендаций, отсортированный по убыванию score
        """
        logger.info(f"🔎 RecommendationEngine: начинаем поиск похожих товаров для {oz_vendor_code}")
        
        try:
            # Получаем информацию об исходном товаре (с punta данными)
            logger.info(f"📋 Получаем информацию об исходном товаре: {oz_vendor_code}")
            step_start = time.time()
            
            source_product = self.data_collector.get_full_product_info(oz_vendor_code)
            step_time = time.time() - step_start
            logger.info(f"✅ Информация о товаре получена за {step_time:.2f}с")
            
            if not source_product:
                logger.warning(f"❌ Товар {oz_vendor_code} не найден в базе данных")
                return []
            
            logger.info(f"📊 Товар найден - тип: {source_product.type}, пол: {source_product.gender}, бренд: {source_product.oz_brand}")
            
            # Ищем кандидатов по обязательным критериям (БЕЗ punta данных - быстро)
            logger.info(f"🔍 Поиск кандидатов по обязательным критериям (без punta данных)")
            step_start = time.time()
            
            candidates = self.data_collector.find_similar_products_candidates(source_product)
            step_time = time.time() - step_start
            logger.info(f"✅ Поиск кандидатов завершен за {step_time:.2f}с, найдено: {len(candidates)} кандидатов")
            
            if not candidates:
                logger.warning(f"❌ Нет кандидатов для товара {oz_vendor_code}")
                return []
            
            # Вычисляем базовый score для всех кандидатов (без punta данных - быстро)
            logger.info(f"🧮 Вычисляем базовый score для {len(candidates)} кандидатов")
            step_start = time.time()
            
            preliminary_recommendations = []
            
            for i, candidate in enumerate(candidates):
                if i % 50 == 0:  # Логируем каждый 50-й кандидат
                    logger.info(f"⏳ Обработано кандидатов: {i}/{len(candidates)}")
                
                # Базовый score без учета punta данных
                base_score = self._calculate_base_similarity_score(source_product, candidate)
                
                # Фильтруем по минимальному порогу
                if base_score >= self.config.min_score_threshold:
                    match_details = self.get_match_details(source_product, candidate)
                    
                    recommendation = Recommendation(
                        product_info=candidate,
                        score=base_score,
                        match_details=match_details
                    )
                    preliminary_recommendations.append(recommendation)
            
            step_time = time.time() - step_start
            logger.info(f"✅ Базовый scoring завершен за {step_time:.2f}с, прошли фильтр: {len(preliminary_recommendations)}")
            
            if not preliminary_recommendations:
                logger.warning(f"❌ Нет рекомендаций после базовой фильтрации")
                return []
            
            # Сортируем и берем больше чем нужно для финального обогащения
            preliminary_recommendations.sort(key=lambda r: r.score, reverse=True)
            top_candidates_count = min(len(preliminary_recommendations), self.config.max_recommendations * 2)
            top_candidates = preliminary_recommendations[:top_candidates_count]
            
            logger.info(f"📊 Отобрано {len(top_candidates)} топ кандидатов для обогащения punta данными")
            
            # Обогащаем только топ кандидатов punta данными
            logger.info(f"🔗 Обогащение топ кандидатов punta данными")
            step_start = time.time()
            
            top_products = [r.product_info for r in top_candidates]
            enriched_products = self.data_collector.enrich_with_punta_data(top_products)
            
            step_time = time.time() - step_start
            logger.info(f"✅ Обогащение завершено за {step_time:.2f}с")
            
            # Пересчитываем score с учетом punta данных для финальных кандидатов
            logger.info(f"🔄 Пересчитываем score с учетом punta данных")
            step_start = time.time()
            
            final_recommendations = []
            for enriched_product in enriched_products:
                # Полный score с учетом punta данных
                final_score = self.calculate_similarity_score(source_product, enriched_product)
                
                # Повторная фильтрация с обновленным score
                if final_score >= self.config.min_score_threshold:
                    match_details = self.get_match_details(source_product, enriched_product)
                    
                    recommendation = Recommendation(
                        product_info=enriched_product,
                        score=final_score,
                        match_details=match_details
                    )
                    final_recommendations.append(recommendation)
            
            step_time = time.time() - step_start
            logger.info(f"✅ Финальный scoring завершен за {step_time:.2f}с")
            
            # Финальная сортировка и ограничение количества
            logger.info(f"📊 Финальная сортировка и ограничение до {self.config.max_recommendations} рекомендаций")
            final_recommendations.sort(key=lambda r: r.score, reverse=True)
            final_recommendations = final_recommendations[:self.config.max_recommendations]
            
            logger.info(f"🎉 Найдено {len(final_recommendations)} итоговых рекомендаций для товара {oz_vendor_code}")
            if final_recommendations:
                scores = [r.score for r in final_recommendations]
                logger.info(f"📊 Score диапазон: {min(scores):.1f} - {max(scores):.1f}")
            
            return final_recommendations
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при поиске похожих товаров для {oz_vendor_code}: {e}")
            return []
    
    def _calculate_base_similarity_score(self, source: ProductInfo, candidate: ProductInfo) -> float:
        """
        Вычисление базового score схожести без учета punta данных (для быстрой предварительной фильтрации)
        
        Args:
            source: Исходный товар
            candidate: Товар-кандидат
            
        Returns:
            Базовый score от 0 до max_score
        """
        score = self.config.base_score
        
        # Размер (критический параметр)
        size_score = self._calculate_size_score(source, candidate)
        score += size_score
        
        # Сезонность
        season_score = self._calculate_season_score(source, candidate)
        score += season_score
        
        # Второстепенные характеристики (без материала и колодки)
        color_score = self._calculate_color_score(source, candidate)
        score += color_score
        
        fastener_score = self._calculate_fastener_score(source, candidate)
        score += fastener_score
        
        # Остатки на складе
        stock_score = self._calculate_stock_score(candidate)
        score += stock_score
        
        # Ограничиваем максимальным значением
        return min(score, self.config.max_score)
    
    def calculate_similarity_score(self, source: ProductInfo, candidate: ProductInfo) -> float:
        """
        Полное вычисление score схожести между товарами (включая punta данные)
        
        Args:
            source: Исходный товар
            candidate: Товар-кандидат
            
        Returns:
            Полный score от 0 до max_score
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
        
        # Колодки (критический параметр для обуви) - только если есть punta данные
        last_score = self._calculate_last_score(source, candidate)
        score += last_score
        
        # Применяем штраф за отсутствие совпадения колодки
        if last_score == 0:
            score *= self.config.no_last_penalty
        
        # Остатки на складе
        stock_score = self._calculate_stock_score(candidate)
        score += stock_score
        
        # Ограничиваем максимальным значением
        return min(score, self.config.max_score)
    
    def _calculate_size_score(self, source: ProductInfo, candidate: ProductInfo) -> float:
        """Вычисление score за размер"""
        if not source.russian_size or not candidate.russian_size:
            return self.config.size_mismatch_penalty
        
        try:
            source_size = float(source.russian_size.replace(',', '.'))
            candidate_size = float(candidate.russian_size.replace(',', '.'))
            
            size_diff = abs(candidate_size - source_size)
            
            if size_diff == 0:
                return self.config.exact_size_weight
            elif size_diff <= 1:
                return self.config.close_size_weight
            else:
                return self.config.size_mismatch_penalty
                
        except (ValueError, AttributeError):
            # Если размеры не числовые, сравниваем как строки
            if source.russian_size == candidate.russian_size:
                return self.config.exact_size_weight
            else:
                return self.config.size_mismatch_penalty
    
    def _calculate_season_score(self, source: ProductInfo, candidate: ProductInfo) -> float:
        """Вычисление score за сезон"""
        if not source.season or not candidate.season:
            return 0
        
        if source.season == candidate.season:
            return self.config.season_match_bonus
        else:
            return self.config.season_mismatch_penalty
    
    def _calculate_color_score(self, source: ProductInfo, candidate: ProductInfo) -> float:
        """Вычисление score за цвет"""
        if not source.color or not candidate.color:
            return 0
        
        if source.color.lower() == candidate.color.lower():
            return self.config.color_match_bonus
        else:
            return 0
    
    def _calculate_material_score(self, source: ProductInfo, candidate: ProductInfo) -> float:
        """Вычисление score за материал"""
        if not source.material_short or not candidate.material_short:
            return 0
        
        if source.material_short.lower() == candidate.material_short.lower():
            return self.config.material_match_bonus
        else:
            return 0
    
    def _calculate_fastener_score(self, source: ProductInfo, candidate: ProductInfo) -> float:
        """Вычисление score за тип застежки"""
        if not source.fastener_type or not candidate.fastener_type:
            return 0
        
        if source.fastener_type.lower() == candidate.fastener_type.lower():
            return self.config.fastener_match_bonus
        else:
            return 0
    
    def _calculate_last_score(self, source: ProductInfo, candidate: ProductInfo) -> float:
        """Вычисление score за колодку (приоритет: mega > best > new)"""
        # MEGA колодка (высший приоритет)
        if (source.mega_last and candidate.mega_last and 
            source.mega_last == candidate.mega_last):
            return self.config.mega_last_bonus
        
        # BEST колодка
        if (source.best_last and candidate.best_last and 
            source.best_last == candidate.best_last):
            return self.config.best_last_bonus
        
        # NEW колодка
        if (source.new_last and candidate.new_last and 
            source.new_last == candidate.new_last):
            return self.config.new_last_bonus
        
        return 0
    
    def _calculate_stock_score(self, candidate: ProductInfo) -> float:
        """Вычисление score за остатки на складе"""
        stock = candidate.oz_fbo_stock
        
        if stock > self.config.stock_threshold_high:
            return self.config.stock_high_bonus
        elif stock > self.config.stock_threshold_medium:
            return self.config.stock_medium_bonus
        elif stock > 0:
            return self.config.stock_low_bonus
        else:
            return 0
    
    def get_match_details(self, source: ProductInfo, candidate: ProductInfo) -> str:
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
        details.append(f"Тип: {candidate.type} ✓")
        details.append(f"Пол: {candidate.gender} ✓")
        details.append(f"Бренд: {candidate.oz_brand} ✓")
        
        # Размер
        size_score = self._calculate_size_score(source, candidate)
        if size_score == self.config.exact_size_weight:
            details.append(f"Размер: {candidate.russian_size} ✓")
            scores.append(f"+{self.config.exact_size_weight} баллов за точный размер")
        elif size_score == self.config.close_size_weight:
            details.append(f"Размер: {candidate.russian_size} (близкий) ✓")
            scores.append(f"+{self.config.close_size_weight} баллов за близкий размер")
        else:
            details.append(f"Размер: {candidate.russian_size} (штраф {self.config.size_mismatch_penalty} баллов)")
        
        # Сезон
        season_score = self._calculate_season_score(source, candidate)
        if season_score == self.config.season_match_bonus:
            details.append(f"Сезон: {candidate.season} ✓")
            scores.append(f"+{self.config.season_match_bonus} баллов за сезон")
        elif season_score == self.config.season_mismatch_penalty:
            details.append(f"Сезон: {candidate.season} (штраф {abs(self.config.season_mismatch_penalty)} баллов)")
        
        # Цвет
        color_score = self._calculate_color_score(source, candidate)
        if color_score > 0:
            details.append(f"Цвет: {candidate.color} ✓")
            scores.append(f"+{self.config.color_match_bonus} баллов за цвет")
        
        # Материал
        material_score = self._calculate_material_score(source, candidate)
        if material_score > 0:
            details.append(f"Материал: {candidate.material_short} ✓")
            scores.append(f"+{self.config.material_match_bonus} баллов за материал")
        
        # Застежка
        fastener_score = self._calculate_fastener_score(source, candidate)
        if fastener_score > 0:
            details.append(f"Застежка: {candidate.fastener_type} ✓")
            scores.append(f"+{self.config.fastener_match_bonus} баллов за застежку")
        
        # Колодка
        last_score = self._calculate_last_score(source, candidate)
        if last_score == self.config.mega_last_bonus:
            details.append(f"Колодка MEGA: {candidate.mega_last} ✓")
            scores.append(f"+{self.config.mega_last_bonus} баллов за колодку MEGA")
        elif last_score == self.config.best_last_bonus:
            details.append(f"Колодка BEST: {candidate.best_last} ✓")
            scores.append(f"+{self.config.best_last_bonus} баллов за колодку BEST")
        elif last_score == self.config.new_last_bonus:
            details.append(f"Колодка NEW: {candidate.new_last} ✓")
            scores.append(f"+{self.config.new_last_bonus} баллов за колодку NEW")
        else:
            details.append(f"Колодка: не совпадает (штраф {int((1 - self.config.no_last_penalty) * 100)}%)")
        
        # Остатки
        stock_score = self._calculate_stock_score(candidate)
        if stock_score == self.config.stock_high_bonus:
            details.append(f"В наличии: {candidate.oz_fbo_stock} шт. ✓✓")
            scores.append(f"+{self.config.stock_high_bonus} баллов за хороший остаток")
        elif stock_score == self.config.stock_medium_bonus:
            details.append(f"В наличии: {candidate.oz_fbo_stock} шт. ✓")
            scores.append(f"+{self.config.stock_medium_bonus} баллов за средний остаток")
        elif stock_score == self.config.stock_low_bonus:
            details.append(f"В наличии: {candidate.oz_fbo_stock} шт.")
            scores.append(f"+{self.config.stock_low_bonus} баллов за наличие")
        
        # Формируем итоговое описание
        result = "\n".join(details)
        if scores:
            result += "\n\nДетали счета:\n" + "\n".join(scores)
        
        return result 

class RichContentGenerator:
    """Генератор JSON структуры Rich Content для Ozon"""
    
    def __init__(self, config: ScoringConfig = None, db_conn=None):
        self.config = config or ScoringConfig()
        self.db_conn = db_conn
    
    def generate_rich_content_json(
        self, 
        recommendations: List[Recommendation],
        template_type: str = "recommendations_carousel",
        parent_product: ProductInfo = None
    ) -> str:
        """
        Генерация JSON строки Rich Content для Ozon
        
        Args:
            recommendations: Список рекомендаций товаров
            template_type: Тип шаблона ('recommendations_carousel', 'recommendations_grid', 'ozon_showcase')
            parent_product: Информация о родительском товаре (для определения пола)
            
        Returns:
            JSON строка для поля rich_content_json
        """
        try:
            if not recommendations:
                logger.warning("Пустой список рекомендаций для генерации Rich Content")
                return self._create_empty_content()
            
            # Выбираем метод генерации в зависимости от типа шаблона
            if template_type == "recommendations_carousel":
                content_data = self._create_recommendations_carousel(recommendations)
            elif template_type == "recommendations_grid":
                content_data = self._create_recommendations_grid(recommendations)
            elif template_type == "ozon_showcase":
                content_data = self._create_ozon_showcase(recommendations, parent_product)
            else:
                logger.warning(f"Неизвестный тип шаблона: {template_type}")
                content_data = self._create_ozon_showcase(recommendations, parent_product)
            
            # Оборачиваем в стандартную структуру Ozon Rich Content
            rich_content = {
                "content": content_data,
                "version": RICH_CONTENT_VERSION
            }
            
            return json.dumps(rich_content, ensure_ascii=False, separators=(',', ':'))
            
        except Exception as e:
            logger.error(f"Ошибка генерации Rich Content JSON: {e}")
            return self._create_empty_content()
    
    def _create_recommendations_carousel(self, recommendations: List[Recommendation]) -> List[Dict[str, Any]]:
        """Создание карусели рекомендаций"""
        
        # Заголовок секции
        title_block = {
            "widgetName": "raText",
            "text": {
                "content": "🔥 Похожие товары",
                "style": {
                    "fontSize": "18px",
                    "fontWeight": "bold",
                    "textAlign": "center",
                    "marginBottom": "16px",
                    "color": "#1a1a1a"
                }
            }
        }
        
        # Карусель товаров (используем настройки конфигурации)
        max_items = self.config.max_recommendations if self.config else 6
        carousel_blocks = []
        for i, rec in enumerate(recommendations[:max_items]):
            carousel_blocks.append(self._create_product_block(rec, i))
        
        carousel_widget = {
            "widgetName": "raShowcase",
            "type": "roll",
            "settings": {
                "autoplay": False,
                "showDots": True,
                "showArrows": True,
                "slidesToShow": 4,
                "slidesToShowMobile": 2
            },
            "blocks": carousel_blocks
        }
        
        return [title_block, carousel_widget]
    
    def _create_recommendations_grid(self, recommendations: List[Recommendation]) -> List[Dict[str, Any]]:
        """Создание сетки рекомендаций"""
        
        # Заголовок секции
        title_block = {
            "widgetName": "raText", 
            "text": {
                "content": "🎯 Рекомендуемые товары",
                "style": {
                    "fontSize": "16px",
                    "fontWeight": "bold",
                    "marginBottom": "12px",
                    "color": "#1a1a1a"
                }
            }
        }
        
        # Сетка товаров (используем настройки конфигурации)
        max_items = self.config.max_recommendations if self.config else 6
        grid_rows = []
        for i in range(0, min(len(recommendations), max_items), 2):
            row_blocks = []
            
            # Левый товар
            if i < len(recommendations):
                row_blocks.append(self._create_compact_product_block(recommendations[i]))
            
            # Правый товар
            if i + 1 < len(recommendations):
                row_blocks.append(self._create_compact_product_block(recommendations[i + 1]))
            
            # Создаем строку сетки
            grid_row = {
                "widgetName": "raColumns",
                "columns": row_blocks,
                "settings": {
                    "gap": "12px",
                    "marginBottom": "8px"
                }
            }
            grid_rows.append(grid_row)
        
        return [title_block] + grid_rows
    
    def _create_ozon_showcase(self, recommendations: List[Recommendation], parent_product: ProductInfo = None) -> List[Dict[str, Any]]:
        """Создание Ozon showcase с заголовком + витриной товаров + карусель"""
        
        # Определяем пол товара из родительского товара для выбора правильного изображения
        gender_specific_image = "https://cdn1.ozone.ru/s3/multimedia-1-e/7697739650.jpg"  # По умолчанию для девочек
        
        # Используем пол родительского товара, если он доступен
        if parent_product and parent_product.gender:
            gender = parent_product.gender.lower()
            if "мальчи" in gender:
                gender_specific_image = "https://cdn1.ozone.ru/s3/multimedia-1-l/7697806689.jpg"
        # Fallback на пол из первой рекомендации, если родительский товар недоступен
        elif recommendations and recommendations[0].product_info.gender:
            gender = recommendations[0].product_info.gender.lower()
            if "мальчи" in gender:
                gender_specific_image = "https://cdn1.ozone.ru/s3/multimedia-1-l/7697806689.jpg"
        
        # Заголовочный блок (баннер)
        header_block = {
            "widgetName": "raShowcase",
            "type": "roll",
            "blocks": [
                {
                    "imgLink": "",
                    "img": {
                        "src": "https://cdn1.ozone.ru/s3/multimedia-1-5/7285284185.jpg",
                        "srcMobile": "https://cdn1.ozone.ru/s3/multimedia-1-5/7285284185.jpg",
                        "alt": "Рекомендуемые товары",
                        "position": "width_full",
                        "positionMobile": "width_full",
                        "widthMobile": 1478,
                        "heightMobile": 665
                    }
                }
            ]
        }
        
        # Витрина товаров (используем настройки конфигурации)
        max_items = self.config.max_recommendations if self.config else 8
        showcase_blocks = []
        
        for i, rec in enumerate(recommendations[:max_items]):
            product = rec.product_info
            product_url = self._get_ozon_product_url(product.oz_vendor_code)
            
            # Создаем блок товара в формате Ozon
            product_block = {
                "img": {
                    "src": product.main_photo_url or "https://via.placeholder.com/900x1200?text=No+Photo",
                    "srcMobile": product.main_photo_url or "https://via.placeholder.com/900x1200?text=No+Photo",
                    "alt": product.product_name or f"{product.type} {product.oz_brand}",
                    "position": "to_the_edge",
                    "positionMobile": "to_the_edge", 
                    "widthMobile": 900,
                    "heightMobile": 1200,
                    "isParandjaMobile": False
                },
                "imgLink": product_url,
                "title": {
                    "content": [
                        self._get_product_title(product, rec.score)
                    ],
                    "size": "size3",
                    "align": "center",
                    "color": "color1"
                }
            }
            showcase_blocks.append(product_block)
        
        # Витрина товаров
        showcase_widget = {
            "widgetName": "raShowcase",
            "type": "tileM",
            "blocks": showcase_blocks
        }
        
        # Карусель с изображениями (предпоследнее изображение меняется в зависимости от пола)
        carousel_blocks = [
            {
                "imgLink": "",
                "img": {
                    "src": "https://cdn1.ozone.ru/s3/multimedia-1-9/7697739753.jpg",
                    "srcMobile": "https://cdn1.ozone.ru/s3/multimedia-1-9/7697739753.jpg",
                    "alt": "",
                    "position": "width_full",
                    "positionMobile": "width_full",
                    "widthMobile": 1440,
                    "heightMobile": 900
                }
            },
            {
                "imgLink": "",
                "img": {
                    "src": "https://cdn1.ozone.ru/s3/multimedia-1-v/7697739775.jpg",
                    "srcMobile": "https://cdn1.ozone.ru/s3/multimedia-1-v/7697739775.jpg",
                    "alt": "",
                    "position": "width_full",
                    "positionMobile": "width_full",
                    "widthMobile": 1440,
                    "heightMobile": 900
                }
            },
            {
                "imgLink": "",
                "img": {
                    "src": "https://cdn1.ozone.ru/s3/multimedia-1-s/7697739592.jpg",
                    "srcMobile": "https://cdn1.ozone.ru/s3/multimedia-1-s/7697739592.jpg",
                    "alt": "",
                    "position": "width_full",
                    "positionMobile": "width_full",
                    "widthMobile": 1440,
                    "heightMobile": 900
                }
            },
            {
                "imgLink": "",
                "img": {
                    "src": "https://cdn1.ozone.ru/s3/multimedia-1-z/7697739599.jpg",
                    "srcMobile": "https://cdn1.ozone.ru/s3/multimedia-1-z/7697739599.jpg",
                    "alt": "",
                    "position": "width_full",
                    "positionMobile": "width_full",
                    "widthMobile": 1440,
                    "heightMobile": 900
                }
            },
            {
                "imgLink": "",
                "img": {
                    "src": gender_specific_image,
                    "srcMobile": gender_specific_image,
                    "alt": "",
                    "position": "width_full",
                    "positionMobile": "width_full",
                    "widthMobile": 1440,
                    "heightMobile": 900
                }
            },
            {
                "imgLink": "",
                "img": {
                    "src": "https://cdn1.ozone.ru/s3/multimedia-1-y/7697739670.jpg",
                    "srcMobile": "https://cdn1.ozone.ru/s3/multimedia-1-y/7697739670.jpg",
                    "alt": "",
                    "position": "width_full",
                    "positionMobile": "width_full",
                    "widthMobile": 1440,
                    "heightMobile": 900
                }
            }
        ]
        
        # Карусель изображений
        carousel_widget = {
            "widgetName": "raShowcase",
            "type": "roll",
            "blocks": carousel_blocks
        }
        
        return [header_block, showcase_widget, carousel_widget]
    
    def _create_product_block(self, recommendation: Recommendation, index: int) -> Dict[str, Any]:
        """Создание блока товара для карусели"""
        product = recommendation.product_info
        
        # Формируем ссылку на товар
        product_url = self._generate_product_url(product.oz_vendor_code)
        
        # Базовый блок товара
        product_block = {
            "imgLink": product_url,
            "img": {
                "src": product.main_photo_url or "https://via.placeholder.com/300x300?text=No+Photo",
                "srcMobile": product.main_photo_url or "https://via.placeholder.com/300x300?text=No+Photo",
                "alt": product.product_name or f"{product.type} {product.oz_brand}",
                "position": "width_full",
                "positionMobile": "width_full"
            },
            "title": {
                "content": f"{product.type} {product.oz_brand}",
                "style": {
                    "fontSize": "14px",
                    "fontWeight": "500",
                    "marginTop": "8px",
                    "textAlign": "center"
                }
            },
            "subtitle": {
                "content": self._format_product_details(product, recommendation.score),
                "style": {
                    "fontSize": "12px",
                    "color": "#666666",
                    "textAlign": "center",
                    "marginTop": "4px"
                }
            }
        }
        
        return product_block
    
    def _create_compact_product_block(self, recommendation: Recommendation) -> Dict[str, Any]:
        """Создание компактного блока товара для сетки"""
        product = recommendation.product_info
        product_url = self._generate_product_url(product.oz_vendor_code)
        
        return {
            "widgetName": "raCard",
            "link": product_url,
            "img": {
                "src": product.main_photo_url or "https://via.placeholder.com/200x200?text=No+Photo",
                "alt": product.product_name or f"{product.type} {product.oz_brand}",
                "width": "100%",
                "height": "120px",
                "objectFit": "cover"
            },
            "content": {
                "title": f"{product.type}",
                "subtitle": f"Размер: {product.russian_size}",
                "badge": f"⭐ {int(recommendation.score)}",
                "description": f"В наличии: {product.oz_fbo_stock} шт."
            },
            "style": {
                "border": "1px solid #e0e0e0",
                "borderRadius": "8px",
                "padding": "8px",
                "backgroundColor": "#ffffff"
            }
        }
    
    def _format_product_details(self, product: ProductInfo, score: float) -> str:
        """Форматирование деталей товара для отображения"""
        details = []
        
        if product.russian_size:
            details.append(f"Размер: {product.russian_size}")
        
        if product.color:
            details.append(f"Цвет: {product.color}")
        
        if product.oz_fbo_stock > 0:
            details.append(f"В наличии: {product.oz_fbo_stock} шт.")
        
        # Добавляем score как "рейтинг совпадения"
        details.append(f"⭐ {int(score)}")
        
        return " • ".join(details)
    
    def _get_product_title(self, product: ProductInfo, score: float) -> str:
        """Генерация заголовка товара"""
        return "НАЖМИ НА ФОТО"
    
    def _get_product_description(self, product: ProductInfo, score: float) -> str:
        """Генерация описания товара"""
        return "НАЖМИ НА ФОТО"
    
    def _get_ozon_product_url(self, oz_vendor_code: str) -> str:
        """Получение реальной ссылки на товар в Ozon"""
        try:
            # Получаем oz_sku из таблицы oz_products
            query = """
            SELECT oz_sku 
            FROM oz_products 
            WHERE oz_vendor_code = ? 
            AND oz_sku IS NOT NULL
            """
            
            result = self.db_conn.execute(query, [oz_vendor_code]).fetchone() if hasattr(self, 'db_conn') else None
            
            if result and result[0]:
                return f"https://www.ozon.ru/product/{result[0]}"
            else:
                # Fallback на базовую структуру с oz_vendor_code
                return f"https://www.ozon.ru/product/{oz_vendor_code.replace('-', '')[:10]}"
                
        except Exception as e:
            logger.warning(f"Ошибка получения URL для {oz_vendor_code}: {e}")
            return f"https://www.ozon.ru/product/{oz_vendor_code.replace('-', '')[:10]}"
    
    def _generate_product_url(self, oz_vendor_code: str) -> str:
        """Генерация URL товара на Ozon (старый метод для обратной совместимости)"""
        return self._get_ozon_product_url(oz_vendor_code)
    
    def _create_empty_content(self) -> str:
        """Создание пустого Rich Content при отсутствии рекомендаций"""
        empty_content = {
            "content": [
                {
                    "widgetName": "raText",
                    "text": {
                        "content": "ℹ️ Для этого товара пока нет рекомендаций",
                        "style": {
                            "fontSize": "14px",
                            "color": "#666666",
                            "textAlign": "center",
                            "fontStyle": "italic"
                        }
                    }
                }
            ],
            "version": RICH_CONTENT_VERSION
        }
        
        return json.dumps(empty_content, ensure_ascii=False, separators=(',', ':'))
    
    def validate_rich_content_json(self, json_string: str) -> bool:
        """
        Валидация сгенерированного Rich Content JSON
        
        Args:
            json_string: JSON строка для проверки
            
        Returns:
            True если JSON валидный для Ozon Rich Content
        """
        try:
            data = json.loads(json_string)
            
            # Проверяем обязательные поля
            if 'content' not in data:
                logger.error("Отсутствует поле 'content' в Rich Content JSON")
                return False
            
            if 'version' not in data:
                logger.error("Отсутствует поле 'version' в Rich Content JSON")
                return False
            
            # Проверяем структуру content
            if not isinstance(data['content'], list):
                logger.error("Поле 'content' должно быть массивом")
                return False
            
            # Проверяем каждый виджет в content
            for i, widget in enumerate(data['content']):
                if not isinstance(widget, dict):
                    logger.error(f"Виджет {i} должен быть объектом")
                    return False
                
                if 'widgetName' not in widget:
                    logger.error(f"У виджета {i} отсутствует поле 'widgetName'")
                    return False
            
            logger.info("Rich Content JSON прошел валидацию")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"Ошибка валидации Rich Content JSON: {e}")
            return False


class RichContentProcessor:
    """Главный класс-оркестратор для обработки Rich Content"""
    
    def __init__(self, db_conn, config: ScoringConfig = None):
        self.db_conn = db_conn
        self.config = config or ScoringConfig()
        self.recommendation_engine = RecommendationEngine(db_conn, self.config)
        self.content_generator = RichContentGenerator(self.config, db_conn)
        
    def process_single_product(self, oz_vendor_code: str) -> ProcessingResult:
        """
        Обработка одного товара - создание рекомендаций и Rich Content
        
        Args:
            oz_vendor_code: Артикул товара для обработки
            
        Returns:
            Результат обработки с созданным Rich Content JSON
        """
        start_time = time.time()
        logger.info(f"🎯 Начинаем обработку товара: {oz_vendor_code}")
        
        try:
            # Получаем информацию о родительском товаре
            logger.info(f"📋 Получаем информацию о родительском товаре: {oz_vendor_code}")
            step_start = time.time()
            
            source_product = self.recommendation_engine.data_collector.get_full_product_info(oz_vendor_code)
            step_time = time.time() - step_start
            logger.info(f"✅ Информация о родительском товаре получена за {step_time:.2f}с")
            
            if not source_product:
                total_time = time.time() - start_time
                logger.warning(f"❌ Родительский товар {oz_vendor_code} не найден (общее время: {total_time:.2f}с)")
                return ProcessingResult(
                    oz_vendor_code=oz_vendor_code,
                    status=ProcessingStatus.NO_DATA,
                    recommendations=[],
                    rich_content_json=self.content_generator._create_empty_content(),
                    error_message="Родительский товар не найден",
                    processing_time=total_time
                )
            
            # Поиск рекомендаций
            logger.info(f"🔍 Поиск рекомендаций для товара: {oz_vendor_code}")
            step_start = time.time()
            
            recommendations = self.recommendation_engine.find_similar_products(oz_vendor_code)
            step_time = time.time() - step_start
            logger.info(f"✅ Поиск рекомендаций завершен за {step_time:.2f}с, найдено: {len(recommendations)}")
            
            # Проверка количества рекомендаций
            if not recommendations:
                total_time = time.time() - start_time
                logger.warning(f"⚠️ Нет рекомендаций для товара {oz_vendor_code} (общее время: {total_time:.2f}с)")
                return ProcessingResult(
                    oz_vendor_code=oz_vendor_code,
                    status=ProcessingStatus.NO_SIMILAR,
                    recommendations=[],
                    rich_content_json=self.content_generator._create_empty_content(),
                    processing_time=total_time
                )
            
            # Проверка минимального количества рекомендаций
            if len(recommendations) < self.config.min_recommendations:
                total_time = time.time() - start_time
                logger.warning(f"⚠️ Недостаточно рекомендаций для товара {oz_vendor_code}: "
                             f"найдено {len(recommendations)}, требуется минимум {self.config.min_recommendations} "
                             f"(общее время: {total_time:.2f}с)")
                return ProcessingResult(
                    oz_vendor_code=oz_vendor_code,
                    status=ProcessingStatus.INSUFFICIENT_RECOMMENDATIONS,
                    recommendations=recommendations,
                    rich_content_json=self.content_generator._create_empty_content(),
                    error_message=f"Недостаточно рекомендаций: {len(recommendations)} < {self.config.min_recommendations}",
                    processing_time=total_time
                )
            
            # Генерация Rich Content JSON
            logger.info(f"📝 Генерация Rich Content JSON для {len(recommendations)} рекомендаций")
            step_start = time.time()
            
            rich_content_json = self.content_generator.generate_rich_content_json(
                recommendations, 
                template_type="ozon_showcase", 
                parent_product=source_product
            )
            step_time = time.time() - step_start
            logger.info(f"✅ Rich Content JSON сгенерирован за {step_time:.2f}с")
            
            # Валидация сгенерированного JSON
            logger.info(f"🔍 Валидация сгенерированного JSON")
            step_start = time.time()
            
            if not self.content_generator.validate_rich_content_json(rich_content_json):
                step_time = time.time() - step_start
                total_time = time.time() - start_time
                logger.error(f"❌ Ошибка валидации JSON за {step_time:.2f}с (общее время: {total_time:.2f}с)")
                return ProcessingResult(
                    oz_vendor_code=oz_vendor_code,
                    status=ProcessingStatus.ERROR,
                    recommendations=recommendations,
                    error_message="Ошибка валидации Rich Content JSON",
                    processing_time=total_time
                )
            
            step_time = time.time() - step_start
            total_time = time.time() - start_time
            logger.info(f"✅ Валидация JSON завершена за {step_time:.2f}с")
            logger.info(f"🎉 Обработка товара {oz_vendor_code} успешно завершена за {total_time:.2f}с")
            
            return ProcessingResult(
                oz_vendor_code=oz_vendor_code,
                status=ProcessingStatus.SUCCESS,
                recommendations=recommendations,
                rich_content_json=rich_content_json,
                processing_time=total_time
            )
            
        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"❌ Критическая ошибка обработки товара {oz_vendor_code} за {total_time:.2f}с: {e}")
            return ProcessingResult(
                oz_vendor_code=oz_vendor_code,
                status=ProcessingStatus.ERROR,
                recommendations=[],
                error_message=str(e),
                processing_time=total_time
            )
    
    def process_batch_optimized(
        self, 
        oz_vendor_codes: List[str], 
        progress_callback: Callable[[int, int, str], None] = None,
        batch_size: int = 50
    ) -> BatchResult:
        """
        Оптимизированная пакетная обработка списка товаров с batch-обогащением punta данными
        
        Args:
            oz_vendor_codes: Список артикулов для обработки
            progress_callback: Callback функция для отслеживания прогресса
            batch_size: Размер батча для обработки (по умолчанию 50)
            
        Returns:
            Результат пакетной обработки
        """
        results = []
        total_items = len(oz_vendor_codes)
        
        logger.info(f"🚀 Начинаем ОПТИМИЗИРОВАННУЮ пакетную обработку {total_items} товаров (batch_size={batch_size})")
        start_time = time.time()
        
        # Обрабатываем товары батчами
        for batch_start in range(0, total_items, batch_size):
            batch_end = min(batch_start + batch_size, total_items)
            batch_codes = oz_vendor_codes[batch_start:batch_end]
            
            logger.info(f"📦 Обработка батча {batch_start//batch_size + 1}: товары {batch_start+1}-{batch_end}")
            batch_results = self._process_batch_chunk(batch_codes, progress_callback, batch_start, total_items)
            results.extend(batch_results)
        
        total_time = time.time() - start_time
        logger.info(f"✅ ОПТИМИЗИРОВАННАЯ пакетная обработка завершена за {total_time:.1f}с")
        
        # Финальный прогресс
        if progress_callback:
            progress_callback(total_items, total_items, "Обработка завершена")
        
        batch_result = BatchResult(
            total_items=total_items,
            processed_items=results
        )
        
        logger.info(f"📊 Статистика: {batch_result.stats}")
        return batch_result
    
    def _process_batch_chunk(
        self, 
        oz_vendor_codes: List[str], 
        progress_callback: Callable[[int, int, str], None] = None,
        offset: int = 0, 
        total_items: int = 0
    ) -> List[ProcessingResult]:
        """
        Обработка одного батча товаров с оптимизациями
        
        Args:
            oz_vendor_codes: Список артикулов батча
            progress_callback: Callback функция для отслеживания прогресса  
            offset: Смещение для правильного отображения прогресса
            total_items: Общее количество товаров
            
        Returns:
            Список результатов обработки батча
        """
        batch_size = len(oz_vendor_codes)
        logger.info(f"🔄 Обработка батча из {batch_size} товаров")
        
        # 1. Получаем информацию о всех товарах батча
        logger.info(f"📋 Получение информации о {batch_size} товарах...")
        step_start = time.time()
        
        source_products = {}
        for i, vendor_code in enumerate(oz_vendor_codes):
            if progress_callback:
                progress_callback(offset + i + 1, total_items, f"Загружаем {vendor_code}")
            
            product_info = self.recommendation_engine.data_collector.get_full_product_info(vendor_code)
            if product_info:
                source_products[vendor_code] = product_info
            else:
                logger.warning(f"⚠️ Товар {vendor_code} не найден")
        
        step_time = time.time() - step_start
        logger.info(f"✅ Информация о товарах получена за {step_time:.2f}с, успешно: {len(source_products)}/{batch_size}")
        
        # 2. Группируем товары по типу/полу/бренду для оптимизации поиска кандидатов
        logger.info(f"📊 Группировка товаров по критериям поиска...")
        groups = {}
        for vendor_code, product in source_products.items():
            key = (product.type, product.gender, product.oz_brand)
            if key not in groups:
                groups[key] = []
            groups[key].append((vendor_code, product))
        
        logger.info(f"📊 Создано {len(groups)} групп для поиска кандидатов")
        
        # 3. Для каждой группы ищем кандидатов и обрабатываем товары
        batch_results = []
        processed_count = 0
        
        for group_key, group_products in groups.items():
            type_name, gender, brand = group_key
            logger.info(f"🔍 Обработка группы: {type_name}/{gender}/{brand} ({len(group_products)} товаров)")
            
            # Ищем кандидатов для группы (один раз на группу)
            if group_products:
                sample_product = group_products[0][1]  # Берем первый товар как образец
                candidates = self.recommendation_engine.data_collector.find_similar_products_candidates(sample_product)
                logger.info(f"📊 Найдено {len(candidates)} кандидатов для группы {type_name}/{gender}/{brand}")
                
                # Пакетное обогащение кандидатов punta данными (один раз для всей группы)
                if candidates:
                    logger.info(f"🔗 Пакетное обогащение {len(candidates)} кандидатов punta данными")
                    enriched_candidates = self.recommendation_engine.data_collector.enrich_with_punta_data(candidates)
                else:
                    enriched_candidates = []
                
                # Обрабатываем каждый товар группы
                for vendor_code, source_product in group_products:
                    try:
                        processed_count += 1
                        if progress_callback:
                            progress_callback(offset + processed_count, total_items, f"Обрабатываем {vendor_code}")
                        
                        # Ищем рекомендации среди уже обогащенных кандидатов
                        recommendations = self._find_recommendations_from_candidates(source_product, enriched_candidates)
                        
                        if len(recommendations) < self.config.min_recommendations:
                            result = ProcessingResult(
                                oz_vendor_code=vendor_code,
                                status=ProcessingStatus.INSUFFICIENT_RECOMMENDATIONS,
                                recommendations=recommendations,
                                rich_content_json=self.content_generator._create_empty_content(),
                                error_message=f"Недостаточно рекомендаций: {len(recommendations)} < {self.config.min_recommendations}",
                                processing_time=0.0
                            )
                        else:
                            # Генерация Rich Content JSON
                            rich_content_json = self.content_generator.generate_rich_content_json(
                                recommendations, 
                                template_type="ozon_showcase", 
                                parent_product=source_product
                            )
                            
                            result = ProcessingResult(
                                oz_vendor_code=vendor_code,
                                status=ProcessingStatus.SUCCESS,
                                recommendations=recommendations,
                                rich_content_json=rich_content_json,
                                processing_time=0.0
                            )
                        
                        batch_results.append(result)
                        
                    except Exception as e:
                        logger.error(f"❌ Ошибка при обработке {vendor_code}: {e}")
                        error_result = ProcessingResult(
                            oz_vendor_code=vendor_code,
                            status=ProcessingStatus.ERROR,
                            recommendations=[],
                            error_message=str(e),
                            processing_time=0.0
                        )
                        batch_results.append(error_result)
        
        return batch_results
    
    def _find_recommendations_from_candidates(self, source_product: ProductInfo, candidates: List[ProductInfo]) -> List[Recommendation]:
        """
        Поиск рекомендаций из уже обогащенного списка кандидатов
        
        Args:
            source_product: Исходный товар
            candidates: Список уже обогащенных кандидатов
            
        Returns:
            Список рекомендаций
        """
        recommendations = []
        
        for candidate in candidates:
            # Исключаем сам товар
            if candidate.oz_vendor_code == source_product.oz_vendor_code:
                continue
                
            # Вычисляем полный score
            score = self.recommendation_engine.calculate_similarity_score(source_product, candidate)
            
            # Фильтруем по минимальному порогу
            if score >= self.config.min_score_threshold:
                match_details = self.recommendation_engine.get_match_details(source_product, candidate)
                
                recommendation = Recommendation(
                    product_info=candidate,
                    score=score,
                    match_details=match_details
                )
                recommendations.append(recommendation)
        
        # Сортируем по score и ограничиваем количество
        recommendations.sort(key=lambda r: r.score, reverse=True)
        return recommendations[:self.config.max_recommendations]
    
    def save_rich_content_to_database(self, result: ProcessingResult) -> bool:
        """
        Сохранение Rich Content JSON в базу данных
        
        Args:
            result: Результат обработки товара
            
        Returns:
            True если сохранение успешно, False в случае ошибки
        """
        if not result.success or not result.rich_content_json:
            logger.warning(f"Попытка сохранить неуспешный результат или пустой JSON для {result.oz_vendor_code}")
            return False
        
        try:
            # Обновляем поле rich_content_json в таблице oz_category_products
            update_query = """
            UPDATE oz_category_products 
            SET rich_content_json = ?
            WHERE oz_vendor_code = ?
            """
            
            self.db_conn.execute(update_query, [result.rich_content_json, result.oz_vendor_code])
            self.db_conn.commit()
            
            logger.info(f"✅ Rich Content JSON сохранен в БД для товара {result.oz_vendor_code}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения Rich Content JSON для {result.oz_vendor_code}: {e}")
            return False

    def process_batch(
        self, 
        oz_vendor_codes: List[str], 
        progress_callback: Callable[[int, int, str], None] = None
    ) -> BatchResult:
        """
        Пакетная обработка списка товаров
        
        Args:
            oz_vendor_codes: Список артикулов для обработки
            progress_callback: Callback функция для отслеживания прогресса
            
        Returns:
            Результат пакетной обработки
        """
        results = []
        total_items = len(oz_vendor_codes)
        
        logger.info(f"Начинаем пакетную обработку {total_items} товаров")
        
        for i, vendor_code in enumerate(oz_vendor_codes):
            try:
                # Обновляем прогресс
                if progress_callback:
                    progress_callback(i + 1, total_items, f"Обрабатываем {vendor_code}")
                
                # Обрабатываем товар
                result = self.process_single_product(vendor_code)
                results.append(result)
                
                # Логируем результат
                if result.success:
                    logger.info(f"✅ {vendor_code}: {len(result.recommendations)} рекомендаций")
                else:
                    logger.warning(f"⚠️ {vendor_code}: {result.status.value}")
                    
            except Exception as e:
                logger.error(f"❌ Критическая ошибка при обработке {vendor_code}: {e}")
                
                # Создаем результат с ошибкой
                error_result = ProcessingResult(
                    oz_vendor_code=vendor_code,
                    status=ProcessingStatus.ERROR,
                    recommendations=[],
                    error_message=str(e),
                    processing_time=0.0
                )
                results.append(error_result)
        
        # Финальный прогресс
        if progress_callback:
            progress_callback(total_items, total_items, "Обработка завершена")
        
        batch_result = BatchResult(
            total_items=total_items,
            processed_items=results
        )
        
        logger.info(f"Пакетная обработка завершена: {batch_result.stats}")
        return batch_result
    
    def save_rich_content_to_database(self, processing_result: ProcessingResult) -> bool:
        """
        Сохранение сгенерированного Rich Content в базу данных
        
        Args:
            processing_result: Результат обработки товара
            
        Returns:
            True если сохранение успешно
        """
        try:
            if not processing_result.success:
                logger.warning(f"Попытка сохранить неуспешный результат для {processing_result.oz_vendor_code}")
                return False
            
            # Обновляем поле rich_content_json в таблице oz_category_products
            update_query = """
                UPDATE oz_category_products 
                SET rich_content_json = ? 
                WHERE oz_vendor_code = ?
            """
            
            self.db_conn.execute(update_query, [
                processing_result.rich_content_json,
                processing_result.oz_vendor_code
            ])
            
            logger.info(f"Rich Content сохранен для товара {processing_result.oz_vendor_code}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения Rich Content для {processing_result.oz_vendor_code}: {e}")
            return False
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Получение статистики обработки из базы данных"""
        try:
            stats_query = """
                SELECT 
                    COUNT(*) as total_products,
                    COUNT(CASE WHEN rich_content_json IS NOT NULL AND rich_content_json != '' 
                          THEN 1 END) as products_with_rich_content,
                    COUNT(CASE WHEN rich_content_json IS NULL OR rich_content_json = '' 
                          THEN 1 END) as products_without_rich_content
                FROM oz_category_products
            """
            
            result = self.db_conn.execute(stats_query).fetchone()
            
            if result:
                total, with_content, without_content = result
                coverage_percent = (with_content / total * 100) if total > 0 else 0
                
                return {
                    'total_products': total,
                    'products_with_rich_content': with_content,
                    'products_without_rich_content': without_content,
                    'coverage_percent': round(coverage_percent, 2)
                }
            else:
                return {
                    'total_products': 0,
                    'products_with_rich_content': 0,
                    'products_without_rich_content': 0,
                    'coverage_percent': 0.0
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {
                'error': str(e)
            } 