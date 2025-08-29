#!/usr/bin/env python3
"""
Модуль для сравнения двух товаров Ozon с детальным анализом scoring алгоритма.
Показывает, по каким параметрам начисляются или убираются баллы.
"""

import duckdb
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from utils.rich_content_oz import ProductDataCollector, RecommendationEngine
from utils.scoring_config_optimized import ScoringConfig
import logging

logger = logging.getLogger(__name__)

@dataclass
class ScoringDetail:
    """Детали начисления баллов по конкретному параметру"""
    parameter: str
    source_value: Any
    candidate_value: Any
    score: float
    max_possible: float
    description: str
    match_type: str  # "exact", "partial", "none", "penalty"

@dataclass
class ComparisonResult:
    """Результат сравнения двух товаров"""
    source_vendor_code: str
    candidate_vendor_code: str
    total_score: float
    scoring_details: List[ScoringDetail]
    source_product: Any
    candidate_product: Any
    similarity_percentage: float

class ProductComparator:
    """Класс для сравнения товаров с детальным анализом scoring"""
    
    def __init__(self, db_conn, config: ScoringConfig = None):
        self.db_conn = db_conn
        self.config = config or ScoringConfig()
        self.data_collector = ProductDataCollector(db_conn)
        self.recommendation_engine = RecommendationEngine(db_conn, self.config)
    
    def compare_products(self, source_vendor_code: str, candidate_vendor_code: str) -> Optional[ComparisonResult]:
        """
        Сравнивает два товара и возвращает детальный анализ scoring
        
        Args:
            source_vendor_code: Артикул первого товара (источник)
            candidate_vendor_code: Артикул второго товара (кандидат)
            
        Returns:
            ComparisonResult с детальным анализом или None при ошибке
        """
        try:
            # Получаем полную информацию о товарах
            source_product = self.data_collector.get_full_product_info(source_vendor_code)
            candidate_product = self.data_collector.get_full_product_info(candidate_vendor_code)
            
            if not source_product:
                logger.error(f"Товар {source_vendor_code} не найден")
                return None
                
            if not candidate_product:
                logger.error(f"Товар {candidate_vendor_code} не найден")
                return None
            
            # Вычисляем общий score
            total_score = self.recommendation_engine.calculate_similarity_score(source_product, candidate_product)
            
            # Получаем детальный анализ по каждому параметру
            scoring_details = self._get_detailed_scoring_breakdown(source_product, candidate_product)
            
            # Вычисляем процент схожести (относительно максимально возможного score)
            max_possible_score = self._calculate_max_possible_score()
            similarity_percentage = min(100, (total_score / max_possible_score) * 100) if max_possible_score > 0 else 0
            
            return ComparisonResult(
                source_vendor_code=source_vendor_code,
                candidate_vendor_code=candidate_vendor_code,
                total_score=total_score,
                scoring_details=scoring_details,
                source_product=source_product,
                candidate_product=candidate_product,
                similarity_percentage=similarity_percentage
            )
            
        except Exception as e:
            logger.error(f"Ошибка сравнения товаров {source_vendor_code} и {candidate_vendor_code}: {e}")
            return None
    
    def _get_detailed_scoring_breakdown(self, source, candidate) -> List[ScoringDetail]:
        """Получает детальный анализ scoring по каждому параметру"""
        details = []
        
        # 1. Размер (обязательное совпадение)
        size_score = self.recommendation_engine._calculate_size_score(source, candidate)
        details.append(ScoringDetail(
            parameter="Размер",
            source_value=getattr(source, 'russian_size', 'Не указан'),
            candidate_value=getattr(candidate, 'russian_size', 'Не указан'),
            score=size_score,
            max_possible=self.config.exact_size_weight,
            description=f"Точное совпадение размера (+{self.config.exact_size_weight} баллов)" if size_score > 0 else "Размеры не совпадают",
            match_type="exact" if size_score > 0 else "none"
        ))
        
        # 2. Сезон
        season_score = self.recommendation_engine._calculate_season_score(source, candidate)
        details.append(ScoringDetail(
            parameter="Сезон",
            source_value=getattr(source, 'season', 'Не указан'),
            candidate_value=getattr(candidate, 'season', 'Не указан'),
            score=season_score,
            max_possible=self.config.season_match_bonus,
            description=f"Совпадение сезона (+{season_score} баллов)" if season_score > 0 else "Сезоны не совпадают",
            match_type="exact" if season_score > 0 else "none"
        ))
        
        # 3. Цвет
        color_score = self.recommendation_engine._calculate_color_score(source, candidate)
        details.append(ScoringDetail(
            parameter="Цвет",
            source_value=getattr(source, 'color', 'Не указан'),
            candidate_value=getattr(candidate, 'color', 'Не указан'),
            score=color_score,
            max_possible=self.config.color_match_bonus,
            description=f"Совпадение цвета (+{color_score} баллов)" if color_score > 0 else "Цвета не совпадают",
            match_type="exact" if color_score > 0 else "none"
        ))
        
        # 4. Материал
        material_score = self.recommendation_engine._calculate_material_score(source, candidate)
        details.append(ScoringDetail(
            parameter="Материал",
            source_value=getattr(source, 'material_short', getattr(source, 'material', 'Не указан')),
            candidate_value=getattr(candidate, 'material_short', getattr(candidate, 'material', 'Не указан')),
            score=material_score,
            max_possible=self.config.material_match_bonus,
            description=f"Совпадение материала (+{material_score} баллов)" if material_score > 0 else "Материалы не совпадают",
            match_type="exact" if material_score > 0 else "none"
        ))
        
        # 5. Застежка
        fastener_score = self.recommendation_engine._calculate_fastener_score(source, candidate)
        details.append(ScoringDetail(
            parameter="Застежка",
            source_value=getattr(source, 'fastener_type', 'Не указан'),
            candidate_value=getattr(candidate, 'fastener_type', 'Не указан'),
            score=fastener_score,
            max_possible=self.config.fastener_match_bonus,
            description=f"Совпадение застежки (+{fastener_score} баллов)" if fastener_score > 0 else "Застежки не совпадают",
            match_type="exact" if fastener_score > 0 else "none"
        ))
        
        # 6. Колодки (MEGA, BEST, NEW)
        last_score = self.recommendation_engine._calculate_last_score(source, candidate)
        mega_last_match = (getattr(source, 'mega_last', None) and getattr(candidate, 'mega_last', None) and 
                          source.mega_last == candidate.mega_last)
        best_last_match = (getattr(source, 'best_last', None) and getattr(candidate, 'best_last', None) and 
                          source.best_last == candidate.best_last)
        new_last_match = (getattr(source, 'new_last', None) and getattr(candidate, 'new_last', None) and 
                         source.new_last == candidate.new_last)
        
        last_description = "Колодки не совпадают"
        last_match_type = "none"
        max_last_bonus = max(self.config.mega_last_bonus, self.config.best_last_bonus, self.config.new_last_bonus)
        
        if mega_last_match:
            last_description = f"MEGA колодка совпадает (+{self.config.mega_last_bonus} баллов)"
            last_match_type = "exact"
        elif best_last_match:
            last_description = f"BEST колодка совпадает (+{self.config.best_last_bonus} баллов)"
            last_match_type = "exact"
        elif new_last_match:
            last_description = f"NEW колодка совпадает (+{self.config.new_last_bonus} баллов)"
            last_match_type = "exact"
        
        details.append(ScoringDetail(
            parameter="Колодка",
            source_value=f"MEGA: {getattr(source, 'mega_last', 'Нет')}, BEST: {getattr(source, 'best_last', 'Нет')}, NEW: {getattr(source, 'new_last', 'Нет')}",
            candidate_value=f"MEGA: {getattr(candidate, 'mega_last', 'Нет')}, BEST: {getattr(candidate, 'best_last', 'Нет')}, NEW: {getattr(candidate, 'new_last', 'Нет')}",
            score=last_score,
            max_possible=max_last_bonus,
            description=last_description,
            match_type=last_match_type
        ))
        
        # 7. Модель
        model_score = self.recommendation_engine._calculate_model_score(source, candidate)
        details.append(ScoringDetail(
            parameter="Модель",
            source_value=getattr(source, 'model_name', 'Не указана'),
            candidate_value=getattr(candidate, 'model_name', 'Не указана'),
            score=model_score,
            max_possible=self.config.model_match_bonus,
            description=f"Совпадение модели (+{model_score} баллов)" if model_score > 0 else "Модели не совпадают",
            match_type="exact" if model_score > 0 else "none"
        ))
        
        # 8. Остатки на складе
        stock_score = self.recommendation_engine._calculate_stock_score(candidate)
        stock_count = getattr(candidate, 'stock_count', 0)
        stock_description = "Нет остатков"
        if stock_count >= 20:
            stock_description = f"Высокие остатки ({stock_count} шт.) (+{self.config.stock_high_bonus} баллов)"
        elif stock_count >= 6:
            stock_description = f"Средние остатки ({stock_count} шт.) (+{self.config.stock_medium_bonus} баллов)"
        elif stock_count >= 1:
            stock_description = f"Низкие остатки ({stock_count} шт.) (+{self.config.stock_low_bonus} баллов)"
        
        details.append(ScoringDetail(
            parameter="Остатки",
            source_value="Не учитывается",
            candidate_value=f"{stock_count} шт.",
            score=stock_score,
            max_possible=self.config.stock_high_bonus,
            description=stock_description,
            match_type="bonus" if stock_score > 0 else "none"
        ))
        
        # 9. Штраф за отсутствие колодки (если применяется)
        if last_score == 0:
            penalty_multiplier = self.config.no_last_penalty
            penalty_percent = int((1 - penalty_multiplier) * 100)
            details.append(ScoringDetail(
                parameter="Штраф за колодку",
                source_value="Нет совпадений",
                candidate_value="Нет совпадений",
                score=0,  # Штраф применяется как множитель
                max_possible=0,
                description=f"Штраф за отсутствие совпадения колодки (-{penalty_percent}% от общего score)",
                match_type="penalty"
            ))
        
        return details
    
    def _calculate_max_possible_score(self) -> float:
        """Вычисляет максимально возможный score"""
        return (
            self.config.exact_size_weight +
            self.config.season_match_bonus +
            self.config.color_match_bonus +
            self.config.material_match_bonus +
            self.config.fastener_match_bonus +
            max(self.config.mega_last_bonus, self.config.best_last_bonus, self.config.new_last_bonus) +
            self.config.model_match_bonus +
            self.config.stock_high_bonus
        )
    
    def print_comparison_report(self, result: ComparisonResult) -> None:
        """Выводит детальный отчет сравнения в консоль"""
        print(f"\n{'='*80}")
        print(f"СРАВНЕНИЕ ТОВАРОВ OZON")
        print(f"{'='*80}")
        
        print(f"\n📦 ИСХОДНЫЙ ТОВАР: {result.source_vendor_code}")
        source = result.source_product
        print(f"   Название: {getattr(source, 'product_name', 'Не указано')}")
        print(f"   Тип: {getattr(source, 'type', 'Не указан')}")
        print(f"   Пол: {getattr(source, 'gender', 'Не указан')}")
        print(f"   Бренд: {getattr(source, 'oz_brand', 'Не указан')}")
        print(f"   Размер: {getattr(source, 'russian_size', 'Не указан')}")
        
        print(f"\n📦 СРАВНИВАЕМЫЙ ТОВАР: {result.candidate_vendor_code}")
        candidate = result.candidate_product
        print(f"   Название: {getattr(candidate, 'product_name', 'Не указано')}")
        print(f"   Тип: {getattr(candidate, 'type', 'Не указан')}")
        print(f"   Пол: {getattr(candidate, 'gender', 'Не указан')}")
        print(f"   Бренд: {getattr(candidate, 'oz_brand', 'Не указан')}")
        print(f"   Размер: {getattr(candidate, 'russian_size', 'Не указан')}")
        
        print(f"\n🎯 РЕЗУЛЬТАТ СРАВНЕНИЯ:")
        print(f"   Общий Score: {result.total_score:.1f}")
        print(f"   Схожесть: {result.similarity_percentage:.1f}%")
        
        print(f"\n📊 ДЕТАЛЬНЫЙ АНАЛИЗ ПАРАМЕТРОВ:")
        print(f"{'Параметр':<15} {'Исходный':<20} {'Сравниваемый':<20} {'Баллы':<8} {'Макс':<6} {'Описание'}")
        print(f"{'-'*80}")
        
        for detail in result.scoring_details:
            source_val = str(detail.source_value)[:18] + ".." if len(str(detail.source_value)) > 20 else str(detail.source_value)
            candidate_val = str(detail.candidate_value)[:18] + ".." if len(str(detail.candidate_value)) > 20 else str(detail.candidate_value)
            
            # Цветовое кодирование для консоли
            if detail.match_type == "exact":
                status = "✅"
            elif detail.match_type == "bonus":
                status = "💰"
            elif detail.match_type == "penalty":
                status = "❌"
            else:
                status = "⚪"
            
            print(f"{detail.parameter:<15} {source_val:<20} {candidate_val:<20} {detail.score:<8.1f} {detail.max_possible:<6.0f} {status} {detail.description}")
        
        print(f"\n{'='*80}")

def compare_ozon_products(db_conn, source_vendor_code: str, candidate_vendor_code: str, 
                         config: ScoringConfig = None, print_report: bool = True) -> Optional[ComparisonResult]:
    """
    Удобная функция для сравнения двух товаров Ozon
    
    Args:
        db_conn: Соединение с базой данных
        source_vendor_code: Артикул первого товара
        candidate_vendor_code: Артикул второго товара
        config: Конфигурация scoring (опционально)
        print_report: Выводить ли детальный отчет в консоль
        
    Returns:
        ComparisonResult или None при ошибке
    """
    comparator = ProductComparator(db_conn, config)
    result = comparator.compare_products(source_vendor_code, candidate_vendor_code)
    
    if result and print_report:
        comparator.print_comparison_report(result)
    
    return result
