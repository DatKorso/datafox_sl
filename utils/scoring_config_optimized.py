"""
Оптимизированная конфигурация алгоритма рекомендаций для улучшения качества подбора.

Основные изменения:
1. Снижен штраф за отсутствие колодки
2. Понижен порог min_score_threshold
3. Увеличены бонусы за ключевые характеристики
4. Добавлены альтернативные конфигурации
"""

from dataclasses import dataclass
from utils.rich_content_oz import ScoringConfig

@dataclass
class OptimizedScoringConfig(ScoringConfig):
    """Оптимизированная конфигурация для лучшего качества рекомендаций"""
    
    # Базовые параметры - повышаем базовый score
    base_score: int = 120
    max_score: int = 900
    
    # Размер - увеличиваем важность
    exact_size_weight: int = 120
    close_size_weight: int = 60
    size_mismatch_penalty: int = -50
    
    # Сезонность - увеличиваем бонус
    season_match_bonus: int = 100
    season_mismatch_penalty: int = -30
    
    # Второстепенные характеристики - повышаем важность
    color_match_bonus: int = 80
    material_match_bonus: int = 100
    material_mismatch_penalty: int = -50
    fastener_match_bonus: int = 40
    
    # Колодки - увеличиваем бонусы
    mega_last_bonus: int = 120
    best_last_bonus: int = 90
    new_last_bonus: int = 60
    no_last_penalty: float = 0.85  # Менее жесткий штраф (15% вместо 30%)
    
    # Модель (новый параметр из punta_table.model_name)
    model_match_bonus: int = 100
    
    # Остатки - увеличиваем бонусы
    stock_high_bonus: int = 50
    stock_medium_bonus: int = 30
    stock_low_bonus: int = 15
    
    # Лимиты - понижаем порог
    min_score_threshold: float = 35.0  # Было 50.0
    max_recommendations: int = 8  # Фиксированное количество
    min_recommendations: int = 8

@dataclass 
class BalancedScoringConfig(ScoringConfig):
    """Сбалансированная конфигурация - компромисс между качеством и строгостью"""
    
    # Умеренные изменения от базовой конфигурации
    base_score: int = 110
    
    # Размер
    exact_size_weight: int = 110
    close_size_weight: int = 50
    size_mismatch_penalty: int = -40
    
    # Сезонность  
    season_match_bonus: int = 90
    season_mismatch_penalty: int = -30
    
    # Второстепенные
    color_match_bonus: int = 50
    material_match_bonus: int = 50
    fastener_match_bonus: int = 35
    
    # Колодки - умеренное снижение штрафа
    mega_last_bonus: int = 100
    best_last_bonus: int = 80
    new_last_bonus: int = 55
    no_last_penalty: float = 0.8  # 20% штраф вместо 30%
    
    # Модель (новый параметр из punta_table.model_name)
    model_match_bonus: int = 60
    
    # Остатки
    stock_high_bonus: int = 45
    stock_medium_bonus: int = 25
    stock_low_bonus: int = 12
    
    # Лимиты
    min_score_threshold: float = 40.0  # Было 50.0
    max_recommendations: int = 8
    min_recommendations: int = 8

@dataclass
class LenientScoringConfig(ScoringConfig):
    """Мягкая конфигурация - максимально включающая"""
    
    base_score: int = 130
    max_score: int = 700
    
    # Размер - менее критично
    exact_size_weight: int = 100
    close_size_weight: int = 70
    size_mismatch_penalty: int = -20
    
    # Сезонность - менее критично
    season_match_bonus: int = 80
    season_mismatch_penalty: int = -10
    
    # Второстепенные - высокие бонусы
    color_match_bonus: int = 70
    material_match_bonus: int = 70
    fastener_match_bonus: int = 50
    
    # Колодки - минимальный штраф
    mega_last_bonus: int = 130
    best_last_bonus: int = 100
    new_last_bonus: int = 70
    no_last_penalty: float = 0.9  # Только 10% штраф
    
    # Модель (новый параметр из punta_table.model_name)
    model_match_bonus: int = 90
    
    # Остатки
    stock_high_bonus: int = 60
    stock_medium_bonus: int = 35
    stock_low_bonus: int = 20
    
    # Лимиты - очень мягкие
    min_score_threshold: float = 25.0  # Очень низкий порог
    max_recommendations: int = 8
    min_recommendations: int = 8

def get_config_presets():
    """Возвращает словарь с предустановленными конфигурациями"""
    return {
        "default": ScoringConfig(),
        "optimized": OptimizedScoringConfig(),
        "balanced": BalancedScoringConfig(), 
        "lenient": LenientScoringConfig()
    }

def get_config_by_name(name: str) -> ScoringConfig:
    """Получение конфигурации по имени"""
    presets = get_config_presets()
    if name not in presets:
        raise ValueError(f"Неизвестная конфигурация: {name}. Доступные: {list(presets.keys())}")
    return presets[name]
