"""
Unit тесты для класса ScoringConfig.
"""

import pytest
from utils.rich_content_oz import ScoringConfig


class TestScoringConfig:
    """Тесты для конфигурации системы оценки схожести"""
    
    def test_default_config_values(self):
        """Тест дефолтных значений конфигурации"""
        config = ScoringConfig()
        
        # Базовые параметры
        assert config.base_score == 100
        assert config.max_score == 500
        
        # Обязательные критерии должны иметь вес 0 (фильтруются отдельно)
        assert config.exact_type_weight == 0
        assert config.exact_gender_weight == 0
        assert config.exact_brand_weight == 0
        
        # Размер - критический параметр
        assert config.exact_size_weight == 100
        assert config.close_size_weight == 40
        assert config.size_mismatch_penalty == -50
        
        # Сезонность
        assert config.season_match_bonus == 80
        assert config.season_mismatch_penalty == -40
        
        # Второстепенные характеристики
        assert config.color_match_bonus == 40
        assert config.material_match_bonus == 40
        assert config.fastener_match_bonus == 30
        
        # Колодки
        assert config.mega_last_bonus == 90
        assert config.best_last_bonus == 70
        assert config.new_last_bonus == 50
        assert config.no_last_penalty == 0.7
        
        # Остатки
        assert config.stock_high_bonus == 40
        assert config.stock_medium_bonus == 20
        assert config.stock_low_bonus == 10
        assert config.stock_threshold_high == 5
        assert config.stock_threshold_medium == 2
        
        # Лимиты
        assert config.max_recommendations == 8
        assert config.min_score_threshold == 50.0
    
    def test_custom_config_creation(self):
        """Тест создания пользовательской конфигурации"""
        config = ScoringConfig(
            base_score=200,
            max_score=600,
            exact_size_weight=150,
            season_match_bonus=100,
            color_match_bonus=60,
            max_recommendations=10,
            min_score_threshold=75.0
        )
        
        assert config.base_score == 200
        assert config.max_score == 600
        assert config.exact_size_weight == 150
        assert config.season_match_bonus == 100
        assert config.color_match_bonus == 60
        assert config.max_recommendations == 10
        assert config.min_score_threshold == 75.0
        
        # Убеждаемся, что остальные параметры остались дефолтными
        assert config.close_size_weight == 40  # Дефолтное значение
        assert config.material_match_bonus == 40  # Дефолтное значение
    
    def test_config_validation_negative_base_score(self):
        """Тест валидации отрицательного base_score"""
        with pytest.raises(ValueError, match="base_score не может быть отрицательным"):
            ScoringConfig(base_score=-10)
    
    def test_config_validation_max_score_less_than_base(self):
        """Тест валидации max_score меньше base_score"""
        with pytest.raises(ValueError, match="max_score должен быть больше base_score"):
            ScoringConfig(base_score=200, max_score=100)
    
    def test_config_validation_zero_max_recommendations(self):
        """Тест валидации нулевого max_recommendations"""
        with pytest.raises(ValueError, match="max_recommendations должен быть больше 0"):
            ScoringConfig(max_recommendations=0)
    
    def test_config_validation_negative_max_recommendations(self):
        """Тест валидации отрицательного max_recommendations"""
        with pytest.raises(ValueError, match="max_recommendations должен быть больше 0"):
            ScoringConfig(max_recommendations=-5)
    
    def test_preset_balanced(self):
        """Тест предустановленной конфигурации 'balanced'"""
        config = ScoringConfig.get_preset("balanced")
        
        # Должна быть идентична дефолтной конфигурации
        default_config = ScoringConfig()
        
        assert config.base_score == default_config.base_score
        assert config.exact_size_weight == default_config.exact_size_weight
        assert config.season_match_bonus == default_config.season_match_bonus
        assert config.color_match_bonus == default_config.color_match_bonus
        assert config.max_recommendations == default_config.max_recommendations
    
    def test_preset_size_focused(self):
        """Тест предустановленной конфигурации 'size_focused'"""
        config = ScoringConfig.get_preset("size_focused")
        
        # Проверяем специфичные для этого preset параметры
        assert config.exact_size_weight == 150  # Повышенный вес для размера
        assert config.close_size_weight == 60   # Повышенный вес для близкого размера
        assert config.season_match_bonus == 60  # Пониженный вес для сезона
        assert config.color_match_bonus == 20   # Пониженный вес для цвета
        
        # Остальные параметры должны быть дефолтными
        assert config.base_score == 100
        assert config.max_score == 500
    
    def test_preset_seasonal(self):
        """Тест предустановленной конфигурации 'seasonal'"""
        config = ScoringConfig.get_preset("seasonal")
        
        # Проверяем акцент на сезонности
        assert config.season_match_bonus == 120      # Повышенный бонус за сезон
        assert config.season_mismatch_penalty == -60 # Повышенный штраф за несовпадение сезона
        assert config.exact_size_weight == 80        # Пониженный вес размера
        assert config.color_match_bonus == 60        # Повышенный вес цвета
    
    def test_preset_material_focused(self):
        """Тест предустановленной конфигурации 'material_focused'"""
        config = ScoringConfig.get_preset("material_focused")
        
        # Проверяем акцент на материалах и колодках
        assert config.material_match_bonus == 80   # Повышенный вес материала
        assert config.fastener_match_bonus == 60   # Повышенный вес застежки
        assert config.mega_last_bonus == 120       # Повышенный вес MEGA колодки
        assert config.best_last_bonus == 90        # Повышенный вес BEST колодки
        assert config.new_last_bonus == 70         # Повышенный вес NEW колодки
    
    def test_preset_conservative(self):
        """Тест предустановленной конфигурации 'conservative'"""
        config = ScoringConfig.get_preset("conservative")
        
        # Проверяем консервативные настройки
        assert config.min_score_threshold == 100.0  # Высокий порог для рекомендаций
        assert config.season_match_bonus == 60       # Пониженные бонусы
        assert config.color_match_bonus == 20        # Пониженные бонусы
        assert config.max_recommendations == 5       # Меньше рекомендаций
    
    def test_preset_unknown(self):
        """Тест получения неизвестного preset"""
        with pytest.raises(ValueError, match="Неизвестный preset: unknown"):
            ScoringConfig.get_preset("unknown")
    
    def test_all_available_presets(self):
        """Тест всех доступных preset'ов"""
        available_presets = ["balanced", "size_focused", "seasonal", "material_focused", "conservative"]
        
        for preset_name in available_presets:
            config = ScoringConfig.get_preset(preset_name)
            
            # Каждый preset должен создавать валидную конфигурацию
            assert isinstance(config, ScoringConfig)
            assert config.base_score >= 0
            assert config.max_score >= config.base_score
            assert config.max_recommendations > 0
    
    def test_config_immutability_after_creation(self):
        """Тест того, что конфигурация может быть изменена после создания"""
        config = ScoringConfig()
        original_base_score = config.base_score
        
        # Изменяем значение
        config.base_score = 200
        assert config.base_score == 200
        assert config.base_score != original_base_score
    
    def test_extreme_values(self):
        """Тест экстремальных значений конфигурации"""
        # Минимальные валидные значения
        config_min = ScoringConfig(
            base_score=1,
            max_score=1,
            max_recommendations=1,
            min_score_threshold=0.0
        )
        assert config_min.base_score == 1
        assert config_min.max_score == 1
        assert config_min.max_recommendations == 1
        assert config_min.min_score_threshold == 0.0
        
        # Большие значения
        config_max = ScoringConfig(
            base_score=1000,
            max_score=10000,
            exact_size_weight=1000,
            season_match_bonus=1000,
            max_recommendations=100,
            min_score_threshold=1000.0
        )
        assert config_max.base_score == 1000
        assert config_max.max_score == 10000
        assert config_max.exact_size_weight == 1000
        assert config_max.season_match_bonus == 1000
        assert config_max.max_recommendations == 100
        assert config_max.min_score_threshold == 1000.0
    
    def test_penalty_values(self):
        """Тест отрицательных значений (штрафов)"""
        config = ScoringConfig(
            size_mismatch_penalty=-100,
            season_mismatch_penalty=-80
        )
        
        assert config.size_mismatch_penalty == -100
        assert config.season_mismatch_penalty == -80
        
        # Множитель должен быть между 0 и 1
        config_penalty_multiplier = ScoringConfig(no_last_penalty=0.5)
        assert config_penalty_multiplier.no_last_penalty == 0.5
    
    def test_threshold_values(self):
        """Тест пороговых значений для остатков"""
        config = ScoringConfig(
            stock_threshold_high=10,
            stock_threshold_medium=5
        )
        
        assert config.stock_threshold_high == 10
        assert config.stock_threshold_medium == 5
        
        # Проверяем логику: high должен быть больше medium
        # (в реальной реализации можно добавить такую валидацию)
        assert config.stock_threshold_high > config.stock_threshold_medium 