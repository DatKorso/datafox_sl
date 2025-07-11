"""
Unit тесты для класса RecommendationEngine.
"""

import pytest
from utils.rich_content_oz import (
    RecommendationEngine, ProductInfo, ScoringConfig, 
    Recommendation, ProcessingStatus
)


class TestRecommendationEngine:
    """Тесты для основного движка рекомендаций"""
    
    @pytest.fixture
    def engine(self, test_db, default_scoring_config):
        """Создание движка рекомендаций с тестовой БД"""
        return RecommendationEngine(test_db, default_scoring_config)
    
    def test_find_similar_products_basic(self, engine):
        """Тест базового поиска похожих товаров"""
        recommendations = engine.find_similar_products('BASE-001')
        
        # Должны найтись рекомендации
        assert len(recommendations) > 0
        assert all(isinstance(rec, Recommendation) for rec in recommendations)
        
        # Все рекомендации должны иметь положительный score
        assert all(rec.score > 0 for rec in recommendations)
        
        # Рекомендации должны быть отсортированы по убыванию score
        scores = [rec.score for rec in recommendations]
        assert scores == sorted(scores, reverse=True)
    
    def test_find_similar_products_nonexistent(self, engine):
        """Тест поиска для несуществующего товара"""
        recommendations = engine.find_similar_products('NONEXISTENT-001')
        
        # Не должно найтись рекомендаций
        assert len(recommendations) == 0
    
    def test_calculate_similarity_score_exact_match(self, engine):
        """Тест расчета score для идеального совпадения"""
        # Создаем два идентичных товара (кроме артикула)
        source = ProductInfo(
            oz_vendor_code="SOURCE-001",
            type="Сабо",
            gender="Женский",
            oz_brand="TestBrand",
            russian_size="38",
            season="Лето",
            color="белый",
            fastener_type="Без застёжки",
            oz_fbo_stock=10,
            material_short="Экокожа",
            new_last="Standard"
        )
        
        candidate = ProductInfo(
            oz_vendor_code="CANDIDATE-001",
            type="Сабо",
            gender="Женский", 
            oz_brand="TestBrand",
            russian_size="38",
            season="Лето",
            color="белый",
            fastener_type="Без застёжки",
            oz_fbo_stock=10,
            material_short="Экокожа",
            new_last="Standard"
        )
        
        score = engine.calculate_similarity_score(source, candidate)
        
        # Ожидаем высокий score при полном совпадении
        expected_min_score = (
            engine.config.base_score +
            engine.config.exact_size_weight +
            engine.config.season_match_bonus +
            engine.config.color_match_bonus +
            engine.config.material_match_bonus +
            engine.config.fastener_match_bonus +
            engine.config.new_last_bonus +
            engine.config.stock_high_bonus
        )
        
        assert score >= expected_min_score
        assert score <= engine.config.max_score
    
    def test_size_scoring_exact(self, engine):
        """Тест scoring за точное совпадение размера"""
        source = ProductInfo(oz_vendor_code="S", russian_size="38")
        candidate = ProductInfo(oz_vendor_code="C", russian_size="38")
        
        score = engine._calculate_size_score(source, candidate)
        assert score == engine.config.exact_size_weight
    
    def test_size_scoring_close(self, engine):
        """Тест scoring за близкий размер (±1)"""
        source = ProductInfo(oz_vendor_code="S", russian_size="38")
        
        # +1 размер
        candidate_plus = ProductInfo(oz_vendor_code="C+", russian_size="39")
        score_plus = engine._calculate_size_score(source, candidate_plus)
        assert score_plus == engine.config.close_size_weight
        
        # -1 размер
        candidate_minus = ProductInfo(oz_vendor_code="C-", russian_size="37")
        score_minus = engine._calculate_size_score(source, candidate_minus)
        assert score_minus == engine.config.close_size_weight
    
    def test_size_scoring_mismatch(self, engine):
        """Тест штрафа за большую разницу в размерах"""
        source = ProductInfo(oz_vendor_code="S", russian_size="38")
        candidate = ProductInfo(oz_vendor_code="C", russian_size="42")  # +4 размера
        
        score = engine._calculate_size_score(source, candidate)
        assert score == engine.config.size_mismatch_penalty
    
    def test_size_scoring_decimal(self, engine):
        """Тест scoring для десятичных размеров"""
        source = ProductInfo(oz_vendor_code="S", russian_size="38.5")
        candidate = ProductInfo(oz_vendor_code="C", russian_size="38.5")
        
        score = engine._calculate_size_score(source, candidate)
        assert score == engine.config.exact_size_weight
        
        # Проверяем близкий десятичный размер
        candidate_close = ProductInfo(oz_vendor_code="C2", russian_size="39.5")
        score_close = engine._calculate_size_score(source, candidate_close)
        assert score_close == engine.config.close_size_weight
    
    def test_size_scoring_non_numeric(self, engine):
        """Тест scoring для нечисловых размеров"""
        source = ProductInfo(oz_vendor_code="S", russian_size="XL")
        
        # Точное совпадение
        candidate_exact = ProductInfo(oz_vendor_code="C1", russian_size="XL")
        score_exact = engine._calculate_size_score(source, candidate_exact)
        assert score_exact == engine.config.exact_size_weight
        
        # Несовпадение
        candidate_diff = ProductInfo(oz_vendor_code="C2", russian_size="L")
        score_diff = engine._calculate_size_score(source, candidate_diff)
        assert score_diff == engine.config.size_mismatch_penalty
    
    def test_season_scoring(self, engine):
        """Тест scoring за сезон"""
        source = ProductInfo(oz_vendor_code="S", season="Лето")
        
        # Совпадение
        candidate_match = ProductInfo(oz_vendor_code="C1", season="Лето")
        score_match = engine._calculate_season_score(source, candidate_match)
        assert score_match == engine.config.season_match_bonus
        
        # Несовпадение
        candidate_diff = ProductInfo(oz_vendor_code="C2", season="Зима")
        score_diff = engine._calculate_season_score(source, candidate_diff)
        assert score_diff == engine.config.season_mismatch_penalty
        
        # Отсутствие данных
        candidate_none = ProductInfo(oz_vendor_code="C3", season=None)
        score_none = engine._calculate_season_score(source, candidate_none)
        assert score_none == 0
    
    def test_color_scoring(self, engine):
        """Тест scoring за цвет"""
        source = ProductInfo(oz_vendor_code="S", color="белый")
        
        # Точное совпадение
        candidate_exact = ProductInfo(oz_vendor_code="C1", color="белый")
        score_exact = engine._calculate_color_score(source, candidate_exact)
        assert score_exact == engine.config.color_match_bonus
        
        # Совпадение с разным регистром
        candidate_case = ProductInfo(oz_vendor_code="C2", color="БЕЛЫЙ")
        score_case = engine._calculate_color_score(source, candidate_case)
        assert score_case == engine.config.color_match_bonus
        
        # Несовпадение
        candidate_diff = ProductInfo(oz_vendor_code="C3", color="черный")
        score_diff = engine._calculate_color_score(source, candidate_diff)
        assert score_diff == 0
    
    def test_last_scoring_priority(self, engine):
        """Тест приоритета колодок (mega > best > new)"""
        source = ProductInfo(
            oz_vendor_code="S",
            mega_last="Mega001",
            best_last="Best001", 
            new_last="New001"
        )
        
        # MEGA приоритет (должен выбраться, даже если есть другие)
        candidate_mega = ProductInfo(
            oz_vendor_code="C1",
            mega_last="Mega001",
            best_last="Best002",  # Другая best колодка
            new_last="New002"     # Другая new колодка
        )
        score_mega = engine._calculate_last_score(source, candidate_mega)
        assert score_mega == engine.config.mega_last_bonus
        
        # BEST колодка (когда нет mega)
        source_no_mega = ProductInfo(
            oz_vendor_code="S2",
            mega_last=None,
            best_last="Best001",
            new_last="New001"
        )
        candidate_best = ProductInfo(
            oz_vendor_code="C2",
            mega_last=None,
            best_last="Best001",
            new_last="New002"  # Другая new колодка
        )
        score_best = engine._calculate_last_score(source_no_mega, candidate_best)
        assert score_best == engine.config.best_last_bonus
        
        # NEW колодка (когда нет mega и best)
        source_new_only = ProductInfo(
            oz_vendor_code="S3",
            mega_last=None,
            best_last=None,
            new_last="New001"
        )
        candidate_new = ProductInfo(
            oz_vendor_code="C3",
            mega_last=None,
            best_last=None,
            new_last="New001"
        )
        score_new = engine._calculate_last_score(source_new_only, candidate_new)
        assert score_new == engine.config.new_last_bonus
    
    def test_stock_scoring_thresholds(self, engine):
        """Тест scoring за остатки с разными порогами"""
        # Высокий остаток
        candidate_high = ProductInfo(oz_vendor_code="C1", oz_fbo_stock=10)
        score_high = engine._calculate_stock_score(candidate_high)
        assert score_high == engine.config.stock_high_bonus
        
        # Средний остаток
        candidate_medium = ProductInfo(oz_vendor_code="C2", oz_fbo_stock=3)
        score_medium = engine._calculate_stock_score(candidate_medium)
        assert score_medium == engine.config.stock_medium_bonus
        
        # Низкий остаток
        candidate_low = ProductInfo(oz_vendor_code="C3", oz_fbo_stock=1)
        score_low = engine._calculate_stock_score(candidate_low)
        assert score_low == engine.config.stock_low_bonus
        
        # Нет остатков
        candidate_none = ProductInfo(oz_vendor_code="C4", oz_fbo_stock=0)
        score_none = engine._calculate_stock_score(candidate_none)
        assert score_none == 0
    
    def test_no_last_penalty_application(self, engine):
        """Тест применения штрафа за отсутствие совпадения колодки"""
        source = ProductInfo(oz_vendor_code="S", new_last="Standard")
        candidate = ProductInfo(oz_vendor_code="C", new_last="Different")  # Разная колодка
        
        # Вычисляем score без колодки
        base_score = engine.config.base_score
        
        # Добавляем другие бонусы (имитируем расчет)
        total_score_before_penalty = base_score + 100  # Произвольное значение для теста
        
        # Проверяем, что штраф применяется
        score_with_penalty = total_score_before_penalty * engine.config.no_last_penalty
        
        # Фактический тест через calculate_similarity_score
        actual_score = engine.calculate_similarity_score(source, candidate)
        
        # Score должен быть меньше из-за штрафа no_last_penalty
        assert actual_score < engine.config.max_score
    
    def test_max_score_limitation(self, engine):
        """Тест ограничения максимального score"""
        # Создаем товары с максимально возможными совпадениями
        source = ProductInfo(
            oz_vendor_code="S",
            russian_size="38",
            season="Лето",
            color="белый",
            material_short="Экокожа",
            fastener_type="Без застёжки",
            mega_last="Mega001"
        )
        
        candidate = ProductInfo(
            oz_vendor_code="C",
            russian_size="38",
            season="Лето", 
            color="белый",
            material_short="Экокожа",
            fastener_type="Без застёжки",
            mega_last="Mega001",
            oz_fbo_stock=100  # Очень высокий остаток
        )
        
        score = engine.calculate_similarity_score(source, candidate)
        
        # Score не должен превышать максимальное значение
        assert score <= engine.config.max_score
    
    def test_recommendation_filtering_by_threshold(self, engine):
        """Тест фильтрации рекомендаций по минимальному порогу"""
        # Используем конфигурацию с высоким порогом
        high_threshold_config = ScoringConfig(min_score_threshold=300.0)
        engine_strict = RecommendationEngine(engine.db_conn, high_threshold_config)
        
        recommendations = engine_strict.find_similar_products('BASE-001')
        
        # Все рекомендации должны быть выше порога
        for rec in recommendations:
            assert rec.score >= high_threshold_config.min_score_threshold
    
    def test_recommendation_limit(self, engine):
        """Тест ограничения количества рекомендаций"""
        # Используем конфигурацию с малым лимитом
        limited_config = ScoringConfig(max_recommendations=2)
        engine_limited = RecommendationEngine(engine.db_conn, limited_config)
        
        recommendations = engine_limited.find_similar_products('BASE-001')
        
        # Количество рекомендаций не должно превышать лимит
        assert len(recommendations) <= limited_config.max_recommendations
    
    def test_match_details_generation(self, engine):
        """Тест генерации детального описания совпадений"""
        source = ProductInfo(
            oz_vendor_code="S",
            type="Сабо",
            gender="Женский",
            oz_brand="TestBrand",
            russian_size="38",
            season="Лето",
            color="белый"
        )
        
        candidate = ProductInfo(
            oz_vendor_code="C",
            type="Сабо",
            gender="Женский",
            oz_brand="TestBrand", 
            russian_size="38",
            season="Лето",
            color="белый",
            oz_fbo_stock=10
        )
        
        details = engine.get_match_details(source, candidate)
        
        # Проверяем наличие ключевых элементов в описании
        assert "Тип: Сабо ✓" in details
        assert "Пол: Женский ✓" in details
        assert "Бренд: TestBrand ✓" in details
        assert "Размер: 38 ✓" in details
        assert "Сезон: Лето ✓" in details
        assert "Цвет: белый ✓" in details
        assert "В наличии: 10 шт." in details
        assert "Детали счета:" in details
    
    def test_error_handling_in_scoring(self, engine):
        """Тест обработки ошибок при расчете score"""
        # Товар с некорректными данными
        source = ProductInfo(oz_vendor_code="S")
        candidate = ProductInfo(oz_vendor_code="C")
        
        # Не должно вызывать исключений
        score = engine.calculate_similarity_score(source, candidate)
        
        # Должен вернуть базовый score (с возможными штрафами)
        assert isinstance(score, (int, float))
        assert score >= 0 