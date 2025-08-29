#!/usr/bin/env python3
"""
Детальный тест scoring алгоритма для выявления проблем с колодками.
Проверяет, почему товары с совпадающими колодками не попадают в топ рекомендации.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from utils.rich_content_oz import RichContentProcessor
from utils.scoring_config_optimized import ScoringConfig
from utils.cross_marketplace_linker import CrossMarketplaceLinker
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_detailed_scoring():
    """Детальный анализ scoring для товара с колодкой"""
    
    # Подключение к базе данных
    db_path = "data/marketplace_data.db"
    if not os.path.exists(db_path):
        logger.error(f"База данных не найдена: {db_path}")
        return
    
    conn = duckdb.connect(db_path)
    
    # Тестируемый товар
    test_vendor_code = "0562002434-черный-34"
    expected_mega_last = "G0562000198"
    
    logger.info(f"🧪 Детальный анализ scoring для товара: {test_vendor_code}")
    
    try:
        # Создаем процессор
        config = ScoringConfig()
        linker = CrossMarketplaceLinker(conn)
        processor = RichContentProcessor(conn, config)
        
        # Получаем исходный товар
        source_product = processor.recommendation_engine.data_collector.get_full_product_info(test_vendor_code)
        
        if not source_product:
            logger.error(f"❌ Товар {test_vendor_code} не найден")
            return
        
        logger.info(f"✅ Исходный товар:")
        logger.info(f"   - MEGA колодка: {source_product.mega_last}")
        logger.info(f"   - BEST колодка: {source_product.best_last}")
        logger.info(f"   - NEW колодка: {source_product.new_last}")
        logger.info(f"   - Модель: {source_product.model_name}")
        logger.info(f"   - WB SKU: {source_product.wb_sku}")
        
        # Получаем кандидатов
        candidates = processor.recommendation_engine.data_collector.find_similar_products_candidates(source_product)
        logger.info(f"🔍 Найдено кандидатов: {len(candidates)}")
        
        # Обогащаем всех кандидатов
        enriched_candidates = processor.recommendation_engine.data_collector.enrich_with_punta_data(candidates)
        logger.info(f"🔗 Обогащено кандидатов: {len(enriched_candidates)}")
        
        # Ищем кандидатов с совпадающими колодками
        matching_last_candidates = []
        for candidate in enriched_candidates:
            if candidate.mega_last == expected_mega_last:
                matching_last_candidates.append(candidate)
        
        logger.info(f"🎯 Кандидатов с совпадающей MEGA колодкой {expected_mega_last}: {len(matching_last_candidates)}")
        
        if matching_last_candidates:
            logger.info(f"\n=== АНАЛИЗ КАНДИДАТОВ С СОВПАДАЮЩИМИ КОЛОДКАМИ ===")
            
            # Анализируем каждого кандидата с совпадающей колодкой
            for i, candidate in enumerate(matching_last_candidates[:5], 1):  # Первые 5
                score = processor.recommendation_engine.calculate_similarity_score(source_product, candidate)
                details = processor.recommendation_engine.get_match_details(source_product, candidate)
                
                logger.info(f"\n{i}. Кандидат: {candidate.oz_vendor_code}")
                logger.info(f"   Score: {score:.1f}")
                logger.info(f"   MEGA колодка: {candidate.mega_last} ✓")
                logger.info(f"   Модель: {candidate.model_name}")
                logger.info(f"   Размер: {candidate.size}")
                logger.info(f"   Остатки: {candidate.stock_count}")
                logger.info(f"   Детали совпадений:")
                for detail in details:
                    logger.info(f"      - {detail}")
        
        # Получаем финальные рекомендации для сравнения
        final_recommendations = processor.recommendation_engine.find_similar_products(test_vendor_code)
        
        logger.info(f"\n=== ФИНАЛЬНЫЕ РЕКОМЕНДАЦИИ (ТОП-8) ===")
        
        mega_last_in_final = 0
        for i, rec in enumerate(final_recommendations, 1):
            candidate = rec.product_info
            is_mega_match = candidate.mega_last == expected_mega_last
            if is_mega_match:
                mega_last_in_final += 1
            
            logger.info(f"{i}. {candidate.oz_vendor_code} | Score: {rec.score:.1f}")
            logger.info(f"   MEGA колодка: {candidate.mega_last} {'✓' if is_mega_match else '✗'}")
            logger.info(f"   Размер: {candidate.size} | Остатки: {candidate.stock_count}")
        
        logger.info(f"\n📊 Итоговая статистика:")
        logger.info(f"   - Кандидатов с совпадающей колодкой: {len(matching_last_candidates)}")
        logger.info(f"   - В финальных рекомендациях: {mega_last_in_final}")
        
        if len(matching_last_candidates) > 0 and mega_last_in_final == 0:
            logger.error(f"❌ ПРОБЛЕМА: Есть кандидаты с совпадающей колодкой, но они не попали в топ-8!")
            
            # Сравниваем лучшего кандидата с колодкой и худшую финальную рекомендацию
            if matching_last_candidates and final_recommendations:
                best_matching = max(matching_last_candidates, 
                                  key=lambda c: processor.recommendation_engine.calculate_similarity_score(source_product, c))
                worst_final = final_recommendations[-1]
                
                best_score = processor.recommendation_engine.calculate_similarity_score(source_product, best_matching)
                worst_score = worst_final.score
                
                logger.info(f"\n🔍 Сравнение:")
                logger.info(f"   Лучший с колодкой: {best_matching.oz_vendor_code} | Score: {best_score:.1f}")
                logger.info(f"   Худший в топ-8: {worst_final.product_info.oz_vendor_code} | Score: {worst_score:.1f}")
                logger.info(f"   Разница: {best_score - worst_score:.1f} баллов")
        
        elif mega_last_in_final > 0:
            logger.info(f"✅ {mega_last_in_final} товаров с совпадающей колодкой попали в финальные рекомендации")
    
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
    
    logger.info("\n🏁 Детальный анализ завершен")

if __name__ == "__main__":
    test_detailed_scoring()
