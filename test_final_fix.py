#!/usr/bin/env python3
"""
Финальный тест исправленного алгоритма WB рекомендаций.
"""

import duckdb
import os
from utils.wb_recommendations import WBRecommendationProcessor, WBScoringConfig

def test_fixed_recommendations():
    """Тестирует исправленный алгоритм рекомендаций"""
    
    print(f"\n🚀 ФИНАЛЬНЫЙ ТЕСТ ИСПРАВЛЕННОГО АЛГОРИТМА WB РЕКОМЕНДАЦИЙ")
    print("=" * 70)
    
    db_path = 'data/marketplace_data.db'
    if not os.path.exists(db_path):
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Инициализация процессора
        config = WBScoringConfig.get_preset('balanced')
        processor = WBRecommendationProcessor(conn, config)
        
        # Тестовый SKU
        test_sku = "191813777"
        print(f"\n🎯 ТЕСТИРУЕМ WB SKU: {test_sku}")
        print("-" * 50)
        
        # Тестируем движок рекомендаций
        engine = processor.recommendation_engine
        recommendations = engine.find_similar_wb_products(test_sku)
        
        print(f"\n📊 РЕЗУЛЬТАТЫ ТЕСТА:")
        print(f"Найдено рекомендаций: {len(recommendations)}")
        
        if recommendations:
            print(f"\n✅ УСПЕХ! Рекомендации найдены:")
            print("-" * 50)
            
            for i, rec in enumerate(recommendations[:10], 1):
                print(f"{i:2d}. WB SKU: {rec.product_info.wb_sku}")
                print(f"    Score: {rec.score:.1f}")
                print(f"    Brand: {rec.product_info.wb_brand}")
                print(f"    Category: {rec.product_info.wb_category}")
                print(f"    Stock: {rec.product_info.wb_fbo_stock}")
                print(f"    Price: {rec.product_info.wb_full_price}")
                
                # Показываем детали совпадения (первые 100 символов)
                match_details = rec.match_details or ""
                if len(match_details) > 100:
                    match_details = match_details[:100] + "..."
                print(f"    Match: {match_details}")
                print()
            
            # Статистика
            scores = [r.score for r in recommendations]
            print(f"📈 СТАТИСТИКА:")
            print(f"   Score диапазон: {min(scores):.1f} - {max(scores):.1f}")
            print(f"   Средний score: {sum(scores)/len(scores):.1f}")
            
            # Проверяем источник обогащения
            source_info = processor.data_collector.get_wb_product_info(test_sku)
            if source_info:
                print(f"\n🔍 ИНФОРМАЦИЯ ОБ ИСТОЧНИКЕ:")
                print(f"   Источник обогащения: {source_info.enrichment_source}")
                print(f"   Качество обогащения: {source_info.get_enrichment_score():.2%}")
                print(f"   Есть обогащенные данные: {source_info.has_enriched_data()}")
            
        else:
            print(f"\n❌ НЕУДАЧА: Рекомендации НЕ найдены")
            
            # Диагностика
            print(f"\n🔍 ДИАГНОСТИКА:")
            source_info = processor.data_collector.get_wb_product_info(test_sku)
            if source_info:
                print(f"   WB товар найден: ✅")
                print(f"   Есть штрихкоды: {'✅' if source_info.wb_barcodes else '❌'}")
                print(f"   Обогащенные данные: {'✅' if source_info.has_enriched_data() else '❌'}")
                print(f"   enriched_type: {source_info.enriched_type}")
                print(f"   enriched_gender: {source_info.enriched_gender}")
                print(f"   enriched_brand: {source_info.enriched_brand}")
            else:
                print(f"   WB товар найден: ❌")
        
        conn.close()
        
        # Итоговый результат
        print(f"\n🎯 ИТОГОВЫЙ РЕЗУЛЬТАТ:")
        print("=" * 70)
        
        if recommendations:
            print("✅ ПРОБЛЕМА РЕШЕНА! Алгоритм рекомендаций работает корректно.")
            print(f"   Найдено {len(recommendations)} рекомендаций для WB SKU {test_sku}")
            print("   Исправления успешно устранили проблему с обогащением данных.")
        else:
            print("❌ ПРОБЛЕМА НЕ РЕШЕНА. Алгоритм по-прежнему не находит рекомендации.")
            print("   Требуется дополнительная диагностика.")
        
    except Exception as e:
        print(f"❌ Критическая ошибка теста: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fixed_recommendations()