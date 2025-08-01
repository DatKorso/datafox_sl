#!/usr/bin/env python3
"""
Тестовый скрипт для проверки обогащения данных в итоговой таблице рекомендаций.
"""

import duckdb
import os
from utils.wb_recommendations import WBRecommendationProcessor, WBScoringConfig

def test_table_enrichment():
    """Тестирует обогащение данных в итоговой таблице"""
    
    print(f"\n🔍 ТЕСТ ОБОГАЩЕНИЯ ДАННЫХ В ИТОГОВОЙ ТАБЛИЦЕ")
    print("=" * 60)
    
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
        print("-" * 40)
        
        # Получаем рекомендации
        engine = processor.recommendation_engine
        recommendations = engine.find_similar_wb_products(test_sku)
        
        if not recommendations:
            print("❌ Нет рекомендаций для тестирования")
            return
        
        print(f"✅ Найдено {len(recommendations)} рекомендаций")
        
        # Проверяем обогащение каждой рекомендации
        print(f"\n📊 АНАЛИЗ ОБОГАЩЕНИЯ РЕКОМЕНДАЦИЙ:")
        print("-" * 60)
        
        for i, rec in enumerate(recommendations[:5], 1):
            product = rec.product_info
            
            print(f"\n{i}. WB SKU: {product.wb_sku}")
            print(f"   Brand: {product.wb_brand}")
            print(f"   Category: {product.wb_category}")
            print(f"   Stock: {product.wb_fbo_stock}")
            
            # Проверяем обогащенные данные
            print(f"\n   📈 ОБОГАЩЕННЫЕ ДАННЫЕ:")
            print(f"   enriched_type: '{product.enriched_type}'")
            print(f"   enriched_gender: '{product.enriched_gender}'")
            print(f"   enriched_brand: '{product.enriched_brand}'")
            print(f"   enriched_season: '{product.enriched_season}'")
            print(f"   enriched_color: '{product.enriched_color}'")
            print(f"   enriched_material: '{product.enriched_material}'")
            
            # Проверяем punta данные
            print(f"\n   🔧 PUNTA ДАННЫЕ:")
            print(f"   material_short: '{product.punta_material_short}'")
            print(f"   mega_last: '{product.punta_mega_last}'")
            print(f"   best_last: '{product.punta_best_last}'")
            print(f"   new_last: '{product.punta_new_last}'")
            
            # Показываем общую статистику
            enrichment_score = product.get_enrichment_score()
            has_enriched = product.has_enriched_data()
            enrichment_source = product.enrichment_source
            
            print(f"\n   📊 КАЧЕСТВО ОБОГАЩЕНИЯ:")
            print(f"   has_enriched_data: {has_enriched}")
            print(f"   enrichment_score: {enrichment_score:.2%}")
            print(f"   enrichment_source: '{enrichment_source}'")
            
            # Анализируем проблему
            missing_fields = []
            if not product.enriched_type:
                missing_fields.append("enriched_type")
            if not product.enriched_gender:
                missing_fields.append("enriched_gender")
            if not product.enriched_season:
                missing_fields.append("enriched_season")
            if not product.enriched_color:
                missing_fields.append("enriched_color")
            
            if missing_fields:
                print(f"   ❌ ПУСТЫЕ ПОЛЯ: {', '.join(missing_fields)}")
            else:
                print(f"   ✅ ВСЕ ОБОГАЩЕННЫЕ ПОЛЯ ЗАПОЛНЕНЫ")
            
            print("-" * 40)
        
        # Проверяем, как создается итоговая таблица
        print(f"\n📋 ТЕСТ СОЗДАНИЯ ИТОГОВОЙ ТАБЛИЦЫ:")
        print("-" * 60)
        
        # Имитируем создание batch_result
        from utils.wb_recommendations import WBBatchResult, WBProcessingResult, WBProcessingStatus
        
        batch_result = WBBatchResult(
            processed_items=[
                WBProcessingResult(
                    wb_sku=test_sku,
                    status=WBProcessingStatus.SUCCESS,
                    recommendations=recommendations[:3],  # Берем первые 3
                    processing_time=1.0,
                    enrichment_info={}
                )
            ],
            success_count=1,
            error_count=0,
            total_processing_time=1.0
        )
        
        # Импортируем функцию создания таблицы
        import sys
        sys.path.append('pages')
        from pages.16_🎯_Рекомендации_WB import create_recommendations_table
        
        # Создаем таблицу
        recommendations_df = create_recommendations_table(batch_result)
        
        print(f"✅ Таблица создана: {len(recommendations_df)} строк")
        
        # Проверяем проблемные столбцы
        problem_columns = ["Тип", "Пол", "Сезон", "Цвет"]
        
        print(f"\n🔍 АНАЛИЗ ПРОБЛЕМНЫХ СТОЛБЦОВ:")
        for col in problem_columns:
            if col in recommendations_df.columns:
                values = recommendations_df[col].tolist()
                empty_count = sum(1 for v in values if not v or str(v).strip() == '' or str(v) == 'None')
                print(f"   {col}: {len(values) - empty_count}/{len(values)} заполнено")
                
                # Показываем примеры значений
                non_empty = [v for v in values if v and str(v).strip() != '' and str(v) != 'None']
                if non_empty:
                    print(f"      Примеры: {non_empty[:3]}")
                else:
                    print(f"      ❌ ВСЕ ЗНАЧЕНИЯ ПУСТЫЕ!")
            else:
                print(f"   {col}: СТОЛБЕЦ НЕ НАЙДЕН!")
        
        conn.close()
        
        # Итоговый диагноз
        print(f"\n🎯 ИТОГОВЫЙ ДИАГНОЗ:")
        print("=" * 60)
        
        all_empty = all(
            sum(1 for v in recommendations_df[col].tolist() 
                if not v or str(v).strip() == '' or str(v) == 'None') == len(recommendations_df)
            for col in problem_columns if col in recommendations_df.columns
        )
        
        if all_empty:
            print("❌ ПРОБЛЕМА ПОДТВЕРЖДЕНА: Все проблемные столбцы пустые")
            print("   Причина: Кандидаты не обогащаются Ozon данными")
        else:
            print("✅ СТОЛБЦЫ ЗАПОЛНЯЮТСЯ КОРРЕКТНО")
        
    except Exception as e:
        print(f"❌ Критическая ошибка теста: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_table_enrichment()