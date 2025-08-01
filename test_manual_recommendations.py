#!/usr/bin/env python3
"""
Тест функционала ручных рекомендаций для WB товаров.

Этот скрипт тестирует интеграцию ManualRecommendationsManager
с системой WB рекомендаций.

Запуск: python test_manual_recommendations.py
"""

import os
import time
import duckdb
import logging
from utils.manual_recommendations_manager import ManualRecommendationsManager
from utils.wb_recommendations import WBRecommendationProcessor, WBScoringConfig

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_manual_recommendations():
    """Основной тест функционала ручных рекомендаций"""
    
    print(f"\n🧪 ТЕСТ ФУНКЦИОНАЛА РУЧНЫХ РЕКОМЕНДАЦИЙ")
    print("=" * 60)
    
    # Проверяем наличие базы данных
    db_path = 'data/marketplace_data.db'
    if not os.path.exists(db_path):
        print(f"❌ База данных не найдена: {db_path}")
        print("   Запустите сначала основное приложение Streamlit для создания базы данных.")
        return False
    
    try:
        # Подключение к базе данных
        conn = duckdb.connect(db_path, read_only=True)
        print(f"✅ Подключение к базе данных успешно")
        
        # 1. Тестируем ManualRecommendationsManager
        print(f"\n🔧 ШАГ 1: Тестирование ManualRecommendationsManager")
        print("-" * 50)
        
        manager = ManualRecommendationsManager()
        
        # Тестовые данные CSV
        test_csv_content = """target_wb_sku,position_1,recommended_sku_1,position_2,recommended_sku_2
191813777,2,232108287,5,226120950
456456,1,789789,3,111222"""
        
        print(f"📄 Загружаем тестовые данные CSV:")
        print(test_csv_content)
        
        success = manager.load_from_csv_string(test_csv_content)
        if success:
            stats = manager.get_statistics()
            print(f"✅ CSV загружен успешно:")
            print(f"   📊 Товаров: {stats['total_targets']}")
            print(f"   📊 Рекомендаций: {stats['total_recommendations']}")
            
            # Проверяем данные
            for target_sku in manager.get_all_target_skus():
                recommendations = manager.get_manual_recommendations(target_sku)
                print(f"   🎯 {target_sku}: {len(recommendations)} рекомендаций")
                for pos, rec_sku in recommendations:
                    print(f"      → Позиция {pos}: {rec_sku}")
        else:
            print(f"❌ Ошибка загрузки CSV")
            return False
        
        # 2. Тестируем интеграцию с WBRecommendationProcessor
        print(f"\n🔧 ШАГ 2: Тестирование интеграции с WBRecommendationProcessor")
        print("-" * 50)
        
        # Создаем процессор с ручными рекомендациями
        config = WBScoringConfig.get_preset('balanced')
        processor = WBRecommendationProcessor(conn, config, manager)
        
        print(f"✅ WBRecommendationProcessor создан с ManualRecommendationsManager")
        
        # Тестируем на реальном WB SKU
        test_sku = "191813777"  # Этот SKU есть в тестовых данных с ручными рекомендациями
        
        print(f"\n🎯 Тестируем рекомендации для WB SKU: {test_sku}")
        print("-" * 40)
        
        start_time = time.time()
        result = processor.process_single_wb_product(test_sku)
        processing_time = time.time() - start_time
        
        print(f"⏱️ Время обработки: {processing_time:.2f}с")
        print(f"📊 Статус: {result.status.value}")
        print(f"📊 Найдено рекомендаций: {len(result.recommendations)}")
        
        if result.success and result.recommendations:
            print(f"\n📋 ДЕТАЛИ РЕКОМЕНДАЦИЙ:")
            print("-" * 40)
            
            manual_count = 0
            algorithmic_count = 0
            
            for i, rec in enumerate(result.recommendations, 1):
                rec_type = "🖐️ РУЧНАЯ" if rec.is_manual else "🤖 Алгоритмическая"
                position_info = f" (требуемая позиция: {rec.manual_position})" if rec.is_manual else ""
                
                print(f"{i:2d}. {rec.product_info.wb_sku} | {rec_type}{position_info}")
                print(f"    Score: {rec.score:.1f}")
                print(f"    Бренд: {rec.product_info.wb_brand}")
                print(f"    Детали: {rec.match_details}")
                print()
                
                if rec.is_manual:
                    manual_count += 1
                else:
                    algorithmic_count += 1
            
            print(f"📊 ИТОГОВАЯ СТАТИСТИКА:")
            print("-" * 40)
            print(f"   🖐️ Ручных рекомендаций: {manual_count}")
            print(f"   🤖 Алгоритмических рекомендаций: {algorithmic_count}")
            print(f"   📈 Всего рекомендаций: {len(result.recommendations)}")
            
            # Проверяем, что ручные рекомендации на правильных местах
            expected_manual = {2: "232108287", 5: "226120950"}  # Из тестовых данных
            
            print(f"\n🔍 ПРОВЕРКА ПОЗИЦИЙ РУЧНЫХ РЕКОМЕНДАЦИЙ:")
            print("-" * 40)
            
            position_correct = True
            for expected_pos, expected_sku in expected_manual.items():
                if expected_pos <= len(result.recommendations):
                    actual_rec = result.recommendations[expected_pos - 1]  # 0-индексированный
                    if actual_rec.product_info.wb_sku == expected_sku and actual_rec.is_manual:
                        print(f"   ✅ Позиция {expected_pos}: {expected_sku} (корректно)")
                    else:
                        print(f"   ❌ Позиция {expected_pos}: ожидался {expected_sku}, получен {actual_rec.product_info.wb_sku}")
                        position_correct = False
                else:
                    print(f"   ❌ Позиция {expected_pos}: отсутствует в результатах")
                    position_correct = False
            
            if position_correct:
                print(f"\n🎉 ТЕСТ УСПЕШНО ПРОЙДЕН!")
                print(f"   ✅ ManualRecommendationsManager работает корректно")
                print(f"   ✅ Интеграция с WBRecommendationProcessor работает")
                print(f"   ✅ Ручные рекомендации размещены на правильных позициях")
                print(f"   ✅ Алгоритмические рекомендации заполняют оставшиеся места")
                
                return True
            else:
                print(f"\n❌ ТЕСТ НЕ ПРОЙДЕН: Ручные рекомендации на неправильных позициях")
                return False
        else:
            print(f"❌ Не удалось получить рекомендации для тестирования")
            print(f"   Причина: {result.error_message}")
            return False
        
    except Exception as e:
        print(f"❌ Критическая ошибка теста: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def test_example_csv_file():
    """Тест примера CSV файла"""
    
    print(f"\n🧪 ТЕСТ ПРИМЕРА CSV ФАЙЛА")
    print("=" * 60)
    
    example_file = "example_manual_recommendations.csv"
    
    if not os.path.exists(example_file):
        print(f"❌ Пример CSV файла не найден: {example_file}")
        return False
    
    try:
        manager = ManualRecommendationsManager()
        
        # Читаем файл
        with open(example_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print(f"📄 Содержимое файла {example_file}:")
        print(content)
        
        # Валидируем формат
        is_valid, message = manager.validate_csv_format(content)
        print(f"\n📊 Валидация формата: {'✅ Корректно' if is_valid else '❌ Ошибка'}")
        print(f"   Сообщение: {message}")
        
        if is_valid:
            # Загружаем данные
            success = manager.load_from_csv_string(content)
            if success:
                stats = manager.get_statistics()
                print(f"\n✅ Файл успешно загружен:")
                print(f"   📊 Товаров: {stats['total_targets']}")
                print(f"   📊 Рекомендаций: {stats['total_recommendations']}")
                return True
            else:
                print(f"❌ Ошибка загрузки файла")
                return False
        else:
            return False
        
    except Exception as e:
        print(f"❌ Ошибка тестирования файла: {e}")
        return False

if __name__ == "__main__":
    print(f"🚀 ЗАПУСК ТЕСТОВ РУЧНЫХ РЕКОМЕНДАЦИЙ")
    print(f"Время: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Тест 1: Пример CSV файла
    test1_result = test_example_csv_file()
    
    # Тест 2: Основной функционал
    test2_result = test_manual_recommendations()
    
    # Итоговый результат
    print(f"\n" + "=" * 60)
    print(f"📊 ИТОГОВЫЕ РЕЗУЛЬТАТЫ ТЕСТОВ:")
    print(f"   Тест примера CSV: {'✅ Пройден' if test1_result else '❌ Не пройден'}")
    print(f"   Тест интеграции: {'✅ Пройден' if test2_result else '❌ Не пройден'}")
    
    if test1_result and test2_result:
        print(f"\n🎉 ВСЕ ТЕСТЫ УСПЕШНО ПРОЙДЕНЫ!")
        print(f"   Функционал ручных рекомендаций готов к использованию.")
    else:
        print(f"\n❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОЙДЕНЫ!")
        print(f"   Требуется дополнительная отладка.")