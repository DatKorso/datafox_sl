#!/usr/bin/env python3
"""
Тест для проверки корректности работы алгоритма поиска по колодкам (last).
Проверяет товар 0562002434-черный-34 с колодкой MEGA G0562000198.
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

def test_last_matching():
    """Тест проверки поиска по колодкам"""
    
    # Подключение к базе данных
    db_path = "data/marketplace_data.db"
    if not os.path.exists(db_path):
        logger.error(f"База данных не найдена: {db_path}")
        return
    
    conn = duckdb.connect(db_path)
    
    # Тестируемый товар
    test_vendor_code = "0562002434-черный-34"
    expected_mega_last = "G0562000198"
    
    logger.info(f"🧪 Тестируем товар: {test_vendor_code}")
    logger.info(f"🎯 Ожидаемая MEGA колодка: {expected_mega_last}")
    
    # 1. Проверяем данные исходного товара
    logger.info("\n=== 1. ПРОВЕРКА ИСХОДНОГО ТОВАРА ===")
    
    query_source = """
    SELECT 
        ocp.oz_vendor_code,
        ocp.type,
        ocp.gender,
        ocp.oz_brand,
        ocp.russian_size,
        pt.mega_last,
        pt.best_last,
        pt.new_last,
        pt.model_name
    FROM oz_category_products ocp
    LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
    LEFT JOIN punta_table pt ON op.oz_sku = pt.wb_sku
    WHERE ocp.oz_vendor_code = ?
    """
    
    source_result = conn.execute(query_source, [test_vendor_code]).fetchone()
    
    if not source_result:
        logger.error(f"❌ Товар {test_vendor_code} не найден в базе данных")
        return
    
    source_data = {
        'oz_vendor_code': source_result[0],
        'type': source_result[1], 
        'gender': source_result[2],
        'oz_brand': source_result[3],
        'size': source_result[4],
        'mega_last': source_result[5],
        'best_last': source_result[6],
        'new_last': source_result[7],
        'model_name': source_result[8]
    }
    
    logger.info(f"✅ Исходный товар найден:")
    logger.info(f"   - Тип: {source_data['type']}")
    logger.info(f"   - Пол: {source_data['gender']}")
    logger.info(f"   - Бренд: {source_data['oz_brand']}")
    logger.info(f"   - Размер: {source_data['size']}")
    logger.info(f"   - MEGA колодка: {source_data['mega_last']}")
    logger.info(f"   - BEST колодка: {source_data['best_last']}")
    logger.info(f"   - NEW колодка: {source_data['new_last']}")
    logger.info(f"   - Модель: {source_data['model_name']}")
    
    # Проверяем соответствие ожидаемой колодке
    if source_data['mega_last'] != expected_mega_last:
        logger.warning(f"⚠️  MEGA колодка не соответствует ожидаемой: {source_data['mega_last']} != {expected_mega_last}")
    else:
        logger.info(f"✅ MEGA колодка соответствует ожидаемой: {expected_mega_last}")
    
    # 2. Поиск товаров с такой же колодкой
    logger.info("\n=== 2. ПОИСК ТОВАРОВ С ТАКОЙ ЖЕ КОЛОДКОЙ ===")
    
    query_same_last = """
    SELECT 
        ocp.oz_vendor_code,
        ocp.type,
        ocp.gender, 
        ocp.oz_brand,
        ocp.russian_size,
        pt.mega_last,
        pt.best_last,
        pt.new_last,
        pt.model_name,
        ocp.stock_count
    FROM oz_category_products ocp
    LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
    LEFT JOIN punta_table pt ON op.oz_sku = pt.wb_sku
    WHERE pt.mega_last = ?
    AND ocp.type = ?
    AND ocp.gender = ?
    AND ocp.oz_brand = ?
    AND ocp.oz_vendor_code != ?
    ORDER BY ocp.stock_count DESC, ocp.size
    """
    
    same_last_results = conn.execute(query_same_last, [
        source_data['mega_last'],
        source_data['type'],
        source_data['gender'], 
        source_data['oz_brand'],
        test_vendor_code
    ]).fetchall()
    
    logger.info(f"🔍 Найдено товаров с колодкой {source_data['mega_last']}: {len(same_last_results)}")
    
    if same_last_results:
        logger.info("📋 Список товаров с такой же колодкой:")
        for i, result in enumerate(same_last_results[:10], 1):  # Показываем первые 10
            logger.info(f"   {i:2d}. {result[0]} | Размер: {result[4]} | Остатки: {result[9]} | Модель: {result[8]}")
        
        if len(same_last_results) > 10:
            logger.info(f"   ... и еще {len(same_last_results) - 10} товаров")
    
    # 3. Проверяем работу алгоритма рекомендаций
    logger.info("\n=== 3. ТЕСТИРОВАНИЕ АЛГОРИТМА РЕКОМЕНДАЦИЙ ===")
    
    try:
        # Создаем процессор с дефолтной конфигурацией
        config = ScoringConfig()
        linker = CrossMarketplaceLinker(conn)
        processor = RichContentProcessor(conn, config, linker)
        
        logger.info(f"🔧 Конфигурация колодок:")
        logger.info(f"   - MEGA last bonus: {config.mega_last_bonus}")
        logger.info(f"   - BEST last bonus: {config.best_last_bonus}")
        logger.info(f"   - NEW last bonus: {config.new_last_bonus}")
        logger.info(f"   - No last penalty: {config.no_last_penalty}")
        
        # Получаем рекомендации
        recommendations = processor.recommendation_engine.find_similar_products(test_vendor_code)
        
        logger.info(f"🎯 Найдено рекомендаций: {len(recommendations)}")
        
        if recommendations:
            logger.info("📊 Анализ рекомендаций по колодкам:")
            
            mega_last_matches = 0
            best_last_matches = 0
            new_last_matches = 0
            no_last_matches = 0
            
            for i, rec in enumerate(recommendations[:8], 1):
                product = rec.product_info
                score = rec.score
                
                # Определяем тип совпадения колодки
                last_match_type = "НЕТ"
                if (source_data['mega_last'] and product.mega_last and 
                    source_data['mega_last'] == product.mega_last):
                    last_match_type = "MEGA"
                    mega_last_matches += 1
                elif (source_data['best_last'] and product.best_last and 
                      source_data['best_last'] == product.best_last):
                    last_match_type = "BEST"
                    best_last_matches += 1
                elif (source_data['new_last'] and product.new_last and 
                      source_data['new_last'] == product.new_last):
                    last_match_type = "NEW"
                    new_last_matches += 1
                else:
                    no_last_matches += 1
                
                logger.info(f"   {i:2d}. {product.oz_vendor_code} | Score: {score:.1f} | Колодка: {last_match_type}")
                logger.info(f"       MEGA: {product.mega_last} | BEST: {product.best_last} | NEW: {product.new_last}")
            
            # Статистика совпадений
            logger.info(f"\n📈 Статистика совпадений колодок:")
            logger.info(f"   - MEGA совпадений: {mega_last_matches}")
            logger.info(f"   - BEST совпадений: {best_last_matches}")
            logger.info(f"   - NEW совпадений: {new_last_matches}")
            logger.info(f"   - Без совпадений: {no_last_matches}")
            
            # Проверяем, есть ли штрафы за колодку
            if no_last_matches > 0:
                logger.warning(f"⚠️  {no_last_matches} рекомендаций получили штраф за несовпадение колодки")
                logger.info(f"   Штраф составляет: {int((1 - config.no_last_penalty) * 100)}%")
            
            if mega_last_matches == 0 and len(same_last_results) > 0:
                logger.error(f"❌ ПРОБЛЕМА: Найдено {len(same_last_results)} товаров с такой же MEGA колодкой, но ни один не попал в рекомендации!")
            elif mega_last_matches > 0:
                logger.info(f"✅ {mega_last_matches} рекомендаций имеют совпадающую MEGA колодку")
        
        else:
            logger.error("❌ Рекомендации не найдены")
    
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании алгоритма: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
    
    logger.info("\n🏁 Тест завершен")

if __name__ == "__main__":
    test_last_matching()
