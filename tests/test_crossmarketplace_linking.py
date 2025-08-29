#!/usr/bin/env python3
"""
Тест для проверки работы CrossMarketplaceLinker с товаром 0562002434-черный-34.
Проверяет, находит ли линкер wb_sku и получает ли данные о колодках из punta_table.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from utils.cross_marketplace_linker import CrossMarketplaceLinker
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_crossmarketplace_linking():
    """Тест проверки CrossMarketplaceLinker для получения wb_sku и данных о колодках"""
    
    # Подключение к базе данных
    db_path = "data/marketplace_data.db"
    if not os.path.exists(db_path):
        logger.error(f"База данных не найдена: {db_path}")
        return
    
    conn = duckdb.connect(db_path)
    
    # Тестируемый товар
    test_vendor_code = "0562002434-черный-34"
    expected_mega_last = "G0562000198"
    
    logger.info(f"🧪 Тестируем CrossMarketplaceLinker для товара: {test_vendor_code}")
    logger.info(f"🎯 Ожидаемая MEGA колодка: {expected_mega_last}")
    
    try:
        # Создаем CrossMarketplaceLinker
        linker = CrossMarketplaceLinker(conn)
        
        # 1. Проверяем базовые данные товара
        logger.info("\n=== 1. ПРОВЕРКА БАЗОВЫХ ДАННЫХ ТОВАРА ===")
        
        base_query = """
        SELECT 
            ocp.oz_vendor_code,
            ocp.type,
            ocp.gender,
            ocp.oz_brand,
            ocp.russian_size,
            ocp.barcode
        FROM oz_category_products ocp
        WHERE ocp.oz_vendor_code = ?
        """
        
        base_result = conn.execute(base_query, [test_vendor_code]).fetchone()
        
        if not base_result:
            logger.error(f"❌ Товар {test_vendor_code} не найден в oz_category_products")
            return
        
        logger.info(f"✅ Базовые данные товара:")
        logger.info(f"   - Артикул: {base_result[0]}")
        logger.info(f"   - Тип: {base_result[1]}")
        logger.info(f"   - Пол: {base_result[2]}")
        logger.info(f"   - Бренд: {base_result[3]}")
        logger.info(f"   - Размер: {base_result[4]}")
        logger.info(f"   - Штрихкод: {base_result[5]}")
        
        # 2. Тестируем поиск связанных WB товаров
        logger.info("\n=== 2. ПОИСК СВЯЗАННЫХ WB ТОВАРОВ ===")
        
        # Используем метод _normalize_and_merge_barcodes
        linked_df = linker._normalize_and_merge_barcodes(
            oz_vendor_codes=[test_vendor_code]
        )
        
        logger.info(f"🔍 Найдено связанных продуктов: {len(linked_df)}")
        
        if not linked_df.empty:
            for i, (_, row) in enumerate(linked_df.iterrows(), 1):
                wb_sku = row.get('wb_sku')
                logger.info(f"   {i}. WB SKU: {wb_sku}")
                logger.info(f"      OZ Vendor Code: {row.get('oz_vendor_code')}")
                logger.info(f"      OZ SKU: {row.get('oz_sku')}")
                logger.info(f"      Barcode: {row.get('barcode')}")
                
                # Проверяем наличие данных в punta_table для этого wb_sku
                if wb_sku:
                    punta_query = """
                    SELECT wb_sku, mega_last, best_last, new_last, model_name
                    FROM punta_table 
                    WHERE wb_sku = ?
                    """
                    
                    punta_result = conn.execute(punta_query, [wb_sku]).fetchone()
                    
                    if punta_result:
                        logger.info(f"      ✅ Найдены данные в punta_table:")
                        logger.info(f"         - MEGA колодка: {punta_result[1]}")
                        logger.info(f"         - BEST колодка: {punta_result[2]}")
                        logger.info(f"         - NEW колодка: {punta_result[3]}")
                        logger.info(f"         - Модель: {punta_result[4]}")
                        
                        # Проверяем соответствие ожидаемой колодке
                        if punta_result[1] == expected_mega_last:
                            logger.info(f"         🎯 MEGA колодка соответствует ожидаемой!")
                        elif punta_result[1]:
                            logger.warning(f"         ⚠️  MEGA колодка не соответствует: {punta_result[1]} != {expected_mega_last}")
                        else:
                            logger.warning(f"         ⚠️  MEGA колодка отсутствует")
                    else:
                        logger.warning(f"      ❌ Данные в punta_table для wb_sku {wb_sku} не найдены")
        else:
            logger.warning("❌ Связанные WB товары не найдены")
        
        # 3. Проверяем прямое связывание через штрихкоды
        logger.info("\n=== 3. ПРЯМАЯ ПРОВЕРКА СВЯЗЫВАНИЯ ЧЕРЕЗ ШТРИХКОДЫ ===")
        
        if base_result[5]:  # barcode
            barcode = base_result[5]
            logger.info(f"🔍 Ищем WB товары с штрихкодом: {barcode}")
            
            wb_barcode_query = """
            SELECT wb_sku, wb_brand, wb_category
            FROM wb_products 
            WHERE wb_barcodes LIKE ?
            """
            
            wb_results = conn.execute(wb_barcode_query, [f"%{barcode}%"]).fetchall()
            
            logger.info(f"📊 Найдено WB товаров с таким штрихкодом: {len(wb_results)}")
            
            for i, wb_result in enumerate(wb_results, 1):
                wb_sku = wb_result[0]
                logger.info(f"   {i}. WB SKU: {wb_sku} | Бренд: {wb_result[1]} | Категория: {wb_result[2]}")
                
                # Проверяем punta_table для каждого найденного wb_sku
                punta_check = conn.execute(
                    "SELECT mega_last, best_last, new_last FROM punta_table WHERE wb_sku = ?", 
                    [wb_sku]
                ).fetchone()
                
                if punta_check:
                    logger.info(f"      Колодки: MEGA={punta_check[0]}, BEST={punta_check[1]}, NEW={punta_check[2]}")
                else:
                    logger.info(f"      Колодки: не найдены в punta_table")
        else:
            logger.warning("⚠️  Штрихкод товара отсутствует")
        
        # 4. Статистика по punta_table
        logger.info("\n=== 4. СТАТИСТИКА PUNTA_TABLE ===")
        
        total_punta = conn.execute("SELECT COUNT(*) FROM punta_table").fetchone()[0]
        with_mega_last = conn.execute("SELECT COUNT(*) FROM punta_table WHERE mega_last IS NOT NULL AND mega_last != ''").fetchone()[0]
        with_expected_last = conn.execute("SELECT COUNT(*) FROM punta_table WHERE mega_last = ?", [expected_mega_last]).fetchone()[0]
        
        logger.info(f"📊 Статистика punta_table:")
        logger.info(f"   - Всего записей: {total_punta}")
        logger.info(f"   - С MEGA колодкой: {with_mega_last}")
        logger.info(f"   - С колодкой {expected_mega_last}: {with_expected_last}")
        
        if with_expected_last > 0:
            logger.info(f"✅ В базе есть товары с ожидаемой колодкой {expected_mega_last}")
            
            # Показываем примеры товаров с такой колодкой
            examples_query = """
            SELECT wb_sku, model_name, best_last, new_last
            FROM punta_table 
            WHERE mega_last = ?
            LIMIT 5
            """
            
            examples = conn.execute(examples_query, [expected_mega_last]).fetchall()
            logger.info(f"   Примеры товаров с колодкой {expected_mega_last}:")
            for example in examples:
                logger.info(f"      WB SKU: {example[0]} | Модель: {example[1]}")
        else:
            logger.warning(f"❌ В базе нет товаров с колодкой {expected_mega_last}")
    
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
    
    logger.info("\n🏁 Тест CrossMarketplaceLinker завершен")

if __name__ == "__main__":
    test_crossmarketplace_linking()
