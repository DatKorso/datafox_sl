#!/usr/bin/env python3
"""
Тест функционала сравнения товаров с детальным анализом scoring.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from utils.product_comparison import compare_ozon_products
from utils.scoring_config_optimized import ScoringConfig
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_product_comparison():
    """Тест сравнения двух товаров"""
    
    # Подключение к базе данных
    db_path = "data/marketplace_data.db"
    if not os.path.exists(db_path):
        logger.error(f"База данных не найдена: {db_path}")
        return
    
    conn = duckdb.connect(db_path)
    
    try:
        # Тестовые товары
        source_product = "0562002434-черный-34"  # Товар с колодкой G0562000198
        candidate_product = "0562002434-черный-35"  # Тот же товар, но другой размер
        
        logger.info(f"🧪 Тестируем сравнение товаров:")
        logger.info(f"   Исходный: {source_product}")
        logger.info(f"   Кандидат: {candidate_product}")
        
        # Выполняем сравнение
        result = compare_ozon_products(
            db_conn=conn,
            source_vendor_code=source_product,
            candidate_vendor_code=candidate_product,
            config=ScoringConfig(),
            print_report=True
        )
        
        if result:
            logger.info(f"✅ Сравнение выполнено успешно")
            logger.info(f"   Score: {result.total_score:.1f}")
            logger.info(f"   Схожесть: {result.similarity_percentage:.1f}%")
        else:
            logger.error(f"❌ Ошибка сравнения товаров")
        
        # Дополнительный тест с товарами разных размеров
        print(f"\n" + "="*80)
        print("ДОПОЛНИТЕЛЬНЫЙ ТЕСТ: Сравнение товаров с одинаковой колодкой")
        print("="*80)
        
        # Найдем товар с такой же колодкой, но другого размера
        same_last_query = """
        SELECT DISTINCT ocp.oz_vendor_code
        FROM oz_category_products ocp
        LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
        LEFT JOIN punta_table pt ON op.oz_sku = pt.wb_sku
        WHERE pt.mega_last = 'G0562000198'
        AND ocp.oz_vendor_code != ?
        AND ocp.type = 'Туфли'
        AND ocp.gender = 'Девочки'
        AND ocp.oz_brand = 'Shuzzi'
        LIMIT 3
        """
        
        same_last_products = conn.execute(same_last_query, [source_product]).fetchall()
        
        if same_last_products:
            for i, (candidate_code,) in enumerate(same_last_products, 1):
                print(f"\n--- Сравнение {i}: {source_product} vs {candidate_code} ---")
                
                result = compare_ozon_products(
                    db_conn=conn,
                    source_vendor_code=source_product,
                    candidate_vendor_code=candidate_code,
                    config=ScoringConfig(),
                    print_report=True
                )
                
                if result:
                    print(f"Score: {result.total_score:.1f}, Схожесть: {result.similarity_percentage:.1f}%")
        else:
            logger.warning("Не найдены товары с такой же колодкой для дополнительного теста")
    
    except Exception as e:
        logger.error(f"❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
    
    logger.info("\n🏁 Тест сравнения товаров завершен")

if __name__ == "__main__":
    test_product_comparison()
