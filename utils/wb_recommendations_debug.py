"""
Отладочные функции для диагностики проблем WB Recommendations.

Этот модуль содержит функции для:
- Диагностики данных конкретного WB SKU
- Проверки связей WB ↔ Ozon через штрихкоды
- Анализа качества обогащения данных
- Поиска причин отсутствия рекомендаций
"""

import logging
from typing import Dict, List, Any, Optional
from utils.db_connection import connect_db
from utils.cross_marketplace_linker import CrossMarketplaceLinker

logger = logging.getLogger(__name__)

def diagnose_wb_sku(wb_sku: str) -> Dict[str, Any]:
    """
    Полная диагностика WB SKU для выявления проблем с рекомендациями
    
    Args:
        wb_sku: WB SKU для диагностики
        
    Returns:
        Словарь с результатами диагностики
    """
    print(f"\n🔍 ДИАГНОСТИКА WB SKU: {wb_sku}")
    print("=" * 50)
    
    diagnosis = {
        'wb_sku': wb_sku,
        'wb_found': False,
        'wb_data': {},
        'barcodes_count': 0,
        'barcodes': [],
        'ozon_links_found': False,
        'ozon_characteristics': [],
        'enrichment_possible': False,
        'recommendations_blocked_by': [],
        'suggested_fixes': []
    }
    
    try:
        conn = connect_db()
        if not conn:
            diagnosis['error'] = "Не удалось подключиться к базе данных"
            return diagnosis
        
        # 1. Проверяем существование WB товара
        print("\n📋 ШАГ 1: Проверка WB товара...")
        wb_query = "SELECT * FROM wb_products WHERE wb_sku = ?"
        wb_data = conn.execute(wb_query, [wb_sku]).fetchone()
        
        if not wb_data:
            print(f"❌ WB товар {wb_sku} не найден в базе данных!")
            diagnosis['recommendations_blocked_by'].append("WB товар не найден в базе данных")
            diagnosis['suggested_fixes'].append("Проверьте импорт WB данных")
            return diagnosis
        
        diagnosis['wb_found'] = True
        wb_dict = dict(zip([col[0] for col in conn.execute(wb_query, [wb_sku]).description], wb_data))
        diagnosis['wb_data'] = wb_dict
        
        print(f"✅ WB товар найден:")
        print(f"   Brand: {wb_dict.get('wb_brand')}")
        print(f"   Category: {wb_dict.get('wb_category')}")
        print(f"   Stock: {wb_dict.get('wb_fbo_stock', 0)}")
        print(f"   Price: {wb_dict.get('wb_full_price')}")
        
        # 2. Проверяем штрихкоды
        print("\n📊 ШАГ 2: Анализ штрихкодов...")
        wb_barcodes_raw = wb_dict.get('wb_barcodes', '')
        if not wb_barcodes_raw or wb_barcodes_raw.strip() == '':
            print("❌ У WB товара нет штрихкодов!")
            diagnosis['recommendations_blocked_by'].append("Отсутствуют штрихкоды WB товара")
            diagnosis['suggested_fixes'].append("Проверьте качество импорта WB данных")
            return diagnosis
        
        barcodes = [bc.strip() for bc in wb_barcodes_raw.split(';') if bc.strip()]
        diagnosis['barcodes_count'] = len(barcodes)
        diagnosis['barcodes'] = barcodes
        
        print(f"✅ Найдено {len(barcodes)} штрихкодов:")
        for i, bc in enumerate(barcodes[:5], 1):  # Показываем первые 5
            print(f"   {i}. {bc}")
        
        # 3. Поиск связей с Ozon
        print("\n🔗 ШАГ 3: Поиск связей с Ozon товарами...")
        ozon_links_found = 0
        linked_oz_vendor_codes = []
        
        for bc in barcodes[:10]:  # Проверяем первые 10 штрихкодов
            oz_query = "SELECT oz_vendor_code FROM oz_barcodes WHERE oz_barcode = ?"
            oz_results = conn.execute(oz_query, [bc]).fetchall()
            if oz_results:
                print(f"   Штрихкод {bc}: найдено {len(oz_results)} Ozon товаров")
                for oz in oz_results:
                    linked_oz_vendor_codes.append(oz[0])
                ozon_links_found += len(oz_results)
        
        diagnosis['ozon_links_found'] = ozon_links_found > 0
        
        if ozon_links_found == 0:
            print("❌ Не найдено связей с Ozon товарами!")
            diagnosis['recommendations_blocked_by'].append("Нет связей WB ↔ Ozon через штрихкоды")
            diagnosis['suggested_fixes'].append("Проверьте качество штрихкодов в oz_barcodes")
            diagnosis['suggested_fixes'].append("Рассмотрите поиск по частичным совпадениям штрихкодов")
        else:
            print(f"✅ Найдено {ozon_links_found} связей с Ozon товарами")
            
            # 4. Анализ Ozon характеристик
            print("\n📈 ШАГ 4: Анализ Ozon характеристик...")
            unique_oz_codes = list(set(linked_oz_vendor_codes))
            
            if unique_oz_codes:
                placeholders = ','.join(['?' for _ in unique_oz_codes])
                oz_chars_query = f"""
                SELECT DISTINCT
                    ocp.oz_vendor_code,
                    ocp.type,
                    ocp.gender,
                    ocp.oz_brand,
                    ocp.season,
                    ocp.color,
                    ocp.fastener_type
                FROM oz_category_products ocp
                WHERE ocp.oz_vendor_code IN ({placeholders})
                """
                
                oz_chars_results = conn.execute(oz_chars_query, unique_oz_codes).fetchall()
                
                if oz_chars_results:
                    print(f"✅ Найдено {len(oz_chars_results)} записей с характеристиками")
                    
                    # Анализируем полноту данных
                    complete_records = 0
                    for record in oz_chars_results:
                        record_dict = dict(zip(['oz_vendor_code', 'type', 'gender', 'oz_brand', 'season', 'color', 'fastener_type'], record))
                        diagnosis['ozon_characteristics'].append(record_dict)
                        
                        if record_dict['type'] and record_dict['gender'] and record_dict['oz_brand']:
                            complete_records += 1
                    
                    print(f"   Полных записей (type+gender+brand): {complete_records}/{len(oz_chars_results)}")
                    
                    if complete_records > 0:
                        diagnosis['enrichment_possible'] = True
                        print("✅ Обогащение данных возможно!")
                    else:
                        print("❌ Нет полных записей для обогащения!")
                        diagnosis['recommendations_blocked_by'].append("Неполные характеристики в oz_category_products")
                        diagnosis['suggested_fixes'].append("Проверьте качество данных в oz_category_products")
                        diagnosis['suggested_fixes'].append("Рассмотрите более гибкие критерии обогащения")
                else:
                    print("❌ Не найдено характеристик в oz_category_products!")
                    diagnosis['recommendations_blocked_by'].append("Отсутствуют характеристики в oz_category_products")
                    diagnosis['suggested_fixes'].append("Проверьте импорт данных oz_category_products")
        
        # 5. Проверяем возможность поиска кандидатов
        if diagnosis['enrichment_possible']:
            print("\n🎯 ШАГ 5: Проверка поиска кандидатов...")
            
            # Берем характеристики первой полной записи
            complete_record = None
            for record in diagnosis['ozon_characteristics']:
                if record['type'] and record['gender'] and record['oz_brand']:
                    complete_record = record
                    break
            
            if complete_record:
                # Ищем похожие товары
                candidates_query = """
                SELECT DISTINCT ocp.oz_vendor_code
                FROM oz_category_products ocp
                LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
                WHERE ocp.type = ?
                AND ocp.gender = ?
                AND ocp.oz_brand = ?
                AND COALESCE(op.oz_fbo_stock, 0) > 0
                LIMIT 10
                """
                
                candidates = conn.execute(candidates_query, [
                    complete_record['type'],
                    complete_record['gender'], 
                    complete_record['oz_brand']
                ]).fetchall()
                
                print(f"✅ Найдено {len(candidates)} потенциальных Ozon кандидатов")
                
                if len(candidates) > 0:
                    # Ищем WB товары по этим кандидатам
                    oz_codes = [c[0] for c in candidates]
                    placeholders = ','.join(['?' for _ in oz_codes])
                    
                    wb_candidates_query = f"""
                    WITH oz_barcodes_list AS (
                        SELECT DISTINCT ozb.oz_barcode
                        FROM oz_barcodes ozb
                        WHERE ozb.oz_vendor_code IN ({placeholders})
                        AND ozb.oz_barcode IS NOT NULL
                        AND TRIM(ozb.oz_barcode) != ''
                    ),
                    wb_barcodes_split AS (
                        SELECT 
                            wb.wb_sku,
                            TRIM(bc.barcode) as individual_barcode
                        FROM wb_products wb,
                        UNNEST(string_split(wb.wb_barcodes, ';')) AS bc(barcode)
                        WHERE wb.wb_barcodes IS NOT NULL 
                          AND TRIM(wb.wb_barcodes) != ''
                          AND TRIM(bc.barcode) != ''
                          AND wb.wb_sku != ?
                    )
                    SELECT DISTINCT wbs.wb_sku
                    FROM wb_barcodes_split wbs
                    INNER JOIN oz_barcodes_list ozb ON wbs.individual_barcode = ozb.oz_barcode
                    LIMIT 10
                    """
                    
                    wb_candidates = conn.execute(wb_candidates_query, oz_codes + [wb_sku]).fetchall()
                    print(f"✅ Найдено {len(wb_candidates)} потенциальных WB кандидатов")
                    
                    if len(wb_candidates) == 0:
                        diagnosis['recommendations_blocked_by'].append("Нет WB кандидатов после фильтрации")
                        diagnosis['suggested_fixes'].append("Проверьте качество связей штрихкодов WB ↔ Ozon")
                        diagnosis['suggested_fixes'].append("Рассмотрите менее строгие критерии поиска")
        
        # 6. Итоговая диагностика
        print("\n📊 ИТОГОВАЯ ДИАГНОСТИКА:")
        if not diagnosis['recommendations_blocked_by']:
            print("✅ Все проверки пройдены! Рекомендации должны работать.")
        else:
            print("❌ Найдены проблемы, блокирующие рекомендации:")
            for i, problem in enumerate(diagnosis['recommendations_blocked_by'], 1):
                print(f"   {i}. {problem}")
            
            print("\n💡 Предлагаемые исправления:")
            for i, fix in enumerate(diagnosis['suggested_fixes'], 1):
                print(f"   {i}. {fix}")
        
        conn.close()
        return diagnosis
        
    except Exception as e:
        diagnosis['error'] = str(e)
        print(f"❌ Ошибка диагностики: {e}")
        return diagnosis

def test_cross_marketplace_linking(wb_sku: str) -> Dict[str, Any]:
    """
    Тестирование CrossMarketplaceLinker для конкретного WB SKU
    """
    print(f"\n🔗 ТЕСТ CrossMarketplaceLinker для WB SKU: {wb_sku}")
    print("=" * 50)
    
    try:
        conn = connect_db()
        if not conn:
            return {'error': 'Не удалось подключиться к базе данных'}
        
        linker = CrossMarketplaceLinker(conn)
        
        # Тестируем связывание
        print("📊 Тестируем link_wb_to_oz...")
        linked_oz = linker.link_wb_to_oz([wb_sku])
        
        if linked_oz and wb_sku in linked_oz:
            oz_codes = linked_oz[wb_sku]
            print(f"✅ Найдено {len(oz_codes)} связанных Ozon товаров:")
            for i, oz_code in enumerate(oz_codes[:5], 1):
                print(f"   {i}. {oz_code}")
            
            # Тестируем find_linked_products
            print("\n📊 Тестируем find_linked_products...")
            linked_products = linker.find_linked_products([wb_sku], include_wb_data=True)
            
            if linked_products:
                print(f"✅ find_linked_products вернул {len(linked_products)} записей")
                for product in linked_products[:3]:
                    print(f"   WB SKU: {product.get('wb_sku')}")
                    print(f"   Type: {product.get('type')}")
                    print(f"   Gender: {product.get('gender')}")
                    print(f"   Brand: {product.get('oz_brand')}")
                    print("   ---")
            else:
                print("❌ find_linked_products не вернул данных")
                
        else:
            print("❌ link_wb_to_oz не нашел связей")
        
        conn.close()
        return linked_oz
        
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        return {'error': str(e)}

if __name__ == "__main__":
    # Тестируем с заданным SKU
    test_sku = "191813777"
    
    print("🚀 ЗАПУСК ДИАГНОСТИКИ WB RECOMMENDATIONS")
    print("=" * 60)
    
    # Полная диагностика
    diagnosis = diagnose_wb_sku(test_sku)
    
    # Тест CrossMarketplaceLinker
    linking_test = test_cross_marketplace_linking(test_sku)
    
    print("\n🎯 ЗАКЛЮЧЕНИЕ:")
    print("=" * 60)
    if diagnosis.get('enrichment_possible'):
        print("✅ Техническая возможность создания рекомендаций ЕСТЬ")
    else:
        print("❌ Техническая возможность создания рекомендаций ОТСУТСТВУЕТ")
        print("\nОсновные проблемы:")
        for problem in diagnosis.get('recommendations_blocked_by', []):
            print(f"   • {problem}")