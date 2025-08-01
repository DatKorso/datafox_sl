#!/usr/bin/env python3
"""
Тестовый скрипт для отладки цепочки обогащения данных WB товара.

Прослеживает полный путь:
WB SKU → штрихкоды → Ozon товары → oz_vendor_code → oz_category_products → обогащение
"""

import duckdb
import os
from typing import List, Dict, Any, Optional

def test_enrichment_chain(wb_sku: str):
    """Тестирует полную цепочку обогащения данных WB товара"""
    
    print(f"\n🔍 ТЕСТ ЦЕПОЧКИ ОБОГАЩЕНИЯ ДЛЯ WB SKU: {wb_sku}")
    print("=" * 60)
    
    db_path = 'data/marketplace_data.db'
    if not os.path.exists(db_path):
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # ШАГ 1: Получаем базовую информацию WB товара
        print("\n📋 ШАГ 1: Базовая информация WB товара")
        print("-" * 40)
        
        wb_query = """
        SELECT wb_sku, wb_brand, wb_category, wb_barcodes
        FROM wb_products 
        WHERE wb_sku = ?
        """
        wb_data = conn.execute(wb_query, [wb_sku]).fetchone()
        
        if not wb_data:
            print(f"❌ WB товар {wb_sku} не найден!")
            return
        
        wb_sku_val, wb_brand, wb_category, wb_barcodes = wb_data
        print(f"✅ WB товар найден:")
        print(f"   SKU: {wb_sku_val}")
        print(f"   Brand: {wb_brand}")
        print(f"   Category: {wb_category}")
        print(f"   Barcodes: {wb_barcodes}")
        
        if not wb_barcodes or wb_barcodes.strip() == '':
            print("❌ ПРОБЛЕМА: Нет штрих-кодов!")
            return
        
        # ШАГ 2: Разбираем штрих-коды
        print("\n🔗 ШАГ 2: Анализ штрих-кодов")
        print("-" * 40)
        
        barcodes = [bc.strip() for bc in wb_barcodes.split(';') if bc.strip()]
        print(f"✅ Найдено {len(barcodes)} штрих-кодов:")
        for i, bc in enumerate(barcodes, 1):
            print(f"   {i}. {bc}")
        
        # ШАГ 3: Поиск связанных Ozon товаров через штрих-коды
        print("\n🔗 ШАГ 3: Поиск связанных Ozon товаров")
        print("-" * 40)
        
        all_oz_vendor_codes = []
        for i, barcode in enumerate(barcodes, 1):
            oz_query = """
            SELECT oz_vendor_code 
            FROM oz_barcodes 
            WHERE oz_barcode = ?
            """
            oz_results = conn.execute(oz_query, [barcode]).fetchall()
            
            print(f"   Штрих-код {i} ({barcode}): найдено {len(oz_results)} Ozon товаров")
            for oz_result in oz_results:
                oz_vendor_code = oz_result[0]
                all_oz_vendor_codes.append(oz_vendor_code)
                print(f"      → {oz_vendor_code}")
        
        if not all_oz_vendor_codes:
            print("❌ ПРОБЛЕМА: Не найдено связанных Ozon товаров!")
            return
        
        unique_oz_codes = list(set(all_oz_vendor_codes))
        print(f"✅ Всего уникальных Ozon vendor_codes: {len(unique_oz_codes)}")
        
        # ШАГ 4: Проверяем наличие в oz_category_products
        print("\n📊 ШАГ 4: Проверка в oz_category_products")
        print("-" * 40)
        
        placeholders = ','.join(['?' for _ in unique_oz_codes])
        oz_chars_query = f"""
        SELECT 
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
        print(f"Найдено записей в oz_category_products: {len(oz_chars_results)}")
        
        if not oz_chars_results:
            print("❌ КРИТИЧЕСКАЯ ПРОБЛЕМА: Связанные oz_vendor_codes НЕ НАЙДЕНЫ в oz_category_products!")
            print("   Проверим каждый oz_vendor_code отдельно:")
            
            for oz_code in unique_oz_codes[:5]:  # Проверим первые 5
                check_query = "SELECT COUNT(*) FROM oz_category_products WHERE oz_vendor_code = ?"
                count = conn.execute(check_query, [oz_code]).fetchone()[0]
                print(f"   {oz_code}: {count} записей")
                
                if count == 0:
                    # Проверим есть ли вообще такой vendor_code в других таблицах
                    products_check = conn.execute("SELECT COUNT(*) FROM oz_products WHERE oz_vendor_code = ?", [oz_code]).fetchone()[0]
                    barcodes_check = conn.execute("SELECT COUNT(*) FROM oz_barcodes WHERE oz_vendor_code = ?", [oz_code]).fetchone()[0]
                    print(f"      → В oz_products: {products_check}")
                    print(f"      → В oz_barcodes: {barcodes_check}")
            
            print("\n🔧 ВОЗМОЖНЫЕ ПРИЧИНЫ:")
            print("   1. oz_vendor_codes из oz_barcodes не синхронизированы с oz_category_products")
            print("   2. Неполный импорт данных oz_category_products")
            print("   3. Различия в форматах vendor_code между таблицами")
            return
        
        # ШАГ 5: Анализ качества характеристик
        print(f"\n✅ ШАГ 5: Анализ характеристик ({len(oz_chars_results)} записей)")
        print("-" * 40)
        
        complete_records = 0
        incomplete_records = []
        
        for record in oz_chars_results:
            oz_vendor_code, type_val, gender_val, oz_brand_val, season, color, fastener = record
            
            print(f"\nВендор код: {oz_vendor_code}")
            print(f"   Type: {type_val}")
            print(f"   Gender: {gender_val}")
            print(f"   Brand: {oz_brand_val}")
            print(f"   Season: {season}")
            print(f"   Color: {color}")
            print(f"   Fastener: {fastener}")
            
            if type_val and gender_val and oz_brand_val:
                complete_records += 1
                print("   ✅ ПОЛНАЯ ЗАПИСЬ")
            else:
                incomplete_records.append({
                    'vendor_code': oz_vendor_code,
                    'missing': [
                        field for field, value in [
                            ('type', type_val),
                            ('gender', gender_val),
                            ('brand', oz_brand_val)
                        ] if not value
                    ]
                })
                print("   ❌ НЕПОЛНАЯ ЗАПИСЬ")
        
        print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
        print(f"   Всего записей: {len(oz_chars_results)}")
        print(f"   Полных записей: {complete_records}")
        print(f"   Неполных записей: {len(incomplete_records)}")
        
        if complete_records > 0:
            print(f"\n✅ ОБОГАЩЕНИЕ ВОЗМОЖНО! ({complete_records} полных записей)")
            
            # Покажем пример обогащения
            for record in oz_chars_results:
                oz_vendor_code, type_val, gender_val, oz_brand_val, season, color, fastener = record
                if type_val and gender_val and oz_brand_val:
                    print(f"\n🎯 ПРИМЕР ОБОГАЩЕНИЯ:")
                    print(f"   enriched_type: {type_val}")
                    print(f"   enriched_gender: {gender_val}")
                    print(f"   enriched_brand: {oz_brand_val}")
                    break
        else:
            print(f"\n❌ ОБОГАЩЕНИЕ НЕВОЗМОЖНО!")
            print("   Причина: Нет записей с полным набором type+gender+brand")
            
            if incomplete_records:
                print("\n🔧 ДЕТАЛИ НЕПОЛНЫХ ЗАПИСЕЙ:")
                for rec in incomplete_records:
                    print(f"   {rec['vendor_code']}: отсутствуют поля {', '.join(rec['missing'])}")
        
        # ШАГ 6: Проверяем CrossMarketplaceLinker
        print(f"\n🔗 ШАГ 6: Тест CrossMarketplaceLinker")
        print("-" * 40)
        
        try:
            # Импортируем и тестируем CrossMarketplaceLinker
            import sys
            sys.path.append('.')
            from utils.cross_marketplace_linker import CrossMarketplaceLinker
            
            linker = CrossMarketplaceLinker(conn)
            
            # Тестируем link_wb_to_oz
            linked_oz = linker.link_wb_to_oz([wb_sku])
            
            if linked_oz and wb_sku in linked_oz:
                linked_codes = linked_oz[wb_sku]
                print(f"✅ CrossMarketplaceLinker нашел {len(linked_codes)} связанных товаров:")
                for code in linked_codes[:5]:
                    print(f"   → {code}")
                
                # Сравниваем с ручным поиском
                manual_codes = set(unique_oz_codes)
                linker_codes = set(linked_codes)
                
                if manual_codes == linker_codes:
                    print("✅ CrossMarketplaceLinker работает корректно")
                else:
                    print(f"⚠️ Различия между ручным поиском и CrossMarketplaceLinker:")
                    print(f"   Только ручной: {manual_codes - linker_codes}")
                    print(f"   Только linker: {linker_codes - manual_codes}")
            else:
                print("❌ CrossMarketplaceLinker не нашел связей!")
                
        except Exception as e:
            print(f"❌ Ошибка тестирования CrossMarketplaceLinker: {e}")
        
        conn.close()
        
        # ИТОГОВЫЙ ДИАГНОЗ
        print(f"\n🎯 ИТОГОВЫЙ ДИАГНОЗ:")
        print("=" * 60)
        
        if complete_records > 0:
            print("✅ ПРОБЛЕМА РЕШЕНА: Данные для обогащения ЕСТЬ")
            print("   Алгоритм должен работать после исправлений")
        else:
            print("❌ ПРОБЛЕМА ПОДТВЕРЖДЕНА: Нет полных характеристик")
            print("   Необходим fallback алгоритм или исправление данных")
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_enrichment_chain("191813777")