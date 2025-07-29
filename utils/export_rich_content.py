#!/usr/bin/env python3
"""
CLI утилита для экспорта Rich Content из базы данных.
Обходит ограничения Streamlit для больших объемов данных.

Использование:
    python utils/export_rich_content.py --output rich_content.csv
    python utils/export_rich_content.py --limit 1000 --output sample.csv
    python utils/export_rich_content.py --where "oz_brand = 'Nike'" --output nike_products.csv
"""

import argparse
import csv
import sys
import time
from pathlib import Path

# Добавляем корневую директорию в путь для импорта utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.db_connection import connect_db
from utils.config_utils import get_db_path


def export_rich_content(output_file: str, limit: int = None, where_clause: str = None, 
                       include_empty_sku: bool = True, verbose: bool = False):
    """
    Экспорт Rich Content из базы данных в CSV файл
    
    Args:
        output_file: Путь к выходному CSV файлу
        limit: Ограничение количества записей
        where_clause: Дополнительное SQL условие WHERE
        include_empty_sku: Включать ли товары без oz_sku
        verbose: Подробный вывод
    """
    if verbose:
        print("🔌 Подключение к базе данных...")
    
    try:
        conn = connect_db()
        if not conn:
            print("❌ Ошибка подключения к базе данных")
            print(f"📁 Проверьте путь к БД: {get_db_path()}")
            return False
        
        if verbose:
            print("✅ База данных подключена")
        
        # Составляем SQL запрос
        query = """
        SELECT 
            ocp.oz_vendor_code,
            COALESCE(op.oz_sku, '') as oz_sku,
            ocp.rich_content_json
        FROM oz_category_products ocp
        LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
        WHERE ocp.rich_content_json IS NOT NULL 
        AND ocp.rich_content_json != ''
        AND LENGTH(ocp.rich_content_json) > 10
        """
        
        # Добавляем дополнительные условия
        if not include_empty_sku:
            query += " AND op.oz_sku IS NOT NULL AND op.oz_sku != ''"
        
        if where_clause:
            query += f" AND ({where_clause})"
        
        if limit:
            query += f" LIMIT {limit}"
        
        if verbose:
            print(f"🔍 SQL запрос:")
            print(query)
            print()
        
        # Выполняем запрос
        if verbose:
            print("⏳ Выполнение запроса...")
        
        start_time = time.time()
        cursor = conn.execute(query)
        results = cursor.fetchall()
        query_time = time.time() - start_time
        
        if not results:
            print("⚠️ Не найдено записей для экспорта")
            return False
        
        if verbose:
            print(f"✅ Найдено {len(results):,} записей за {query_time:.2f} секунд")
        
        # Записываем в CSV файл
        if verbose:
            print(f"📝 Запись в файл: {output_file}")
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Заголовки
            writer.writerow(['oz_vendor_code', 'oz_sku', 'rich_content'])
            
            # Данные
            for i, row in enumerate(results, 1):
                writer.writerow([row[0], row[1], row[2]])
                
                # Показываем прогресс для больших файлов
                if verbose and i % 1000 == 0:
                    print(f"📝 Записано {i:,}/{len(results):,} строк...")
        
        # Информация о файле
        file_size = output_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"""
✅ **Экспорт завершен успешно!**
📁 Файл: {output_path.absolute()}
📊 Записей: {len(results):,}
📏 Размер файла: {file_size_mb:.2f} МБ
⏱️ Время выполнения: {query_time:.2f} секунд
""")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка экспорта: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return False
    
    finally:
        if conn:
            conn.close()


def get_statistics(verbose: bool = False):
    """
    Получение статистики по Rich Content в базе данных
    """
    if verbose:
        print("🔌 Подключение к базе данных...")
    
    try:
        conn = connect_db()
        if not conn:
            print("❌ Ошибка подключения к базе данных")
            return False
        
        # Общая статистика
        stats_query = """
        SELECT 
            COUNT(*) as total_products,
            COUNT(CASE WHEN rich_content_json IS NOT NULL AND rich_content_json != '' 
                  AND LENGTH(rich_content_json) > 10 THEN 1 END) as with_rich_content,
            COUNT(CASE WHEN rich_content_json IS NULL THEN 1 END) as null_content,
            COUNT(CASE WHEN rich_content_json = '' THEN 1 END) as empty_content,
            MAX(LENGTH(rich_content_json)) as max_json_size,
            AVG(LENGTH(rich_content_json)) as avg_json_size
        FROM oz_category_products
        """
        
        stats = conn.execute(stats_query).fetchone()
        
        if stats:
            print(f"""
📊 **Статистика Rich Content:**
- Всего товаров в БД: {stats[0]:,}
- С валидным Rich Content: {stats[1]:,}
- С NULL Rich Content: {stats[2]:,}
- С пустым Rich Content: {stats[3]:,}
- Максимальный размер JSON: {stats[4] or 0:,} символов
- Средний размер JSON: {stats[5] or 0:.0f} символов
""")
            
            if stats[1] > 0:
                estimated_size_mb = (stats[1] * (stats[5] or 0)) / (1024 * 1024)
                print(f"📁 **Примерный размер полного экспорта:** {estimated_size_mb:.2f} МБ")
        
        # Детализированная статистика
        detailed_query = """
        SELECT 
            CASE 
                WHEN rich_content_json IS NULL THEN 'NULL'
                WHEN rich_content_json = '' THEN 'Empty'
                WHEN LENGTH(rich_content_json) < 10 THEN 'Too Short'
                ELSE 'Valid'
            END as content_status,
            COUNT(*) as count,
            ROUND(AVG(LENGTH(rich_content_json)), 2) as avg_length
        FROM oz_category_products 
        GROUP BY content_status
        ORDER BY count DESC
        """
        
        print("\n📋 **Детальная статистика:**")
        print(f"{'Статус':<12} {'Количество':<12} {'Средний размер'}")
        print("-" * 40)
        
        for row in conn.execute(detailed_query):
            count = f"{row[1]:,}"
            avg_len = f"{row[2] or 0:.0f}"
            print(f"{row[0]:<12} {count:<12} {avg_len}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")
        return False
    
    finally:
        if conn:
            conn.close()


def main():
    """Главная функция CLI"""
    parser = argparse.ArgumentParser(
        description="Экспорт Rich Content из базы данных",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
    
  Экспорт всех товаров с Rich Content:
    python utils/export_rich_content.py --output rich_content.csv
    
  Экспорт только первых 1000 записей:
    python utils/export_rich_content.py --limit 1000 --output sample.csv
    
  Экспорт товаров определенного бренда:
    python utils/export_rich_content.py --where "oz_brand = 'Nike'" --output nike.csv
    
  Экспорт с дополнительными условиями:
    python utils/export_rich_content.py --where "LENGTH(rich_content_json) > 1000" --output large_content.csv
    
  Получение статистики:
    python utils/export_rich_content.py --stats
    
  Подробный вывод:
    python utils/export_rich_content.py --output export.csv --verbose
        """
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Путь к выходному CSV файлу'
    )
    
    parser.add_argument(
        '--limit', '-l',
        type=int,
        help='Ограничение количества записей'
    )
    
    parser.add_argument(
        '--where', '-w',
        help='Дополнительное SQL условие WHERE'
    )
    
    parser.add_argument(
        '--exclude-empty-sku',
        action='store_true',
        help='Исключить товары без oz_sku'
    )
    
    parser.add_argument(
        '--stats', '-s',
        action='store_true',
        help='Показать только статистику (без экспорта)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Подробный вывод'
    )
    
    args = parser.parse_args()
    
    # Показываем статистику
    if args.stats:
        return get_statistics(args.verbose)
    
    # Проверяем обязательные параметры для экспорта
    if not args.output:
        parser.error("Для экспорта требуется указать --output")
    
    # Выполняем экспорт
    success = export_rich_content(
        output_file=args.output,
        limit=args.limit,
        where_clause=args.where,
        include_empty_sku=not args.exclude_empty_sku,
        verbose=args.verbose
    )
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)