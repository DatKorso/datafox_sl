#!/usr/bin/env python3
"""
🆘 Экстренная утилита для экспорта Rich Content из базы данных

Эта утилита предназначена для экспорта больших объемов Rich Content данных
без ограничений памяти браузера и WebSocket соединений.

Использование:
    python utils/emergency_rich_content_export.py --help
    python utils/emergency_rich_content_export.py --all
    python utils/emergency_rich_content_export.py --brand "Nike" --output nike_rich_content.csv
    python utils/emergency_rich_content_export.py --limit 1000 --format json
"""

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

# Добавляем корневую директорию в путь для импорта мод��лей
sys.path.append(str(Path(__file__).parent.parent))

from utils.db_connection import connect_db

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('emergency_export.log')
    ]
)
logger = logging.getLogger(__name__)


class EmergencyRichContentExporter:
    """Экстренный экспортер Rich Content данных"""
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Инициализация экспортера
        
        Args:
            db_path: Путь к базе данных (если None, используется стандартное подключение)
        """
        self.db_conn = connect_db(db_path) if db_path else connect_db()
        if not self.db_conn:
            raise ConnectionError("Не удалось подключиться к базе данных")
        
        logger.info("✅ Подключение к базе данных установлено")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Получение статистики по Rich Content в базе данных
        
        Returns:
            Словарь со статистикой
        """
        logger.info("📊 Получение статистики Rich Content...")
        
        try:
            # Общая статистика
            total_query = """
            SELECT COUNT(*) as total_products
            FROM oz_category_products
            """
            total_result = self.db_conn.execute(total_query).fetchone()
            total_products = total_result[0] if total_result else 0
            
            # Статистика Rich Content
            rich_content_query = """
            SELECT 
                COUNT(*) as with_rich_content,
                COUNT(CASE WHEN LENGTH(rich_content_json) > 100 THEN 1 END) as valid_rich_content,
                AVG(LENGTH(rich_content_json)) as avg_size,
                MAX(LENGTH(rich_content_json)) as max_size,
                MIN(LENGTH(rich_content_json)) as min_size
            FROM oz_category_products
            WHERE rich_content_json IS NOT NULL 
            AND rich_content_json != ''
            """
            rich_result = self.db_conn.execute(rich_content_query).fetchone()
            
            # Статистика по брендам
            brands_query = """
            SELECT 
                oz_brand,
                COUNT(*) as total,
                COUNT(CASE WHEN rich_content_json IS NOT NULL AND rich_content_json != '' 
                      THEN 1 END) as with_rich_content
            FROM oz_category_products
            WHERE oz_brand IS NOT NULL
            GROUP BY oz_brand
            HAVING with_rich_content > 0
            ORDER BY with_rich_content DESC
            LIMIT 10
            """
            brands_results = self.db_conn.execute(brands_query).fetchall()
            
            stats = {
                'total_products': total_products,
                'with_rich_content': rich_result[0] if rich_result else 0,
                'valid_rich_content': rich_result[1] if rich_result else 0,
                'avg_size_bytes': int(rich_result[2]) if rich_result and rich_result[2] else 0,
                'max_size_bytes': rich_result[3] if rich_result else 0,
                'min_size_bytes': rich_result[4] if rich_result else 0,
                'coverage_percent': round((rich_result[0] / total_products * 100), 2) if total_products > 0 else 0,
                'top_brands': [
                    {
                        'brand': row[0],
                        'total': row[1],
                        'with_rich_content': row[2],
                        'coverage_percent': round((row[2] / row[1] * 100), 2)
                    }
                    for row in brands_results
                ]
            }
            
            logger.info(f"📊 Статистика получена: {stats['with_rich_content']} товаров с Rich Content")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
    
    def export_all(
        self, 
        output_file: str = None, 
        format: str = 'csv',
        chunk_size: int = 1000
    ) -> bool:
        """
        Экспорт всех Rich Content данных
        
        Args:
            output_file: Имя выходного файла
            format: Формат экспорта ('csv' или 'json')
            chunk_size: Размер чанка для обработки
            
        Returns:
            True если экспорт успешен
        """
        if not output_file:
            timestamp = int(time.time())
            output_file = f"rich_content_full_export_{timestamp}.{format}"
        
        logger.info(f"📥 Начинаем полный экспорт Rich Content в файл: {output_file}")
        
        try:
            # Получаем общее количество записей
            count_query = """
            SELECT COUNT(*)
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.rich_content_json IS NOT NULL 
            AND ocp.rich_content_json != ''
            AND LENGTH(ocp.rich_content_json) > 10
            """
            total_count = self.db_conn.execute(count_query).fetchone()[0]
            logger.info(f"📊 Найдено {total_count} записей для экспорта")
            
            if total_count == 0:
                logger.warning("⚠️ Нет данных для экспорта")
                return False
            
            # Основной запрос с пагинацией
            base_query = """
            SELECT 
                ocp.oz_vendor_code,
                op.oz_sku,
                ocp.product_name,
                ocp.type,
                ocp.gender,
                ocp.oz_brand,
                ocp.russian_size,
                ocp.season,
                ocp.color,
                op.oz_fbo_stock,
                ocp.rich_content_json,
                LENGTH(ocp.rich_content_json) as json_size
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.rich_content_json IS NOT NULL 
            AND ocp.rich_content_json != ''
            AND LENGTH(ocp.rich_content_json) > 10
            ORDER BY ocp.oz_vendor_code
            LIMIT ? OFFSET ?
            """
            
            exported_count = 0
            
            if format == 'csv':
                with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = [
                        'oz_vendor_code', 'oz_sku', 'product_name', 'type', 'gender',
                        'oz_brand', 'russian_size', 'season', 'color', 'oz_fbo_stock',
                        'rich_content_json', 'json_size_bytes'
                    ]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    # Экспорт по чанкам
                    offset = 0
                    while offset < total_count:
                        logger.info(f"⏳ Обработка чанка {offset + 1}-{min(offset + chunk_size, total_count)} из {total_count}")
                        
                        results = self.db_conn.execute(base_query, [chunk_size, offset]).fetchall()
                        
                        for row in results:
                            writer.writerow({
                                'oz_vendor_code': row[0],
                                'oz_sku': row[1] or '',
                                'product_name': row[2] or '',
                                'type': row[3] or '',
                                'gender': row[4] or '',
                                'oz_brand': row[5] or '',
                                'russian_size': row[6] or '',
                                'season': row[7] or '',
                                'color': row[8] or '',
                                'oz_fbo_stock': row[9] or 0,
                                'rich_content_json': row[10],
                                'json_size_bytes': row[11]
                            })
                            exported_count += 1
                        
                        offset += chunk_size
            
            elif format == 'json':
                all_data = []
                
                # Экспорт по чанкам
                offset = 0
                while offset < total_count:
                    logger.info(f"⏳ Обработка чанка {offset + 1}-{min(offset + chunk_size, total_count)} из {total_count}")
                    
                    results = self.db_conn.execute(base_query, [chunk_size, offset]).fetchall()
                    
                    for row in results:
                        # Парсим Rich Content JSON для валидации
                        try:
                            rich_content_data = json.loads(row[10])
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️ Невалидный JSON для товара {row[0]}")
                            rich_content_data = {"error": "Invalid JSON"}
                        
                        all_data.append({
                            'oz_vendor_code': row[0],
                            'oz_sku': row[1],
                            'product_name': row[2],
                            'type': row[3],
                            'gender': row[4],
                            'oz_brand': row[5],
                            'russian_size': row[6],
                            'season': row[7],
                            'color': row[8],
                            'oz_fbo_stock': row[9],
                            'rich_content_json': rich_content_data,
                            'json_size_bytes': row[11]
                        })
                        exported_count += 1
                    
                    offset += chunk_size
                
                # Сохраняем JSON файл
                with open(output_file, 'w', encoding='utf-8') as jsonfile:
                    json.dump(all_data, jsonfile, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Экспорт завершен: {exported_count} записей сохранено в {output_file}")
            
            # Показываем размер файла
            file_size = Path(output_file).stat().st_size
            file_size_mb = file_size / (1024 * 1024)
            logger.info(f"📊 Размер файла: {file_size_mb:.2f} МБ")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта: {e}")
            return False
    
    def export_by_brand(
        self, 
        brand: str, 
        output_file: str = None, 
        format: str = 'csv'
    ) -> bool:
        """
        Экспорт Rich Content для конкретного бренда
        
        Args:
            brand: Название бренда
            output_file: Имя выходного файла
            format: Формат экспорта
            
        Returns:
            True если экспорт успешен
        """
        if not output_file:
            safe_brand = "".join(c for c in brand if c.isalnum() or c in (' ', '-', '_')).rstrip()
            timestamp = int(time.time())
            output_file = f"rich_content_{safe_brand}_{timestamp}.{format}"
        
        logger.info(f"📥 Экспорт Rich Content для бренда '{brand}' в файл: {output_file}")
        
        try:
            query = """
            SELECT 
                ocp.oz_vendor_code,
                op.oz_sku,
                ocp.product_name,
                ocp.type,
                ocp.gender,
                ocp.oz_brand,
                ocp.russian_size,
                ocp.season,
                ocp.color,
                op.oz_fbo_stock,
                ocp.rich_content_json,
                LENGTH(ocp.rich_content_json) as json_size
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.oz_brand = ?
            AND ocp.rich_content_json IS NOT NULL 
            AND ocp.rich_content_json != ''
            AND LENGTH(ocp.rich_content_json) > 10
            ORDER BY ocp.oz_vendor_code
            """
            
            results = self.db_conn.execute(query, [brand]).fetchall()
            
            if not results:
                logger.warning(f"⚠️ Нет данных Rich Content для бренда '{brand}'")
                return False
            
            logger.info(f"📊 Найдено {len(results)} то��аров с Rich Content для бренда '{brand}'")
            
            if format == 'csv':
                with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = [
                        'oz_vendor_code', 'oz_sku', 'product_name', 'type', 'gender',
                        'oz_brand', 'russian_size', 'season', 'color', 'oz_fbo_stock',
                        'rich_content_json', 'json_size_bytes'
                    ]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for row in results:
                        writer.writerow({
                            'oz_vendor_code': row[0],
                            'oz_sku': row[1] or '',
                            'product_name': row[2] or '',
                            'type': row[3] or '',
                            'gender': row[4] or '',
                            'oz_brand': row[5] or '',
                            'russian_size': row[6] or '',
                            'season': row[7] or '',
                            'color': row[8] or '',
                            'oz_fbo_stock': row[9] or 0,
                            'rich_content_json': row[10],
                            'json_size_bytes': row[11]
                        })
            
            elif format == 'json':
                data = []
                for row in results:
                    try:
                        rich_content_data = json.loads(row[10])
                    except json.JSONDecodeError:
                        logger.warning(f"⚠️ Невалидный JSON для товара {row[0]}")
                        rich_content_data = {"error": "Invalid JSON"}
                    
                    data.append({
                        'oz_vendor_code': row[0],
                        'oz_sku': row[1],
                        'product_name': row[2],
                        'type': row[3],
                        'gender': row[4],
                        'oz_brand': row[5],
                        'russian_size': row[6],
                        'season': row[7],
                        'color': row[8],
                        'oz_fbo_stock': row[9],
                        'rich_content_json': rich_content_data,
                        'json_size_bytes': row[11]
                    })
                
                with open(output_file, 'w', encoding='utf-8') as jsonfile:
                    json.dump(data, jsonfile, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Экспорт завершен: {len(results)} записей сохранено в {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта для бренда '{brand}': {e}")
            return False
    
    def export_limited(
        self, 
        limit: int, 
        output_file: str = None, 
        format: str = 'csv',
        order_by: str = 'oz_vendor_code'
    ) -> bool:
        """
        Экспорт ограниченного количества записей
        
        Args:
            limit: Максимальное количество записей
            output_file: Имя выходного файла
            format: Формат экспорта
            order_by: Поле для сортировки
            
        Returns:
            True если экспорт успешен
        """
        if not output_file:
            timestamp = int(time.time())
            output_file = f"rich_content_limited_{limit}_{timestamp}.{format}"
        
        logger.info(f"📥 Экспорт {limit} записей Rich Content в файл: {output_file}")
        
        try:
            # Валидация поля сортировки
            valid_order_fields = [
                'oz_vendor_code', 'product_name', 'oz_brand', 'type', 
                'oz_fbo_stock', 'json_size'
            ]
            if order_by not in valid_order_fields:
                order_by = 'oz_vendor_code'
                logger.warning(f"⚠️ Неверное поле сортировки, используется 'oz_vendor_code'")
            
            query = f"""
            SELECT 
                ocp.oz_vendor_code,
                op.oz_sku,
                ocp.product_name,
                ocp.type,
                ocp.gender,
                ocp.oz_brand,
                ocp.russian_size,
                ocp.season,
                ocp.color,
                op.oz_fbo_stock,
                ocp.rich_content_json,
                LENGTH(ocp.rich_content_json) as json_size
            FROM oz_category_products ocp
            LEFT JOIN oz_products op ON ocp.oz_vendor_code = op.oz_vendor_code
            WHERE ocp.rich_content_json IS NOT NULL 
            AND ocp.rich_content_json != ''
            AND LENGTH(ocp.rich_content_json) > 10
            ORDER BY {order_by}
            LIMIT ?
            """
            
            results = self.db_conn.execute(query, [limit]).fetchall()
            
            if not results:
                logger.warning("⚠️ Нет данных для экспорта")
                return False
            
            logger.info(f"📊 Найдено {len(results)} записей для экспорта")
            
            if format == 'csv':
                with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = [
                        'oz_vendor_code', 'oz_sku', 'product_name', 'type', 'gender',
                        'oz_brand', 'russian_size', 'season', 'color', 'oz_fbo_stock',
                        'rich_content_json', 'json_size_bytes'
                    ]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for row in results:
                        writer.writerow({
                            'oz_vendor_code': row[0],
                            'oz_sku': row[1] or '',
                            'product_name': row[2] or '',
                            'type': row[3] or '',
                            'gender': row[4] or '',
                            'oz_brand': row[5] or '',
                            'russian_size': row[6] or '',
                            'season': row[7] or '',
                            'color': row[8] or '',
                            'oz_fbo_stock': row[9] or 0,
                            'rich_content_json': row[10],
                            'json_size_bytes': row[11]
                        })
            
            elif format == 'json':
                data = []
                for row in results:
                    try:
                        rich_content_data = json.loads(row[10])
                    except json.JSONDecodeError:
                        logger.warning(f"⚠️ Невалидный JSON для товара {row[0]}")
                        rich_content_data = {"error": "Invalid JSON"}
                    
                    data.append({
                        'oz_vendor_code': row[0],
                        'oz_sku': row[1],
                        'product_name': row[2],
                        'type': row[3],
                        'gender': row[4],
                        'oz_brand': row[5],
                        'russian_size': row[6],
                        'season': row[7],
                        'color': row[8],
                        'oz_fbo_stock': row[9],
                        'rich_content_json': rich_content_data,
                        'json_size_bytes': row[11]
                    })
                
                with open(output_file, 'w', encoding='utf-8') as jsonfile:
                    json.dump(data, jsonfile, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Экспорт завершен: {len(results)} записей сохранено в {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка ограниченного экспорта: {e}")
            return False
    
    def validate_rich_content(self, limit: int = 100) -> Dict[str, Any]:
        """
        Валидация Rich Content JSON в базе данных
        
        Args:
            limit: Количество записей для проверки
            
        Returns:
            Статистика валидации
        """
        logger.info(f"🔍 Валидация {limit} записей Rich Content...")
        
        try:
            query = """
            SELECT oz_vendor_code, rich_content_json
            FROM oz_category_products
            WHERE rich_content_json IS NOT NULL 
            AND rich_content_json != ''
            LIMIT ?
            """
            
            results = self.db_conn.execute(query, [limit]).fetchall()
            
            validation_stats = {
                'total_checked': len(results),
                'valid_json': 0,
                'invalid_json': 0,
                'valid_structure': 0,
                'invalid_structure': 0,
                'errors': []
            }
            
            for vendor_code, rich_content_json in results:
                # Проверка валидности JSON
                try:
                    data = json.loads(rich_content_json)
                    validation_stats['valid_json'] += 1
                    
                    # Проверка структуры Rich Content
                    if isinstance(data, dict) and 'content' in data and 'version' in data:
                        if isinstance(data['content'], list):
                            validation_stats['valid_structure'] += 1
                        else:
                            validation_stats['invalid_structure'] += 1
                            validation_stats['errors'].append(f"{vendor_code}: content не является массивом")
                    else:
                        validation_stats['invalid_structure'] += 1
                        validation_stats['errors'].append(f"{vendor_code}: отсутствуют обязательные поля")
                        
                except json.JSONDecodeError as e:
                    validation_stats['invalid_json'] += 1
                    validation_stats['errors'].append(f"{vendor_code}: JSON ошибка - {str(e)}")
            
            logger.info(f"✅ Валидация завершена: {validation_stats['valid_structure']}/{validation_stats['total_checked']} корректных записей")
            return validation_stats
            
        except Exception as e:
            logger.error(f"❌ Ошибка валидации: {e}")
            return {}
    
    def close(self):
        """Закрытие соединения с базой данных"""
        if self.db_conn:
            self.db_conn.close()
            logger.info("🔌 Соединение с базой данных закрыто")


def main():
    """Главная функция CLI"""
    parser = argparse.ArgumentParser(
        description="🆘 Экстренная утилита для экспорта Rich Content",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Показать статистику
  python utils/emergency_rich_content_export.py --stats

  # Экспорт всех данных в CSV
  python utils/emergency_rich_content_export.py --all

  # Экспорт всех данных в JSON
  python utils/emergency_rich_content_export.py --all --format json

  # Экспорт для конкретного бренда
  python utils/emergency_rich_content_export.py --brand "Nike" --output nike_data.csv

  # Экспорт ограниченного количества записей
  python utils/emergency_rich_content_export.py --limit 1000 --order-by oz_fbo_stock

  # Валидация Rich Content JSON
  python utils/emergency_rich_content_export.py --validate 500
        """
    )
    
    parser.add_argument('--stats', action='store_true', help='Показать статистику Rich Content')
    parser.add_argument('--all', action='store_true', help='Экспорт всех данных')
    parser.add_argument('--brand', type=str, help='Экспорт для конкретного бренда')
    parser.add_argument('--limit', type=int, help='Ограничить количество записей')
    parser.add_argument('--output', type=str, help='Имя выходного файла')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv', help='Формат экспорта')
    parser.add_argument('--order-by', type=str, default='oz_vendor_code', help='Поле для сортировки')
    parser.add_argument('--chunk-size', type=int, default=1000, help='Размер чанка для обработки')
    parser.add_argument('--validate', type=int, help='Валидировать N записей Rich Content JSON')
    parser.add_argument('--db-path', type=str, help='Путь к базе данных')
    
    args = parser.parse_args()
    
    # Проверяем, что указана хотя бы одна операция
    if not any([args.stats, args.all, args.brand, args.limit, args.validate]):
        parser.print_help()
        return
    
    try:
        # Создаем экспортер
        exporter = EmergencyRichContentExporter(args.db_path)
        
        # Выполняем операции
        if args.stats:
            print("\n📊 СТАТИСТИКА RICH CONTENT")
            print("=" * 50)
            stats = exporter.get_statistics()
            
            if stats:
                print(f"Всего товаров: {stats['total_products']:,}")
                print(f"С Rich Content: {stats['with_rich_content']:,}")
                print(f"Валидный Rich Content: {stats['valid_rich_content']:,}")
                print(f"Покрытие: {stats['coverage_percent']}%")
                print(f"Средний размер JSON: {stats['avg_size_bytes']:,} байт")
                print(f"Максимальный размер: {stats['max_size_bytes']:,} байт")
                
                if stats['top_brands']:
                    print("\nТОП-10 бр��ндов с Rich Content:")
                    for i, brand in enumerate(stats['top_brands'], 1):
                        print(f"{i:2d}. {brand['brand']:<20} {brand['with_rich_content']:>6,} товаров ({brand['coverage_percent']:>5.1f}%)")
        
        if args.validate:
            print(f"\n🔍 ВАЛИДАЦИЯ {args.validate} ЗАПИСЕЙ")
            print("=" * 50)
            validation_stats = exporter.validate_rich_content(args.validate)
            
            if validation_stats:
                print(f"Проверено записей: {validation_stats['total_checked']}")
                print(f"Валидный JSON: {validation_stats['valid_json']}")
                print(f"Невалидный JSON: {validation_stats['invalid_json']}")
                print(f"Корректная структура: {validation_stats['valid_structure']}")
                print(f"Некорректная структура: {validation_stats['invalid_structure']}")
                
                if validation_stats['errors']:
                    print(f"\nПервые 10 ошибок:")
                    for error in validation_stats['errors'][:10]:
                        print(f"  ❌ {error}")
        
        if args.all:
            print(f"\n📥 ПОЛНЫЙ ЭКСПОРТ В ФОРМАТЕ {args.format.upper()}")
            print("=" * 50)
            success = exporter.export_all(args.output, args.format, args.chunk_size)
            if success:
                print("✅ Полный экспорт завершен успешно")
            else:
                print("❌ Ошибка полного экспорта")
        
        if args.brand:
            print(f"\n📥 ЭКСПОРТ БРЕНДА '{args.brand}' В ФОРМАТЕ {args.format.upper()}")
            print("=" * 50)
            success = exporter.export_by_brand(args.brand, args.output, args.format)
            if success:
                print(f"✅ Экспорт бренда '{args.brand}' завершен успешно")
            else:
                print(f"❌ Ошибка экспорта бренда '{args.brand}'")
        
        if args.limit:
            print(f"\n📥 ОГРАН��ЧЕННЫЙ ЭКСПОРТ ({args.limit} записей) В ФОРМАТЕ {args.format.upper()}")
            print("=" * 50)
            success = exporter.export_limited(args.limit, args.output, args.format, args.order_by)
            if success:
                print(f"✅ Ограниченный экспорт ({args.limit} записей) завершен успешно")
            else:
                print(f"❌ Ошибка ограниченного экспорта")
        
        # Закрываем соединение
        exporter.close()
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()