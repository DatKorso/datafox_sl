#!/usr/bin/env python3
"""
Утилита для экспорта схемы базы данных DuckDB в SQL файл.

Поддерживает экспорт:
- CREATE TABLE statements для всех таблиц
- CREATE SEQUENCE statements
- CREATE INDEX statements  
- Опционально: INSERT statements с данными

Использование:
    python utils/export_db_schema.py --output schema.sql
    python utils/export_db_schema.py --output full_backup.sql --include-data
    python utils/export_db_schema.py --tables oz_products,wb_products --output selected.sql
"""

import argparse
import sys
import time
from pathlib import Path
from typing import List, Optional, Dict, Any

# Добавляем корневую директорию в путь для импорта utils
sys.path.append(str(Path(__file__).parent.parent))

import duckdb

from utils.db_connection import connect_db
from utils.config_utils import get_db_path
from utils.db_schema import get_defined_table_names


def get_database_info(conn) -> Dict[str, Any]:
    """
    Получение общей информации о базе данных
    
    Returns:
        Словарь с информацией о БД
    """
    try:
        # Получаем список всех таблиц
        tables_query = """
        SELECT table_name, table_type 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
        ORDER BY table_name
        """
        try:
            tables = conn.execute(tables_query).fetchall()
        except:
            # Fallback для старых версий DuckDB
            tables_query_alt = "SHOW TABLES"
            try:
                result = conn.execute(tables_query_alt).fetchall()
                tables = [(row[0], 'BASE TABLE') for row in result]
            except:
                tables = []
        
        # Получаем список последовательностей
        sequences = []
        try:
            # Сначала пробуем pg_catalog.pg_sequences
            sequences_query = """
            SELECT sequence_name 
            FROM pg_catalog.pg_sequences 
            WHERE schemaname = 'main'
            ORDER BY sequence_name
            """
            seq_result = conn.execute(sequences_query).fetchall()
            sequences = [row[0] for row in seq_result]
        except:
            # Попробуем duckdb_sequences()
            try:
                seq_result = conn.execute("SELECT sequence_name FROM duckdb_sequences()").fetchall()
                sequences = [row[0] for row in seq_result]
            except:
                # Попробуем альтернативный способ
                try:
                    seq_result = conn.execute("SHOW SEQUENCES").fetchall()
                    sequences = [row[0] for row in seq_result]
                except:
                    sequences = []
        
        return {
            'tables': tables,
            'sequences': sequences,
            'database_version': conn.execute("SELECT version()").fetchone()[0]
        }
        
    except Exception as e:
        print(f"⚠️ Предупреждение: Не удалось получить полную информацию о БД: {e}")
        return {
            'tables': [],
            'sequences': [],
            'database_version': 'unknown'
        }


def get_table_schema(conn, table_name: str) -> Optional[str]:
    """
    Получение CREATE TABLE statement для таблицы
    
    Args:
        conn: Подключение к БД
        table_name: Имя таблицы
        
    Returns:
        SQL строка CREATE TABLE или None если ошибка
    """
    try:
        # Получаем информацию о колонках
        try:
            columns_query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                is_identity
            FROM information_schema.columns 
            WHERE table_name = ? AND table_schema = 'main'
            ORDER BY ordinal_position
            """
            columns = conn.execute(columns_query, [table_name]).fetchall()
        except:
            # Fallback: используем DESCRIBE
            try:
                describe_result = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
                columns = []
                for row in describe_result:
                    # DuckDB DESCRIBE возвращает: column_name, column_type, null, key, default, extra
                    col_name = row[0]
                    col_type = row[1]
                    is_nullable = "YES" if row[2] == "YES" else "NO"
                    col_default = row[4] if len(row) > 4 else None
                    is_identity = "NO"  # По умолчанию
                    columns.append((col_name, col_type, is_nullable, col_default, is_identity))
            except Exception as e:
                print(f"❌ Ошибка получения информации о колонках для {table_name}: {e}")
                return None
        
        if not columns:
            return None
            
        # Формируем CREATE TABLE statement
        table_sql = f'CREATE TABLE "{table_name}" (\n'
        
        column_definitions = []
        for col in columns:
            col_name, data_type, is_nullable, col_default, is_identity = col
            
            # Формируем определение колонки
            col_def = f'    "{col_name}" {data_type}'
            
            # Добавляем ограничения
            if is_identity == 'YES':
                col_def += ' GENERATED ALWAYS AS IDENTITY'
            elif col_default and col_default != 'NULL':
                col_def += f' DEFAULT {col_default}'
                
            if is_nullable == 'NO':
                col_def += ' NOT NULL'
                
            column_definitions.append(col_def)
        
        table_sql += ',\n'.join(column_definitions)
        table_sql += '\n);'
        
        return table_sql
        
    except Exception as e:
        print(f"❌ Ошибка получения схемы таблицы {table_name}: {e}")
        return None


def get_table_indexes(conn, table_name: str) -> List[str]:
    """
    Получение CREATE INDEX statements для таблицы
    
    Args:
        conn: Подключение к БД
        table_name: Имя таблицы
        
    Returns:
        Список SQL строк CREATE INDEX
    """
    try:
        # DuckDB не всегда поддерживает information_schema для индексов
        # Попробуем альтернативный способ
        indexes_query = """
        SELECT sql 
        FROM sqlite_master 
        WHERE type = 'index' AND tbl_name = ?
        AND sql IS NOT NULL
        """
        
        try:
            indexes = conn.execute(indexes_query, [table_name]).fetchall()
            return [row[0] + ';' for row in indexes if row[0]]
        except:
            # Если sqlite_master не доступен, возвращаем пустой список
            return []
            
    except Exception as e:
        print(f"⚠️ Предупреждение: Не удалось получить индексы для {table_name}: {e}")
        return []


def get_sequence_schema(conn, sequence_name: str) -> Optional[str]:
    """
    Получение CREATE SEQUENCE statement
    
    Args:
        conn: Подключение к БД
        sequence_name: Имя последовательности
        
    Returns:
        SQL строка CREATE SEQUENCE или None
    """
    try:
        # Сначала пробуем получить готовый CREATE statement из duckdb_sequences()
        try:
            sql_query = "SELECT sql FROM duckdb_sequences() WHERE sequence_name = ?"
            result = conn.execute(sql_query, [sequence_name]).fetchone()
            if result and result[0]:
                return result[0]
        except:
            pass
        
        # Если не получилось, пробуем pg_catalog.pg_sequences
        try:
            seq_query = """
            SELECT 
                start_value,
                min_value,
                max_value,
                increment_by
            FROM pg_catalog.pg_sequences 
            WHERE sequence_name = ? AND schemaname = 'main'
            """
            
            seq_info = conn.execute(seq_query, [sequence_name]).fetchone()
            
            if seq_info:
                start_val, min_val, max_val, increment = seq_info
                seq_sql = f'CREATE SEQUENCE "{sequence_name}"'
                
                if start_val is not None:
                    seq_sql += f' START {start_val}'
                if increment and increment != 1:
                    seq_sql += f' INCREMENT {increment}'
                if min_val is not None:
                    seq_sql += f' MINVALUE {min_val}'
                if max_val is not None:
                    seq_sql += f' MAXVALUE {max_val}'
                    
                return seq_sql + ';'
        except:
            pass
        
        return None
        
    except Exception as e:
        print(f"❌ Ошибка получения схемы последовательности {sequence_name}: {e}")
        return None


def get_table_data(conn, table_name: str, limit: Optional[int] = None) -> List[str]:
    """
    Получение INSERT statements с данными таблицы
    
    Args:
        conn: Подключение к БД
        table_name: Имя таблицы
        limit: Ограничение количества записей
        
    Returns:
        Список SQL строк INSERT
    """
    try:
        # Получаем информацию о колонках
        try:
            columns_query = """
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = ? AND table_schema = 'main'
            ORDER BY ordinal_position
            """
            columns = conn.execute(columns_query, [table_name]).fetchall()
            column_names = [col[0] for col in columns]
        except:
            # Fallback: используем DESCRIBE
            describe_result = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
            column_names = [row[0] for row in describe_result]
        
        if not column_names:
            return []
        
        # Формируем запрос для получения данных
        select_query = f'SELECT * FROM "{table_name}"'
        if limit:
            select_query += f' LIMIT {limit}'
            
        rows = conn.execute(select_query).fetchall()
        
        if not rows:
            return []
        
        # Формируем INSERT statements
        insert_statements = []
        quoted_columns = ', '.join([f'"{col}"' for col in column_names])
        
        for row in rows:
            # Форматируем значения
            formatted_values = []
            for value in row:
                if value is None:
                    formatted_values.append('NULL')
                elif isinstance(value, str):
                    # Экранируем одинарные кавычки
                    escaped_value = value.replace("'", "''")
                    formatted_values.append(f"'{escaped_value}'")
                elif isinstance(value, (int, float)):
                    formatted_values.append(str(value))
                else:
                    # Для остальных типов конвертируем в строку
                    formatted_values.append(f"'{str(value)}'")
            
            values_str = ', '.join(formatted_values)
            insert_sql = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({values_str});'
            insert_statements.append(insert_sql)
        
        return insert_statements
        
    except Exception as e:
        print(f"❌ Ошибка получения данных таблицы {table_name}: {e}")
        return []


def export_database_schema(
    output_file: str,
    tables: Optional[List[str]] = None,
    include_data: bool = False,
    data_limit: Optional[int] = None,
    verbose: bool = False,
    db_path: Optional[str] = None
) -> bool:
    """
    Экспорт схемы базы данных в SQL файл
    
    Args:
        output_file: Путь к выходному файлу
        tables: Список таблиц для экспорта (None = все таблицы)
        include_data: Включать ли данные таблиц
        data_limit: Ограничение количества записей на таблицу
        verbose: Подробный вывод
        db_path: Путь к базе данных (None = из конфигурации)
        
    Returns:
        True если экспорт успешен
    """
    if verbose:
        print("🔌 Подключение к базе данных...")
    
    try:
        # Определяем путь к БД
        if db_path is None:
            # Попробуем подключиться через модуль проекта
            conn = connect_db()
            if not conn:
                # Если не удалось, попробуем прямое подключение
                db_path = get_db_path()
                if verbose:
                    print(f"🔄 Попытка прямого подключения к: {db_path}")
                try:
                    conn = duckdb.connect(database=db_path, read_only=True)
                except Exception as e:
                    print(f"❌ Ошибка прямого подключения: {e}")
                    print(f"📁 Проверьте путь к БД: {db_path}")
                    return False
        else:
            # Используем указанный путь
            if verbose:
                print(f"🔄 Подключение к указанной БД: {db_path}")
            try:
                conn = duckdb.connect(database=db_path, read_only=True)
            except Exception as e:
                print(f"❌ Ошибка подключения к {db_path}: {e}")
                return False
        
        # Проверяем подключение простым запросом
        try:
            conn.execute("SELECT 1").fetchone()
        except Exception as e:
            print(f"❌ Ошибка проверки подключения: {e}")
            return False
        
        if verbose:
            print("✅ База данных подключена")
        
        # Получаем информацию о БД
        db_info = get_database_info(conn)
        
        if verbose:
            print(f"📊 Найдено таблиц: {len(db_info['tables'])}")
            print(f"📊 Найдено последовательностей: {len(db_info['sequences'])}")
        
        # Определяем список таблиц для экспорта
        if tables:
            export_tables = [(table, 'BASE TABLE') for table in tables if table in [t[0] for t in db_info['tables']]]
            if len(export_tables) != len(tables):
                missing = set(tables) - set([t[0] for t in export_tables])
                print(f"⚠️ Предупреждение: таблицы не найдены: {missing}")
        else:
            export_tables = db_info['tables']
        
        if verbose:
            print(f"📋 Экспортируем {len(export_tables)} таблиц")
        
        # Создаем выходной файл
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            # Заголовок файла
            f.write("-- ===============================================\n")
            f.write("-- Экспорт схемы базы данных DuckDB\n")
            f.write(f"-- Создано: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- База данных: {get_db_path()}\n")
            f.write(f"-- Версия DuckDB: {db_info['database_version']}\n")
            f.write("-- ===============================================\n\n")
            
            # Экспорт последовательностей
            if db_info['sequences']:
                f.write("-- ===============================================\n")
                f.write("-- ПОСЛЕДОВАТЕЛЬНОСТИ\n")
                f.write("-- ===============================================\n\n")
                
                for sequence_name in db_info['sequences']:
                    if verbose:
                        print(f"📝 Экспорт последовательности: {sequence_name}")
                    
                    seq_sql = get_sequence_schema(conn, sequence_name)
                    if seq_sql:
                        f.write(f"-- Последовательность: {sequence_name}\n")
                        f.write(f"{seq_sql}\n\n")
            
            # Экспорт таблиц
            f.write("-- ===============================================\n")
            f.write("-- ТАБЛИЦЫ\n")
            f.write("-- ===============================================\n\n")
            
            for table_name, table_type in export_tables:
                if verbose:
                    print(f"📝 Экспорт таблицы: {table_name}")
                
                f.write(f"-- Таблица: {table_name} ({table_type})\n")
                
                # Получаем схему таблицы
                table_schema = get_table_schema(conn, table_name)
                if table_schema:
                    f.write(f"{table_schema}\n\n")
                    
                    # Получаем индексы
                    indexes = get_table_indexes(conn, table_name)
                    if indexes:
                        f.write(f"-- Индексы для таблицы {table_name}\n")
                        for index_sql in indexes:
                            f.write(f"{index_sql}\n")
                        f.write("\n")
                else:
                    f.write(f"-- ОШИБКА: Не удалось получить схему таблицы {table_name}\n\n")
                    continue
                
                # Экспорт данных если требуется
                if include_data:
                    if verbose:
                        print(f"📊 Экспорт данных таблицы: {table_name}")
                    
                    data_statements = get_table_data(conn, table_name, data_limit)
                    if data_statements:
                        f.write(f"-- Данные таблицы {table_name}\n")
                        if data_limit:
                            f.write(f"-- (ограничено {data_limit} записями)\n")
                        
                        for insert_sql in data_statements:
                            f.write(f"{insert_sql}\n")
                        f.write("\n")
                    else:
                        f.write(f"-- Нет данных в таблице {table_name}\n\n")
            
            # Завершение файла
            f.write("-- ===============================================\n")
            f.write("-- КОНЕЦ ЭКСПОРТА\n")
            f.write("-- ===============================================\n")
        
        # Информация о результате
        file_size = output_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"""
✅ **Экспорт завершен успешно!**
📁 Файл: {output_path.absolute()}
📊 Экспортировано таблиц: {len(export_tables)}
📊 Экспортировано последовательностей: {len(db_info['sequences'])}
📏 Размер файла: {file_size_mb:.2f} МБ
💾 Данные включены: {'Да' if include_data else 'Нет'}
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


def main():
    """Главная функция CLI"""
    parser = argparse.ArgumentParser(
        description="Экспорт схемы базы данных DuckDB в SQL файл",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
    
  Экспорт только схемы всех таблиц:
    python utils/export_db_schema.py --output schema.sql
    
  Экспорт конкретной базы данных:
    python utils/export_db_schema.py --database test.db --output test_schema.sql
    
  Экспорт схемы и данных:
    python utils/export_db_schema.py --output full_backup.sql --include-data
    
  Экспорт конкретных таблиц:
    python utils/export_db_schema.py --tables oz_products,wb_products --output selected.sql
    
  Экспорт с ограничением данных:
    python utils/export_db_schema.py --include-data --data-limit 1000 --output sample.sql
    
  Подробный вывод:
    python utils/export_db_schema.py --output schema.sql --verbose
        """
    )
    
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Путь к выходному SQL файлу'
    )
    
    parser.add_argument(
        '--database', '-d',
        help='Путь к базе данных DuckDB (по умолчанию: из конфигурации)'
    )
    
    parser.add_argument(
        '--tables', '-t',
        help='Список таблиц через запятую (по умолчанию: все таблицы)'
    )
    
    parser.add_argument(
        '--include-data',
        action='store_true',
        help='Включить данные таблиц в экспорт'
    )
    
    parser.add_argument(
        '--data-limit',
        type=int,
        help='Ограничение количества записей на таблицу при экспорте данных'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Подробный вывод'
    )
    
    args = parser.parse_args()
    
    # Парсим список таблиц
    tables = None
    if args.tables:
        tables = [table.strip() for table in args.tables.split(',')]
        if args.verbose:
            print(f"📋 Заданы таблицы для экспорта: {tables}")
    
    # Выполняем экспорт
    success = export_database_schema(
        output_file=args.output,
        tables=tables,
        include_data=args.include_data,
        data_limit=args.data_limit,
        verbose=args.verbose,
        db_path=args.database
    )
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())