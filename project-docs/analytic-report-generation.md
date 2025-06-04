# Custom Analytic Report Generation - Feature Specification

## Overview

Страница `8_Analytic_Report.py` предоставляет пользователю интерфейс для формирования пользовательских аналитических отчетов на основе данных, хранящихся в базе данных. Отчет создается в формате Excel (.xlsx) с динамическим заполнением данных по WB SKU.

## User Journey

1. **Выбор файла отчета**: Пользователь может либо:
   - Выбрать .xlsx файл вручную через интерфейс загрузки
   - Использовать путь из настроек (аналогично другим отчетам)

2. **Анализ структуры файла**: Система анализирует файл и проверяет:
   - Наличие листа "analytic_report"
   - Корректность структуры заголовков в 7-й строке
   - Наличие данных начиная с 9-й строки

3. **Обработка данных**: Для каждого WB_SKU в отчете система:
   - Находит связанные Ozon SKU через общие штрихкоды
   - Распределяет Ozon SKU по размерам обуви
   - Рассчитывает общие остатки
   - Формирует статистику заказов по дням

4. **Обновление файла**: Система:
   - Создает резервную копию оригинального файла
   - Обновляет данные в исходном файле
   - Сохраняет результат

## Technical Specifications

### File Structure Requirements

**Лист**: `analytic_report`

**Структура**:
- **Строка 7**: Заголовки колонок (рабочие заголовки для системы)
- **Строка 8**: Дополнительные описания (игнорируется при обработке)
- **Строки 9+**: Данные для обработки

### Column Specifications

#### Core Columns
- **N**: Порядковый номер (опционально)
- **WB_SKU**: Артикул WB (обязательно) - основа для поиска всех остальных данных

#### Size Distribution Columns (OZ_SIZE_XX)
- **OZ_SIZE_22** до **OZ_SIZE_44**: Размеры обуви (22-44)
- Содержат: Ozon SKU, относящиеся к соответствующему размеру WB товара
- Размер определяется через сопоставление штрихкодов и wb_products.wb_size

#### Summary Columns
- **OZ_SIZES**: Диапазон доступных размеров (формат "XX-YY")
- **OZ_STOCK**: Суммарный остаток по всем Ozon SKU (oz_products.oz_fbo_stock)

#### Order Statistics Columns
- **ORDERS_TODAY-30** до **ORDERS_TODAY-1**: Количество заказов по дням
- Цифра после дефиса указывает на количество дней от текущей даты
- Суммируются заказы всех Ozon SKU, принадлежащих WB SKU

#### Punta Reference Data Columns (NEW)
- **PUNTA_XXX**: Справочные данные из таблицы punta_table
- Формат: PUNTA_ + название_колонки_в_punta_table
- Данные загружаются по общему полю wb_sku
- Доступные колонки: gender, season, model_name, material, new_last, mega_last, best_last

### Примеры PUNTA_ колонок:
- **PUNTA_season**: Сезон (Лето/Деми/Зима)
- **PUNTA_gender**: Пол (Мальчики/Девочки)
- **PUNTA_material**: Описание материала
- **PUNTA_model_name**: Название модели
- **PUNTA_new_last**: Код new_last

## Data Processing Algorithm

### 1. WB SKU to Ozon SKU Mapping
```sql
-- Поиск связанных Ozon SKU через штрихкоды с размерами из WB
WITH wb_data AS (
    SELECT 
        p.wb_sku,
        p.wb_size,
        TRIM(b.barcode) AS individual_barcode
    FROM wb_products p,
    UNNEST(regexp_split_to_array(COALESCE(p.wb_barcodes, ''), E'[\\s;]+')) AS b(barcode)
    WHERE p.wb_sku = :input_wb_sku
        AND NULLIF(TRIM(b.barcode), '') IS NOT NULL
),
ozon_data AS (
    SELECT DISTINCT
        p.oz_sku,
        b.oz_barcode
    FROM oz_products p
    JOIN oz_barcodes b ON p.oz_product_id = b.oz_product_id
    WHERE NULLIF(TRIM(b.oz_barcode), '') IS NOT NULL
),
matches AS (
    SELECT wb.wb_sku, wb.wb_size, oz.oz_sku, wb.individual_barcode as common_barcode
    FROM wb_data wb
    JOIN ozon_data oz ON wb.individual_barcode = oz.oz_barcode
)
```

### 2. Size Distribution Logic
- Для каждого найденного совпадения берется размер из wb_products.wb_size
- Ozon SKU размещается в столбце OZ_SIZE_XX согласно размеру WB продукта
- Если несколько Ozon SKU связаны с одним WB размером, они разделяются символом ";"
- **Логика**: WB размер определяет в какую колонку попадет связанный Ozon SKU

### 3. Size Range Calculation
- Определяется минимальный и максимальный размер с непустыми Ozon SKU
- Формируется строка вида "27-38"

### 4. Stock Aggregation
- Суммируются oz_fbo_stock всех найденных Ozon SKU
- Учитываются только активные товары

### 5. Order Statistics
- Берутся заказы за последние 30 дней (исключая статус "Отменён")
- Группируются по дням и суммируются по всем связанным Ozon SKU

### 6. Punta Reference Data Processing (NEW)
```sql
-- Получение справочных данных Punta по WB SKU
SELECT wb_sku, gender, season, model_name, material, new_last, mega_last, best_last
FROM punta_table 
WHERE wb_sku IN (:input_wb_skus)
```

**Логика обработки:**
- Для каждого WB SKU ищутся соответствующие записи в punta_table
- wb_sku в punta_table имеет тип INTEGER (конвертируется при импорте из Google Sheets)
- Колонки PUNTA_XXX заполняются значениями из соответствующих колонок punta_table
- Например: PUNTA_season ← punta_table.season, PUNTA_gender ← punta_table.gender
- Если данные не найдены, колонки остаются пустыми

**Важно**: При импорте данных из Google Sheets wb_sku автоматически конвертируется из VARCHAR в INTEGER для обеспечения совместимости с другими таблицами.

## File Handling

### For Uploaded Files
- Файлы, загруженные через интерфейс, сохраняются как временные файлы с префиксом `temp_`
- Резервная копия НЕ создается для временных файлов
- После обработки пользователю предлагается скачать обработанный файл
- Временные файлы автоматически очищаются при завершении сеанса

### For Configured Path Files
- Файлы, выбранные через настройки, обрабатываются на месте
- Создается резервная копия с timestamp перед обновлением
- Исходный файл перезаписывается с новыми данными

## Configuration Settings

Добавляется новый ключ в config.json:
```json
{
  "report_paths": {
    "analytic_report_xlsx": "path/to/analytic_report.xlsx"
  }
}
```

## Error Handling

### Validation Checks
- Проверка существования листа "analytic_report"
- Проверка наличия колонки WB_SKU в 7-й строке
- Проверка валидности WB SKU (числовые значения)

### Error Scenarios
- **Файл не найден**: Показать ошибку и предложить выбрать другой файл
- **Неверная структура**: Показать детальное описание ожидаемой структуры
- **Нет данных WB SKU**: Показать предупреждение о пустых строках
- **Ошибки БД**: Показать техническую информацию об ошибке

## Dependencies

### Existing Functions to Reuse
- `get_normalized_wb_barcodes()` из utils/db_search_helpers.py
- `get_ozon_barcodes_and_identifiers()` из utils/db_search_helpers.py
- `fetch_ozon_order_stats()` из pages/6_Ozon_Order_Stats.py (адаптировать)

### New Functions to Implement
- `load_analytic_report_file()`: Загрузка и валидация Excel файла
- `map_wb_to_ozon_by_size()`: Сопоставление Ozon SKU по размерам
- `calculate_size_range()`: Расчет диапазона размеров
- `aggregate_stock_data()`: Агрегация остатков
- `generate_order_statistics()`: Формирование статистики заказов
- `update_analytic_report()`: Обновление Excel файла с сохранением бэкапа

## Performance Considerations

- Батчевая обработка WB SKU для оптимизации запросов к БД
- Кэширование результатов поиска штрихкодов
- Ограничение размера файла (предупреждение при > 1000 строк)

## Future Enhancements

- Поддержка множественных листов в одном файле
- Настраиваемые диапазоны дат для статистики заказов
- Экспорт результатов в различные форматы
- Автоматическое обновление отчетов по расписанию 