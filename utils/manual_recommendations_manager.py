"""
Модуль для управления ручными рекомендациями в системе WB рекомендаций.

Основная цель: позволить пользователям добавлять ручные рекомендации товаров
на конкретные позиции в итоговый список рекомендаций.

Принцип работы:
1. Загрузка CSV файла с ручными рекомендациями
2. Валидация и кэширование данных
3. Интеграция с основным алгоритмом рекомендаций

Формат CSV файла:
target_wb_sku,position_1,recommended_sku_1,position_2,recommended_sku_2,...

Пример:
123123,2,321321,5,321456
456456,1,789789,3,111222,7,333444

Автор: DataFox SL Project
Версия: 1.0.0
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional, Union
from dataclasses import dataclass
import io

# Настройка логирования
logger = logging.getLogger(__name__)

@dataclass
class ManualRecommendation:
    """Модель ручной рекомендации"""
    target_wb_sku: str
    position: int
    recommended_wb_sku: str
    
    def __post_init__(self):
        """Валидация данных после создания"""
        if not str(self.target_wb_sku).strip():
            raise ValueError("target_wb_sku не может быть пустым")
        if not str(self.recommended_wb_sku).strip():
            raise ValueError("recommended_wb_sku не может быть пустым")
        if self.position < 1:
            raise ValueError("position должна быть >= 1")
        
        # Нормализация к строкам
        self.target_wb_sku = str(self.target_wb_sku).strip()
        self.recommended_wb_sku = str(self.recommended_wb_sku).strip()


class ManualRecommendationsManager:
    """
    Менеджер ручных рекомендаций для системы WB рекомендаций
    
    Функционал:
    - Загрузка и валидация CSV файлов с ручными рекомендациями
    - Кэширование данных для быстрого доступа
    - Интеграция с алгоритмом рекомендаций
    """
    
    def __init__(self):
        """Инициализация менеджера"""
        # Кэш ручных рекомендаций: {target_sku: [(position, recommended_sku), ...]}
        self.manual_data: Dict[str, List[Tuple[int, str]]] = {}
        self.loaded_data_info = {
            'total_targets': 0,
            'total_recommendations': 0,
            'source': 'none'
        }
        logger.info("📋 ManualRecommendationsManager инициализирован")
    
    def load_from_csv_file(self, csv_file) -> bool:
        """
        Загрузка ручных рекомендаций из загруженного CSV файла Streamlit
        
        Args:
            csv_file: Загруженный файл из st.file_uploader
            
        Returns:
            bool: True если загрузка успешна, False иначе
        """
        try:
            # Читаем содержимое файла
            content = csv_file.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            
            logger.info(f"📄 Загрузка ручных рекомендаций из CSV файла: {csv_file.name}")
            return self._load_from_csv_content(content, source=f"file:{csv_file.name}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки CSV файла {csv_file.name}: {e}")
            return False
    
    def load_from_excel_file(self, excel_file) -> bool:
        """
        Загрузка ручных рекомендаций из загруженного Excel файла Streamlit
        
        Args:
            excel_file: Загруженный файл из st.file_uploader
            
        Returns:
            bool: True если загрузка успешна, False иначе
        """
        try:
            logger.info(f"📄 Загрузка ручных рекомендаций из Excel файла: {excel_file.name}")
            return self._load_from_excel_content(excel_file, source=f"file:{excel_file.name}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки Excel файла {excel_file.name}: {e}")
            return False
    
    def load_from_csv_string(self, csv_content: str) -> bool:
        """
        Загрузка ручных рекомендаций из строки CSV
        
        Args:
            csv_content: Содержимое CSV в виде строки
            
        Returns:
            bool: True если загрузка успешна, False иначе
        """
        logger.info("📄 Загрузка ручных рекомендаций из строки")
        return self._load_from_csv_content(csv_content, source="string")
    
    def _load_from_csv_content(self, csv_content: str, source: str = "unknown") -> bool:
        """
        Внутренний метод для загрузки CSV содержимого
        
        Args:
            csv_content: Содержимое CSV
            source: Источник данных для логирования
            
        Returns:
            bool: True если загрузка успешна, False иначе
        """
        try:
            # Очищаем предыдущие данные
            self.clear()
            
            # Читаем CSV
            df = pd.read_csv(io.StringIO(csv_content))
            logger.info(f"📊 CSV прочитан: {len(df)} строк, {len(df.columns)} столбцов")
            
            return self._process_dataframe(df, source)
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка загрузки CSV: {e}")
            self.clear()
            return False
    
    def _load_from_excel_content(self, excel_file, source: str = "unknown") -> bool:
        """
        Внутренний метод для загрузки Excel содержимого
        
        Args:
            excel_file: Загруженный Excel файл из Streamlit
            source: Источник данных для логирования
            
        Returns:
            bool: True если загрузка успешна, False иначе
        """
        try:
            # Очищаем предыдущие данные
            self.clear()
            
            # Читаем Excel файл (первый лист)
            df = pd.read_excel(excel_file, sheet_name=0)
            logger.info(f"📊 Excel прочитан: {len(df)} строк, {len(df.columns)} столбцов")
            
            return self._process_dataframe(df, source)
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка загрузки Excel: {e}")
            self.clear()
            return False
    
    def _process_dataframe(self, df: pd.DataFrame, source: str = "unknown") -> bool:
        """
        Общий метод для обработки DataFrame (из CSV или Excel)
        
        Args:
            df: DataFrame с данными
            source: Источник данных для логирования
            
        Returns:
            bool: True если обработка успешна, False иначе
        """
        try:
            # Валидируем структуру
            if df.empty:
                logger.warning("⚠️ Файл пустой")
                return False
            
            if len(df.columns) < 3:
                logger.error(f"❌ Недостаточно столбцов: {len(df.columns)} (минимум 3)")
                return False
            
            # Парсим данные
            total_recommendations = 0
            
            for index, row in df.iterrows():
                try:
                    target_sku = str(row.iloc[0]).strip()
                    if not target_sku or target_sku.lower() == 'nan':
                        logger.warning(f"⚠️ Пропущена строка {index + 1}: пустой target_sku")
                        continue
                    
                    # Парсим пары (позиция, артикул)
                    recommendations = []
                    
                    # Начинаем с 1-го столбца (position_1)
                    for col_idx in range(1, len(row), 2):
                        if col_idx + 1 >= len(row):
                            break  # Нет пары
                        
                        position_val = row.iloc[col_idx]
                        sku_val = row.iloc[col_idx + 1]
                        
                        # Проверяем на NaN или пустые значения
                        if (pd.isna(position_val) or pd.isna(sku_val) or 
                            str(position_val).strip() == '' or str(sku_val).strip() == ''):
                            continue
                        
                        try:
                            position = int(position_val)
                            recommended_sku = str(sku_val).strip()
                            
                            # Валидация
                            manual_rec = ManualRecommendation(target_sku, position, recommended_sku)
                            recommendations.append((manual_rec.position, manual_rec.recommended_wb_sku))
                            
                        except (ValueError, TypeError) as e:
                            logger.warning(f"⚠️ Некорректная пара в строке {index + 1}: {position_val}, {sku_val} - {e}")
                            continue
                    
                    if recommendations:
                        # Сортируем по позиции для удобства
                        recommendations.sort(key=lambda x: x[0])
                        self.manual_data[target_sku] = recommendations
                        total_recommendations += len(recommendations)
                        
                        logger.debug(f"✅ Загружены рекомендации для {target_sku}: {len(recommendations)} шт.")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка обработки строки {index + 1}: {e}")
                    continue
            
            # Сохраняем статистику
            self.loaded_data_info = {
                'total_targets': len(self.manual_data),
                'total_recommendations': total_recommendations,
                'source': source
            }
            
            logger.info(f"✅ Ручные рекомендации загружены:")
            logger.info(f"   📊 Товаров с ручными рекомендациями: {self.loaded_data_info['total_targets']}")
            logger.info(f"   📊 Всего ручных рекомендаций: {self.loaded_data_info['total_recommendations']}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка обработки данных: {e}")
            self.clear()
            return False
    
    def get_manual_recommendations(self, target_wb_sku: str) -> List[Tuple[int, str]]:
        """
        Получение ручных рекомендаций для конкретного товара
        
        Args:
            target_wb_sku: WB SKU товара
            
        Returns:
            List[Tuple[int, str]]: Список кортежей (позиция, рекомендуемый_sku)
        """
        target_key = str(target_wb_sku).strip()
        result = self.manual_data.get(target_key, [])
        
        if result:
            logger.debug(f"📋 Найдены ручные рекомендации для {target_key}: {len(result)} шт.")
        
        return result
    
    def has_manual_data(self, target_wb_sku: str) -> bool:
        """
        Проверка наличия ручных рекомендаций для товара
        
        Args:
            target_wb_sku: WB SKU товара
            
        Returns:
            bool: True если есть ручные рекомендации
        """
        target_key = str(target_wb_sku).strip()
        return target_key in self.manual_data
    
    def get_all_target_skus(self) -> List[str]:
        """
        Получение списка всех товаров с ручными рекомендациями
        
        Returns:
            List[str]: Список target WB SKU
        """
        return list(self.manual_data.keys())
    
    def get_statistics(self) -> Dict:
        """
        Получение статистики загруженных данных
        
        Returns:
            Dict: Статистика
        """
        return self.loaded_data_info.copy()
    
    def clear(self):
        """Очистка всех загруженных данных"""
        self.manual_data.clear()
        self.loaded_data_info = {
            'total_targets': 0,
            'total_recommendations': 0,
            'source': 'none'
        }
        logger.info("🧹 Ручные рекомендации очищены")
    
    def is_empty(self) -> bool:
        """
        Проверка, загружены ли какие-либо данные
        
        Returns:
            bool: True если данные не загружены
        """
        return len(self.manual_data) == 0
    
    def _detect_csv_separator(self, csv_content: str) -> str:
        """
        Автоматическое определение разделителя CSV
        
        Args:
            csv_content: Содержимое CSV
            
        Returns:
            str: Определенный разделитель
        """
        # Убираем BOM символы если есть
        if csv_content.startswith('\ufeff'):
            csv_content = csv_content[1:]
        
        first_line = csv_content.split('\n')[0] if csv_content else ""
        
        # Подсчитываем количество разных разделителей
        comma_count = first_line.count(',')
        semicolon_count = first_line.count(';')
        
        # Если есть точки с запятой и они преобладают, используем их
        if semicolon_count > comma_count:
            logger.info(f"🔧 Определен разделитель: ';' (найдено {semicolon_count} символов)")
            return ';'
        else:
            logger.info(f"🔧 Определен разделитель: ',' (найдено {comma_count} символов)")
            return ','

    def validate_csv_format(self, csv_content: str) -> Tuple[bool, str]:
        """
        Валидация формата CSV файла без загрузки данных
        
        Args:
            csv_content: Содержимое CSV
            
        Returns:
            Tuple[bool, str]: (является_валидным, сообщение_об_ошибке)
        """
        try:
            # Убираем BOM символы если есть
            if csv_content.startswith('\ufeff'):
                csv_content = csv_content[1:]
            
            # Определяем разделитель
            separator = self._detect_csv_separator(csv_content)
            
            df = pd.read_csv(io.StringIO(csv_content), sep=separator)
            
            if df.empty:
                return False, "CSV файл пустой"
            
            if len(df.columns) < 3:
                return False, f"Недостаточно столбцов: {len(df.columns)} (минимум 3: target_sku, position_1, recommended_sku_1)"
            
            if len(df.columns) % 2 == 0:
                return False, f"Нечетное количество столбцов: {len(df.columns)}. Ожидается: target_sku + пары (position, recommended_sku)"
            
            # Проверяем заголовки
            expected_pattern = ["target_wb_sku"]
            for i in range(1, len(df.columns), 2):
                expected_pattern.extend([f"position_{(i+1)//2}", f"recommended_sku_{(i+1)//2}"])
            
            return True, f"Формат корректный: {len(df)} строк, {(len(df.columns)-1)//2} макс. рекомендаций на товар (разделитель: '{separator}')"
            
        except Exception as e:
            return False, f"Ошибка чтения CSV: {e}"
    
    def generate_example_csv(self) -> str:
        """
        Генерация примера CSV файла для пользователей
        
        Returns:
            str: Содержимое примера CSV
        """
        example_data = """target_wb_sku,position_1,recommended_sku_1,position_2,recommended_sku_2,position_3,recommended_sku_3
123123,2,321321,5,321456,
456456,1,789789,3,111222,7,333444
789789,4,123456,
999888,1,111111,2,222222,3,333333"""
        
        return example_data
    
    def generate_example_excel(self) -> bytes:
        """
        Генерация примера Excel файла для пользователей
        
        Returns:
            bytes: Содержимое примера Excel файла
        """
        # Создаем DataFrame с примером данных
        data = {
            'target_wb_sku': ['123123', '456456', '789789', '999888'],
            'position_1': [2, 1, 4, 1],
            'recommended_sku_1': ['321321', '789789', '123456', '111111'],
            'position_2': [5, 3, None, 2],
            'recommended_sku_2': ['321456', '111222', None, '222222'],
            'position_3': [None, 7, None, 3],
            'recommended_sku_3': [None, '333444', None, '333333']
        }
        
        df = pd.DataFrame(data)
        
        # Сохраняем в BytesIO
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Manual_Recommendations')
        
        return output.getvalue()
    
    def __repr__(self) -> str:
        """Строковое представление объекта"""
        stats = self.get_statistics()
        return (f"ManualRecommendationsManager("
                f"targets={stats['total_targets']}, "
                f"recommendations={stats['total_recommendations']}, "
                f"source='{stats['source']}')")


# Вспомогательные функции для удобства использования

def create_example_csv_file(file_path: str) -> bool:
    """
    Создание файла с примером CSV
    
    Args:
        file_path: Путь для сохранения файла
        
    Returns:
        bool: True если файл успешно создан
    """
    try:
        manager = ManualRecommendationsManager()
        example_content = manager.generate_example_csv()
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(example_content)
        
        logger.info(f"✅ Пример CSV файла создан: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания примера CSV: {e}")
        return False


def validate_manual_recommendations_csv(csv_file_path: str) -> Tuple[bool, str]:
    """
    Валидация CSV файла с ручными рекомендациями
    
    Args:
        csv_file_path: Путь к CSV файлу
        
    Returns:
        Tuple[bool, str]: (является_валидным, сообщение)
    """
    try:
        manager = ManualRecommendationsManager()
        
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return manager.validate_csv_format(content)
        
    except Exception as e:
        return False, f"Ошибка чтения файла: {e}"


if __name__ == "__main__":
    # Простой тест функционала
    print("🧪 Тестирование ManualRecommendationsManager")
    
    manager = ManualRecommendationsManager()
    
    # Создаем тестовые данные
    test_csv = manager.generate_example_csv()
    print("\n📄 Пример CSV:")
    print(test_csv)
    
    # Тестируем загрузку
    success = manager.load_from_csv_string(test_csv)
    print(f"\n📊 Загрузка: {'✅ Успешно' if success else '❌ Ошибка'}")
    
    if success:
        print(f"📈 Статистика: {manager.get_statistics()}")
        
        # Тестируем получение рекомендаций
        test_sku = "123123"
        recommendations = manager.get_manual_recommendations(test_sku)
        print(f"\n🎯 Рекомендации для {test_sku}: {recommendations}")
        
        print(f"📋 Все товары с ручными рекомендациями: {manager.get_all_target_skus()}")