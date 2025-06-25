"""
Утилиты для очистки и нормализации данных.

Этот модуль централизует логику очистки данных, которая дублировалась
в 15+ местах проекта. Применяет Template Method Pattern для стандартизации
процессов очистки данных маркетплейсов.

Техники рефакторинга (Context7):
- Extract Method: выделение повторяющихся блоков очистки
- Template Method Pattern: стандартный алгоритм с настраиваемыми шагами
- Consolidate Duplicate Conditional Fragments: объединение условий
"""
import pandas as pd
import streamlit as st
from typing import Tuple, List, Optional, Union, Dict, Any
import logging

# Настройка логирования
logger = logging.getLogger(__name__)


class DataCleaningUtils:
    """
    Централизованные утилиты для очистки и нормализации данных маркетплейсов.
    
    Заменяет дублированную логику очистки из 15+ мест в проекте:
    - cross_marketplace_linker.py
    - cards_matcher_helpers.py
    - search-algorithms.md
    - страницы Streamlit
    """
    
    @staticmethod
    def clean_barcode_dataframe(
        df: pd.DataFrame, 
        barcode_col: str = 'barcode',
        additional_filters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Стандартная очистка DataFrame со штрихкодами.
        
        Template Method Pattern: базовый алгоритм очистки с настраиваемыми фильтрами.
        
        Args:
            df: DataFrame для очистки
            barcode_col: название колонки со штрихкодами
            additional_filters: дополнительные фильтры {column: value_to_exclude}
            
        Returns:
            Очищенный DataFrame без дубликатов и пустых значений
            
        Example:
            >>> df_clean = DataCleaningUtils.clean_barcode_dataframe(
            ...     df, 
            ...     barcode_col='barcode',
            ...     additional_filters={'status': 'deleted'}
            ... )
        """
        if df.empty:
            logger.debug(f"DataFrame пуст, возвращаем как есть")
            return df
        
        try:
            # Шаг 1: Проверяем наличие колонки штрихкодов
            if barcode_col not in df.columns:
                logger.warning(f"Колонка '{barcode_col}' не найдена в DataFrame")
                return df
            
            # Шаг 2: Нормализуем штрихкоды (убираем пустые и None)
            cleaned_df = df.copy()
            cleaned_df = cleaned_df[
                cleaned_df[barcode_col].notna() & 
                (cleaned_df[barcode_col].astype(str).str.strip() != '')
            ]
            
            # Шаг 3: Применяем дополнительные фильтры
            if additional_filters:
                for col, exclude_value in additional_filters.items():
                    if col in cleaned_df.columns:
                        cleaned_df = cleaned_df[cleaned_df[col] != exclude_value]
            
            # Шаг 4: Удаляем дубликаты (ключевая операция)
            initial_count = len(cleaned_df)
            
            # ИСПРАВЛЕНИЕ: Более агрессивное удаление дубликатов
            # Сначала удаляем полные дубликаты
            cleaned_df = cleaned_df.drop_duplicates()
            
            # Для данных маркетплейсов дополнительно удаляем дубликаты по ключевым полям
            if barcode_col in cleaned_df.columns:
                # Если есть колонки SKU, удаляем дубликаты по штрихкоду + SKU
                sku_cols = [col for col in cleaned_df.columns if 'sku' in col.lower()]
                if sku_cols:
                    key_columns = [barcode_col] + sku_cols
                    cleaned_df = cleaned_df.drop_duplicates(subset=key_columns, keep='first')
            
            removed_count = initial_count - len(cleaned_df)
            
            if removed_count > 0:
                logger.debug(f"Удалено {removed_count} дубликатов из {initial_count} записей")
            
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Ошибка при очистке DataFrame: {e}")
            return df  # Возвращаем исходный DataFrame в случае ошибки
    
    @staticmethod
    def clean_marketplace_data(
        wb_df: pd.DataFrame, 
        oz_df: pd.DataFrame,
        wb_barcode_col: str = 'barcode',
        oz_barcode_col: str = 'barcode'
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Очистка данных маркетплейсов WB и Ozon по стандартному алгоритму.
        
        Template Method Pattern: применяет одинаковую логику очистки к двум маркетплейсам.
        Заменяет дублированную логику из cross_marketplace_linker.py и других файлов.
        
        Args:
            wb_df: DataFrame с данными Wildberries
            oz_df: DataFrame с данными Ozon
            wb_barcode_col: название колонки штрихкодов в WB
            oz_barcode_col: название колонки штрихкодов в Ozon
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: очищенные WB и Ozon DataFrames
            
        Example:
            >>> wb_clean, oz_clean = DataCleaningUtils.clean_marketplace_data(wb_df, oz_df)
        """
        logger.debug("Начинаем очистку данных маркетплейсов")
        
        # Применяем стандартную очистку к каждому маркетплейсу
        wb_clean = DataCleaningUtils.clean_barcode_dataframe(wb_df, wb_barcode_col)
        oz_clean = DataCleaningUtils.clean_barcode_dataframe(oz_df, oz_barcode_col)
        
        logger.debug(f"WB: {len(wb_df)} → {len(wb_clean)} записей")
        logger.debug(f"Ozon: {len(oz_df)} → {len(oz_clean)} записей")
        
        return wb_clean, oz_clean
    
    @staticmethod
    def standardize_dataframe_types(
        df: pd.DataFrame,
        type_mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Стандартизация типов данных в DataFrame.
        
        Extract Method: выделенная логика приведения типов, которая дублировалась.
        
        Args:
            df: DataFrame для стандартизации
            type_mapping: mapping колонка -> тип {'sku': 'str', 'price': 'float'}
            
        Returns:
            DataFrame со стандартизированными типами
        """
        if df.empty:
            return df
        
        standardized_df = df.copy()
        
        for column, target_type in type_mapping.items():
            if column in standardized_df.columns:
                try:
                    if target_type == 'str':
                        standardized_df[column] = standardized_df[column].astype(str).str.strip()
                    elif target_type == 'int':
                        standardized_df[column] = pd.to_numeric(standardized_df[column], errors='coerce').fillna(0).astype(int)
                    elif target_type == 'float':
                        standardized_df[column] = pd.to_numeric(standardized_df[column], errors='coerce').fillna(0.0)
                    
                    logger.debug(f"Приведен тип колонки '{column}' к {target_type}")
                    
                except Exception as e:
                    logger.warning(f"Не удалось привести тип колонки '{column}': {e}")
        
        return standardized_df
    
    @staticmethod
    def remove_duplicates_by_columns(
        df: pd.DataFrame,
        subset_columns: Union[str, List[str]],
        keep: str = 'first'
    ) -> pd.DataFrame:
        """
        Удаление дубликатов по указанным колонкам.
        
        Parameterize Method: обобщенная версия множественных вызовов drop_duplicates.
        
        Args:
            df: DataFrame для обработки
            subset_columns: колонка(и) для определения дубликатов
            keep: какую запись оставить ('first', 'last', False)
            
        Returns:
            DataFrame без дубликатов
        """
        if df.empty:
            return df
        
        # Нормализуем входные параметры
        if isinstance(subset_columns, str):
            subset_columns = [subset_columns]
        
        # Проверяем наличие колонок
        missing_cols = [col for col in subset_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Колонки не найдены: {missing_cols}")
            return df
        
        initial_count = len(df)
        
        # Удаляем дубликаты
        cleaned_df = df.drop_duplicates(subset=subset_columns, keep=keep)
        
        removed_count = initial_count - len(cleaned_df)
        if removed_count > 0:
            logger.debug(f"Удалено {removed_count} дубликатов по колонкам: {subset_columns}")
        
        return cleaned_df
    
    @staticmethod
    def comprehensive_marketplace_cleaning(
        df: pd.DataFrame,
        marketplace_type: str = 'generic'
    ) -> pd.DataFrame:
        """
        Комплексная очистка данных маркетплейса.
        
        Template Method Pattern: полный цикл очистки с настройками под тип маркетплейса.
        
        Args:
            df: DataFrame для очистки
            marketplace_type: тип маркетплейса ('wb', 'ozon', 'generic')
            
        Returns:
            Полностью очищенный DataFrame
        """
        if df.empty:
            return df
        
        logger.debug(f"Комплексная очистка данных маркетплейса: {marketplace_type}")
        
        cleaned_df = df.copy()
        
        # Шаг 1: Настройки очистки под тип маркетплейса
        if marketplace_type == 'wb':
            # Специфичная логика для Wildberries
            type_mapping = {
                'wb_sku': 'str',
                'wb_actual_price': 'float',
                'wb_size': 'int'
            }
            barcode_col = 'individual_barcode_wb'
            
        elif marketplace_type == 'ozon':
            # Специфичная логика для Ozon
            type_mapping = {
                'oz_sku': 'str',
                'oz_vendor_code': 'str',
                'oz_actual_price': 'float'
            }
            barcode_col = 'oz_barcode'
            
        else:
            # Общая логика
            type_mapping = {}
            barcode_col = 'barcode'
        
        # Шаг 2: Стандартизация типов
        if type_mapping:
            cleaned_df = DataCleaningUtils.standardize_dataframe_types(cleaned_df, type_mapping)
        
        # Шаг 3: Очистка штрихкодов (если есть)
        if barcode_col in cleaned_df.columns:
            cleaned_df = DataCleaningUtils.clean_barcode_dataframe(cleaned_df, barcode_col)
        else:
            # Общая очистка дубликатов
            cleaned_df = cleaned_df.drop_duplicates()
        
        logger.debug(f"Очистка завершена: {len(df)} → {len(cleaned_df)} записей")
        
        return cleaned_df


class ValidationHelper:
    """
    Параметризированные утилиты валидации и обработки ошибок.
    
    Consolidate Duplicate Conditional Fragments: объединяет повторяющиеся 
    паттерны валидации из разных частей проекта.
    """
    
    @staticmethod
    def validate_dataframe_not_empty(
        df: pd.DataFrame, 
        error_message: str = "DataFrame не должен быть пустым"
    ) -> bool:
        """
        Валидация непустого DataFrame с единообразной обработкой ошибок.
        
        Args:
            df: DataFrame для проверки
            error_message: сообщение об ошибке
            
        Returns:
            True если DataFrame валиден
            
        Raises:
            ValueError: если DataFrame пуст
        """
        if df.empty:
            logger.error(error_message)
            if st:  # Если доступен Streamlit
                st.error(f"❌ {error_message}")
            raise ValueError(error_message)
        return True
    
    @staticmethod
    def validate_required_columns(
        df: pd.DataFrame,
        required_columns: List[str],
        error_prefix: str = "Отсутствуют обязательные колонки"
    ) -> bool:
        """
        Валидация наличия обязательных колонок.
        
        Args:
            df: DataFrame для проверки
            required_columns: список обязательных колонок
            error_prefix: префикс сообщения об ошибке
            
        Returns:
            True если все колонки присутствуют
            
        Raises:
            ValueError: если отсутствуют обязательные колонки
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            error_message = f"{error_prefix}: {missing_columns}"
            logger.error(error_message)
            if st:  # Если доступен Streamlit
                st.error(f"❌ {error_message}")
            raise ValueError(error_message)
        
        return True
    
    @staticmethod
    def safe_operation_with_fallback(
        operation_func,
        fallback_value=None,
        error_message_prefix: str = "Ошибка операции"
    ):
        """
        Безопасное выполнение операции с fallback значением.
        
        Parameterize Method: обобщает паттерны try-catch из проекта.
        
        Args:
            operation_func: функция для выполнения
            fallback_value: значение по умолчанию при ошибке
            error_message_prefix: префикс для логирования ошибки
            
        Returns:
            Результат операции или fallback_value
        """
        try:
            return operation_func()
        except Exception as e:
            logger.error(f"{error_message_prefix}: {e}")
            if st:  # Если доступен Streamlit
                st.warning(f"⚠️ {error_message_prefix}: {e}")
            return fallback_value


# Обратная совместимость: функции-обертки для существующего кода
def clean_barcode_dataframe(df: pd.DataFrame, barcode_col: str = 'barcode') -> pd.DataFrame:
    """Обертка для обратной совместимости"""
    return DataCleaningUtils.clean_barcode_dataframe(df, barcode_col)


def clean_marketplace_data(wb_df: pd.DataFrame, oz_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Обертка для обратной совместимости"""
    return DataCleaningUtils.clean_marketplace_data(wb_df, oz_df)


def remove_duplicates_by_columns(df: pd.DataFrame, subset_columns: Union[str, List[str]]) -> pd.DataFrame:
    """Обертка для обратной совместимости"""
    return DataCleaningUtils.remove_duplicates_by_columns(df, subset_columns) 