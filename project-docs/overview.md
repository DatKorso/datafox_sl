# 📊 DataFox SL - Project Overview

> **Система аналитики маркетплейсов**: Комплексное решение для анализа данных Ozon и Wildberries

## 🎯 Цель проекта

DataFox SL - это система для аналитики и управления товарами на маркетплейсах Ozon и Wildberries. Проект предоставляет пользователям мощные инструменты для:

- 📈 Анализа продаж и статистики
- 🔍 Поиска и сравнения товаров между платформами
- 🎯 Генерации рекомендаций и контента
- 📊 Создания аналитических отчетов
- 🔗 Связывания товаров через штрихкоды

## ⭐ Основные возможности

### 📊 Анализ и статистика
- **Статистика заказов OZ**: Детальный анализ продаж по датам, товарам и категориям
- **Менеджер рекламы OZ**: Управление рекламными кампаниями
- **Аналитические отчеты**: Универсальный инструмент создания отчетов

### 🔍 Поиск и сравнение
- **Поиск между МП**: Кросс-платформенный поиск товаров
- **Сверка категорий OZ**: Сравнение категорий товаров
- **Группировка товаров**: Интеллектуальная группировка похожих товаров

### 🎯 Рекомендации и контент
- **Rich Content OZ**: Генерация Rich Content для карточек Ozon
- **WB Рекомендации**: Поиск похожих товаров на Wildberries ⭐ **НОВОЕ**
- **Сбор WB SKU**: Автоматический сбор артикулов

### 🔧 Управление данными
- **Импорт отчетов**: Автоматический импорт данных из Excel
- **Просмотр БД**: Универсальный просмотрщик данных
- **Объединение Excel**: Продвинутый инструмент слияния файлов

## 🌟 Новые возможности (Декабрь 2024)

### 🎯 WB Рекомендации
**Революционная система поиска похожих товаров для Wildberries**

- **Алгоритм обогащения**: Использует связи WB ↔ Ozon для получения детальных характеристик
- **Similarity Scoring**: Продвинутый алгоритм оценки схожести с настраиваемыми весами
- **Пакетная обработка**: Обработка до 200 WB SKU за один раз
- **Экспорт результатов**: CSV и Excel форматы для дальнейшего анализа

**Принцип работы:**
1. Пользователь загружает список WB SKU
2. Система обогащает WB товары данными из Ozon через штрихкоды
3. Применяет алгоритм рекомендаций для поиска похожих WB товаров
4. Формирует итоговую таблицу с рекомендациями и score

## 🏗️ Архитектура системы

### 🗄️ База данных
- **DuckDB**: Встраиваемая аналитическая база данных
- **Схема данных**: Нормализованная структура для маркетплейсов
- **Кросс-платформенные связи**: Через штрихкоды и артикулы

### 🔗 Связывание маркетплейсов
- **CrossMarketplaceLinker**: Централизованная система связывания
- **Штрихкоды**: Основной способ связи товаров
- **Punta Table**: Универсальный справочник товаров

### 📊 Алгоритмы рекомендаций
- **Rich Content OZ**: Для генерации контента Ozon
- **WB Recommendations**: Для поиска похожих товаров WB
- **Similarity Scoring**: Многофакторная оценка схожести

## 📋 Структура страниц

### 🏠 Основные страницы
1. **🏠 Главная** - Обзор системы и быстрый доступ
2. **🖇 Импорт отчетов МП** - Загрузка данных из Excel
3. **⚙️ Настройки** - Конфигурация системы
4. **📖 Просмотр БД** - Универсальный просмотрщик данных

### 🔍 Поиск и анализ
5. **🔎 Поиск между МП** - Кросс-платформенный поиск
6. **📊 Статистика заказов OZ** - Анализ продаж
7. **🎯 Менеджер рекламы OZ** - Управление рекламой
8. **📋 Аналитический отчет OZ** - Генерация отчетов
9. **🔄 Сверка категорий OZ** - Сравнение категорий

### 🎯 Рекомендации и контент
10. **🚧 Склейка карточек OZ** - Объединение карточек
11. **🚧 Rich контент OZ** - Генерация Rich Content
12. **🚨 Проблемы карточек OZ** - Анализ проблем
13. **🔗 Сбор WB SKU по Озон** - Автоматический сбор артикулов
14. **🎯 Улучшенная группировка товаров** - Интеллектуальная группировка
15. **📊 Объединение Excel** - Слияние файлов
16. **🎯 Рекомендации WB** - Поиск похожих товаров WB ⭐ **НОВОЕ**

## 🛠️ Технический стек

### Backend
- **Python 3.8+**: Основной язык разработки
- **DuckDB**: Аналитическая база данных
- **Pandas**: Обработка данных
- **SQLAlchemy**: ORM для работы с БД

### Frontend
- **Streamlit**: Веб-интерфейс
- **Plotly**: Интерактивные графики
- **Pandas**: Отображение таблиц

### Интеграции
- **Google Sheets**: Импорт данных
- **Excel**: Импорт/экспорт файлов
- **REST API**: Интеграция с внешними системами

## 📊 Ключевые алгоритмы

### 🔗 Связывание товаров
```python
# Поиск связей через штрихкоды
wb_barcodes ↔ oz_barcodes → oz_category_products
```

### 🎯 Система рекомендаций
```python
# Алгоритм similarity scoring
score = base_score + size_bonus + season_bonus + color_bonus + 
        material_bonus + last_bonus + stock_bonus + price_bonus
```

### 📊 Обогащение данных
```python
# Стратегия "наиболее популярные"
characteristic = most_common_value(linked_products, field)
```

## 🚀 Новые функциональности

### 🎯 WB Recommendations (v1.0.0)
**Дата релиза**: Декабрь 2024

**Основные компоненты:**
- `WBRecommendationProcessor`: Главный оркестратор
- `WBRecommendationEngine`: Алгоритм поиска
- `WBDataCollector`: Обогащение данных
- `WBScoringConfig`: Конфигурация весов

**Поддерживаемые форматы ввода:**
- Текстовый ввод WB SKU
- Файлы: TXT, CSV, XLSX
- Пакетная обработка до 200 SKU

**Алгоритм оценки:**
- Базовый score: 100 баллов
- Максимальный score: 500 баллов
- 8 категорий параметров с настраиваемыми весами
- 5 предустановленных конфигураций

### 📊 Система оценки схожести
**Параметры оценки:**
- **Размер**: ±0 (+100), ±1 (+40), >1 (-50)
- **Сезон**: совпадение (+80), различие (-40)
- **Цвет**: совпадение (+40)
- **Материал**: совпадение (+40)
- **Колодки**: MEGA (+90), BEST (+70), NEW (+50)
- **Остатки**: >10 (+40), 3-10 (+20), 1-2 (+10)
- **Цена**: схожесть ±20% (+20)
- **Качество данных**: >70% (+30)

## 📈 Производительность

### 📊 Метрики системы
- **Время обработки**: 1-3 секунды на товар
- **Пакетная обработка**: 100-200 товаров за сессию
- **Точность связывания**: 85-95% (зависит от качества данных)
- **Покрытие характеристик**: 70-90% для связанных товаров

### ⚡ Оптимизации
- Кэширование данных (TTL 5 минут)
- Пакетная обработка SQL-запросов
- Прогрессивная загрузка результатов
- Индексы для критических запросов

## 🎯 Целевая аудитория

### 👥 Основные пользователи
- **Продавцы маркетплейсов**: Управление товарами и анализ продаж
- **Аналитики**: Глубокий анализ данных и трендов
- **Менеджеры**: Стратегическое планирование и отчетность

### 🎯 Сценарии использования
- **Поиск конкурентов**: Нахождение похожих товаров
- **Анализ рынка**: Изучение трендов и цен
- **Оптимизация карточек**: Улучшение контента товаров
- **Управление ассортиментом**: Группировка и категоризация

## 🔮 Планы развития

### 🚀 Краткосрочные планы (Q1 2025)
- **Machine Learning**: Алгоритмы обучения для улучшения рекомендаций
- **API интеграция**: REST API для внешних систем
- **Мобильное приложение**: Доступ с мобильных устройств
- **Расширенная аналитика**: Предиктивные модели

### 🔮 Долгосрочные планы (2025)
- **Автоматизация**: Автоматическое обновление данных
- **Искусственный интеллект**: GPT-интеграция для контента
- **Масштабирование**: Поддержка других маркетплейсов
- **Корпоративная версия**: Мультитенантность и роли

## 📞 Поддержка и документация

### 📚 Документация
- **Руководства пользователя**: Пошаговые инструкции
- **Техническая документация**: Архитектура и API
- **Troubleshooting**: Решение типовых проблем

### 🛠️ Техническая поддержка
- **База знаний**: Часто задаваемые вопросы
- **Система тикетов**: Обработка обращений
- **Онлайн-чат**: Оперативная помощь

---

*Документация обновлена: Декабрь 2024*  
*Версия системы: 2.1.0*  
*Новые возможности: WB Recommendations v1.0.0* 