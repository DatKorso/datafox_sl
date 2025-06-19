# 🔧 Техническая документация DataFox SL

> Подробная информация для разработчиков, администраторов и технических специалистов

## 🎯 Назначение

**Целевая аудитория**: Разработчики, системные администраторы, DevOps  
**Уровень знаний**: Средний - продвинутый  
**Формат**: Техническая документация с примерами кода

## 📋 Архитектура и структура

### 🏗️ Основная архитектура  
- [📖 Обзор архитектуры](../overview.md) - Общая архитектура системы
- [🗄️ Схема базы данных](../db_schema.md) - Структура таблиц и связей
- [📊 Схемы отчетов МП](../mp_reports_schema.md) - Форматы данных маркетплейсов

### 🔗 Интеграции
- [📊 Google Sheets](../google-sheets-integration.md) - Подключение к Google Sheets API
- [🔌 Подключение к БД](implementation/database-connection.md) - Настройка DuckDB

### 📁 Структура данных
- [🗂️ Структура пользователей](../user-structure.md) - Организация пользовательских данных
- [🏢 Универсальная таблица Punta](../universal-punta-table.md) - Справочная система

## 🛠️ Разработка и развертывание

### 📋 Требования системы
- [⚙️ Системные требования](../requirements.md) - Зависимости и окружение
- [🔧 Технические спецификации](../tech-specs.md) - Детальные требования

### 🚀 Развертывание
- [🗂️ Структура проекта](implementation/project-structure.md) - Организация кодовой базы  
- [🐍 Python окружение](implementation/python-environment.md) - Настройка виртуального окружения
- [📊 Streamlit конфигурация](implementation/streamlit-config.md) - Настройки веб-интерфейса

## 🔄 Разработка функций

### 🛠️ Реализация модулей
- [🔍 Cards Matcher](implementation/cards-matcher-technical.md) - Алгоритм группировки карточек
- [📊 Аналитические отчеты](implementation/analytics-engine.md) - Движок аналитики
- [🎯 Менеджер рекламы](implementation/advertising-manager-tech.md) - Техническая реализация

### 🗄️ Работа с данными
- [📥 Система импорта](implementation/import-system.md) - Архитектура импорта данных
- [🔍 Поисковые алгоритмы](implementation/search-algorithms.md) - Алгоритмы поиска
- [📊 Обработка данных](implementation/data-processing.md) - ETL процессы

## 📚 API и Utils

### 🔧 Вспомогательные модули
- [🗄️ Database Utils](api/database-utils.md) - Утилиты работы с БД
- [📊 Data Helpers](api/data-helpers.md) - Помощники обработки данных
- [🎨 Theme Utils](api/theme-utils.md) - Управление темами интерфейса

### 🌐 Web API
- [🔌 REST Endpoints](api/rest-endpoints.md) - HTTP API эндпоинты
- [📊 Data Export API](api/export-api.md) - API экспорта данных

## 🚀 Разработка и поддержка

### 📋 Планы развития
- [🗺️ Roadmap улучшений](../improvement-roadmap.md) - Планы развития
- [📅 Timeline проекта](../timeline.md) - Временные рамки
- [📝 Changelog](../changelog.md) - История изменений

### 🧪 Тестирование и качество
- [🧪 Unit тесты](testing/unit-tests.md) - Модульное тестирование
- [🔍 Интеграционные тесты](testing/integration-tests.md) - Тестирование интеграций
- [📊 Производительность](testing/performance.md) - Тесты производительности

## 🛡️ Безопасность и мониторинг

### 🔐 Безопасность
- [🔒 Аутентификация](security/authentication.md) - Система доступа
- [📊 Аудит данных](security/data-audit.md) - Логирование и аудит
- [🛡️ Защита данных](security/data-protection.md) - Обеспечение безопасности

### 📊 Мониторинг
- [📈 Метрики системы](monitoring/system-metrics.md) - Мониторинг производительности
- [📋 Логирование](monitoring/logging.md) - Система логов
- [🚨 Алерты](monitoring/alerts.md) - Уведомления о проблемах

## 🆘 Техническая поддержка

### 🔧 Диагностика
- [🔍 Диагностика проблем](support/diagnostics.md) - Поиск и устранение неисправностей
- [📊 Анализ производительности](support/performance-analysis.md) - Оптимизация работы
- [🗄️ Обслуживание БД](support/database-maintenance.md) - Поддержка базы данных

### 📚 Ресурсы
- [📋 Best Practices](guidelines/best-practices.md) - Лучшие практики разработки
- [🎨 Code Style](guidelines/code-style.md) - Стандарты кодирования
- [📖 Contribution Guide](guidelines/contribution.md) - Руководство для контрибьюторов

---

## 📝 Метаданные

**Последнее обновление**: 2024-12-19  
**Статус**: В разработке  
**Связанные документы**: 
- [Пользовательские руководства](../user-guides/README.md)
- [Обзор проекта](../overview.md)

*Техническая документация постоянно развивается вместе с проектом.* 