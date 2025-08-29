# 🧭 DataFox SL - Navigation Guide

> **Smart navigation system** for efficient project exploration and development workflows

## 📋 Quick Navigation Matrix

### **🎯 By Role & Need**

| **I am a...** | **I want to...** | **Go to...** | **Key Documents** |
|---------------|------------------|---------------|-------------------|
| **👩‍💼 Product Manager** | Understand features & roadmap | [📊 Overview](overview.md) | [Roadmap](project-management/planning/improvement-roadmap.md), [Changelog](project-management/changelog.md) |
| **👨‍💻 Developer** | Build & debug features | [🔧 CLAUDE.md](../CLAUDE.md) | [Tech Specs](tech-specs.md), [Architecture](technical/architecture-overview.md) |
| **📊 Data Analyst** | Explore data & create reports | [📖 DB Schema](data-structures/db_schema.md) | [User Guides](user-guides/), [Analytics](user-guides/analytic-reports.md) |
| **🎯 End User** | Use the platform effectively | [🏠 User Guides](user-guides/) | [Home Guide](user-guides/home-dashboard.md), [Import Guide](user-guides/data-import.md) |
| **🏗️ Architect** | Understand system design | [🏗️ Architecture](technical/architecture-overview.md) | [Database Design](data-structures/), [Implementation](technical/implementation/) |
| **🧪 QA Engineer** | Test & validate features | [🧪 Tests](../tests/) | [Performance](technical/rich-content-oz-memory-optimization.md), [Debug Guides](technical/wb-recommendations-debug-guide.md) |

### **🔍 By Task & Action**

| **I need to...** | **Quick Link** | **Context** |
|-------------------|----------------|-------------|
| **🚀 Get started quickly** | [📖 README](../README.md) + [⚙️ Settings](user-guides/settings.md) | Setup & first steps |
| **🔎 Search for specific info** | [📋 Project Index](PROJECT-INDEX.md) | Comprehensive cross-references |
| **🐛 Debug an issue** | [🚨 Debug Guides](technical/wb-recommendations-debug-guide.md) | Troubleshooting workflows |
| **📊 Understand the database** | [📊 Schema Docs](data-structures/db_schema.md) | Data models & relationships |
| **🎯 Use WB Recommendations** | [🎯 WB Guide](user-guides/wb-recommendations.md) | Feature walkthrough |
| **🌟 Generate Rich Content** | [🌟 Rich Content Guide](user-guides/rich-content.md) | Content creation workflows |
| **🔗 Link marketplaces** | [🔗 Cross-MP Search](user-guides/cross-marketplace-search.md) | Product linking process |
| **📈 Create analytics** | [📋 Analytics Guide](user-guides/analytic-reports.md) | Reporting workflows |

## 🗂️ Hierarchical Navigation

### **📁 Root Level**
```
📁 datafox_sl/
├── 🚀 Quick Start → README.md, CLAUDE.md
├── ⚙️ Configuration → config.example.json, requirements.txt
├── 📱 Application → app.py, pages/, utils/
├── 📊 Data Sources → marketplace_reports/
├── 🧪 Testing → tests/
└── 📚 Documentation → project-docs/
```

### **📚 Documentation Hierarchy**
```
📚 project-docs/
├── 🎯 ESSENTIAL
│   ├── 📋 PROJECT-INDEX.md ← **START HERE**
│   ├── 🧭 NAVIGATION.md ← **YOU ARE HERE**
│   ├── 📊 overview.md
│   └── 🏗️ tech-specs.md
│
├── 📖 USER-GUIDES ← **For End Users**
│   ├── 🏠 home-dashboard.md
│   ├── 🖇️ data-import.md
│   ├── 🎯 wb-recommendations.md
│   ├── 🌟 rich-content.md
│   └── 📊 analytic-reports.md
│
├── 🔧 TECHNICAL ← **For Developers**
│   ├── 🏗️ architecture-overview.md
│   ├── 📝 implementation/
│   ├── 🚨 debug guides
│   └── ⚡ performance docs
│
├── 📊 DATA-STRUCTURES ← **For Data Engineers**
│   ├── 📊 db_schema.md
│   ├── 🔗 database-relations.md
│   └── 📋 schemas/
│
└── 📈 PROJECT-MANAGEMENT ← **For Managers**
    ├── 📈 planning/
    ├── 📊 reports/
    └── 🔄 changelog.md
```

## 🎯 Feature Navigation Map

### **🏠 Dashboard & Core (Pages 1-4)**
| Page | Feature | Navigation Path | Key Documentation |
|------|---------|-----------------|-------------------|
| **🏠 Главная** | System overview | `app.py` → Dashboard | [Home Guide](user-guides/home-dashboard.md) |
| **🖇️ Импорт отчетов** | Data import | `pages/2_🖇_Импорт_Отчетов_МП.py` | [Import Guide](user-guides/data-import.md) |
| **⚙️ Настройки** | Configuration | `pages/3_⚙️_Настройки.py` | [Settings Guide](user-guides/settings.md) |
| **📖 Просмотр БД** | Database viewer | `pages/4_📖_Просмотр_БД.py` | [DB Viewer Guide](user-guides/data-viewing.md) |

### **🔍 Search & Analysis (Pages 5-9)**
| Page | Feature | Navigation Path | Key Documentation |
|------|---------|-----------------|-------------------|
| **🔎 Поиск между МП** | Cross-marketplace search | `pages/5_🔎_Поиск_Между_МП.py` | [Cross-MP Guide](user-guides/cross-marketplace-search.md) |
| **📊 Статистика заказов** | Order analytics | `pages/6_📊_Статистика_Заказов_OZ.py` | [Order Stats Guide](user-guides/order-statistics.md) |
| **🎯 Менеджер рекламы** | Ad management | `pages/7_🎯_Менеджер_Рекламы_OZ.py` | [Ad Manager Guide](user-guides/advertising-manager.md) |
| **📋 Аналитический отчет** | Report generation | `pages/8_📋_Аналитический_Отчет_OZ.py` | [Analytics Guide](user-guides/analytic-reports.md) |
| **🔄 Сверка категорий** | Category comparison | `pages/9_🔄_Сверка_Категорий_OZ.py` | [Category Guide](user-guides/category-comparison.md) |

### **🎯 Advanced Features (Pages 10-17)**
| Page | Feature | Navigation Path | Key Documentation |
|------|---------|-----------------|-------------------|
| **🚧 Склейка карточек** | Card matching | `pages/10_🚧_Склейка_Карточек_OZ.py` | [Cards Matcher](user-guides/cards-matcher.md) |
| **🚧 Rich контент** | Content generation | `pages/11_🚧_Rich_Контент_OZ.py` | [Rich Content Guide](user-guides/rich-content.md) |
| **🚨 Проблемы карточек** | Quality control | `pages/12_🚨_Проблемы_Карточек_OZ.py` | [Card Problems Guide](user-guides/card-problems.md) |
| **🔗 Сбор WB SKU** | SKU collection | `pages/13_🔗_Сбор_WB_SKU_по_Озон.py` | [Data Collection Guide](technical/implementation/wb-data-collection.md) |
| **🎯 Группировка товаров** | Product grouping | `pages/14_🎯_Улучшенная_Группировка_Товаров.py` | [Advanced Grouping](technical/implementation/advanced-grouping.md) |
| **📊 Объединение Excel** | Excel merging | `pages/15_📊_Объединение_Excel.py` | [Excel Merger](user-guides/excel-merger.md) |
| **🎯 Рекомендации WB** | ⭐ **WB Recommendations** | `pages/16_🎯_Рекомендации_WB.py` | [WB Recommendations](user-guides/wb-recommendations.md) |
| **📊 Дробление Excel** | Excel splitting | `pages/17_📊_Дробление_Excel.py` | [Excel Splitter](user-guides/excel-splitting.md) |

## 🛠️ Utility Navigation Map

### **🔗 Core Integration Utils**
| Utility | Purpose | Key Functions | Documentation |
|---------|---------|---------------|---------------|
| `cross_marketplace_linker.py` | WB ↔ Ozon linking | `find_linked_products()`, barcode normalization | [Linker Implementation](technical/implementation/cross-marketplace-linker.md) |
| `wb_recommendations.py` | WB similarity engine | `get_recommendations()`, scoring algorithms | [WB Implementation](technical/implementation/wb-recommendations-implementation.md) |
| `rich_content_oz.py` | Rich content generation | `process_batch_optimized()`, JSON generation | [Rich Content Implementation](technical/implementation/rich-content-implementation-report.md) |

### **🗄️ Database Utils**
| Utility | Purpose | Key Functions | Documentation |
|---------|---------|---------------|---------------|
| `db_connection.py` | Connection management | `connect_db()`, connection pooling | [Database Utils](technical/api/database-utils.md) |
| `db_schema.py` | Schema management | `setup_database()`, table creation | [Schema Management](data-structures/schemas/db_schema.md) |
| `db_migration.py` | Database migrations | `auto_migrate_if_needed()` | [Migration Guide](migration-status.md) |

### **📊 Analytics Utils**
| Utility | Purpose | Key Functions | Documentation |
|---------|---------|---------------|---------------|
| `analytic_report_helpers.py` | Report generation | Excel integration, data formatting | [Analytics Implementation](technical/implementation/analytics-engine.md) |
| `advanced_product_grouper.py` | Product grouping | Multi-criteria grouping, rating compensation | [Advanced Grouping](technical/implementation/advanced-product-grouper.md) |
| `category_helpers.py` | Category analysis | Category mapping, comparison | [Category Enhancement](technical/implementation/category-comparison-enhancement.md) |

## 🔍 Search Strategies

### **🎯 Find by Feature**
```bash
# Search for specific functionality
grep -r "class.*Recommendation" utils/
grep -r "def.*rich_content" utils/
grep -r "CrossMarketplaceLinker" pages/
```

### **📊 Find by Data**
```bash
# Search for database operations
grep -r "oz_category_products" pages/
grep -r "wb_products" utils/
grep -r "SELECT.*FROM" pages/
```

### **🐛 Find by Problem**
```bash
# Search for error handling
grep -r "try.*except" utils/
grep -r "st.error" pages/
grep -r "logging" utils/
```

### **📝 Find by Documentation**
```bash
# Search documentation
find project-docs/ -name "*.md" -exec grep -l "WB.*recommend" {} \;
find project-docs/ -name "*.md" -exec grep -l "Rich.*Content" {} \;
```

## 🚀 Workflow Navigation

### **🆕 New Developer Workflow**
1. **📖 Read** → [README.md](../README.md) + [CLAUDE.md](../CLAUDE.md)
2. **⚙️ Setup** → [Settings Guide](user-guides/settings.md)
3. **🏗️ Understand** → [Architecture](technical/architecture-overview.md)
4. **🎯 Practice** → [Sample data](../marketplace_reports/ozon/CustomFiles/)
5. **📋 Reference** → [Project Index](PROJECT-INDEX.md)

### **🐛 Debugging Workflow**
1. **🚨 Identify** → Error logs & symptoms
2. **🔍 Locate** → [Debug Guides](technical/wb-recommendations-debug-guide.md)
3. **📊 Analyze** → [Performance Docs](technical/rich-content-oz-memory-optimization.md)
4. **🔧 Fix** → Code patterns in [CLAUDE.md](../CLAUDE.md)
5. **🧪 Test** → [Test Suite](../tests/)

### **📊 Data Analysis Workflow**
1. **📖 Understand** → [Database Schema](data-structures/db_schema.md)
2. **🔍 Explore** → [Database Viewer](user-guides/data-viewing.md)
3. **🔗 Link** → [Cross-Marketplace Search](user-guides/cross-marketplace-search.md)
4. **📊 Analyze** → [Analytics Reports](user-guides/analytic-reports.md)
5. **📈 Export** → Excel/CSV workflows

### **🎯 Feature Development Workflow**
1. **📋 Plan** → [Requirements](requirements.md)
2. **🏗️ Design** → [Architecture Patterns](technical/architecture-overview.md)
3. **💻 Code** → [Development Patterns](../CLAUDE.md)
4. **🧪 Test** → [Test Strategy](../tests/)
5. **📚 Document** → [Documentation Standards](doc-standards.md)

## 📞 Help & Support Navigation

### **🆘 Emergency Procedures**
| **Crisis** | **Immediate Action** | **Reference** |
|------------|---------------------|---------------|
| **Database corrupted** | `python -c "from utils.db_schema import setup_database; setup_database()"` | [Database Recovery](technical/database-recovery.md) |
| **Memory overflow** | Clear session state, enable memory-safe mode | [Memory Guide](technical/rich-content-oz-memory-optimization.md) |
| **Import failure** | Check data format, validate file structure | [Import Troubleshooting](user-guides/data-import.md#troubleshooting) |
| **Linking broken** | Verify barcode data, check normalization | [Linking Debug](technical/implementation/cross-marketplace-linker.md#debugging) |

### **📋 Checklist Navigation**
- ✅ **New Users**: [README](../README.md) → [Settings](user-guides/settings.md) → [Home Guide](user-guides/home-dashboard.md)
- ✅ **Developers**: [CLAUDE.md](../CLAUDE.md) → [Architecture](technical/architecture-overview.md) → [Tests](../tests/)
- ✅ **Data Users**: [Schema](data-structures/db_schema.md) → [Viewer](user-guides/data-viewing.md) → [Analytics](user-guides/analytic-reports.md)
- ✅ **Managers**: [Overview](overview.md) → [Roadmap](project-management/planning/improvement-roadmap.md) → [Reports](project-management/reports/)

### **🔗 External Resources**
- **Streamlit Docs**: https://docs.streamlit.io/
- **DuckDB Docs**: https://duckdb.org/docs/
- **Pandas Docs**: https://pandas.pydata.org/docs/
- **Python Best Practices**: https://pep8.org/

## 🎯 Navigation Tips

### **⚡ Speed Navigation**
- **Ctrl+F**: Search within any document
- **File search**: Use VS Code/IDE search across all files
- **Pattern search**: `grep -r "pattern" project-docs/`
- **Quick links**: Bookmark key documents for fast access

### **🧩 Context Switching**
- **Development**: CLAUDE.md → Technical docs → Utils
- **Usage**: User guides → Feature pages → Database viewer
- **Planning**: Overview → Roadmap → Requirements → Reports
- **Debugging**: Error → Debug guide → Code → Tests

### **📊 Information Density**
- **High-level**: Overview, README, Navigation (this doc)
- **Medium-level**: User guides, Feature docs, Architecture
- **Low-level**: Code comments, Implementation docs, API docs

---

## 🗺️ Site Map

```
🏠 DataFox SL
├── 📋 Project Index ← **Master Reference**
├── 🧭 Navigation ← **You Are Here**
├── 📊 Overview ← **Project Vision**
├── 🏗️ Technical Specs
├── 📖 User Guides/
│   ├── 🏠 Dashboard
│   ├── 🖇️ Data Import  
│   ├── 🎯 WB Recommendations ⭐
│   └── 🌟 Rich Content
├── 🔧 Technical/
│   ├── 🏗️ Architecture
│   ├── 📝 Implementation/
│   └── 🚨 Debug Guides
├── 📊 Data Structures/
│   ├── 📊 Database Schema
│   └── 🔗 Relations
└── 📈 Project Management/
    ├── 📈 Planning/
    └── 📊 Reports/
```

---

*🧭 Navigation Guide | Last Updated: August 2025 | Your compass through DataFox SL*  
*🎯 Quick Access: [📋 Index](PROJECT-INDEX.md) | [📊 Overview](overview.md) | [🔧 CLAUDE.md](../CLAUDE.md)*