Checkpoint — Indexing automation & barcode SQL

Commit: 08f5a7f (main)
Files touched:
- utils/db_indexing.py (added indexes: wb_prices_sku, oz_card_rating_sku, oz_card_rating_vendor_code, category_mapping_wb_category, category_mapping_wb_oz)
- utils/db_search_helpers.py (SQL-based barcode positions using generate_series + array_length)
- utils/db_cleanup.py (recreate indexes after oz_barcodes rebuild)
- utils/db_migration.py (create all priority indexes after DROP loops)
- utils/db_schema.py (startup create all priorities [1..3])

Recovery Plan:
- If behavior deviates, toggle startup indexing back to critical only by switching db_schema.create_performance_indexes to ensure_critical_indexes.
- For barcode positions, fallback is prior Python-side calculation if needed.

State:
- Tests indicate index recreation and SQL barcode positions work in-memory; repo pushed.

Next Actions:
- (Optional) Add ANALYZE post-import; add index status UI.