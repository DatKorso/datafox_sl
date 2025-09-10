"""
Streamlit page for the Home screen of the Marketplace Data Analyzer.

This page displays:
- A welcome message.
- Connection status to the DuckDB database.
- Key database analytics if connected (total tables, total records, DB file size).
- Record counts for each managed table.
- Prompts and navigation buttons to the Settings page if the database is not configured
  or if connection issues occur.
- A prompt to import data if the database is empty.
"""
import streamlit as st
import pandas as pd
from utils.config_utils import load_config, get_db_path, get_db_mode, get_motherduck_db_name
from utils.db_connection import connect_db
from utils.db_crud import get_db_stats
from utils.db_migration import auto_migrate_if_needed
import os

st.set_page_config(page_title="Home - Marketplace Analyzer", layout="wide")

# show_navigation_links() # REMOVING: Call to non-existent function

st.title("🏠 Home")
st.write("Welcome to the Marketplace Data Analyzer!")
st.markdown("---")

st.header("Database Analytics")

# Detect database mode and handle accordingly
mode = get_db_mode()
db_path = get_db_path()
connection_active = False
conn = None

if mode == "motherduck":
    # Connect using MotherDuck settings from config (handled inside connect_db)
    conn = connect_db()
    if conn:
        connection_active = True
    else:
        md_name = get_motherduck_db_name() or "(not set)"
        st.error(f"Failed to connect to MotherDuck database md:{md_name}. Please check the DB name and token in Settings.")
        if st.button("Go to Settings", key="settings_md_fail_connect"):
            st.switch_page("pages/3_⚙️_Настройки.py")
else:
    # Local file mode: validate path and existence before connecting
    if not db_path:
        st.warning("Database path not configured. Please go to the Settings page to set up the database.")
        if st.button("Go to Settings", key="settings_db_not_configured"):
            st.switch_page("pages/3_⚙️_Настройки.py")
    elif not os.path.exists(db_path):
        st.error(f"Database file not found at the configured path: {db_path}. Please check the path in Settings or create the database.")
        if st.button("Go to Settings", key="settings_db_not_found"):
            st.switch_page("pages/3_⚙️_Настройки.py")
    else:
        conn = connect_db(db_path)
        if conn:
            connection_active = True
        else:
            st.error(f"Failed to connect to the database at {db_path}. Please check settings or ensure the database file is accessible.")
            if st.button("Go to Settings", key="settings_db_fail_connect"):
                st.switch_page("pages/3_⚙️_Настройки.py")

if connection_active and conn:
    # Check for schema migration needs first
    migration_success = auto_migrate_if_needed(conn)
    if not migration_success:
        st.stop()  # Stop page execution if migration is needed but not performed
    
    # stats = db_utils.get_db_stats(conn) # Old call
    stats = get_db_stats(conn) # Corrected call - Fetch stats
    conn.close() # Important: Close connection after use

    st.subheader("Live Database Statistics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Connection Status", "✅ Connected")
    with col2:
        st.metric("Total Tables in DB", stats.get('table_count', "N/A"))
    with col3:
        db_size = stats.get('db_file_size_mb', "N/A")
        method = stats.get('db_size_method')
        label_suffix = " (estimated)" if method and method.startswith('md_') else ""
        value = f"{db_size} MB" if isinstance(db_size, (int, float)) else "N/A"
        st.metric("Database Size" + label_suffix, value)

    st.metric("Total Records (in managed tables)", stats.get('total_records', "N/A"))

    if 'error' in stats and stats['error']:
        st.error(f"Error retrieving some stats: {stats['error']}")

    with st.expander("Record Counts per Table"):
        if stats.get('table_record_counts'):
            for table, count in stats['table_record_counts'].items():
                st.text(f"- {table}: {count} records")
        else:
            st.caption("No table-specific record counts available or no managed tables found.")

elif db_path and os.path.exists(db_path) and not connection_active:
    # This case is if db_path exists but connection failed above, already handled by error message.
    # Could add a more specific message here if needed, but the above should suffice.
    pass
