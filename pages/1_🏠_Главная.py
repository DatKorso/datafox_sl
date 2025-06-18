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
from utils.config_utils import load_config, get_db_path
# from utils.db_utils import connect_db, get_db_stats # Old import
from utils.db_connection import connect_db # New import
from utils.db_crud import get_db_stats # New import
from utils.db_migration import auto_migrate_if_needed  # New import for migration check
# from utils.ui_utils import show_navigation_links # REMOVING: Assuming this exists or will be created
import os # For checking db file existence if needed directly

st.set_page_config(page_title="Home - Marketplace Analyzer", layout="wide")

# show_navigation_links() # REMOVING: Call to non-existent function

st.title("🏠 Home")
st.write("Welcome to the Marketplace Data Analyzer!")
st.markdown("---")

st.header("Database Analytics")

# Check database configuration and connection
# db_path = config_utils.get_db_path() # Old call causing NameError
db_path = get_db_path() # Corrected call
db_exists_and_configured = bool(db_path and os.path.exists(db_path))
connection_active = False
conn = None

if not db_path:
    st.warning("Database path not configured. Please go to the Settings page to set up the database.")
    if st.button("Go to Settings"):
        st.switch_page("pages/3_Settings.py")
elif not os.path.exists(db_path):
    st.error(f"Database file not found at the configured path: {db_path}. Please check the path in Settings or create the database.")
    if st.button("Go to Settings ", key="settings_db_not_found"):
        st.switch_page("pages/3_Settings.py")
else:
    # Attempt to connect if DB path is configured and seems to exist
    # conn = db_utils.connect_db(db_path) # Old call
    conn = connect_db(db_path) # Corrected call
    if conn:
        connection_active = True
        # st.success(f"Connected to database: {db_path}") # Keep UI cleaner, status is shown in metrics
    else:
        st.error(f"Failed to connect to the database at {db_path}. Please check settings or ensure the database file is accessible.")
        if st.button("Go to Settings  ", key="settings_db_fail_connect"):
            st.switch_page("pages/3_Settings.py")

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
        st.metric("Database File Size", f"{db_size} MB" if isinstance(db_size, (int, float)) else "N/A")

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

# General guidance if no data is shown but DB is configured and connected
if db_exists_and_configured and connection_active and stats.get('total_records', 0) == 0:
    st.info("Database is connected, but no records found in the managed tables. You might need to import some reports first!")
    if st.button("Go to Import Reports"):
        st.switch_page("pages/2_Import_Reports.py") # Navigate to import page 