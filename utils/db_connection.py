"""
Database connection utilities for DuckDB.

This module provides functions for establishing and managing connections to DuckDB databases.
"""

import duckdb
import streamlit as st
import os

# Import config utility to get DB path
from .config_utils import get_db_path

# --- Database Connection and Basic Operations ---

def connect_db(db_path: str = None) -> duckdb.DuckDBPyConnection | None:
    """
    Establishes a connection to the DuckDB database file.
    If db_path is not provided, it tries to get it from config.

    Args:
        db_path (str, optional): Path to the DuckDB file. Defaults to None.

    Returns:
        duckdb.DuckDBPyConnection | None: A connection object or None if connection fails.
    """
    if db_path is None:
        db_path = get_db_path()

    if not db_path:
        # Using print for non-Streamlit context, st.error for Streamlit context
        message = "Error: Database path is not configured."
        if callable(st.error):
            st.error(message)
        else:
            print(message)
        return None
    
    try:
        # Ensure the directory for the database file exists
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            info_message = f"Created directory for database: {db_dir}"
            if callable(st.info): # Check if in Streamlit context
                 st.info(info_message)
            else:
                print(f"Info: {info_message}")
            
        con = duckdb.connect(database=db_path, read_only=False)
        return con
    except Exception as e:
        error_message = f"Failed to connect to database at '{db_path}'. Error: {e}"
        if callable(st.error):
            st.error(error_message)
        else:
            print(f"Error: {error_message}")
        return None

def test_db_connection(db_path: str = None) -> bool:
    """
    Tests the connection to the DuckDB database.

    Args:
        db_path (str, optional): Path to the DuckDB file. Defaults to None.

    Returns:
        bool: True if connection is successful, False otherwise.
    """
    con = connect_db(db_path)
    if con:
        try:
            con.execute("SELECT 42;").fetchall() # Simple query to test connection
            con.close()
            return True
        except Exception as e:
            # Using print for non-Streamlit context
            print(f"Error: Connection test query failed: {e}")
            if con: con.close()
            return False
    return False 

@st.cache_resource 
def get_connection_and_ensure_schema():
    """
    Get database connection and ensure schema exists.
    This function is cached by Streamlit to avoid repeated connections.
    
    Returns:
        DuckDB connection object or None if connection fails
    """
    from utils.config_utils import get_db_path
    from utils.db_schema import create_tables_from_schema
    
    current_db_path = get_db_path()
    if not current_db_path:
        return None
    
    conn = connect_db(current_db_path)
    if conn:
        create_tables_from_schema(conn)
    return conn 