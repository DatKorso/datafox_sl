"""
Database connection utilities for DuckDB.

This module provides functions for establishing and managing connections to DuckDB databases.
"""

import duckdb
import streamlit as st
import os

# Import config utility to get DB settings
from .config_utils import (
    get_db_path,
    get_db_mode,
    get_motherduck_db_name,
    get_motherduck_token,
)

# --- Database Connection and Basic Operations ---

def connect_db(db_path: str = None) -> duckdb.DuckDBPyConnection | None:
    """
    Establishes a connection to the database.

    Behavior:
    - If db_path is provided and starts with "md:", connects to MotherDuck.
    - If db_path is provided and is a file path, connects to local DuckDB file.
    - If db_path is None, reads mode and settings from config:
        * database_mode == "motherduck" -> use md:<db_name>
        * database_mode == "local" -> use database_path

    Returns: duckdb.DuckDBPyConnection | None
    """
    try:
        # Decide on target based on explicit argument or config
        explicit = db_path is not None
        mode = get_db_mode()

        if db_path is None:
            if mode == "motherduck":
                md_name = (get_motherduck_db_name() or "").strip()
                if not md_name:
                    msg = "Error: MotherDuck database name is not configured."
                    st.error(msg) if callable(st.error) else print(msg)
                    return None
                target = f"md:{md_name}"
            else:
                target = get_db_path()
        else:
            target = db_path

        # If using MotherDuck
        if isinstance(target, str) and target.startswith("md:"):
            # Try to ensure token is available via env or config
            token = get_motherduck_token() or os.environ.get("MOTHERDUCK_TOKEN", "")
            if token and not os.environ.get("MOTHERDUCK_TOKEN"):
                os.environ["MOTHERDUCK_TOKEN"] = token

            # First try direct connect, if extension not available, install+load then retry
            try:
                con = duckdb.connect(database=target, read_only=False)
            except Exception:
                try:
                    bootstrap = duckdb.connect()
                    bootstrap.execute("INSTALL motherduck;")
                    bootstrap.execute("LOAD motherduck;")
                    bootstrap.close()
                    con = duckdb.connect(database=target, read_only=False)
                except Exception as e2:
                    error_message = f"Failed to connect to MotherDuck at '{target}'. Error: {e2}"
                    st.error(error_message) if callable(st.error) else print(f"Error: {error_message}")
                    return None

            # If we have a token, set it explicitly on the session as well
            try:
                if token:
                    con.execute("SET motherduck_token = ?;", [token])
            except Exception:
                pass

            return con

        # Local DuckDB file
        if not target:
            message = "Error: Database path is not configured."
            st.error(message) if callable(st.error) else print(message)
            return None

        # Ensure the directory for the database file exists
        db_dir = os.path.dirname(target)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            info_message = f"Created directory for database: {db_dir}"
            st.info(info_message) if callable(st.info) else print(f"Info: {info_message}")

        con = duckdb.connect(database=target, read_only=False)
        return con
    except Exception as e:
        where = db_path if db_path is not None else ("md:" if mode == "motherduck" else get_db_path())
        error_message = f"Failed to connect to database at '{where}'. Error: {e}"
        st.error(error_message) if callable(st.error) else print(f"Error: {error_message}")
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
    from utils.db_schema import create_tables_from_schema, create_performance_indexes

    conn = connect_db()
    if not conn:
        return None

    # Создаем таблицы
    tables_created = create_tables_from_schema(conn)

    # Создаем индексы после успешного создания таблиц
    if tables_created:
        create_performance_indexes(conn)

    return conn 
