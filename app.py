import streamlit as st
from utils.theme_utils import apply_theme

def main():
    st.set_page_config(
        page_title="DataFox Manager", 
        page_icon="🦊", 
        layout="wide",
        initial_sidebar_state="expanded"
    )
    apply_theme()

    st.title("Welcome to DataFox Manager 🦊")

    st.sidebar.success("Navigate features using the sidebar.")

    st.markdown("""
    Your central hub for managing local DuckDB databases.
    Import/export data, view tables, and execute SQL queries with ease.

    **👈 Select a feature from the sidebar to begin!**
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    # TODO: Initialize DB connection here if not already done
    # from utils.db_utils import get_db_connection
    # if 'db_conn' not in st.session_state:
    #     st.session_state.db_conn = get_db_connection()
    main() 