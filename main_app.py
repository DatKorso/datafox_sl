"""
Main Streamlit application script.

This script sets the global Streamlit page configuration (title, icon, layout)
and displays the main title for the application.

Streamlit handles multipage navigation automatically by recognizing files
in the `pages/` directory.
"""
import streamlit as st

# Set page config
st.set_page_config(
    page_title="Marketplace Data Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 Marketplace Data Analyzer")

st.sidebar.success("Select a page above.")

# This is a basic structure.
# More complex navigation or initial setup can be added here.
# For now, Streamlit's default multipage handling via the `pages/` directory will be used.

if __name__ == "__main__":
    # You can add any app-wide initialization logic here if needed
    # For example, loading configuration or initializing services.
    # However, it's often better to do this within the specific pages
    # or using session state to manage it across pages.
    pass 