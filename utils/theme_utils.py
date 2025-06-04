"""
Theme utilities for the DataFox Manager application.

This module provides functions to apply custom themes and styling
to the Streamlit application interface.
"""

import streamlit as st


def apply_theme():
    """
    Apply custom theme and styling to the Streamlit application.
    
    This function injects custom CSS to improve the visual appearance
    of the DataFox Manager interface.
    """
    
    # Custom CSS for improved theme
    custom_css = """
    <style>
    /* Main container styling */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        padding-top: 2rem;
    }
    
    /* Title styling */
    .css-10trblm {
        color: #1f77b4;
        font-weight: bold;
    }
    
    /* Success message styling */
    .stSuccess {
        background-color: #d4edda;
        border-color: #c3e6cb;
        color: #155724;
    }
    
    /* Info message styling */
    .stInfo {
        background-color: #d1ecf1;
        border-color: #bee5eb;
        color: #0c5460;
    }
    
    /* Warning message styling */
    .stWarning {
        background-color: #fff3cd;
        border-color: #ffeaa7;
        color: #856404;
    }
    
    /* Error message styling */
    .stError {
        background-color: #f8d7da;
        border-color: #f5c6cb;
        color: #721c24;
    }
    
    /* Button styling */
    .stButton > button {
        background-color: #1f77b4;
        color: white;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: bold;
        transition: background-color 0.3s;
    }
    
    .stButton > button:hover {
        background-color: #145a8a;
    }
    
    /* Metric styling */
    .metric-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    
    /* Table styling */
    .dataframe {
        border: 1px solid #dee2e6;
        border-radius: 5px;
    }
    
    /* Footer styling */
    .footer {
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        background-color: #f8f9fa;
        color: #6c757d;
        text-align: center;
        padding: 10px;
        font-size: 12px;
    }
    
    /* Hide Streamlit default elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f1f1;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #888;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #555;
    }
    </style>
    """
    
    # Apply the custom CSS
    st.markdown(custom_css, unsafe_allow_html=True)


def apply_dark_theme():
    """
    Apply dark theme styling to the application.
    
    This function provides an alternative dark theme for users
    who prefer dark mode interfaces.
    """
    
    dark_css = """
    <style>
    /* Dark theme styles */
    .stApp {
        background-color: #1e1e1e;
        color: #ffffff;
    }
    
    .main .block-container {
        background-color: #1e1e1e;
        color: #ffffff;
    }
    
    .stSidebar {
        background-color: #2d2d2d;
    }
    
    .stButton > button {
        background-color: #0066cc;
        color: white;
    }
    
    .stButton > button:hover {
        background-color: #0052a3;
    }
    
    .stSelectbox > div > div {
        background-color: #2d2d2d;
        color: #ffffff;
    }
    
    .stTextInput > div > div > input {
        background-color: #2d2d2d;
        color: #ffffff;
    }
    
    .dataframe {
        background-color: #2d2d2d;
        color: #ffffff;
    }
    </style>
    """
    
    st.markdown(dark_css, unsafe_allow_html=True)


def get_custom_colors():
    """
    Get custom color palette for charts and visualizations.
    
    Returns:
        dict: Dictionary containing color codes for different elements
    """
    
    return {
        'primary': '#1f77b4',
        'secondary': '#ff7f0e', 
        'success': '#2ca02c',
        'warning': '#d62728',
        'info': '#9467bd',
        'light': '#f8f9fa',
        'dark': '#343a40',
        'background': '#ffffff',
        'text': '#212529'
    }


def style_metric_cards():
    """
    Apply styling to metric cards for better visual presentation.
    """
    
    metric_css = """
    <style>
    div[data-testid="metric-container"] {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    div[data-testid="metric-container"] > label {
        font-weight: 600;
        color: #495057;
    }
    
    div[data-testid="metric-container"] > div {
        font-weight: bold;
        color: #1f77b4;
    }
    </style>
    """
    
    st.markdown(metric_css, unsafe_allow_html=True) 