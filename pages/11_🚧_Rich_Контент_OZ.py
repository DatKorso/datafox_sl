"""
Streamlit page for OZ Rich Content - создание rich-контента для карточек на Озоне.

Эта страница предоставляет функционал для:
- Создания rich-контента для товарных карточек
- Генерации описаний товаров
- Оптимизации контента для поисковых запросов
- Работы с мультимедиа контентом
"""
import streamlit as st
import pandas as pd
import os
from utils.db_connection import connect_db
from utils.config_utils import get_db_path

st.set_page_config(page_title="[🚧] OZ Rich", layout="wide")
st.title("✨ OZ Rich Content - Создание rich-контента для карточек")
st.markdown("---")

# --- Database Connection ---
conn = connect_db()
if not conn:
    st.error("❌ База данных не подключена. Пожалуйста, настройте подключение в настройках.")
    if st.button("Go to Settings"):
        st.switch_page("pages/3_Settings.py")
    st.stop()

# --- Development Notice ---
st.markdown("""
<div style="text-align: center; padding: 40px; background-color: #f0f2f6; border-radius: 10px; margin: 20px 0;">
    <h2 style="color: #1f77b4;">🚧 В разработке</h2>
    <p style="font-size: 18px; color: #666;">
        Функционал создания rich-контента для карточек Озон находится в стадии разработки.
    </p>
    <p style="color: #888;">
        Скоро здесь появятся инструменты для создания качественного контента!
    </p>
</div>
""", unsafe_allow_html=True)

# Close database connection
if conn:
    conn.close() 