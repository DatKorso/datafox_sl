"""
Streamlit page for viewing data from the marketplace database.

This page allows users to:
- Select a table from a list of managed database tables.
- View the data contained within the selected table using a scrollable DataFrame.
- See basic information about the table (number of rows and columns).
- Download the data from the selected table as a CSV file.

Requires a valid database connection; prompts to configure if not connected.
"""
import streamlit as st
import pandas as pd
from utils.config_utils import get_db_path, load_config
from utils.db_connection import connect_db
from utils.db_crud import get_all_db_tables
import io # For CSV download

st.set_page_config(page_title="Просмотр данных", layout="wide")
st.title("📄 Просмотр данных из БД")

config = load_config()
db_path = get_db_path()

if not db_path:
    st.warning("Путь к базе данных не настроен. Пожалуйста, настройте его на странице 'Настройки'.")
    if st.button("Перейти в Настройки"):
        st.switch_page("pages/3_Settings.py")
    st.stop()

db_connection = connect_db(db_path)

if not db_connection:
    st.error(f"Не удалось подключиться к базе данных: {db_path}.")
    if st.button("Перейти в Настройки"):
        st.switch_page("pages/3_Settings.py")
    st.stop()
else:
    st.success(f"Соединение с базой данных '{db_path}' установлено.")

    # Get ALL tables from the database using the updated db_utils function
    try:
        all_tables = get_all_db_tables(db_connection)
    except Exception as e:
        st.error(f"Ошибка при получении списка таблиц из БД: {e}")
        all_tables = []
        st.stop()

    if not all_tables:
        st.info("В базе данных нет таблиц для просмотра. Вы можете импортировать данные на странице 'Импорт отчетов'.")
        if st.button("Перейти к Импорту"):
            st.switch_page("pages/2_Import_Reports.py")
        st.stop()

    selected_table = st.selectbox(
        "Выберите таблицу для просмотра:",
        options=all_tables,
        index=0 if all_tables else None,
        placeholder="Выберите таблицу..."
    )

    if selected_table:
        st.subheader(f"Данные из таблицы: `{selected_table}`")
        try:
            # Query to get all data from the selected table
            # Using f-string for table name is generally safe here because table names
            # are sourced from information_schema and controlled by our app's schema creation.
            # However, for user-inputted table names, parameterization or strict validation would be critical.
            query = f'SELECT * FROM "{selected_table}";'
            df = db_connection.execute(query).fetchdf()

            if df.empty:
                st.info(f"Таблица `{selected_table}` пуста.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.markdown(f"**Количество строк:** {df.shape[0]}")
                st.markdown(f"**Количество колонок:** {df.shape[1]}")
                
                # CSV Download
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, sep=';') # Using semicolon as often preferred in RU locale
                csv_data = csv_buffer.getvalue()
                
                st.download_button(
                    label="📥 Скачать данные (CSV)",
                    data=csv_data,
                    file_name=f"{selected_table}_data.csv",
                    mime="text/csv",
                    key=f"download_csv_{selected_table}"
                )

        except Exception as e:
            st.error(f"Ошибка при загрузке данных из таблицы `{selected_table}`: {e}")
    else:
        if all_tables: # Only show if tables exist but none is selected (should not happen with index=0)
            st.info("Пожалуйста, выберите таблицу из списка выше.")

# No need to explicitly close connection here if using Streamlit's connection management or if pages handle it.
# If db_connection is a raw DuckDB connection, it's good practice to close it when done with the app/session.
# However, for Streamlit pages, the script reruns, so connection might be re-established anyway.
# For now, let's assume connect_db handles pooling or re-connection efficiently. 