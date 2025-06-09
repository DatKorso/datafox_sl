"""
Streamlit page for viewing data from the marketplace database.

This page allows users to:
- Select a table from a list of managed database tables.
- View paginated data with optimized column display (excluding large text fields).
- Configure pagination size and column visibility.
- View detailed record information in expandable sections.
- Download filtered data as CSV.

Requires a valid database connection; prompts to configure if not connected.
"""
import streamlit as st
import pandas as pd
from utils.config_utils import get_db_path, load_config
from utils.db_connection import connect_db
from utils.db_crud import get_all_db_tables
from utils.db_schema import get_table_schema_definition
import io # For CSV download
import math

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
        
        # Configuration section
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            page_size = st.selectbox(
                "Записей на странице:",
                options=[50, 100, 500, 1000],
                index=1,
                key=f"page_size_{selected_table}"
            )
        
        with col2:
            show_all_columns = st.checkbox(
                "Показать все колонки",
                value=False,
                key=f"show_all_{selected_table}",
                help="Включить большие текстовые поля (может быть медленно)"
            )
        
        try:
            # Get total count first
            count_query = f'SELECT COUNT(*) as total FROM "{selected_table}";'
            total_records = db_connection.execute(count_query).fetchone()[0]
            
            if total_records == 0:
                st.info(f"Таблица `{selected_table}` пуста.")
                st.stop()
            
            # Calculate pagination
            total_pages = math.ceil(total_records / page_size)
            
            with col3:
                if total_pages > 1:
                    page_number = st.number_input(
                        f"Страница (из {total_pages}):",
                        min_value=1,
                        max_value=total_pages,
                        value=1,
                        key=f"page_num_{selected_table}"
                    )
                else:
                    page_number = 1
                    st.write(f"Всего записей: {total_records}")
            
            # Define columns to exclude for optimization (large text fields)
            large_text_columns = [
                'annotation', 'rich_content_json', 'additional_photos_urls', 
                'photo_360_urls', 'hashtags', 'keywords', 'size_info', 
                'model_features', 'decorative_elements', 'size_table_json',
                'error', 'warning'
            ]
            
            # Get table columns
            columns_query = f'PRAGMA table_info("{selected_table}");'
            columns_info = db_connection.execute(columns_query).fetchall()
            all_columns = [col[1] for col in columns_info]  # col[1] is column name
            
            # Filter columns based on user choice
            if show_all_columns:
                selected_columns = all_columns
                st.warning("⚠️ Показаны все колонки включая большие текстовые поля. Это может привести к медленной загрузке.")
            else:
                selected_columns = [col for col in all_columns if col not in large_text_columns]
                excluded_columns = [col for col in all_columns if col in large_text_columns and col in all_columns]
                if excluded_columns:
                    st.info(f"🚀 Скрытые большие поля: {', '.join(excluded_columns)}. Включите 'Показать все колонки' для их просмотра.")
            
            # Build optimized query with pagination
            columns_str = ', '.join([f'"{col}"' for col in selected_columns])
            offset = (page_number - 1) * page_size
            
            query = f'''
                SELECT {columns_str}
                FROM "{selected_table}"
                LIMIT {page_size} 
                OFFSET {offset};
            '''
            
            df = db_connection.execute(query).fetchdf()

            if df.empty:
                st.info(f"На странице {page_number} нет данных.")
            else:
                # Display main dataframe
                st.dataframe(df, use_container_width=True, hide_index=True, height=400)
                
                # Stats
                start_record = offset + 1
                end_record = min(offset + len(df), total_records)
                
                col_stats1, col_stats2, col_stats3 = st.columns(3)
                with col_stats1:
                    st.metric("Показано записей", f"{start_record}-{end_record} из {total_records}")
                with col_stats2:
                    st.metric("Колонок отображено", f"{len(selected_columns)} из {len(all_columns)}")
                with col_stats3:
                    st.metric("Текущая страница", f"{page_number} из {total_pages}")
                
                # Detailed view section
                if not show_all_columns and any(col in all_columns for col in large_text_columns):
                    st.subheader("🔍 Детальный просмотр записи")
                    
                    # Create a unique identifier for each row (using row index in current page)
                    record_options = []
                    if 'oz_vendor_code' in df.columns:
                        record_options = [f"Запись {i+1}: {row['oz_vendor_code']}" for i, (_, row) in enumerate(df.iterrows())]
                    elif 'wb_sku' in df.columns:
                        record_options = [f"Запись {i+1}: {row['wb_sku']}" for i, (_, row) in enumerate(df.iterrows())]
                    else:
                        record_options = [f"Запись {i+1}" for i in range(len(df))]
                    
                    selected_record = st.selectbox(
                        "Выберите запись для детального просмотра:",
                        options=range(len(record_options)),
                        format_func=lambda x: record_options[x],
                        key=f"detailed_view_{selected_table}_{page_number}"
                    )
                    
                    if selected_record is not None:
                        # Get the full record with all columns
                        record_offset = offset + selected_record
                        detailed_query = f'''
                            SELECT *
                            FROM "{selected_table}"
                            LIMIT 1 
                            OFFSET {record_offset};
                        '''
                        detailed_df = db_connection.execute(detailed_query).fetchdf()
                        
                        if not detailed_df.empty:
                            record = detailed_df.iloc[0]
                            
                            # Show excluded large fields
                            for col in large_text_columns:
                                if col in record.index and pd.notna(record[col]) and str(record[col]).strip():
                                    with st.expander(f"📝 {col}"):
                                        st.text_area(
                                            f"Содержимое поля '{col}':",
                                            value=str(record[col]),
                                            height=200,
                                            key=f"detail_{col}_{record_offset}",
                                            disabled=True
                                        )
                
                # CSV Download section
                st.subheader("💾 Скачать данные")
                
                download_col1, download_col2 = st.columns(2)
                
                with download_col1:
                    # Download current page
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False, sep=';')
                    csv_data = csv_buffer.getvalue()
                    
                    st.download_button(
                        label=f"📥 Скачать текущую страницу ({len(df)} записей)",
                        data=csv_data,
                        file_name=f"{selected_table}_page_{page_number}.csv",
                        mime="text/csv",
                        key=f"download_page_{selected_table}_{page_number}"
                    )
                
                with download_col2:
                    # Download all data (with warning for large tables)
                    if total_records > 10000:
                        st.warning(f"⚠️ Таблица содержит {total_records} записей. Скачивание может занять время.")
                    
                    if st.button(
                        f"📦 Подготовить полную выгрузку ({total_records} записей)",
                        key=f"prepare_full_{selected_table}"
                    ):
                        with st.spinner("Подготовка данных для скачивания..."):
                            full_query = f'SELECT {columns_str} FROM "{selected_table}";'
                            full_df = db_connection.execute(full_query).fetchdf()
                            
                            full_csv_buffer = io.StringIO()
                            full_df.to_csv(full_csv_buffer, index=False, sep=';')
                            full_csv_data = full_csv_buffer.getvalue()
                            
                            st.download_button(
                                label=f"📥 Скачать все данные ({len(full_df)} записей)",
                                data=full_csv_data,
                                file_name=f"{selected_table}_full_data.csv",
                                mime="text/csv",
                                key=f"download_full_{selected_table}"
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