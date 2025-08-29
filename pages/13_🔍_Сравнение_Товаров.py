"""
Streamlit page for comparing two Ozon products using the scoring algorithm.
Provides detailed scoring breakdown for analysis and debugging.
"""

import streamlit as st
import sys
import os
import duckdb
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.product_comparison import ProductComparator

def main():
    st.set_page_config(
        page_title="Сравнение Товаров Ozon",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 Сравнение Товаров Ozon")
    st.markdown("Детальное сравнение двух товаров с разбивкой по параметрам скоринга")
    
    # Input section
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Товар 1 (Источник)")
        vendor_code_1 = st.text_input(
            "Vendor Code 1",
            placeholder="Например: 0562002434-черный-34",
            key="vc1"
        )
    
    with col2:
        st.subheader("Товар 2 (Кандидат)")
        vendor_code_2 = st.text_input(
            "Vendor Code 2", 
            placeholder="Например: MEGA G0562000198",
            key="vc2"
        )
    
    # Compare button
    if st.button("🔍 Сравнить товары", type="primary"):
        if not vendor_code_1 or not vendor_code_2:
            st.error("Пожалуйста, введите оба vendor code")
            return
            
        if vendor_code_1 == vendor_code_2:
            st.warning("Vendor codes должны быть разными")
            return
        
        # Show comparison
        with st.spinner("Анализируем товары..."):
            try:
                # Connect to database
                db_path = project_root / "data" / "marketplace_data.db"
                
                if not db_path.exists():
                    st.error(f"База данных не найдена: {db_path}")
                    return
                
                with duckdb.connect(str(db_path)) as db_conn:
                    comparator = ProductComparator(db_conn)
                    result = comparator.compare_products(vendor_code_1, vendor_code_2)
                    
                    if result:
                        display_comparison_result(result)
                    else:
                        st.error("Не удалось найти один или оба товара в базе данных")
                    
            except Exception as e:
                st.error(f"Ошибка при сравнении: {str(e)}")
                st.exception(e)  # Show full traceback for debugging

def display_comparison_result(result):
    """Display the comparison result in a structured format."""
    
    # Product info section
    st.subheader("📋 Информация о товарах")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Товар 1 (Источник)**")
        source = result.source_product
        st.write(f"**Vendor Code:** {source.oz_vendor_code}")
        st.write(f"**Название:** {source.product_name or 'N/A'}")
        st.write(f"**Бренд:** {source.oz_brand or 'N/A'}")
        st.write(f"**Тип:** {source.type or 'N/A'}")
        st.write(f"**Пол:** {source.gender or 'N/A'}")
        st.write(f"**Размер:** {source.russian_size or 'N/A'}")
        st.write(f"**Сезон:** {source.season or 'N/A'}")
        st.write(f"**Цвет:** {source.color or 'N/A'}")
        st.write(f"**Материал:** {source.material_short or 'N/A'}")
        st.write(f"**Застежка:** {source.fastener_type or 'N/A'}")
        st.write(f"**Mega Last:** {source.mega_last or 'N/A'}")
        st.write(f"**Best Last:** {source.best_last or 'N/A'}")
        st.write(f"**New Last:** {source.new_last or 'N/A'}")
        st.write(f"**Модель:** {source.model_name or 'N/A'}")
        st.write(f"**Остатки:** {source.oz_fbo_stock}")
    
    with col2:
        st.markdown("**Товар 2 (Кандидат)**")
        candidate = result.candidate_product
        st.write(f"**Vendor Code:** {candidate.oz_vendor_code}")
        st.write(f"**Название:** {candidate.product_name or 'N/A'}")
        st.write(f"**Бренд:** {candidate.oz_brand or 'N/A'}")
        st.write(f"**Тип:** {candidate.type or 'N/A'}")
        st.write(f"**Пол:** {candidate.gender or 'N/A'}")
        st.write(f"**Размер:** {candidate.russian_size or 'N/A'}")
        st.write(f"**Сезон:** {candidate.season or 'N/A'}")
        st.write(f"**Цвет:** {candidate.color or 'N/A'}")
        st.write(f"**Материал:** {candidate.material_short or 'N/A'}")
        st.write(f"**Застежка:** {candidate.fastener_type or 'N/A'}")
        st.write(f"**Mega Last:** {candidate.mega_last or 'N/A'}")
        st.write(f"**Best Last:** {candidate.best_last or 'N/A'}")
        st.write(f"**New Last:** {candidate.new_last or 'N/A'}")
        st.write(f"**Модель:** {candidate.model_name or 'N/A'}")
        st.write(f"**Остатки:** {candidate.oz_fbo_stock}")
    
    # Scoring breakdown
    st.subheader("📊 Детальный скоринг")
    
    total_score = result.total_score
    similarity = result.similarity_percentage
    
    # Total score display
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Общий балл", f"{total_score:.1f}")
    with col2:
        st.metric("Схожесть", f"{similarity:.1f}%")
    
    # Individual scores
    st.markdown("**Разбивка по параметрам:**")
    
    score_data = []
    for detail in result.scoring_details:
        score_data.append({
            'Параметр': detail.parameter,
            'Исходное значение': str(detail.source_value),
            'Значение кандидата': str(detail.candidate_value),
            'Балл': f"{detail.score:.1f}",
            'Макс. балл': f"{detail.max_possible:.1f}",
            'Статус': '✅' if detail.score > 0 else ('❌' if detail.score < 0 else '➖'),
            'Тип совпадения': detail.match_type
        })
    
    # Display as table
    import pandas as pd
    df = pd.DataFrame(score_data)
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    # Detailed explanations
    with st.expander("📝 Подробные объяснения"):
        for detail in result.scoring_details:
            st.write(f"**{detail.parameter}:** {detail.description}")
    
    # Recommendations
    st.subheader("💡 Рекомендации")
    
    if total_score >= 200:
        st.success("🎯 Отличное соответствие! Товары очень похожи.")
    elif total_score >= 100:
        st.info("👍 Хорошее соответствие. Товары подходят для рекомендации.")
    elif total_score >= 50:
        st.warning("⚠️ Среднее соответствие. Возможны улучшения.")
    else:
        st.error("❌ Слабое соответствие. Товары сильно отличаются.")

if __name__ == "__main__":
    main()
