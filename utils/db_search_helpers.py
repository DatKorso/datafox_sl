import duckdb
import streamlit as st
import pandas as pd

# --- Cross-Marketplace Search Helper Functions ---

def get_normalized_wb_barcodes(con: duckdb.DuckDBPyConnection, wb_skus: list[str] = None) -> pd.DataFrame:
    """
    Retrieves Wildberries products and normalizes their barcodes.
    Each row in the output DataFrame will have one wb_sku and one individual barcode.
    Each barcode is stored in a separate row in wb_products table.
    (Formerly _get_normalized_wb_barcodes in db_utils.py)

    Args:
        con: Active DuckDB connection.
        wb_skus (list[str], optional): A list of specific WB SKUs to process.
                                     If None or empty, processes all WB products.

    Returns:
        pd.DataFrame: DataFrame with columns ['wb_sku', 'individual_barcode_wb']
                      Returns an empty DataFrame if no data or on error.
    """
    if not con:
        if callable(st.error): st.error("DB connection not available for normalizing WB barcodes.")
        else: print("Error: DB connection not available for normalizing WB barcodes.")
        return pd.DataFrame()

    query_parts = []
    params = {}

    base_query = """
    SELECT
        p.wb_sku,
        TRIM(p.wb_barcodes) AS individual_barcode_wb
    FROM
        wb_products p
    WHERE NULLIF(TRIM(p.wb_barcodes), '') IS NOT NULL
    """

    if wb_skus:
        try:
            skus_for_query = [s for s in wb_skus if str(s).strip().isdigit()]  # Keep as strings, remove int() casting
            if not skus_for_query:
                msg = "Provided WB SKUs were invalid or empty for query, returning no normalized barcodes."
                if callable(st.warning): st.warning(msg)
                else: print(f"Warning: {msg}")
                return pd.DataFrame()
        except ValueError:
            msg = "WB SKUs must be numeric. Cannot normalize barcodes."
            if callable(st.error): st.error(msg)
            else: print(f"Error: {msg}")
            return pd.DataFrame()
        
        base_query += " AND p.wb_sku IN ({})".format(", ".join("?" * len(skus_for_query)))
        params = tuple(skus_for_query)
        query_parts.append(base_query)
    else:
        query_parts.append(base_query)
    
    final_query = " ".join(query_parts)
    
    try:
        if params:
            result_df = con.execute(final_query, params).fetchdf()
        else:
            result_df = con.execute(final_query).fetchdf()
        return result_df
    except Exception as e:
        err_msg = f"Error normalizing WB barcodes: {e}"
        if callable(st.error): st.error(err_msg)
        else: print(f"Error: {err_msg}")
        return pd.DataFrame()

def get_ozon_barcodes_and_identifiers(
    con: duckdb.DuckDBPyConnection,
    oz_skus: list[str] = None,
    oz_vendor_codes: list[str] = None,
    oz_product_ids: list[str] = None
) -> pd.DataFrame:
    """
    Retrieves Ozon product identifiers (sku, vendor_code, product_id) and their associated barcodes.
    Filters by the provided identifier lists if any are given.
    (Formerly _get_ozon_barcodes_and_identifiers in db_utils.py)

    Args:
        con: Active DuckDB connection.
        oz_skus (list[str], optional): List of Ozon SKUs.
        oz_vendor_codes (list[str], optional): List of Ozon vendor codes.
        oz_product_ids (list[str], optional): List of Ozon product IDs.

    Returns:
        pd.DataFrame: DataFrame with columns 
                      ['oz_barcode', 'oz_sku', 'oz_vendor_code', 'oz_product_id'].
                      Returns an empty DataFrame if no relevant data or on error.
    """
    if not con:
        if callable(st.error): st.error("DB connection not available for fetching Ozon barcodes.")
        else: print("Error: DB connection not available for fetching Ozon barcodes.")
        return pd.DataFrame()

    base_query = """ 
    SELECT DISTINCT
        b.oz_barcode,
        p.oz_sku, 
        p.oz_vendor_code AS product_oz_vendor_code, 
        p.oz_product_id AS product_oz_product_id
    FROM oz_barcodes b
    LEFT JOIN oz_products p ON b.oz_product_id = p.oz_product_id
    """ # Note: product_id from oz_barcodes is b.oz_product_id, vendor_code is b.oz_vendor_code
        # These can be selected too if needed for more detailed trace, but current goal is product identifiers.

    params = []
    where_clauses = []

    # UI implies one criterion type at a time. This logic handles if one is passed.
    if oz_skus:
        try:
            skus_for_query = [s for s in oz_skus if str(s).strip().isdigit()]  # Keep as strings, remove int() casting
            if not skus_for_query: raise ValueError("Empty or invalid SKU list after conversion")
            where_clauses.append(f"p.oz_sku IN ({', '.join(['?'] * len(skus_for_query))})")
            params.extend(skus_for_query)
        except ValueError as e:
            msg = f"Ozon SKUs must be numeric. Cannot fetch Ozon barcodes. Details: {e}"
            if callable(st.error): st.error(msg)
            else: print(f"Error: {msg}")
            return pd.DataFrame()
    elif oz_vendor_codes:
        vendor_codes_for_query = [str(vc) for vc in oz_vendor_codes if str(vc).strip()]
        if vendor_codes_for_query:
            where_clauses.append(f"p.oz_vendor_code IN ({', '.join(['?'] * len(vendor_codes_for_query))})")
            params.extend(vendor_codes_for_query)
        else:
            msg = "Provided Ozon Vendor Codes were invalid or empty. Returning no Ozon barcodes."
            if callable(st.warning): st.warning(msg)
            else: print(f"Warning: {msg}")
            return pd.DataFrame()
    elif oz_product_ids:
        try:
            product_ids_for_query = [int(pid) for pid in oz_product_ids if str(pid).strip().isdigit()]
            if not product_ids_for_query: raise ValueError("Empty or invalid Product ID list after conversion")
            where_clauses.append(f"p.oz_product_id IN ({ ', '.join(['?'] * len(product_ids_for_query))})")
            params.extend(product_ids_for_query)
        except ValueError as e:
            msg = f"Ozon Product IDs must be numeric. Cannot fetch Ozon barcodes. Details: {e}"
            if callable(st.error): st.error(msg)
            else: print(f"Error: {msg}")
            return pd.DataFrame()
    
    # If specific filters are applied, add the WHERE clause
    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses) # Should only be one main clause due to elif structure
    elif oz_skus is not None or oz_vendor_codes is not None or oz_product_ids is not None:
        # This means a list was provided for filtering, but it was empty or invalid after validation
        # Appropriate messages are already shown by the validation blocks. We should return empty.
        return pd.DataFrame()
        
    try:
        result_df = con.execute(base_query, params if params else None).fetchdf()
        
        # Standardize output column names for clarity
        output_df = pd.DataFrame()
        if not result_df.empty:
            output_df['oz_barcode'] = result_df['oz_barcode']
            output_df['oz_sku'] = result_df['oz_sku'] 
            output_df['oz_vendor_code'] = result_df['product_oz_vendor_code'] 
            output_df['oz_product_id'] = result_df['product_oz_product_id']
            return output_df.drop_duplicates()
        return pd.DataFrame() # Return empty if result_df was empty

    except Exception as e:
        err_msg = f"Error fetching Ozon barcodes and identifiers: {e}"
        if callable(st.error): 
            st.error(err_msg)
            # st.code(base_query) # Optional: show query for debug in streamlit
        else: 
            print(f"Error: {err_msg}")
            # print("Query attempted:", base_query) # For non-streamlit debugging
        return pd.DataFrame()

def find_cross_marketplace_matches(
    con: duckdb.DuckDBPyConnection,
    search_criterion: str, 
    search_values: list[str],
    selected_fields_map: dict
) -> pd.DataFrame:
    """
    Finds matching products between Ozon and Wildberries based on shared barcodes.

    Args:
        con: Active DuckDB connection.
        search_criterion: The primary field to search by ('wb_sku', 'oz_sku', 'oz_vendor_code', 'barcode').
        search_values: A list of values for the search criterion.
        selected_fields_map: A dictionary mapping user-selected display labels to internal
                                 table aliases/column names or special identifiers like 'common_matched_barcode'.

    Returns:
        pd.DataFrame: A DataFrame containing the matched products and selected information.
                      Columns are named based on the keys of selected_fields_map.
    """
    if not con:
        if callable(st.error): st.error("DB connection not available for cross-marketplace search.")
        else: print("DB connection not available for cross-marketplace search.")
        return pd.DataFrame()
    if not search_values:
        if callable(st.info): st.info("No search values provided.")
        else: print("No search values provided.")
        return pd.DataFrame()
    if not selected_fields_map:
        if callable(st.warning): st.warning("No fields selected for display.")
        else: print("No fields selected for display.")
        return pd.DataFrame()

    ozon_barcodes_df = pd.DataFrame()
    wb_normalized_barcodes_df = pd.DataFrame()
    input_barcodes_df = pd.DataFrame()

    if search_criterion == 'wb_sku':
        wb_normalized_barcodes_df = get_normalized_wb_barcodes(con, wb_skus=search_values) # UPDATED Call
        if wb_normalized_barcodes_df.empty:
            # Message already handled by get_normalized_wb_barcodes or here if needed
            return pd.DataFrame()
        ozon_barcodes_df = get_ozon_barcodes_and_identifiers(con) # UPDATED Call
        if ozon_barcodes_df.empty:
            # Message already handled by get_ozon_barcodes_and_identifiers or here if needed
            return pd.DataFrame()

    elif search_criterion == 'oz_sku':
        ozon_barcodes_df = get_ozon_barcodes_and_identifiers(con, oz_skus=search_values) # UPDATED Call
        if ozon_barcodes_df.empty:
            return pd.DataFrame()
        wb_normalized_barcodes_df = get_normalized_wb_barcodes(con) # UPDATED Call
        if wb_normalized_barcodes_df.empty:
            return pd.DataFrame()
            
    elif search_criterion == 'oz_vendor_code':
        ozon_barcodes_df = get_ozon_barcodes_and_identifiers(con, oz_vendor_codes=search_values) # UPDATED Call
        if ozon_barcodes_df.empty:
            return pd.DataFrame()
        wb_normalized_barcodes_df = get_normalized_wb_barcodes(con) # UPDATED Call
        if wb_normalized_barcodes_df.empty:
            return pd.DataFrame()

    elif search_criterion == 'barcode':
        # Ensure search_values are clean if they are barcodes
        cleaned_search_values = [str(val).strip() for val in search_values if str(val).strip()]
        if not cleaned_search_values:
            msg = "Provided barcode search values are empty or invalid."
            if callable(st.info): st.info(msg)
            else: print(msg)
            return pd.DataFrame()
        input_barcodes_df = pd.DataFrame(cleaned_search_values, columns=['input_barcode'])
        
        ozon_barcodes_df = get_ozon_barcodes_and_identifiers(con) # UPDATED Call
        wb_normalized_barcodes_df = get_normalized_wb_barcodes(con) # UPDATED Call
        if ozon_barcodes_df.empty and wb_normalized_barcodes_df.empty:
            # Messages handled by helper functions or here
            return pd.DataFrame()
    else:
        err_msg = f"Invalid search criterion: {search_criterion}"
        if callable(st.error): st.error(err_msg)
        else: print(err_msg)
        return pd.DataFrame()

    # Register DataFrames with DuckDB
    if not ozon_barcodes_df.empty: con.register('temp_ozon_barcodes_ids', ozon_barcodes_df)
    if not wb_normalized_barcodes_df.empty: con.register('temp_wb_norm_barcodes', wb_normalized_barcodes_df)
    if not input_barcodes_df.empty: con.register('temp_input_barcodes', input_barcodes_df)

    select_clauses = []
    join_clauses = set()
    from_core = ""

    # Define base FROM and initial SELECT based on search criterion
    first_column_alias = "Search_Value_Criterion"
    if search_criterion == 'wb_sku':
        from_core = "FROM temp_wb_norm_barcodes wb_b_norm JOIN temp_ozon_barcodes_ids oz_b_ids ON wb_b_norm.individual_barcode_wb = oz_b_ids.oz_barcode"
        # wb_b_norm is already filtered by input wb_skus by the call to get_normalized_wb_barcodes
        select_clauses.append(f"wb_b_norm.wb_sku AS \"{first_column_alias}\"")
    elif search_criterion in ['oz_sku', 'oz_vendor_code']:
        from_core = "FROM temp_ozon_barcodes_ids oz_b_ids JOIN temp_wb_norm_barcodes wb_b_norm ON oz_b_ids.oz_barcode = wb_b_norm.individual_barcode_wb"
        # oz_b_ids is already filtered by the call to get_ozon_barcodes_and_identifiers
        if search_criterion == 'oz_sku':
            select_clauses.append(f"oz_b_ids.oz_sku AS \"{first_column_alias}\"")
        else: # oz_vendor_code
            select_clauses.append(f"oz_b_ids.oz_vendor_code AS \"{first_column_alias}\"")
    elif search_criterion == 'barcode':
        from_core = """
        FROM temp_input_barcodes tib
        JOIN temp_ozon_barcodes_ids oz_b_ids ON tib.input_barcode = oz_b_ids.oz_barcode
        JOIN temp_wb_norm_barcodes wb_b_norm ON tib.input_barcode = wb_b_norm.individual_barcode_wb
        """ 
        select_clauses.append(f"tib.input_barcode AS \"{first_column_alias}\"")

    # Dynamically add other SELECT clauses and JOINs based on selected_fields_map
    for ui_label, field_detail in selected_fields_map.items():
        if ui_label == first_column_alias: continue # Avoid duplicating the search criterion column

        if isinstance(field_detail, tuple):
            source_table_key, db_col = field_detail
            if source_table_key == 'oz_products':
                if db_col in ['oz_sku', 'oz_vendor_code', 'oz_product_id']:
                     select_clauses.append(f'oz_b_ids.{db_col} AS "{ui_label}"')
                else:
                    join_clauses.add("LEFT JOIN oz_products op ON oz_b_ids.oz_product_id = op.oz_product_id")
                    select_clauses.append(f'op.{db_col} AS "{ui_label}"')
            elif source_table_key == 'oz_barcodes':
                 if db_col == 'oz_barcode':
                    select_clauses.append(f'oz_b_ids.oz_barcode AS "{ui_label}"')
            elif source_table_key == 'wb_products':
                if db_col == 'wb_sku':
                    select_clauses.append(f'wb_b_norm.wb_sku AS "{ui_label}"')
                else:
                    join_clauses.add("LEFT JOIN wb_products wp ON wb_b_norm.wb_sku = wp.wb_sku")
                    select_clauses.append(f'wp.{db_col} AS "{ui_label}"')
            elif source_table_key == 'wb_prices':
                join_clauses.add("LEFT JOIN wb_prices wpr ON wb_b_norm.wb_sku = wpr.wb_sku") # Assumes wb_b_norm has wb_sku
                select_clauses.append(f'wpr.{db_col} AS "{ui_label}"')
        elif field_detail == 'common_matched_barcode':
            select_clauses.append(f'oz_b_ids.oz_barcode AS "{ui_label}"')

    if not select_clauses:
        # This should not happen if first_column_alias is always added
        return pd.DataFrame()
    
    final_select_expr = []
    selected_aliases_set = set()
    for clause in select_clauses:
        # Extract alias, handles if alias is quoted or not
        alias_part = clause.split(' AS ')[-1]
        alias = alias_part.strip('" ') 
        if alias not in selected_aliases_set:
            final_select_expr.append(clause)
            selected_aliases_set.add(alias)
    
    if not final_select_expr:
         return pd.DataFrame()

    full_query = f"SELECT DISTINCT { ', '.join(final_select_expr) } {from_core} {' '.join(list(join_clauses))}"
    
    results_df = pd.DataFrame()
    try:
        # Ensure tables are registered before query and unregistered after
        # Registration is outside try/finally for this specific structure, relies on earlier checks
        results_df = con.execute(full_query).fetchdf()
    except Exception as e:
        err_msg_query = f"Error executing cross-marketplace search query: {e}"
        if callable(st.error): 
            st.error(err_msg_query)
            # st.code(full_query, language="sql") # Optional debug
        else: 
            print(err_msg_query)
            # print("Full query:", full_query) # Optional debug
    finally:
        if not ozon_barcodes_df.empty: con.unregister('temp_ozon_barcodes_ids')
        if not wb_normalized_barcodes_df.empty: con.unregister('temp_wb_norm_barcodes')
        if not input_barcodes_df.empty: con.unregister('temp_input_barcodes')
            
    return results_df 