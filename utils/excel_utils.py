import io
from typing import Any, Dict, Optional, Union, List

import pandas as pd


class ExcelReadError(RuntimeError):
    pass


def read_excel_with_fallback(
    file_path_or_buffer: Union[str, bytes, io.BytesIO],
    *,
    engine: str = "openpyxl",
    fallback_engines: Optional[list[str]] = None,
    expected_columns: Optional[List[str]] = None,
    skip_rows_after_header: int = 0,
    **read_params: Any,
) -> pd.DataFrame:
    """
    Read an Excel file robustly with fallbacks for malformed XLSX stylesheets.

    Tries the preferred engine first (default: openpyxl). If it fails with common
    stylesheet/XML errors produced by some exporters, attempts the following fallbacks:
      - pandas-calamine (engine="calamine") if available (very tolerant reader)
      - xlsx2csv -> pandas.read_csv (if available) as a last resort

    Parameters are passed through to pandas.read_excel/read_csv as appropriate.

    Raises ExcelReadError with actionable suggestions on failure.
    """
    fallback_engines = fallback_engines or ["calamine"]

    def _normalize(v: Any) -> str:
        return str(v).strip() if v is not None else ""

    def _has_expected_columns(df: pd.DataFrame) -> bool:
        if not expected_columns:
            return True
        cols_norm = {_normalize(c) for c in df.columns}
        needed = {_normalize(c) for c in expected_columns}
        return needed.issubset(cols_norm)

    # 1) Try the primary engine first
    try:
        df = pd.read_excel(file_path_or_buffer, engine=engine, **read_params)
        if _has_expected_columns(df):
            return df
        # If columns are missing, try smart header detection below
    except Exception as e:
        base_err = e
        err_msg = str(e).lower()

        def _try_calamine() -> Optional[pd.DataFrame]:
            try:
                import pandas_calamine  # noqa: F401  (ensures engine is available)
            except Exception:
                return None
            try:
                params = dict(read_params)
                # calamine does not support all kwargs; be permissive
                df = pd.read_excel(file_path_or_buffer, engine="calamine", **params)
                return df
            except Exception:
                return None

        def _try_xlsx2csv() -> Optional[pd.DataFrame]:
            try:
                from xlsx2csv import Xlsx2csv
            except Exception:
                return None

            # Convert the first sheet to CSV in-memory
            csv_buffer = io.StringIO()
            try:
                sheet_name = read_params.get("sheet_name")
                if isinstance(sheet_name, str):
                    Xlsx2csv(file_path_or_buffer, sheetname=sheet_name).convert(csv_buffer)
                else:
                    Xlsx2csv(file_path_or_buffer).convert(csv_buffer)
                csv_buffer.seek(0)
                # Map a subset of read_params to read_csv
                csv_kwargs: Dict[str, Any] = {}
                if "dtype" in read_params:
                    csv_kwargs["dtype"] = read_params["dtype"]
                if "usecols" in read_params:
                    csv_kwargs["usecols"] = read_params["usecols"]
                if "header" in read_params:
                    csv_kwargs["header"] = read_params["header"]
                df = pd.read_csv(csv_buffer, **csv_kwargs)
                return df
            except Exception:
                return None

        # Only attempt fallbacks for typical XML/stylesheet issues
        xml_related = any(
            kw in err_msg
            for kw in [
                "stylesheet",
                "invalid xml",
                "xml",
                "lxml",
                "zipfile",
                "read workbook",
            ]
        )

        if xml_related:
            # 2) Try calamine if present
            df = _try_calamine()
            if df is not None and _has_expected_columns(df):
                return df

            # 3) Try xlsx2csv if present
            df = _try_xlsx2csv()
            if df is not None and _has_expected_columns(df):
                return df

            # 4) Give actionable guidance
            hints = [
                "Установите более терпимый парсер: 'pandas-calamine' и перезапустите импорт.",
                "Команда (UV): uv add pandas-calamine",
                "Команда (pip): pip install pandas-calamine",
                "Либо откройте файл в Excel и сохраните как новый .xlsx (пересохранение чинит XML).",
                "Также можно сохранить файл как CSV и импортировать CSV.",
            ]
            raise ExcelReadError(
                "Не удалось прочитать Excel из-за проблем с XML/stylesheet. "
                f"Оригинальная ошибка: {base_err}. "
                + " | ".join(hints)
            )

        # Non-XML/stylesheet error: re-raise with context
        raise ExcelReadError(
            f"Не удалось прочитать Excel: {base_err}. Проверьте параметры чтения и формат файла."
        )

    # If we get here, the initial engine read succeeded but columns are missing.
    # Try smart header detection by re-reading without header and scanning for expected header row.
    try:
        raw_params = dict(read_params)
        raw_params["header"] = None

        # Try primary engine raw read
        try:
            raw_df = pd.read_excel(file_path_or_buffer, engine=engine, **raw_params)
        except Exception:
            # Try calamine raw
            raw_df = None
            try:
                import pandas_calamine  # noqa: F401
                raw_df = pd.read_excel(file_path_or_buffer, engine="calamine", **raw_params)
            except Exception:
                pass
            # Try xlsx2csv raw
            if raw_df is None:
                try:
                    from xlsx2csv import Xlsx2csv
                    csv_buffer = io.StringIO()
                    sheet_name = read_params.get("sheet_name")
                    if isinstance(sheet_name, str):
                        Xlsx2csv(file_path_or_buffer, sheetname=sheet_name).convert(csv_buffer)
                    else:
                        Xlsx2csv(file_path_or_buffer).convert(csv_buffer)
                    csv_buffer.seek(0)
                    raw_df = pd.read_csv(csv_buffer, header=None)
                except Exception:
                    raw_df = None

        if raw_df is not None and expected_columns:
            # Find the row that contains all expected column names
            search_limit = min(len(raw_df), 20)
            expected_norm = {_normalize(c) for c in expected_columns}
            header_row_idx = None
            for i in range(search_limit):
                row_vals = {_normalize(v) for v in raw_df.iloc[i].tolist()}
                if expected_norm.issubset(row_vals):
                    header_row_idx = i
                    break

            if header_row_idx is not None:
                # Build DataFrame with proper headers and skip rows after header
                new_columns = [_normalize(v) for v in raw_df.iloc[header_row_idx].tolist()]
                data_start = header_row_idx + 1 + (skip_rows_after_header or 0)
                rebuilt = raw_df.iloc[data_start:].reset_index(drop=True)
                rebuilt.columns = new_columns[: len(rebuilt.columns)]
                return rebuilt
    except Exception:
        # Ignore and fall back to original df
        pass

    return df
