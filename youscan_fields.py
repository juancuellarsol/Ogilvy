"""
youscan_fields.py
-----------------
Extrae y mapea campos específicos de exports de YouScan al esquema unificado.
Combina Date (DD.MM.YYYY) + Time (HH:MM 24h) en date/hora unificados.
Completamente independiente sin depender de youscan.py
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional, Sequence, Union
import pandas as pd

try:
    import pytz
except Exception:
    pytz = None


YOUSCAN_FIELD_MAPPING = {
    'Author': 'author',
    'Text snippet': 'message',
    'URL': 'link',
    'Source': 'source',
    'Sentiment': 'sentiment',
    'Country': 'country',
    'Engagement': 'engagement'
}


def _read_any(file_path: Union[str, Path], skiprows=None, header: Optional[int] = 0) -> pd.DataFrame:
    """Lee archivos .xlsx, .xls o .csv"""
    file_path = Path(file_path)
    ext = file_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(file_path, skiprows=skiprows, header=header)
    if ext == ".csv":
        return pd.read_csv(file_path, skiprows=skiprows, header=header)
    raise ValueError(f"Formato no soportado: {ext}. Usa .xlsx, .xls o .csv.")


def _ensure_naive(dt_series: pd.Series) -> pd.Series:
    """Quita zona horaria si viene con tz"""
    if hasattr(dt_series.dtype, "tz") and dt_series.dtype.tz is not None:
        return dt_series.dt.tz_convert(None)
    return dt_series


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia encabezados"""
    cols = (
        df.columns.astype(str)
          .str.strip()
          .str.replace(r"\s+", " ", regex=True)
    )
    df.columns = cols
    return df


def process_dataframe(
    df: pd.DataFrame,
    date_col: str = "Date",
    time_col: str = "Time",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_date_time: bool = True,
    add_mentions: bool = True
) -> pd.DataFrame:
    """
    Procesa DataFrame de YouScan:
    - Combina Date (DD.MM.YYYY) + Time (HH:MM 24h)
    - Genera date (M/D/YYYY) y hora (12h AM/PM)
    - Agrega columna mentions (1 por fila)
    """
    if df is None or not hasattr(df, "columns"):
        raise TypeError("Se espera un pandas.DataFrame válido.")
    
    out = _normalize_columns(df.copy())
    
    if date_col not in out.columns or time_col not in out.columns:
        raise KeyError(f"No encuentro columnas '{date_col}' y '{time_col}'")
    
    # Combinar Date + Time
    ts = pd.to_datetime(
        out[date_col].astype(str).str.strip() + " " + out[time_col].astype(str).str.strip(),
        format="%d.%m.%Y %H:%M",
        errors="coerce"
    )
    
    # Zona horaria opcional
    if (tz_from or tz_to) and pytz is None:
        raise RuntimeError("pytz no disponible.")
    if tz_from and pytz is not None and ts.notna().any():
        if not hasattr(ts.dtype, "tz") or ts.dtype.tz is None:
            ts = ts.dt.tz_localize(pytz.timezone(tz_from), nonexistent="NaT", ambiguous="NaT")
    if tz_to and pytz is not None and ts.notna().any():
        ts = ts.dt.tz_convert(pytz.timezone(tz_to))
    
    ts = _ensure_naive(ts)
    
    # Generar date (M/D/YYYY) y hora (12h AM/PM)
    date_series = (
        ts.dt.month.astype("Int64").astype(str) + "/" +
        ts.dt.day.astype("Int64").astype(str) + "/" +
        ts.dt.year.astype("Int64").astype(str)
    )
    
    hora_series = (
        pd.to_datetime(out[time_col].astype(str).str.strip(), format="%H:%M", errors="coerce")
        .dt.floor("h")
        .dt.strftime("%I:%M:%S %p")
        .str.lstrip("0")
    )
    
    # Insertar columnas
    for c in ("date", "hora"):
        if c in out.columns:
            out = out.drop(columns=[c])
    
    out.insert(0, "hora", hora_series)
    out.insert(0, "date", date_series)
    
    # Agregar mentions si se pide
    if add_mentions:
        if "mentions" in out.columns:
            out = out.drop(columns=["mentions"])
        out.insert(2, "mentions", 1)
    
    # Mapear campos
    mapping_disponible = {k: v for k, v in YOUSCAN_FIELD_MAPPING.items() if k in out.columns}
    out = out.rename(columns=mapping_disponible)
    
    # Eliminar originales si se pide
    if drop_original_date_time:
        for c in (date_col, time_col):
            if c in out.columns:
                out = out.drop(columns=[c])
    
    return out


def process_file(
    file_path: Union[str, Path],
    date_col: str = "Date",
    time_col: str = "Time",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_date_time: bool = True,
    add_mentions: bool = True
) -> pd.DataFrame:
    """Lee archivo YouScan y lo procesa"""
    df = _read_any(file_path, skiprows=skiprows, header=header)
    return process_dataframe(
        df,
        date_col=date_col,
        time_col=time_col,
        tz_from=tz_from,
        tz_to=tz_to,
        drop_original_date_time=drop_original_date_time,
        add_mentions=add_mentions
    )


def extract_and_map_fields(df: pd.DataFrame, add_mentions: bool = True) -> pd.DataFrame:
    """Mapea solo los campos especificados"""
    out = df.copy()
    
    if add_mentions and "mentions" not in out.columns:
        out["mentions"] = 1
    
    mapping_disponible = {k: v for k, v in YOUSCAN_FIELD_MAPPING.items() if k in out.columns}
    return out.rename(columns=mapping_disponible)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extrae campos específicos de YouScan")
    parser.add_argument("--file", help="Ruta a archivo YouScan (.xlsx/.csv)")
    parser.add_argument("--date-col", default="Date", help="Nombre de columna de fecha")
    parser.add_argument("--time-col", default="Time", help="Nombre de columna de hora")
    parser.add_argument("--skiprows", type=int, default=None, help="Filas a saltar")
    parser.add_argument("--header", type=int, default=0, help="Fila de encabezado")
    parser.add_argument("--tz-from", default=None, help="Zona horaria origen")
    parser.add_argument("--tz-to", default=None, help="Zona horaria destino")
    parser.add_argument("--no-mentions", action="store_true", help="No agregar columna mentions")
    
    args = parser.parse_args()
    
    if args.file:
        df = process_file(
            args.file,
            date_col=args.date_col,
            time_col=args.time_col,
            skiprows=args.skiprows,
            header=args.header,
            tz_from=args.tz_from,
            tz_to=args.tz_to,
            add_mentions=not args.no_mentions
        )
        print(df.head(10))
        print(f"\nColumnas: {list(df.columns)}")
    else:
        parser.error("Debes pasar --file")
