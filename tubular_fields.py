"""
tubular_fields.py
-----------------
Extrae y mapea campos específicos de exports de Tubular al esquema unificado.
Completamente independiente sin depender de tubular.py
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional, Sequence, Union
import re
import pandas as pd

try:
    import pytz
except Exception:
    pytz = None


TUBULAR_FIELD_MAPPING = {
    'Creator': 'author',
    'Video_Title': 'message',
    'Video_URL': 'link',
    'Published_Date': 'date_original',
    'Platform': 'source',
    'Creator_Country': 'country',
    'Total_Engagements': 'engagement',
    'Views': 'views'
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


def _find_created_col(columns: Sequence[str], preferred: Optional[str]) -> str:
    """Detecta columna de fecha"""
    cols = [str(c).strip() for c in columns]
    
    candidates = [
        "Published_Date", "Published Date", "published_date",
        "Created Time", "Fecha", "Timestamp"
    ]
    
    if preferred and preferred.strip() in cols:
        return preferred.strip()
    
    for name in candidates:
        if name in cols:
            return name
    
    lowered = {c.lower(): c for c in cols}
    for key, original in lowered.items():
        if any(w in key for w in ("publish", "creat", "fecha", "time", "date")):
            return original
    
    raise KeyError("No se encontró columna de fecha.")


def _normalize_ampm(s: pd.Series) -> pd.Series:
    """Convierte variantes a AM/PM"""
    pattern = re.compile(r'(\s*[ap][.\s]?m[.]?)', flags=re.I)
    
    def _fix(t: str) -> str:
        t = t or ""
        m = pattern.search(t)
        if not m:
            return t
        frag = m.group(1).lower()
        return pattern.sub(" AM", t) if "a" in frag else pattern.sub(" PM", t)
    
    return s.astype(str).map(_fix)


def _parse_datetime_smart(raw: pd.Series) -> pd.Series:
    """Parsea fechas inteligentemente"""
    raw = _normalize_ampm(raw)
    sample = raw.dropna().astype(str).str.strip().head(50)
    has_slash = sample.str.contains("/").mean() > 0.5

    if has_slash:
        for fmt in ("%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
            out = pd.to_datetime(raw, format=fmt, errors="coerce", dayfirst=True)
            if out.notna().any():
                return out
        return pd.to_datetime(raw, errors="coerce", dayfirst=True)

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        out = pd.to_datetime(raw, format=fmt, errors="coerce")
        if out.notna().any():
            return out
    
    return pd.to_datetime(raw, errors="coerce")


def process_dataframe(
    df: pd.DataFrame,
    created_col: Optional[str] = None,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    add_mentions: bool = True
) -> pd.DataFrame:
    """Procesa DataFrame de Tubular"""
    if df is None or not hasattr(df, "columns"):
        raise TypeError("Se espera un pandas.DataFrame válido.")
    
    out = _normalize_columns(df.copy())
    col_fecha = _find_created_col(out.columns, created_col)
    
    # Parsear fecha
    out[col_fecha] = _parse_datetime_smart(out[col_fecha])
    
    # Zona horaria opcional
    if (tz_from or tz_to) and pytz is None:
        raise RuntimeError("pytz no disponible.")
    if tz_from and pytz is not None and out[col_fecha].notna().any():
        if not hasattr(out[col_fecha].dtype, "tz") or out[col_fecha].dtype.tz is None:
            out[col_fecha] = out[col_fecha].dt.tz_localize(
                pytz.timezone(tz_from), nonexistent="NaT", ambiguous="NaT"
            )
    if tz_to and pytz is not None and out[col_fecha].notna().any():
        out[col_fecha] = out[col_fecha].dt.tz_convert(pytz.timezone(tz_to))
    
    out[col_fecha] = _ensure_naive(out[col_fecha])
    
    # Generar date y hora
    date_series = out[col_fecha].dt.strftime("%m/%d/%Y")
    hora_series = (
        out[col_fecha]
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
    mapping_disponible = {k: v for k, v in TUBULAR_FIELD_MAPPING.items() if k in out.columns}
    out = out.rename(columns=mapping_disponible)
    
    # Eliminar original si se pide
    if drop_original_created and col_fecha in out.columns:
        out = out.drop(columns=[col_fecha])
    
    return out


def process_file(
    file_path: Union[str, Path],
    created_col: Optional[str] = None,
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    add_mentions: bool = True
) -> pd.DataFrame:
    """Lee archivo Tubular y lo procesa"""
    df = _read_any(file_path, skiprows=skiprows, header=header)
    return process_dataframe(
        df,
        created_col=created_col,
        tz_from=tz_from,
        tz_to=tz_to,
        drop_original_created=drop_original_created,
        add_mentions=add_mentions
    )


def extract_and_map_fields(df: pd.DataFrame, add_mentions: bool = True) -> pd.DataFrame:
    """Mapea solo los campos especificados"""
    out = df.copy()
    
    if add_mentions and "mentions" not in out.columns:
        out["mentions"] = 1
    
    mapping_disponible = {k: v for k, v in TUBULAR_FIELD_MAPPING.items() if k in out.columns}
    return out.rename(columns=mapping_disponible)
