#!/usr/bin/env python3
"""
Unify.py (VERSIÓN CONSOLIDADA - ALL-IN-ONE)

Unifica exports de Sprinklr, Tubular y YouScan en un único .xlsx con:
 - Una hoja por fuente presente (sprinklr, tubular, youscan)
 - Una hoja "combined" con todas las filas unificadas y columnas canonizadas
 - Soporte para skiprows DIFERENTES por cada fuente
 - TODO integrado en un único archivo sin dependencias externas de módulos

Características:
 - Acepta 0..N archivos por cada fuente
 - Procesa campos, fechas y zonas horarias de forma independiente por plataforma
 - Mapea nombres de columnas habituales a un conjunto canónico
 - Guarda un único .xlsx con hojas por fuente y la hoja "combined"
 - Fácil de usar desde Google Colab con un solo import

Ejemplo de uso en Google Colab:
  from unify import unify_files
  
  result = unify_files(
      sprinklr_files=["XandNewsCO.xlsx"],
      tubular_files=["tubular1.xlsx"],
      youscan_files=["youscan.csv"],
      skiprows_sprinklr=2,
      skiprows_tubular=0,
      skiprows_youscan=1,
      out_path="unified.xlsx",
      tz_from="UTC",
      tz_to="America/Bogota"
  )
  files.download(str(result))

Ejemplo de uso en línea de comandos:
  python unify.py \\
    --sprinklr XandNewsCO.xlsx \\
    --tubular tubular1.xlsx \\
    --youscan youscan.csv \\
    --skiprows-sprinklr 2 \\
    --skiprows-tubular 0 \\
    --skiprows-youscan 1 \\
    --tz-from UTC \\
    --tz-to America/Bogota \\
    -o unified.xlsx
"""

from __future__ import annotations
import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

import pandas as pd

try:
    import pytz
except ImportError:
    pytz = None


# ============================================================================
# CONFIGURACIÓN DE CAMPOS Y MAPEOS POR PLATAFORMA
# ============================================================================

SPRINKLR_FIELD_MAPPING = {
    'From User': 'author',
    'Conversation Stream': 'message',
    'Sender Profile Image Url': 'link',
    'Created Time': 'date_original',
    'snTypeColumn': 'source',
    'Sentiment': 'sentiment',
    'Country': 'country',
    'Reach (SUM)': 'reach',
    'Earned Engagements (Recalibrated) (SUM)': 'engagement',
    'Mentions (SUM)': 'mentions',
}

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

YOUSCAN_FIELD_MAPPING = {
    'Author': 'author',
    'Text snippet': 'message',
    'URL': 'link',
    'Source': 'source',
    'Sentiment': 'sentiment',
    'Country': 'country',
    'Engagement': 'engagement'
}

# Columnas canónicas en la salida unificada
CANONICAL_COLUMNS = [
    "date",            # fecha en M/D/YYYY
    "hora",            # hora en 12h AM/PM
    "mentions",        # número de menciones
    "source",          # plataforma: sprinklr / tubular / youscan
    "original_file",   # nombre de archivo origen
    "author",          # autor del contenido
    "message",         # contenido/texto
    "link",            # URL del post/video
    "sentiment",       # sentimiento (Sprinklr/YouScan)
    "country",         # país
    "engagement",      # métricas de engagement
    "reach",           # alcance (Sprinklr)
    "views",           # vistas (Tubular/YouScan)
]


# ============================================================================
# UTILIDADES GENERALES DE LECTURA Y NORMALIZACIÓN
# ============================================================================

def _read_any(
    file_path: Union[str, Path],
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0
) -> pd.DataFrame:
    """Lee archivos .xlsx, .xls o .csv de forma agnóstica."""
    file_path = Path(file_path)
    ext = file_path.suffix.lower()
    
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(file_path, skiprows=skiprows, header=header)
    elif ext == ".csv":
        return pd.read_csv(file_path, skiprows=skiprows, header=header)
    else:
        raise ValueError(f"Formato no soportado: {ext}. Usa .xlsx, .xls o .csv.")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas: trim, espacios múltiples, etc."""
    cols = (
        df.columns.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    df.columns = cols
    return df


def _ensure_naive(dt_series: pd.Series) -> pd.Series:
    """Quita zona horaria si la serie tiene tz."""
    if hasattr(dt_series.dtype, "tz") and dt_series.dtype.tz is not None:
        return dt_series.dt.tz_convert(None)
    return dt_series


def _apply_timezone(
    dt_series: pd.Series,
    tz_from: Optional[str],
    tz_to: Optional[str]
) -> pd.Series:
    """Aplica transformación de zona horaria si es especificada."""
    if (tz_from or tz_to) and pytz is None:
        raise RuntimeError("pytz no disponible. Instala: pip install pytz")
    
    ts = dt_series.copy()
    
    if tz_from and pytz is not None and ts.notna().any():
        if not hasattr(ts.dtype, "tz") or ts.dtype.tz is None:
            ts = ts.dt.tz_localize(
                pytz.timezone(tz_from),
                nonexistent="NaT",
                ambiguous="NaT"
            )
    
    if tz_to and pytz is not None and ts.notna().any():
        ts = ts.dt.tz_convert(pytz.timezone(tz_to))
    
    return _ensure_naive(ts)


def _normalize_col_name(col: str) -> str:
    """Normaliza a versión simple para búsqueda (lower, strip, collapse espacios)."""
    s = str(col).strip().lower()
    s = s.replace("-", " ").replace("_", " ").replace(".", " ")
    s = " ".join(s.split())
    return s


def _ensure_minimal_columns(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Garantiza que existan las columnas 'source' y 'original_file'."""
    df = df.copy()
    if "source" not in df.columns:
        df["source"] = source_name
    if "original_file" not in df.columns:
        df["original_file"] = None
    return df


def _order_and_fill(df: pd.DataFrame, canonical: List[str]) -> pd.DataFrame:
    """Reindexa columnas para incluir las canónicas primero, luego otras."""
    existing = [c for c in canonical if c in df.columns]
    others = [c for c in df.columns if c not in existing]
    return df.reindex(columns=(existing + others))


# ============================================================================
# PROCESAMIENTO SPRINKLR
# ============================================================================

def _find_created_col_sprinklr(
    columns: Sequence[str],
    preferred: Optional[str] = "Created Time"
) -> str:
    """Detecta la columna de fecha en Sprinklr."""
    cols = [str(c).strip() for c in columns]
    
    candidates = [
        "Created Time",
        "created time",
        "Fecha",
        "Date",
    ]
    
    if preferred and preferred.strip() in cols:
        return preferred.strip()
    
    for name in candidates:
        if name in cols:
            return name
    
    lowered = {c.lower(): c for c in cols}
    for key, original in lowered.items():
        if any(w in key for w in ("creat", "fecha", "date", "time")):
            return original
    
    raise KeyError("No se encontró columna de fecha en Sprinklr.")


def _process_sprinklr_dataframe(
    df: pd.DataFrame,
    created_col: Optional[str] = None,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    add_mentions: bool = True
) -> pd.DataFrame:
    """Procesa un DataFrame de Sprinklr."""
    if df is None or not hasattr(df, "columns"):
        raise TypeError("Se espera un pandas.DataFrame válido.")
    
    out = _normalize_columns(df.copy())
    
    # Detectar columna de fecha
    col_fecha = _find_created_col_sprinklr(out.columns, created_col)
    
    # Parsear fecha
    out[col_fecha] = pd.to_datetime(
        out[col_fecha],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    )
    
    # Aplicar zona horaria
    if out[col_fecha].notna().any():
        out[col_fecha] = _apply_timezone(out[col_fecha], tz_from, tz_to)
    
    # Generar date (M/D/YYYY) y hora (12h AM/PM)
    date_series = out[col_fecha].dt.strftime("%m/%d/%Y").fillna("")
    hora_series = (
        out[col_fecha]
        .dt.floor("h")
        .dt.strftime("%I:%M:%S %p")
        .str.lstrip("0")
        .fillna("")
    )
    
    # Remover columnas date/hora existentes
    for c in ("date", "hora"):
        if c in out.columns:
            out = out.drop(columns=[c])
    
    # Insertar columnas en posición inicial
    out.insert(0, "hora", hora_series)
    out.insert(0, "date", date_series)
    
    # Agregar mentions si es necesario
    if add_mentions:
        if "mentions" in out.columns:
            out = out.drop(columns=["mentions"])
        out.insert(2, "mentions", 1)
    
    # Mapear nombres de columnas
    mapping_disponible = {
        k: v for k, v in SPRINKLR_FIELD_MAPPING.items() if k in out.columns
    }
    out = out.rename(columns=mapping_disponible)
    
    # Remover columna de fecha original
    if col_fecha in out.columns:
        out = out.drop(columns=[col_fecha])
    
    return out


def _process_sprinklr(
    files: List[Path],
    created_col: Optional[str] = None,
    skiprows: Optional[int] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Procesa múltiples archivos Sprinklr."""
    if not files:
        return None
    
    frames = []
    for f in files:
        try:
            df = _read_any(f, skiprows=skiprows, header=header)
            df = _process_sprinklr_dataframe(
                df,
                created_col=created_col,
                tz_from=tz_from,
                tz_to=tz_to,
                add_mentions=True
            )
            df["source"] = "sprinklr"
            df["original_file"] = Path(f).name
            frames.append(df)
            print(f"[OK] Sprinklr: {Path(f).name} ({len(df)} filas)")
        except Exception as e:
            print(f"[WARN] Error procesando {f} (Sprinklr): {e}", file=sys.stderr)
    
    if not frames:
        return None
    
    concat = pd.concat(frames, ignore_index=True, sort=False)
    return _ensure_minimal_columns(concat, "sprinklr")


# ============================================================================
# PROCESAMIENTO TUBULAR
# ============================================================================

def _find_created_col_tubular(
    columns: Sequence[str],
    preferred: Optional[str] = "Published_Date"
) -> str:
    """Detecta la columna de fecha en Tubular."""
    cols = [str(c).strip() for c in columns]
    
    candidates = [
        "Published_Date",
        "published_date",
        "Published Date",
        "Date",
    ]
    
    if preferred and preferred.strip() in cols:
        return preferred.strip()
    
    for name in candidates:
        if name in cols:
            return name
    
    lowered = {c.lower(): c for c in cols}
    for key, original in lowered.items():
        if any(w in key for w in ("publish", "date", "time")):
            return original
    
    raise KeyError("No se encontró columna de fecha en Tubular.")


def _normalize_ampm(s: pd.Series) -> pd.Series:
    """Normaliza variantes de AM/PM en una serie de strings."""
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
    """Parsea fechas inteligentemente detectando formato."""
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


def _process_tubular_dataframe(
    df: pd.DataFrame,
    created_col: Optional[str] = None,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    add_mentions: bool = True
) -> pd.DataFrame:
    """Procesa un DataFrame de Tubular."""
    if df is None or not hasattr(df, "columns"):
        raise TypeError("Se espera un pandas.DataFrame válido.")
    
    out = _normalize_columns(df.copy())
    
    # Detectar columna de fecha
    col_fecha = _find_created_col_tubular(out.columns, created_col)
    
    # Parsear fecha inteligentemente
    out[col_fecha] = _parse_datetime_smart(out[col_fecha])
    
    # Aplicar zona horaria
    if out[col_fecha].notna().any():
        out[col_fecha] = _apply_timezone(out[col_fecha], tz_from, tz_to)
    
    # Generar date (M/D/YYYY) y hora (12h AM/PM)
    date_series = out[col_fecha].dt.strftime("%m/%d/%Y").fillna("")
    hora_series = (
        out[col_fecha]
        .dt.floor("h")
        .dt.strftime("%I:%M:%S %p")
        .str.lstrip("0")
        .fillna("")
    )
    
    # Remover columnas date/hora existentes
    for c in ("date", "hora"):
        if c in out.columns:
            out = out.drop(columns=[c])
    
    # Insertar columnas en posición inicial
    out.insert(0, "hora", hora_series)
    out.insert(0, "date", date_series)
    
    # Agregar mentions si es necesario
    if add_mentions:
        if "mentions" in out.columns:
            out = out.drop(columns=["mentions"])
        out.insert(2, "mentions", 1)
    
    # Mapear nombres de columnas
    mapping_disponible = {
        k: v for k, v in TUBULAR_FIELD_MAPPING.items() if k in out.columns
    }
    out = out.rename(columns=mapping_disponible)
    
    # Remover columna de fecha original
    if col_fecha in out.columns:
        out = out.drop(columns=[col_fecha])
    
    return out


def _process_tubular(
    files: List[Path],
    created_col: Optional[str] = None,
    skiprows: Optional[int] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Procesa múltiples archivos Tubular."""
    if not files:
        return None
    
    frames = []
    for f in files:
        try:
            df = _read_any(f, skiprows=skiprows, header=header)
            df = _process_tubular_dataframe(
                df,
                created_col=created_col,
                tz_from=tz_from,
                tz_to=tz_to,
                add_mentions=True
            )
            df["source"] = "tubular"
            df["original_file"] = Path(f).name
            frames.append(df)
            print(f"[OK] Tubular: {Path(f).name} ({len(df)} filas)")
        except Exception as e:
            print(f"[WARN] Error procesando {f} (Tubular): {e}", file=sys.stderr)
    
    if not frames:
        return None
    
    concat = pd.concat(frames, ignore_index=True, sort=False)
    return _ensure_minimal_columns(concat, "tubular")


# ============================================================================
# PROCESAMIENTO YOUSCAN
# ============================================================================

def _process_youscan_dataframe(
    df: pd.DataFrame,
    date_col: str = "Date",
    time_col: str = "Time",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    add_mentions: bool = True
) -> pd.DataFrame:
    """
    Procesa un DataFrame de YouScan:
    - Combina Date (DD.MM.YYYY) + Time (HH:MM 24h)
    - Genera date (M/D/YYYY) y hora (12h AM/PM)
    """
    if df is None or not hasattr(df, "columns"):
        raise TypeError("Se espera un pandas.DataFrame válido.")
    
    out = _normalize_columns(df.copy())
    
    if date_col not in out.columns or time_col not in out.columns:
        raise KeyError(f"No encuentro columnas '{date_col}' y '{time_col}'")
    
    # Combinar Date (DD.MM.YYYY) + Time (HH:MM)
    ts = pd.to_datetime(
        out[date_col].astype(str).str.strip() + " " + out[time_col].astype(str).str.strip(),
        format="%d.%m.%Y %H:%M",
        errors="coerce"
    )
    
    # Aplicar zona horaria
    if ts.notna().any():
        ts = _apply_timezone(ts, tz_from, tz_to)
    
    # Generar date (M/D/YYYY) y hora (12h AM/PM)
    date_series = (
        ts.dt.month.astype("Int64").astype(str) + "/" +
        ts.dt.day.astype("Int64").astype(str) + "/" +
        ts.dt.year.astype("Int64").astype(str)
    )
    date_series = date_series.fillna("")
    
    hora_series = (
        pd.to_datetime(
            out[time_col].astype(str).str.strip(),
            format="%H:%M",
            errors="coerce"
        )
        .dt.floor("h")
        .dt.strftime("%I:%M:%S %p")
        .str.lstrip("0")
        .fillna("")
    )
    
    # Remover columnas date/hora existentes
    for c in ("date", "hora"):
        if c in out.columns:
            out = out.drop(columns=[c])
    
    # Insertar columnas en posición inicial
    out.insert(0, "hora", hora_series)
    out.insert(0, "date", date_series)
    
    # Agregar mentions si es necesario
    if add_mentions:
        if "mentions" in out.columns:
            out = out.drop(columns=["mentions"])
        out.insert(2, "mentions", 1)
    
    # Mapear nombres de columnas
    mapping_disponible = {
        k: v for k, v in YOUSCAN_FIELD_MAPPING.items() if k in out.columns
    }
    out = out.rename(columns=mapping_disponible)
    
    # Remover columnas de fecha/hora originales
    for c in (date_col, time_col):
        if c in out.columns:
            out = out.drop(columns=[c])
    
    return out


def _process_youscan(
    files: List[Path],
    date_col: str = "Date",
    time_col: str = "Time",
    skiprows: Optional[int] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Procesa múltiples archivos YouScan."""
    if not files:
        return None
    
    frames = []
    for f in files:
        try:
            df = _read_any(f, skiprows=skiprows, header=header)
            df = _process_youscan_dataframe(
                df,
                date_col=date_col,
                time_col=time_col,
                tz_from=tz_from,
                tz_to=tz_to,
                add_mentions=True
            )
            df["source"] = "youscan"
            df["original_file"] = Path(f).name
            frames.append(df)
            print(f"[OK] YouScan: {Path(f).name} ({len(df)} filas)")
        except Exception as e:
            print(f"[WARN] Error procesando {f} (YouScan): {e}", file=sys.stderr)
    
    if not frames:
        return None
    
    concat = pd.concat(frames, ignore_index=True, sort=False)
    return _ensure_minimal_columns(concat, "youscan")


# ============================================================================
# FUNCIONES PRINCIPALES: UNIFICACIÓN Y EXPORTACIÓN
# ============================================================================

def _expand_paths(paths: Optional[List[Union[str, Path]]]) -> List[Path]:
    """Normaliza strings->Path y expande globs si es necesario."""
    out: List[Path] = []
    if not paths:
        return out
    
    for p in paths:
        p = str(p)
        if any(ch in p for ch in ["*", "?", "["]):
            import glob
            hits = glob.glob(p)
            out.extend([Path(h) for h in hits])
        else:
            out.append(Path(p))
    
    return out


def unify_and_export(
    sprinklr_files: List[Path],
    tubular_files: List[Path],
    youscan_files: List[Path],
    out_path: Path,
    created_col_spr: Optional[str],
    created_col_tub: Optional[str],
    date_col_you: Optional[str],
    time_col_you: Optional[str],
    skiprows_spr: Optional[int],
    skiprows_tub: Optional[int],
    skiprows_you: Optional[int],
    header: Optional[int],
    tz_from: Optional[str],
    tz_to: Optional[str],
) -> Path:
    """
    Procesa archivos por plataforma y escribe un xlsx con hojas individuales y combined.
    TODO integrado en un único módulo sin dependencias externas.
    """
    sheets: Dict[str, pd.DataFrame] = {}
    
    print("\n=== PROCESANDO FUENTES ===\n")
    
    # Sprinklr
    if sprinklr_files:
        df_s = _process_sprinklr(
            sprinklr_files,
            created_col=created_col_spr,
            skiprows=skiprows_spr,
            header=header,
            tz_from=tz_from,
            tz_to=tz_to
        )
        if df_s is not None:
            sheets["sprinklr"] = df_s

    # Tubular
    if tubular_files:
        df_t = _process_tubular(
            tubular_files,
            created_col=created_col_tub,
            skiprows=skiprows_tub,
            header=header,
            tz_from=tz_from,
            tz_to=tz_to
        )
        if df_t is not None:
            sheets["tubular"] = df_t

    # YouScan
    if youscan_files:
        df_y = _process_youscan(
            youscan_files,
            date_col=date_col_you,
            time_col=time_col_you,
            skiprows=skiprows_you,
            header=header,
            tz_from=tz_from,
            tz_to=tz_to
        )
        if df_y is not None:
            sheets["youscan"] = df_y

    if not sheets:
        raise RuntimeError("No hubo hojas para exportar (no se procesó ningún archivo).")

    print(f"\n[RESUMEN] Se cargaron {len(sheets)} fuentes:")
    for name, df in sheets.items():
        print(f"  - {name}: {len(df)} filas")

    # Crear hoja combined
    print("\n=== COMBINANDO DATOS ===\n")
    all_frames = list(sheets.values())
    combined = pd.concat(all_frames, ignore_index=True, sort=False)
    combined = _order_and_fill(combined, CANONICAL_COLUMNS)
    print(f"[OK] Combined: {len(combined)} filas totales")

    # Reordenar hojas individuales
    for k in list(sheets.keys()):
        sheets[k] = _order_and_fill(sheets[k], CANONICAL_COLUMNS)

    # Escribir .xlsx
    out_path = Path(out_path)
    if out_path.suffix.lower() != ".xlsx":
        out_path = out_path.with_suffix(".xlsx")

    print(f"\n=== EXPORTANDO ===\n")
    try:
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            for name, df in sheets.items():
                sheet_name = name[:31]  # Excel límite 31 caracteres
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  ✓ Hoja '{sheet_name}': {len(df)} filas")
            
            combined.to_excel(writer, sheet_name="combined", index=False)
            print(f"  ✓ Hoja 'combined': {len(combined)} filas")
    except Exception as e:
        print(f"[WARN] xlsxwriter no disponible, usando openpyxl: {e}")
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for name, df in sheets.items():
                sheet_name = name[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            combined.to_excel(writer, sheet_name="combined", index=False)

    return out_path


# ============================================================================
# FUNCIÓN PRINCIPAL PARA USO PROGRAMÁTICO (GOOGLE COLAB)
# ============================================================================

def unify_files(
    sprinklr_files: Optional[List[Union[str, Path]]] = None,
    tubular_files: Optional[List[Union[str, Path]]] = None,
    youscan_files: Optional[List[Union[str, Path]]] = None,
    out_path: Union[str, Path] = "unified.xlsx",
    created_col_spr: Optional[str] = "Created Time",
    created_col_tub: Optional[str] = "Published_Date",
    date_col_you: Optional[str] = "Date",
    time_col_you: Optional[str] = "Time",
    skiprows_sprinklr: Optional[int] = None,
    skiprows_tubular: Optional[int] = None,
    skiprows_youscan: Optional[int] = None,
    header: int = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
) -> Path:
    """
    Función principal para unificar archivos programáticamente desde Python/Colab.
    
    Todo está integrado en un único módulo. Solo necesitas hacer:
    >>> from unify import unify_files
    
    Args:
        sprinklr_files: Lista de archivos Sprinklr (paths o strings)
        tubular_files: Lista de archivos Tubular
        youscan_files: Lista de archivos YouScan
        out_path: Ruta del archivo de salida .xlsx (default: "unified.xlsx")
        created_col_spr: Nombre de columna de fecha en Sprinklr (default: "Created Time")
        created_col_tub: Nombre de columna de fecha en Tubular (default: "Published_Date")
        date_col_you: Nombre de columna de fecha en YouScan (default: "Date")
        time_col_you: Nombre de columna de hora en YouScan (default: "Time")
        skiprows_sprinklr: Número de filas a saltar en Sprinklr (default: None = 0)
        skiprows_tubular: Número de filas a saltar en Tubular (default: None = 0)
        skiprows_youscan: Número de filas a saltar en YouScan (default: None = 0)
        header: Fila del encabezado (default: 0)
        tz_from: Zona horaria origen (opcional, ej: "UTC", "America/Bogota")
        tz_to: Zona horaria destino (opcional, ej: "America/New_York")
    
    Returns:
        Path del archivo generado
    
    Examples:
        >>> # Ejemplo 1: Unificar los tres orígenes desde Google Colab
        >>> from unify import unify_files
        >>> result = unify_files(
        ...     sprinklr_files=["XandNewsCO.xlsx"],
        ...     tubular_files=["tubular1.xlsx"],
        ...     youscan_files=["youscan.csv"],
        ...     skiprows_sprinklr=2,
        ...     skiprows_tubular=0,
        ...     skiprows_youscan=1,
        ...     tz_from="UTC",
        ...     tz_to="America/Bogota",
        ...     out_path="unified_output.xlsx"
        ... )
        >>> print(f"✓ Archivo creado: {result}")
        
        >>> # Ejemplo 2: Solo Sprinklr y Tubular
        >>> result = unify_files(
        ...     sprinklr_files=["file1.xlsx", "file2.xlsx"],
        ...     tubular_files=["tubular.csv"],
        ...     skiprows_sprinklr=2,
        ...     out_path="unified.xlsx"
        ... )
    """
    spr_paths = _expand_paths(sprinklr_files) if sprinklr_files else []
    tub_paths = _expand_paths(tubular_files) if tubular_files else []
    you_paths = _expand_paths(youscan_files) if youscan_files else []

    if not (spr_paths or tub_paths or you_paths):
        raise ValueError("Debes proporcionar al menos un archivo de alguna fuente.")

    try:
        result = unify_and_export(
            sprinklr_files=spr_paths,
            tubular_files=tub_paths,
            youscan_files=you_paths,
            out_path=Path(out_path),
            created_col_spr=created_col_spr,
            created_col_tub=created_col_tub,
            date_col_you=date_col_you,
            time_col_you=time_col_you,
            skiprows_spr=skiprows_sprinklr,
            skiprows_tub=skiprows_tubular,
            skiprows_you=skiprows_youscan,
            header=header,
            tz_from=tz_from,
            tz_to=tz_to,
        )
        print(f"\n[SUCCESS] ✓ Archivo unificado creado: {result}")
        return result
    except Exception as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        raise


# ============================================================================
# INTERFAZ DE LÍNEA DE COMANDOS (CLI)
# ============================================================================

def main(argv: Optional[List[str]] = None):
    """Interfaz de línea de comandos para unify.py"""
    parser = argparse.ArgumentParser(
        description="Unifica Sprinklr/Tubular/YouScan en un único .xlsx con skiprows separados"
    )
    parser.add_argument("--sprinklr", nargs="*", default=[], 
                       help="Archivo(s) Sprinklr (ej: file1.xlsx file2.xlsx)")
    parser.add_argument("--tubular", nargs="*", default=[], 
                       help="Archivo(s) Tubular")
    parser.add_argument("--youscan", nargs="*", default=[], 
                       help="Archivo(s) YouScan")
    parser.add_argument("-o", "--out", default="unified.xlsx", 
                       help="Ruta de salida .xlsx (default: unified.xlsx)")
    parser.add_argument("--created-col-spr", default="Created Time",
                       help="Columna de fecha en Sprinklr (default: Created Time)")
    parser.add_argument("--created-col-tub", default="Published_Date",
                       help="Columna de fecha en Tubular (default: Published_Date)")
    parser.add_argument("--date-col-you", default="Date",
                       help="Columna de fecha en YouScan (default: Date)")
    parser.add_argument("--time-col-you", default="Time",
                       help="Columna de hora en YouScan (default: Time)")
    parser.add_argument("--skiprows-sprinklr", type=int, default=None,
                       help="Filas a saltar en Sprinklr (default: 0)")
    parser.add_argument("--skiprows-tubular", type=int, default=None,
                       help="Filas a saltar en Tubular (default: 0)")
    parser.add_argument("--skiprows-youscan", type=int, default=None,
                       help="Filas a saltar en YouScan (default: 0)")
    parser.add_argument("--header", type=int, default=0,
                       help="Fila del encabezado (default: 0)")
    parser.add_argument("--tz-from", dest="tz_from", default=None,
                       help="Zona horaria origen (ej: UTC, America/Bogota)")
    parser.add_argument("--tz-to", dest="tz_to", default=None,
                       help="Zona horaria destino (ej: UTC, America/Bogota)")
    
    args = parser.parse_args(argv)

    try:
        result = unify_files(
            sprinklr_files=args.sprinklr if args.sprinklr else None,
            tubular_files=args.tubular if args.tubular else None,
            youscan_files=args.youscan if args.youscan else None,
            out_path=args.out,
            created_col_spr=args.created_col_spr,
            created_col_tub=args.created_col_tub,
            date_col_you=args.date_col_you,
            time_col_you=args.time_col_you,
            skiprows_sprinklr=args.skiprows_sprinklr,
            skiprows_tubular=args.skiprows_tubular,
            skiprows_youscan=args.skiprows_youscan,
            header=args.header,
            tz_from=args.tz_from,
            tz_to=args.tz_to,
        )
        print(f"\n✓ ÉXITO: {result}")
    except Exception as e:
        print(f"\n✗ ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
