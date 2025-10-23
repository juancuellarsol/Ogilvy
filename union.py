#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unify.py
--------
Unifica exports de Sprinklr, YouScan y Tubular en un único .xlsx:
- Una hoja por fuente presente (sprinklr, tubular, youscan)
- Una hoja "combined" con todas las filas unificadas
- Esquema canónico de columnas
- Soporte de timezone (tz_from -> tz_to) y de formatos de fecha distintos

Uso CLI (ejemplos al final) o como librería: from unify import etl_unify
"""

from __future__ import annotations

import argparse
import glob
import os
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, Dict

import pandas as pd

try:
    import pytz
except Exception:
    pytz = None  # se maneja abajo con chequeo

# -----------------------------
# Configuración y utilidades
# -----------------------------

CANON_COLUMNS: List[str] = [
    "original_file",   # nombre de archivo origen
    "author",          # autor del contenido
    "message",         # contenido/texto
    "link",            # URL del post/video
    "date",            # fecha local (YYYY-MM-DD)
    "hora",            # hora local (HH:MM)
    "source",          # fuente: Sprinklr/Tubular/YouScan
    "sentiment",       # sentimiento (Sprinklr/YouScan)
    "country",         # país
    "engagement",      # métricas de engagement
    "reach",           # alcance (Sprinklr)
    "views",           # vistas (Tubular/YouScan)
    "mentions",        # conteo de menciones (default=1 si no existe)
]

# sinónimos por campo y por fuente (robusto a exports distintos)
SYN_SPRINKLR: Dict[str, Sequence[str]] = {
    "author": ["From User","User Name","Author","Author Name","User"],
    "message": ["Conversation Stream","Message","Text","Content","Message Text","Post Message"],
    "link": ["Post Url","URL","Link","Permalink"],
    "created": ["Created Time.1","Created At","Fecha","Timestamp","Published Date","Published_Date"],
    "source": ["snTypeColumn","Source","Source Type","Network"],
    "sentiment": ["Sentiment","Recalibrated Sentiment","Post Sentiment","Message Sentiment"],
    "country": ["Country","Author Country","Profile Country"],
    "reach": ["Reach (SUM)","Reach","Total Reach"],
    "engagement": ["Earned Engagements (Recalibrated) (SUM)","Engagements","Total Engagement","Interactions"],
    "mentions": ["Mentions (SUM)","Mentions","Count"],
}

SYN_YOUSCAN: Dict[str, Sequence[str]] = {
    "author": ["Author","Author Name","Nickname","User"],
    "message": ["Text","Message","Content", "Text snippet"],
    "link": ["Link","URL","Permalink"],
    "date": ["Date"],   # dd.mm.yyyy
    "time": ["Time"],   # HH:MM 24h
    "sentiment": ["Sentiment"],
    "country": ["Country","Location"],
    "engagement": ["Engagement","Interactions","Total engagement"],
    "views": ["Views","View count"],
    "mentions": ["Mentions","Count"],
}

SYN_TUBULAR: Dict[str, Sequence[str]] = {
    "author": ["Channel Name","Creator","Owner","Author"],
    "message": ["Title","Video_Title","Description"],
    "link": ["Video Link","URL","Link","Permalink","Video_URL"],
    "created": ["Published Date","Published_Date","Created Time","Timestamp","Fecha"],
    "country": ["Country","Owner Country","Channel Country"],
    "views": ["Views","View Count","Total Views"],
    "engagement": ["Engagement","Total_Engagements","Interactions","Reactions"],
    "mentions": ["Mentions","Count"],
}

# -----------------------------
# Helpers de lectura y parsing
# -----------------------------

def _detect_is_excel(path: str) -> bool:
    ext = Path(path).suffix.lower()
    return ext in (".xlsx",".xls",".xlsm",".xlsb")

def read_any(
    path: str,
    skiprows: int = 0,
    header: int = 0,
    dtype: Optional[Dict] = None
) -> pd.DataFrame:
    """
    Lee CSV/Excel de forma robusta.
    - Excel: usa read_excel(sheet_name=0)
    - CSV: intenta con UTF-8 y si falla prueba latin-1/utf-16
    """
    if _detect_is_excel(path):
        return pd.read_excel(path, skiprows=skiprows, header=header, dtype=dtype)
    # CSV
    for enc in ("utf-8-sig","utf-8","latin-1","utf-16","cp1252"):
        try:
            return pd.read_csv(path, skiprows=skiprows, header=header, dtype=dtype, encoding=enc)
        except Exception:
            continue
    # último intento con engine python sin encoding explícito
    return pd.read_csv(path, skiprows=skiprows, header=header, dtype=dtype, engine="python")


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.columns = [str(c).strip() for c in df2.columns]
    return df2


def _first_match(cols: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    norm = {c.strip().lower(): c for c in cols}
    for cand in candidates:
        key = cand.strip().lower()
        if key in norm:
            return norm[key]
    return None


def _ensure_tz(dt: pd.Series, tz_from: Optional[str], tz_to: Optional[str]) -> Tuple[pd.Series, pd.Series]:
    """
    Devuelve (date_str, time_str) con tz convertida si corresponde.
    - date_str: YYYY-MM-DD (en tz_to si se da)
    - time_str: HH:MM (en tz_to si se da)
    """
    if tz_from and tz_to and pytz is not None:
        try:
            src = pytz.timezone(tz_from)
            dst = pytz.timezone(tz_to)
            # si viene naive, localize con tz_from
            dt_localized = dt.dt.tz_localize(src, nonexistent='NaT', ambiguous='NaT') if dt.dt.tz is None else dt.dt.tz_convert(dst)
            dt_converted = dt_localized.dt.tz_convert(dst)
        except Exception:
            # fallback; asumir naive -> tz_to sin conversión
            dt_converted = dt
    else:
        dt_converted = dt

    date_str = dt_converted.dt.strftime("%Y-%m-%d")
    time_str = dt_converted.dt.strftime("%H:%M")
    return date_str, time_str


def _coerce_datetime(series: pd.Series, dayfirst: bool = False) -> pd.Series:
    """Convierte a datetime; errores -> NaT."""
    return pd.to_datetime(series, errors="coerce", dayfirst=dayfirst, infer_datetime_format=True)


# -----------------------------
# Procesamiento por FUENTE
# -----------------------------

def process_sprinklr(
    paths: Sequence[str],
    skiprows: int = 0,
    header: int = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None
) -> pd.DataFrame:
    rows = []
    for p in paths:
        df = _normalize_cols(read_any(p, skiprows=skiprows, header=header))
        cols = list(df.columns)

        c_author = _first_match(cols, SYN_SPRINKLR["author"])
        c_msg = _first_match(cols, SYN_SPRINKLR["message"])
        c_link = _first_match(cols, SYN_SPRINKLR["link"])
        c_created = _first_match(cols, SYN_SPRINKLR["created"])
        c_source = _first_match(cols, SYN_SPRINKLR["source"])
        c_sent = _first_match(cols, SYN_SPRINKLR["sentiment"])
        c_country = _first_match(cols, SYN_SPRINKLR["country"])
        c_reach = _first_match(cols, SYN_SPRINKLR["reach"])
        c_eng = _first_match(cols, SYN_SPRINKLR["engagement"])
        c_mentions = _first_match(cols, SYN_SPRINKLR["mentions"])

        if c_created is None and "Created Time" in cols:
            c_created = "Created Time"

        tmp = pd.DataFrame()
        tmp["original_file"] = Path(p).name
        tmp["author"] = df.get(c_author, pd.Series([None]*len(df)))
        tmp["message"] = df.get(c_msg, pd.Series([None]*len(df)))
        tmp["link"] = df.get(c_link, pd.Series([None]*len(df)))
        created_raw = df.get(c_created, pd.Series([None]*len(df)))
        created_dt = _coerce_datetime(created_raw, dayfirst=False)
        date_str, time_str = _ensure_tz(created_dt, tz_from, tz_to)
        tmp["date"] = date_str
        tmp["hora"] = time_str
        tmp["source"] = df.get(c_source, "Sprinklr")
        tmp["sentiment"] = df.get(c_sent, pd.Series([None]*len(df)))
        tmp["country"] = df.get(c_country, pd.Series([None]*len(df)))
        tmp["engagement"] = df.get(c_eng, pd.Series([None]*len(df)))
        tmp["reach"] = df.get(c_reach, pd.Series([None]*len(df)))
        tmp["views"] = None
        tmp["mentions"] = df.get(c_mentions, pd.Series([1]*len(df)))
        rows.append(tmp)

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=CANON_COLUMNS)
    return out.reindex(columns=CANON_COLUMNS)


def process_youscan(
    paths: Sequence[str],
    skiprows: int = 0,
    header: int = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None
) -> pd.DataFrame:
    rows = []
    for p in paths:
        df = _normalize_cols(read_any(p, skiprows=skiprows, header=header))
        cols = list(df.columns)

        c_author = _first_match(cols, SYN_YOUSCAN["author"])
        c_msg = _first_match(cols, SYN_YOUSCAN["message"])
        c_link = _first_match(cols, SYN_YOUSCAN["link"])
        c_date = _first_match(cols, SYN_YOUSCAN["date"])
        c_time = _first_match(cols, SYN_YOUSCAN["time"])
        c_sent = _first_match(cols, SYN_YOUSCAN["sentiment"])
        c_country = _first_match(cols, SYN_YOUSCAN["country"])
        c_eng = _first_match(cols, SYN_YOUSCAN["engagement"])
        c_views = _first_match(cols, SYN_YOUSCAN["views"])
        c_mentions = _first_match(cols, SYN_YOUSCAN["mentions"])

        tmp = pd.DataFrame()
        tmp["original_file"] = Path(p).name
        tmp["author"] = df.get(c_author, pd.Series([None]*len(df)))
        tmp["message"] = df.get(c_msg, pd.Series([None]*len(df)))
        tmp["link"] = df.get(c_link, pd.Series([None]*len(df)))

        # YouScan típico: Date (dd.mm.yyyy) + Time (HH:MM)
        date_raw = df.get(c_date, pd.Series([None]*len(df)))
        time_raw = df.get(c_time, pd.Series([None]*len(df)))

        dt_combined = pd.to_datetime(
            date_raw.astype(str).str.strip() + " " + time_raw.astype(str).str.strip(),
            errors="coerce", dayfirst=True, infer_datetime_format=True
        )

        date_str, time_str = _ensure_tz(dt_combined, tz_from, tz_to)
        tmp["date"] = date_str
        tmp["hora"] = time_str

        tmp["source"] = "YouScan"
        tmp["sentiment"] = df.get(c_sent, pd.Series([None]*len(df)))
        tmp["country"] = df.get(c_country, pd.Series([None]*len(df)))
        tmp["engagement"] = df.get(c_eng, pd.Series([None]*len(df)))
        tmp["reach"] = None
        tmp["views"] = df.get(c_views, pd.Series([None]*len(df)))
        tmp["mentions"] = df.get(c_mentions, pd.Series([1]*len(df)))

        rows.append(tmp)

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=CANON_COLUMNS)
    return out.reindex(columns=CANON_COLUMNS)


def process_tubular(
    paths: Sequence[str],
    skiprows: int = 0,
    header: int = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None
) -> pd.DataFrame:
    rows = []
    for p in paths:
        df = _normalize_cols(read_any(p, skiprows=skiprows, header=header))
        cols = list(df.columns)

        c_author = _first_match(cols, SYN_TUBULAR["author"])
        c_msg = _first_match(cols, SYN_TUBULAR["message"])
        c_link = _first_match(cols, SYN_TUBULAR["link"])
        c_created = _first_match(cols, SYN_TUBULAR["created"])
        c_country = _first_match(cols, SYN_TUBULAR["country"])
        c_views = _first_match(cols, SYN_TUBULAR["views"])
        c_eng = _first_match(cols, SYN_TUBULAR["engagement"])
        c_mentions = _first_match(cols, SYN_TUBULAR["mentions"])

        tmp = pd.DataFrame()
        tmp["original_file"] = Path(p).name
        tmp["author"] = df.get(c_author, pd.Series([None]*len(df)))
        # Usual en Tubular: si "Title" existe, úsalo como message; si no, "Description"
        tmp["message"] = df.get(c_msg, pd.Series([None]*len(df)))
        tmp["link"] = df.get(c_link, pd.Series([None]*len(df)))

        created_raw = df.get(c_created, pd.Series([None]*len(df)))
        # Tubular a veces viene como 'YYYY-MM-DD HH:MM:SS' UTC
        created_dt = _coerce_datetime(created_raw, dayfirst=False)
        date_str, time_str = _ensure_tz(created_dt, tz_from, tz_to)
        tmp["date"] = date_str
        tmp["hora"] = time_str

        tmp["source"] = "Tubular"
        tmp["sentiment"] = None
        tmp["country"] = df.get(c_country, pd.Series([None]*len(df)))
        tmp["engagement"] = df.get(c_eng, pd.Series([None]*len(df)))
        tmp["reach"] = None
        tmp["views"] = df.get(c_views, pd.Series([None]*len(df)))
        tmp["mentions"] = df.get(c_mentions, pd.Series([1]*len(df)))

        rows.append(tmp)

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=CANON_COLUMNS)
    return out.reindex(columns=CANON_COLUMNS)


# -----------------------------
# Motor principal
# -----------------------------

def _expand_globs(patterns: Sequence[str]) -> List[str]:
    out: List[str] = []
    for pat in patterns:
        out.extend(sorted(glob.glob(pat)))
    # Remueve duplicados manteniendo orden
    seen = set()
    uniq = []
    for p in out:
        if p not in seen:
            uniq.append(p); seen.add(p)
    return uniq


def etl_unify(
    sprinklr_files: Sequence[str] = (),
    tubular_files: Sequence[str] = (),
    youscan_files: Sequence[str] = (),
    out_xlsx: str = "etl_unificado.xlsx",
    skiprows_sprinklr: int = 0,
    skiprows_tubular: int = 0,
    skiprows_youscan: int = 0,
    header: int = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None
) -> str:
    """
    Ejecuta el ETL completo y guarda un .xlsx con:
      - Hoja por fuente presente
      - Hoja "combined" final
    Retorna la ruta del archivo generado.
    """
    spr_paths = _expand_globs(sprinklr_files)
    tub_paths = _expand_globs(tubular_files)
    ysc_paths = _expand_globs(youscan_files)

    sheets = {}
    if spr_paths:
        sheets["sprinklr"] = process_sprinklr(
            spr_paths, skiprows=skiprows_sprinklr, header=header, tz_from=tz_from, tz_to=tz_to
        )
    if tub_paths:
        sheets["tubular"] = process_tubular(
            tub_paths, skiprows=skiprows_tubular, header=header, tz_from=tz_from, tz_to=tz_to
        )
    if ysc_paths:
        sheets["youscan"] = process_youscan(
            ysc_paths, skiprows=skiprows_youscan, header=header, tz_from=tz_from, tz_to=tz_to
        )

    if not sheets:
        raise ValueError("No se encontraron archivos de ninguna fuente (Sprinklr/Tubular/YouScan).")

    # Combine
    combined = pd.concat([sheets[k] for k in sheets], ignore_index=True).reindex(columns=CANON_COLUMNS)

    # Orden básico por fecha/hora si están presentes
    with pd.option_context("mode.use_inf_as_na", True):
        dt = pd.to_datetime(combined["date"].astype(str) + " " + combined["hora"].astype(str), errors="coerce")
    combined = combined.assign(_dt=dt).sort_values("_dt").drop(columns=["_dt"])

    # Guardado
    out_xlsx = str(out_xlsx)
    out_dir = os.path.dirname(out_xlsx)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
        combined.to_excel(writer, sheet_name="combined", index=False)

    return out_xlsx


# -----------------------------
# CLI
# -----------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Unifica Sprinklr, YouScan y Tubular en un .xlsx",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    p.add_argument("--sprinklr", nargs="*", default=(), help="Rutas o patrones (glob) para Sprinklr, p.ej. '/content/data/spr_*.xlsx'")
    p.add_argument("--tubular", nargs="*", default=(), help="Rutas o patrones (glob) para Tubular, p.ej. '/content/data/tub_*.csv'")
    p.add_argument("--youscan", nargs="*", default=(), help="Rutas o patrones (glob) para YouScan, p.ej. '/content/data/ys_*.xlsx'")

    p.add_argument("--out", default="etl_unificado.xlsx", help="Ruta de salida .xlsx")

    p.add_argument("--skiprows_sprinklr", type=int, default=0, help="Filas a saltar al inicio (Sprinklr)")
    p.add_argument("--skiprows_tubular", type=int, default=0, help="Filas a saltar al inicio (Tubular)")
    p.add_argument("--skiprows_youscan", type=int, default=0, help="Filas a saltar al inicio (YouScan)")

    p.add_argument("--header", type=int, default=0, help="Fila de encabezado (misma para las 3 fuentes)")

    p.add_argument("--tz_from", default=None, help="Timezone de origen, p.ej. 'UTC'")
    p.add_argument("--tz_to", default=None, help="Timezone destino, p.ej. 'America/Bogota'")

    return p.parse_args(argv)


def main():
    args = parse_args()
    # Validación TZ
    if (args.tz_from or args.tz_to) and pytz is None:
        raise RuntimeError("Se requiere 'pytz' para conversión de zonas horarias. Instala con: pip install pytz")

    result = etl_unify(
        sprinklr_files=args.sprinklr,
        tubular_files=args.tubular,
        youscan_files=args.youscan,
        out_xlsx=args.out,
        skiprows_sprinklr=args.skiprows_sprinklr,
        skiprows_tubular=args.skiprows_tubular,
        skiprows_youscan=args.skiprows_youscan,
        header=args.header,
        tz_from=args.tz_from,
        tz_to=args.tz_to,
    )
    print(f"\n✓ Archivo generado: {result}")


if __name__ == "__main__":
    main()
