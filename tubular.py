"""
tubular.py
------------------------------
Limpieza reutilizable para exports de Tubular.

Funciones:
- process_dataframe(df, ...): procesa un DataFrame y agrega columnas 'date' y 'hora'
- process_file(path, ...): lee archivo y devuelve DF procesado
- export_df(df, out_path): exporta .xlsx/.csv
- auto_export(path, ...): lee, procesa y guarda en un paso
- batch_export(files, ...): procesa varios archivos

Notas:
- No usa infer_datetime_format (deprecado).
- Normaliza 'a.m.'/'p.m.' -> 'AM'/'PM'.
- Autodetecta formato base (slash => dayfirst; dash => ISO) para evitar warnings.
- 'hora' se trunca a la hora inferior (17:01 -> 17:00) y se almacena como fracción del día para Excel.
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional, Sequence, Union, List
import re
import glob as _glob

import pandas as pd

try:
    import pytz
except Exception:
    pytz = None

# =========================
# Helpers
# =========================

_AMPM_NORMALIZER = re.compile(r'(\s*[ap][.\s]?m[.]?)', flags=re.I)

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Quita espacios raros en encabezados y colapsa espacios internos."""
    cols = (
        df.columns.astype(str)
          .str.strip()
          .str.replace(r"\s+", " ", regex=True)
    )
    df.columns = cols
    return df

def _normalize_ampm(s: pd.Series) -> pd.Series:
    """Convierte 'a.m./p.m.' (variantes) a 'AM/PM'."""
    def _fix(t: str) -> str:
        t = t or ""
        m = _AMPM_NORMALIZER.search(t)
        if not m:
            return t
        frag = m.group(1).lower()
        return _AMPM_NORMALIZER.sub(" AM", t) if "a" in frag else _AMPM_NORMALIZER.sub(" PM", t)
    return s.astype(str).map(_fix)

def _parse_datetime_smart(raw: pd.Series) -> pd.Series:
    """
    Intenta parsear fechas evitando warnings:
      - si predominan '/', asume dd/mm/yyyy -> dayfirst=True con formatos comunes
      - si predominan '-', intenta ISO explícito
      - luego cae a pd.to_datetime con heurística mínima
    """
    raw = _normalize_ampm(raw)
    sample = raw.dropna().astype(str).str.strip().head(50)
    has_slash = sample.str.contains("/").mean() > 0.5
    has_dash  = sample.str.contains("-").mean() > 0.5

    if has_slash and not has_dash:
        # Formatos latam más comunes
        for fmt in ("%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
            out = pd.to_datetime(raw, format=fmt, errors="coerce", dayfirst=True)
            if out.notna().any():
                return out
        # fallback latam
        return pd.to_datetime(raw, errors="coerce", dayfirst=True)

    # ISO u otros (NO usar dayfirst para silenciar warning)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        out = pd.to_datetime(raw, format=fmt, errors="coerce")
        if out.notna().any():
            return out
    return pd.to_datetime(raw, errors="coerce")

_CANDIDATE_CREATED_COLS = [
    # Tubular típicos
    "Published_Date", "Published Date", "published_date", "published date",
    # genéricos
    "Created Time", "Created time", "created_time", "created time",
    "Fecha de creación", "Fecha", "Date Created", "Timestamp", "Time",
]

def _find_created_col(columns: Sequence[str], preferred: Optional[str]) -> str:
    cols = [str(c).strip() for c in columns]
    if preferred and preferred.strip() in cols:
        return preferred.strip()
    for name in _CANDIDATE_CREATED_COLS:
        if name in cols:
            return name
    lowered = {c.lower(): c for c in cols}
    for key, original in lowered.items():
        if any(w in key for w in ("publish", "creat", "fecha", "time", "date", "timestamp")):
            return original
    raise KeyError("No pude encontrar la columna de fecha/hora. Revisa 'created_col' y 'skiprows'.")

def _read_any(file_path: Union[str, Path], skiprows=None, header: Optional[int]=0) -> pd.DataFrame:
    file_path = Path(file_path)
    ext = file_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(file_path, skiprows=skiprows, header=header)
    if ext == ".csv":
        return pd.read_csv(file_path, skiprows=skiprows, header=header)
    raise ValueError(f"Formato no soportado: {ext}. Usa .xlsx, .xls o .csv.")

def _ensure_naive(dt_series: pd.Series) -> pd.Series:
    if hasattr(dt_series.dtype, "tz") and dt_series.dtype.tz is not None:
        return dt_series.dt.tz_convert(None)
    return dt_series

# =========================
# API principal
# =========================

def process_dataframe(
    df: pd.DataFrame,
    created_col: Optional[str] = "Published_Date",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    keep_columns: Optional[Sequence[str]] = None,  # columnas extra a mantener
    use_default_columns: bool = False              # usa un set por defecto útil
) -> pd.DataFrame:
    """
    Procesa un DataFrame:
    - Detecta/parsea la columna de fecha/hora.
    - Agrega 'date' (M/D/YYYY) y 'hora' (como fracción del día para Excel).
    - NO crea 'tag' ni 'tag_text'.
    - Permite conservar un subconjunto de columnas (`keep_columns`).
    """
    if df is None or not hasattr(df, "columns"):
        raise TypeError("process_dataframe espera un pandas.DataFrame válido.")

    df = _normalize_columns(df)
    col = _find_created_col(df.columns, created_col)
    out = df.copy()

    # Parseo robusto sin infer_datetime_format
    out[col] = _parse_datetime_smart(out[col])

    # TZ opcional
    if (tz_from or tz_to) and pytz is None:
        raise RuntimeError("pytz no disponible. Instálalo para usar conversión de zona horaria.")
    if tz_from and pytz is not None and out[col].notna().any():
        if not hasattr(out[col].dtype, "tz") or out[col].dtype.tz is None:
            out[col] = out[col].dt.tz_localize(pytz.timezone(tz_from), nonexistent="NaT", ambiguous="NaT")
    if tz_to and pytz is not None and out[col].notna().any():
        out[col] = out[col].dt.tz_convert(pytz.timezone(tz_to))
    out[col] = _ensure_naive(out[col])

    # Derivadas
    date_series = pd.to_datetime(out[col])

    # CORRECCIÓN: almacenar 'hora' como fracción del día para Excel
    hour_bucket = out[col].dt.floor("h")
    hora_seconds = (hour_bucket.dt.hour * 3600
                + hour_bucket.dt.minute * 60
                + hour_bucket.dt.second)
    hora_series = hora_seconds / 86400.0   # fracción del día (0..1)

    # Insertar evitando duplicados
    for c in ("date", "hora"):
        if c in out.columns:
            out = out.drop(columns=[c])
    out.insert(0, "hora", hora_series)
    out.insert(0, "date", date_series)

    if drop_original_created and col in out.columns:
        out = out.drop(columns=[col])

    # --- Selección final de columnas ---
    # Si pides columnas, filtramos SOLO esas (más 'date' y 'hora').
    if use_default_columns and keep_columns is None:
        keep_columns = [
            "Published_Date", "Platform", "Creator", "Video_Title",
            "Video_URL", "Total_Engagements", "Views"
        ]

    if keep_columns is not None:
        # Asegura que sea lista (y no un string suelto)
        keep_columns = list(keep_columns)
        final_cols = ["date", "hora"] + [c for c in keep_columns if c in out.columns]
        # reindex garantiza que NO se cuelen otras columnas
        out = out.reindex(columns=final_cols)
        return out

    # Si NO pasas keep_columns -> conservamos todo, pero con date/hora al frente
    ordered = ["date", "hora"] + [c for c in out.columns if c not in ("date", "hora")]
    return out[ordered]


def process_file(
    file_path: Union[str, Path],
    created_col: Optional[str] = "Published_Date",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    keep_columns: Optional[Sequence[str]] = None,
    use_default_columns: bool = False,
) -> pd.DataFrame:
    df = _read_any(file_path, skiprows=skiprows, header=header)
    df = _normalize_columns(df)
    return process_dataframe(
        df,
        created_col=created_col,
        tz_from=tz_from,
        tz_to=tz_to,
        drop_original_created=drop_original_created,
        keep_columns=keep_columns,
        use_default_columns=use_default_columns,
    )

def _col_idx_to_xlsx_col(idx: int) -> str:
    s = ""
    n = idx + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def export_df(df: pd.DataFrame, out_path: Union[str, Path]) -> Path:
    out_path = Path(out_path)

    # Si tienes columna 'date' y quieres guardarla como fecha real:
    if "date" in df.columns:
        d = pd.to_datetime(df["date"], errors="coerce")
        df = df.copy()
        df["date"] = d.dt.normalize()  # 00:00:00 (Excel la verá como fecha)

    ext = out_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        try:
            import xlsxwriter  # asegura que use este motor
            with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
                sheet = "Sheet1"
                df.to_excel(writer, index=False, sheet_name=sheet)

                wb = writer.book
                ws = writer.sheets[sheet]

                # --- Formato fecha (opcional) ---
                if "date" in df.columns:
                    fmt_date = wb.add_format({"num_format": "dd/mm/yy"})
                    c_date = df.columns.get_loc("date")
                    ws.set_column(c_date, c_date, 10, fmt_date)

                # --- FORMATEO REAL DE 'hora' ---
                # 'hora' VIENE como fracción del día (0..1). Aplicamos formato de hora
                if "hora" in df.columns:
                    fmt_time = wb.add_format({"num_format": "h:mm:ss AM/PM"})
                    c_hora = df.columns.get_loc("hora")
                    ws.set_column(c_hora, c_hora, 12, fmt_time)

        except ModuleNotFoundError:
            # --- Fallback sin xlsxwriter: usa openpyxl ---
            df.to_excel(out_path, index=False)
            try:
                from openpyxl import load_workbook
                from openpyxl.utils import get_column_letter
                wb = load_workbook(out_path)
                ws = wb.active

                if "date" in df.columns:
                    from openpyxl.styles import numbers
                    c_date = df.columns.get_loc("date") + 1
                    for col_cells in ws.iter_cols(min_col=c_date, max_col=c_date,
                                                  min_row=2, max_row=ws.max_row):
                        for c in col_cells:
                            c.number_format = numbers.FORMAT_DATE_DDMMYY

                if "hora" in df.columns:
                    c_hora = df.columns.get_loc("hora") + 1  # 1-based
                    col_letter = get_column_letter(c_hora)
                    for r in range(2, ws.max_row + 1):
                        cell = ws[f"{col_letter}{r}"]
                        if cell.value is not None:
                            cell.number_format = "h:mm:ss AM/PM"
                    wb.save(out_path)
            except Exception:
                # si algo falla, al menos queda el archivo
                pass

    elif ext == ".csv":
        df.to_csv(out_path, index=False)
    else:
        raise ValueError("Extensión no soportada. Usa .xlsx o .csv.")

    return out_path

def _derive_out_path(file_path: Union[str, Path], suffix: str, fmt: str) -> Path:
    file_path = Path(file_path)
    out = file_path.with_stem(file_path.stem + suffix)
    if fmt.lower() == "csv":
        return out.with_suffix(".csv")
    return out.with_suffix(".xlsx")

def auto_export(
    file_path: Union[str, Path],
    created_col: Optional[str] = "Published_Date",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    suffix: str = "_limpio",
    fmt: str = "xlsx",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    keep_columns: Optional[Sequence[str]] = None,
    use_default_columns: bool = False,
) -> Path:
    out_path = _derive_out_path(file_path, suffix, fmt)
    df = process_file(
        file_path=file_path,
        created_col=created_col,
        skiprows=skiprows,
        header=header,
        tz_from=tz_from,
        tz_to=tz_to,
        drop_original_created=drop_original_created,
        keep_columns=keep_columns,
        use_default_columns=use_default_columns,
    )
    export_df(df, out_path)
    return out_path

def batch_export(
    files: Sequence[Union[str, Path]],
    created_col: Optional[str] = "Published_Date",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    suffix: str = "_limpio",
    fmt: str = "xlsx",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = True,
    keep_columns: Optional[Sequence[str]] = None,
    use_default_columns: bool = False,
) -> List[Path]:
    outs: List[Path] = []
    for f in files:
        try:
            out = auto_export(
                f,
                created_col=created_col,
                skiprows=skiprows,
                header=header,
                suffix=suffix,
                fmt=fmt,
                tz_from=tz_from,
                tz_to=tz_to,
                drop_original_created=drop_original_created,
                keep_columns=keep_columns,
                use_default_columns=use_default_columns,
            )
            outs.append(out)
        except Exception as e:
            print(f"[WARN] Falló {f}: {e}")
    return outs

# =========================
# CLI
# =========================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Tubular → columnas date/hora (AM/PM truncada), export, batch.")
    parser.add_argument("--file", help="Ruta a un archivo .xlsx/.csv (modo single).")
    parser.add_argument("--glob", help="Patrón glob para procesar varios archivos (ej. '*.xlsx').")
    parser.add_argument("--created-col", default="Published_Date")
    parser.add_argument("--skiprows", type=int, default=None)
    parser.add_argument("--header", type=int, default=0)
    parser.add_argument("--suffix", default="_limpio")
    parser.add_argument("--fmt", default="xlsx", choices=("xlsx", "csv"))
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--tz-from", dest="tz_from", default=None)
    parser.add_argument("--tz-to", dest="tz_to", default=None)
    parser.add_argument("--keep-created", action="store_true")
    parser.add_argument("--use-default-columns", action="store_true")
    parser.add_argument("--keep-columns", nargs="*", default=None)

    args = parser.parse_args()

    targets: List[str] = []
    if args.file:
        targets = [args.file]
    elif args.glob:
        targets = _glob.glob(args.glob)
    else:
        parser.error("Debes pasar --file o --glob.")

    if args.export:
        outs = batch_export(
            targets,
            created_col=args.created_col,
            skiprows=args.skiprows,
            header=args.header,
            suffix=args.suffix,
            fmt=args.fmt,
            tz_from=args.tz_from,
            tz_to=args.tz_to,
            drop_original_created=not args.keep_created,
            keep_columns=args.keep_columns,
            use_default_columns=args.use_default_columns,
        )
        for o in outs:
            print(f"[OK] Exportado: {o}")
    else:
        df = process_file(
            targets[0],
            created_col=args.created_col,
            skiprows=args.skiprows,
            header=args.header,
            tz_from=args.tz_from,
            tz_to=args.tz_to,
            drop_original_created=not args.keep_created,
            keep_columns=args.keep_columns,
            use_default_columns=args.use_default_columns,
        )
        print(df.head())
