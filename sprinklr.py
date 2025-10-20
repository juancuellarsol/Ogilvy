"""
sprinklr_fechas.py
------------------------------
Refactor reutilizable y robusto para exports de Sprinklr.

Incluye:
- process_file(file_path, ...): lee el archivo y devuelve DF con columnas 'date' y 'hora' (AM/PM)
- process_dataframe(df, ...): procesa un DataFrame ya cargado
- export_df(df, out_path): exporta a .xlsx o .csv
- auto_export(file_path, ...): lee, procesa y guarda en un paso (sufijo y formato)
- batch_export(files, ...): procesa varios archivos

Detalles:
- Hora en formato 12h con AM/PM (sin cero inicial). Ej: '8:00:00 AM' / '8:00:00 PM'
- Fecha en M/D/YYYY (sin ceros a la izquierda)
- Detección flexible de la columna de fecha (por defecto 'Created Time')
- Conversión opcional de zona horaria (tz_from / tz_to) si 'pytz' está disponible

Ejemplos rápidos:
-----------------
# 1) Solo exportar desde archivo
from sprinklr_fechas import auto_export
out = auto_export("XandNewsCO.xlsx", created_col="Created Time", skiprows=2, suffix="_final", fmt="xlsx")

# 2) Revisar primero y luego exportar
from sprinklr_fechas import process_file, export_df
df = process_file("XandNewsCO.xlsx", created_col="Created Time", skiprows=2)
export_df(df, "XandNewsCO_final.xlsx")

# 3) Si ya tienes un DataFrame en memoria
from sprinklr_fechas import process_dataframe
df2 = process_dataframe(df_original, created_col="Created Time")
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Optional, Sequence, Union, List
import glob as _glob


try:
    import pytz
except Exception:
    pytz = None

_CANDIDATE_CREATED_COLS = [
    "Created Time", "Created time", "created_time", "created time", "Created Time.1",
    "Fecha de creación", "Fecha", "Date Created", "Timestamp", "Time"
]

def _find_created_col(columns: Sequence[str], preferred: Optional[str]) -> str:
    if preferred and preferred in columns:
        return preferred
    for name in _CANDIDATE_CREATED_COLS:
        if name in columns:
            return name
    lowered = {c.lower(): c for c in columns}
    for key, original in lowered.items():
        if any(w in key for w in ("creat", "fecha", "time", "date", "timestamp")):
            return original
    raise KeyError("No pude encontrar la columna de fecha/hora. Especifica 'created_col'.")

def _read_any(file_path: Union[str, Path], skiprows=None, header: Optional[int]=0) -> pd.DataFrame:
    file_path = Path(file_path)
    ext = file_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(file_path, skiprows=skiprows, header=header)
    if ext == ".csv":
        return pd.read_csv(file_path, skiprows=skiprows, header=header)
    raise ValueError(f"Formato no soportado: {ext}. Usa .xlsx, .xls o .csv.")

def _ensure_naive(dt_series: pd.Series) -> pd.Series:
    # quita zona horaria si viene con tz
    if hasattr(dt_series.dtype, "tz") and dt_series.dtype.tz is not None:
        return dt_series.dt.tz_convert(None)
    return dt_series

def _coerce_datetime(s: pd.Series) -> pd.Series:
    if not pd.api.types.is_datetime64_any_dtype(s):
        s = pd.to_datetime(s, errors="coerce")#, infer_datetime_format=True)
    return s

def process_dataframe(
    df: pd.DataFrame,
    created_col: Optional[str] = "Created Time.1",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = False
) -> pd.DataFrame:
    """
    Procesa un DataFrame ya cargado y devuelve un nuevo DF con 'date' y 'hora' (AM/PM) al inicio.
    """
    if df is None or not hasattr(df, "columns"):
        raise TypeError("process_dataframe espera un pandas.DataFrame válido.")

    col = _find_created_col(df.columns, created_col)
    out = df.copy()
    out = out.drop(columns=["Sender Profile Image Url", "Associated Cases", "Created Time"], errors="ignore")

    out[col] = _coerce_datetime(out[col])

    # Conversión opcional de zona horaria
    if (tz_from or tz_to) and pytz is None:
        raise RuntimeError("pytz no disponible. Instálalo para usar conversión de zona horaria.")
    if tz_from and pytz is not None and out[col].notna().any():
        if not hasattr(out[col].dtype, "tz") or out[col].dtype.tz is None:
            out[col] = out[col].dt.tz_localize(pytz.timezone(tz_from), nonexistent="NaT", ambiguous="NaT")
    if tz_to and pytz is not None and out[col].notna().any():
        out[col] = out[col].dt.tz_convert(pytz.timezone(tz_to))

    out[col] = _ensure_naive(out[col])

    # Fecha y hora (12h AM/PM sin cero inicial)

    date_series = pd.to_datetime(out[col])
    
    hora_series = out[col].dt.floor("h").dt.strftime("%I:%M:%S %p").str.lstrip("0")#.str.replace("AM", "a. m.").str.replace("PM", "p. m.")
    
    # Insertar las nuevas columnas
    for c in ("hora", "date", "tag"):#, "Día"):
        if c in out.columns: out = out.drop(columns=[c])
    #out.insert(0, "day", dia_series)
    #out.insert(0, "tag", tag_series)
    out.insert(0, "hora", hora_series)
    out.insert(0, "date", date_series)
    
    if drop_original_created:
        out = out.drop(columns=[col])

    ordered = ["date", "hora" ] + [c for c in out.columns if c not in ("date", "hora")] #, "day" , "tag" , "tag"
    return out[ordered]

def process_file(
    file_path: Union[str, Path],
    created_col: Optional[str] = "Created Time.1",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = False
) -> pd.DataFrame:
    df = _read_any(file_path, skiprows=skiprows, header=header)
    return process_dataframe(
        df, created_col=created_col, tz_from=tz_from, tz_to=tz_to,
        drop_original_created=drop_original_created
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

    # --- Normaliza 'date' a SOLO fecha (sin hora) ---
    if "date" in df.columns:
        df = df.copy()
        d = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = d.dt.normalize()   # 00:00:00

    # --- Asegura que 'hora' sea valor de tiempo (no texto) ---
    if "hora" in df.columns:
        df = df.copy()
           # 1) Primero, parsea la hora como datetime (si aún es texto)
        hora_dt = pd.to_datetime(df["hora"], format="%I:%M:%S %p", errors="coerce")
            # 2) AQUÍ: Aplica floor a la hora más cercana ANTES de normalizar
        hora_dt = hora_dt.dt.floor("h")
     # 3) Normaliza unicode y variantes de "a. m." / "p. m."
        hora_norm = (
            hora_dt.dt.strftime("%I:%M:%S %p")
              .str.normalize("NFKC")
              .str.replace("\u00A0", " ", regex=False)
              .str.replace("\u202F", " ", regex=False)
              .str.strip()
              .str.replace(r"(?i)\s*a\s*\.?\s*m\.?\s*$", " AM", regex=True)
              .str.replace(r"(?i)\s*p\s*\.?\s*m\.?\s*$", " PM", regex=True)
        )

        # 4) Parseo estricto a datetime (solo hora)
        h = pd.to_datetime(hora_norm, format="%I:%M:%S %p", errors="coerce")

        # 5) Convierte a fracción de día
        frac = (h.dt.hour * 3600 + h.dt.minute * 60 + h.dt.second) / 86400.0

        df["hora"] = frac
    
    ext = out_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        try:
            import xlsxwriter  # motor para formatear
            with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
                sheet = "Sheet1"
                df.to_excel(writer, index=False, sheet_name=sheet)

                workbook  = writer.book
                ws        = writer.sheets[sheet]

                # Formato fecha dd/mm/yy
                if "date" in df.columns:
                    fmt_date = workbook.add_format({"num_format": "dd/mm/yy"})
                    cidx = df.columns.get_loc("date")
                    col  = _col_idx_to_xlsx_col(cidx)
                    ws.set_column(f"{col}:{col}", 10, fmt_date)

                # Formato hora h:mm:ss AM/PM (Excel ES muestra a. m./p. m.)
                if "hora" in df.columns:
                    fmt_time = workbook.add_format({"num_format": "h:mm:ss AM/PM"})
                    cidx = df.columns.get_loc("hora")
                    col  = _col_idx_to_xlsx_col(cidx)
                    ws.set_column(f"{col}:{col}", 12, fmt_time)

        except ModuleNotFoundError:
            # Fallback: guardar y formatear con openpyxl
            df.to_excel(out_path, index=False)
            try:
                from openpyxl import load_workbook
                wb = load_workbook(out_path)
                ws = wb.active
                from openpyxl.styles import numbers

                if "date" in df.columns:
                    cidx = df.columns.get_loc("date") + 1
                    for col_cells in ws.iter_cols(min_col=cidx, max_col=cidx,
                                                  min_row=2, max_row=ws.max_row):
                        for c in col_cells:
                            c.number_format = numbers.FORMAT_DATE_DDMMYY  # dd/mm/yy

                if "hora" in df.columns:
                    cidx = df.columns.get_loc("hora") + 1
                    for col_cells in ws.iter_cols(min_col=cidx, max_col=cidx,
                                                  min_row=2, max_row=ws.max_row):
                        for c in col_cells:
                            c.number_format = "h:mm:ss AM/PM"  # Excel ES -> a. m./p. m.

                wb.save(out_path)
            except Exception:
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
    created_col: Optional[str] = "Created Time.1",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    suffix: str = "_limpio",
    fmt: str = "xlsx",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = False
) -> Path:
    out = _derive_out_path(file_path, suffix, fmt)
    df = process_file(
        file_path=file_path,
        created_col=created_col,
        skiprows=skiprows,
        header=header,
        tz_from=tz_from,
        tz_to=tz_to,
        drop_original_created=drop_original_created
    )
    export_df(df, out)
    return out

def batch_export(
    files: Sequence[Union[str, Path]],
    created_col: Optional[str] = "Created Time.1",
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    suffix: str = "_limpio",
    fmt: str = "xlsx",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_created: bool = False
) -> List[Path]:
    outs: List[Path] = []
    for f in files:
        try:
            out = auto_export(
                f, created_col=created_col, skiprows=skiprows, header=header,
                suffix=suffix, fmt=fmt, tz_from=tz_from, tz_to=tz_to,
                drop_original_created=drop_original_created
            )
            outs.append(out)
        except Exception as e:
            print(f"[WARN] Falló {f}: {e}")
    return outs

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sprinklr → columnas date/hora (AM/PM), export, batch y DF support.")
    parser.add_argument("--file", help="Ruta a un archivo .xlsx/.csv (modo single).")
    parser.add_argument("--glob", help="Patrón glob para procesar varios archivos (ej. '*.xlsx').")
    parser.add_argument("--created-col", default="Created Time.1")
    parser.add_argument("--skiprows", type=int, default=None)
    parser.add_argument("--header", type=int, default=0)
    parser.add_argument("--suffix", default="_limpio")
    parser.add_argument("--fmt", default="xlsx", choices=("xlsx", "csv"))
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--tz-from", dest="tz_from", default=None)
    parser.add_argument("--tz-to", dest="tz_to", default=None)
    parser.add_argument("--keep-created", action="store_true")

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
            targets, created_col=args.created_col, skiprows=args.skiprows, header=args.header,
            suffix=args.suffix, fmt=args.fmt, tz_from=args.tz_from, tz_to=args.tz_to,
            drop_original_created=not args.keep_created
        )
        for o in outs:
            print(f"[OK] Exportado: {o}")
    else:
        df = process_file(
            targets[0], created_col=args.created_col, skiprows=args.skiprows, header=args.header,
            tz_from=args.tz_from, tz_to=args.tz_to, drop_original_created=not args.keep_created
        )
        print(df.head())
