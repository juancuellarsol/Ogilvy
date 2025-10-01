"""
youscan.py
------------------------------
Limpieza reutilizable para exports de YouScan.

Supuestos por defecto:
- Columnas separadas 'Date' (DD.MM.YYYY) y 'Time' (HH:MM 24h).
- Se combinan para formar un datetime robusto.
- Se generan:
    * date: M/D/YYYY (sin ceros a la izquierda)
    * hora: 12h AM/PM con segundos en :00 (ej. 6:28:00 AM)

Opciones:
- floor_to_hour: si True, la hora se trunca a la hora inferior (X:00:00 AM/PM).
- keep_columns: lista de columnas a conservar (además de date/hora).
- use_default_columns: usa un set por defecto de columnas útiles si no pasas keep_columns.

Funciones:
- process_dataframe, process_file, export_df, auto_export, batch_export
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional, Sequence, Union, List
import glob as _glob
import pandas as pd

try:
    import pytz
except Exception:
    pytz = None


# ========= Helpers =========

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia encabezados: strip y colapsa espacios internos."""
    cols = (
        df.columns.astype(str)
          .str.strip()
          .str.replace(r"\s+", " ", regex=True)
    )
    df.columns = cols
    return df


def _read_any(file_path: Union[str, Path], skiprows=None, header: Optional[int] = 0) -> pd.DataFrame:
    file_path = Path(file_path)
    ext = file_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(file_path, skiprows=skiprows, header=header)
    if ext == ".csv":
        return pd.read_csv(file_path, skiprows=skiprows, header=header)
    raise ValueError(f"Formato no soportado: {ext}. Usa .xlsx, .xls o .csv.")


def _ensure_naive(dt_series: pd.Series) -> pd.Series:
    """Quita tz si la hay (deja datetime naive)."""
    if hasattr(dt_series.dtype, "tz") and dt_series.dtype.tz is not None:
        return dt_series.dt.tz_convert(None)
    return dt_series


# ========= API =========

def process_dataframe(
    df: pd.DataFrame,
    date_col: str = "Date",            # DD.MM.YYYY
    time_col: str = "Time",            # HH:MM (24h)
    created_col: Optional[str] = None, # por si YouScan trae un timestamp único
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_date_time: bool = True,
    keep_columns: Optional[Sequence[str]] = None,
    use_default_columns: bool = False,
    floor_to_hour: bool = False,
) -> pd.DataFrame:
    """
    Procesa un DF de YouScan:
      - Combina Date (DD.MM.YYYY) + Time (HH:MM 24h) o usa created_col si se pasa.
      - Genera 'date' y 'hora' (12h, segundos :00).
      - Opcional: truncar a hora (floor_to_hour).
      - Opcional: filtrar columnas a `keep_columns` (o set por defecto).
    """
    if df is None or not hasattr(df, "columns"):
        raise TypeError("process_dataframe espera un pandas.DataFrame válido.")

    out = _normalize_columns(df.copy())

    # 1) Construcción del timestamp base
    if created_col and created_col in out.columns:
        # Si trajiste un timestamp único, parsea con to_datetime "normal"
        ts = pd.to_datetime(out[created_col].astype(str).str.strip(),
                    format="%d.%m.%Y",
                    errors="coerce")
    else:
        if date_col not in out.columns or time_col not in out.columns:
            raise KeyError(f"No encuentro columnas '{date_col}' y '{time_col}'. "
                           f"Encabezados disponibles: {list(out.columns)}")
        # YouScan envía '22.09.2025' y '06:28' (24h, sin segundos)
        # Forzamos formato explícito para evitar warnings.
        # Si hay valores raros, caen a NaT con errors='coerce'.
        ts = pd.to_datetime(
            out[date_col].astype(str).str.strip() + " " + out[time_col].astype(str).str.strip(),
            format="%d.%m.%Y %H:%M",
            errors="coerce"
        )

    # 2) TZ opcional
    if (tz_from or tz_to) and pytz is None:
        raise RuntimeError("pytz no disponible. Instálalo para usar conversión de zona horaria.")
    if tz_from and pytz is not None and ts.notna().any():
        if not hasattr(ts.dtype, "tz") or ts.dtype.tz is None:
            ts = ts.dt.tz_localize(pytz.timezone(tz_from), nonexistent="NaT", ambiguous="NaT")
    if tz_to and pytz is not None and ts.notna().any():
        ts = ts.dt.tz_convert(pytz.timezone(tz_to))
    ts = _ensure_naive(ts)

    # 3) Derivadas: date + hora
    date_series = (
        ts.dt.day.astype("Int64").astype(str) + "/" +
        ts.dt.month.astype("Int64").astype(str) + "/" +
        ts.dt.year.astype("Int64").astype(str)
    )
    #date_series = pd.to_datetime(out[col])


# Parseamos HH:MM (24h) y lo pasamos a 12h con segundos en :00
    hora_series = (
        pd.to_datetime(out[time_col].astype(str).str.strip(), format="%H:%M", errors="coerce")
        .dt.floor("h")
        .dt.strftime("%I:%M:%S %p")   # ejemplo: 18:23 -> "06:23:00 PM"
        .str.lstrip("0")              # quita el cero inicial (06 -> 6)
    )

    hora_original = (
        pd.to_datetime(out[time_col].astype(str).str.strip(), format="%H:%M", errors="coerce")
        .dt.strftime("%I:%M:%S %p")   # ejemplo: 18:23 -> "06:23:00 PM"
        .str.lstrip("0")              # quita el cero inicial (06 -> 6)
    )

    # Insertamos la nueva columna 'hora' al frente
    if "hora" in out.columns:
        out = out.drop(columns=["hora"])
    out.insert(0, "hora", hora_series)

    # Si quieres eliminar la columna Time original:
    if drop_original_date_time and time_col in out.columns:
        out = out.drop(columns=[time_col])

    # 4) Insertar columnas nuevas al frente
    for c in ("date", "hora"):
        if c in out.columns:
            out = out.drop(columns=[c])
    out.insert(0, "hora", hora_series)
    out.insert(0, "date", date_series)
    out.insert(0, "hora_original", hora_original)

    # 5) Limpieza: quitar columnas originales si se pide
    if drop_original_date_time:
        for c in (date_col, time_col, created_col):
            if c and c in out.columns:
                out = out.drop(columns=[c])

    # 6) Selección final de columnas
    if use_default_columns and keep_columns is None:
        # Ajusta esta lista a lo que más uses en YouScan:
        keep_columns = [
            # ejemplo de campos frecuentes; edítalos según tu export
            "Author", "Source", "Saved at", "Text snippet", "Sentiment", "Likes", "Comments", "Shares",
            "Reach", "Engagement", "Views"
        ]

    if keep_columns is not None:
        keep_columns = list(keep_columns)
        final_cols = ["date", "hora", "hora_original"] + [c for c in keep_columns if c in out.columns]
        out = out.reindex(columns=final_cols)
        return out

    # Si no filtras, devuelve todo con date/hora al frente
    ordered = ["date", "hora", "hora_original"] + [c for c in out.columns if c not in ("date", "hora", "hora_original")]
    return out[ordered]


def process_file(
    file_path: Union[str, Path],
    date_col: str = "Date",
    time_col: str = "Time",
    created_col: Optional[str] = None,
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_date_time: bool = True,
    keep_columns: Optional[Sequence[str]] = None,
    use_default_columns: bool = False,
    floor_to_hour: bool = False,
) -> pd.DataFrame:
    df = _read_any(file_path, skiprows=skiprows, header=header)
    df = _normalize_columns(df)
    return process_dataframe(
        df,
        date_col=date_col,
        time_col=time_col,
        created_col=created_col,
        tz_from=tz_from,
        tz_to=tz_to,
        drop_original_date_time=drop_original_date_time,
        keep_columns=keep_columns,
        use_default_columns=use_default_columns,
        floor_to_hour=floor_to_hour,
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

    # Normaliza 'date'
    if "date" in df.columns:
        df = df.copy()
        d = pd.to_datetime(df["date"].astype(str).str.strip(), dayfirst=True, errors="coerce")
        df["date"] = d.dt.normalize()

    # Normaliza 'hora' a datetime
    if "hora" in df.columns:
        hora_norm = (
            df["hora"].astype(str)
              .str.normalize("NFKC")
              .str.replace("\u00A0", " ", regex=False)   # NBSP
              .str.replace("\u202F", " ", regex=False)   # NARROW NBSP
              .str.strip()
              .str.replace(r"(?i)\s*a\s*\.?\s*m\.?\s*$", " AM", regex=True)
              .str.replace(r"(?i)\s*p\s*\.?\s*m\.?\s*$", " PM", regex=True)
        )
        h = pd.to_datetime(hora_norm, format="%I:%M:%S %p", errors="coerce")
        frac = (h.dt.hour * 3600 + h.dt.minute * 60 + h.dt.second) / 86400.0
        df = df.copy()
        df["hora"] = frac
    
   


#if "hora" in df.columns:
 #       df = df.copy()
 #       df["hora"] = pd.to_datetime(df["hora"], format="%I:%M:%S %p", errors="coerce")

    ext = out_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        try:
            import xlsxwriter
            with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
                sheet = "Sheet1"
                df.to_excel(writer, index=False, sheet_name=sheet)

                workbook  = writer.book
                ws        = writer.sheets[sheet]

                if "date" in df.columns:
                    fmt = workbook.add_format({"num_format": "dd/mm/yy"})
                    cidx = df.columns.get_loc("date")
                    col  = _col_idx_to_xlsx_col(cidx)
                    ws.set_column(f"{col}:{col}", 10, fmt)

                if "hora" in df.columns:
                    fmt_hora = workbook.add_format({"num_format": "h:mm:ss AM/PM"})
                    cidx = df.columns.get_loc("hora")
                    col  = _col_idx_to_xlsx_col(cidx)
                    ws.set_column(f"{col}:{col}", 12, fmt_hora)

        except ModuleNotFoundError:
            df.to_excel(out_path, index=False)
            try:
                from openpyxl import load_workbook
                wb = load_workbook(out_path)
                ws = wb.active
                if "date" in df.columns:
                    cidx = df.columns.get_loc("date") + 1
                    for cell in ws.iter_cols(min_col=cidx, max_col=cidx,
                                             min_row=2, max_row=ws.max_row):
                        for c in cell:
                            c.number_format = 'dd/mm/yy'
                if "hora" in df.columns:
                    cidx = df.columns.get_loc("hora") + 1
                    for cell in ws.iter_cols(min_col=cidx, max_col=cidx,
                                             min_row=2, max_row=ws.max_row):
                        for c in cell:
                            c.number_format = 'h:mm:ss AM/PM'
                wb.save(out_path)
            except Exception:
                pass
    elif ext == ".csv":
        df.to_csv(out_path, index=False)
    else:
        raise ValueError("Extensión no soportada. Usa .xlsx o .csv.")

    return out_path


def _derive_out_path(file_path: Union[str, Path], suffix: str, fmt: str) -> Path:
    fp = Path(file_path)
    out = fp.with_stem(fp.stem + suffix)
    return out.with_suffix(".csv") if fmt.lower() == "csv" else out.with_suffix(".xlsx")


def auto_export(
    file_path: Union[str, Path],
    date_col: str = "Date",
    time_col: str = "Time",
    created_col: Optional[str] = None,
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    suffix: str = "_limpio",
    fmt: str = "xlsx",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_date_time: bool = True,
    keep_columns: Optional[Sequence[str]] = None,
    use_default_columns: bool = False,
    floor_to_hour: bool = False,
) -> Path:
    out_path = _derive_out_path(file_path, suffix, fmt)
    df = process_file(
        file_path=file_path,
        date_col=date_col,
        time_col=time_col,
        created_col=created_col,
        skiprows=skiprows,
        header=header,
        tz_from=tz_from,
        tz_to=tz_to,
        drop_original_date_time=drop_original_date_time,
        keep_columns=keep_columns,
        use_default_columns=use_default_columns,
        floor_to_hour=floor_to_hour,
    )
    export_df(df, out_path)
    return out_path


def batch_export(
    files: Sequence[Union[str, Path]],
    date_col: str = "Date",
    time_col: str = "Time",
    created_col: Optional[str] = None,
    skiprows: Optional[Union[int, Sequence[int]]] = None,
    header: Optional[int] = 0,
    suffix: str = "_limpio",
    fmt: str = "xlsx",
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    drop_original_date_time: bool = True,
    keep_columns: Optional[Sequence[str]] = None,
    use_default_columns: bool = False,
    floor_to_hour: bool = False,
) -> List[Path]:
    outs: List[Path] = []
    for f in files:
        try:
            out = auto_export(
                f,
                date_col=date_col,
                time_col=time_col,
                created_col=created_col,
                skiprows=skiprows,
                header=header,
                suffix=suffix,
                fmt=fmt,
                tz_from=tz_from,
                tz_to=tz_to,
                drop_original_date_time=drop_original_date_time,
                keep_columns=keep_columns,
                use_default_columns=use_default_columns,
                floor_to_hour=floor_to_hour,
            )
            outs.append(out)
        except Exception as e:
            print(f"[WARN] Falló {f}: {e}")
    return outs


# ===== CLI =====
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="YouScan → columnas date/hora (12h con :00), export/batch.")
    parser.add_argument("--file", help="Ruta a un archivo .xlsx/.csv (modo single).")
    parser.add_argument("--glob", help="Patrón glob para procesar varios archivos (ej. '*.xlsx').")
    parser.add_argument("--date-col", default="Date")
    parser.add_argument("--time-col", default="Time")
    parser.add_argument("--created-col", default=None)
    parser.add_argument("--skiprows", type=int, default=None)
    parser.add_argument("--header", type=int, default=0)
    parser.add_argument("--suffix", default="_limpio")
    parser.add_argument("--fmt", default="xlsx", choices=("xlsx", "csv"))
    parser.add_argument("--tz-from", dest="tz_from", default=None)
    parser.add_argument("--tz-to", dest="tz_to", default=None)
    parser.add_argument("--drop-original", action="store_true")
    parser.add_argument("--use-default-columns", action="store_true")
    parser.add_argument("--keep-columns", nargs="*", default=None)
    parser.add_argument("--floor-to-hour", action="store_true")

    args = parser.parse_args()

    targets: List[str] = []
    if args.file:
        targets = [args.file]
    elif args.glob:
        targets = _glob.glob(args.glob)
    else:
        parser.error("Debes pasar --file o --glob.")

    if args.keep_columns == []:
        args.keep_columns = None  # normaliza

    if args.file or args.glob:
        if args.keep_columns:
            print("Mantendré solo:", args.keep_columns)

    if args.floor_to_hour:
        print("Hora truncada a hora cerrada (H:00:00).")

    if args.drop_original:
        print("Eliminando columnas originales de fecha/hora.")

    if args.export:
        outs = batch_export(
            targets,
            date_col=args.date_col,
            time_col=args.time_col,
            created_col=args.created_col,
            skiprows=args.skiprows,
            header=args.header,
            suffix=args.suffix,
            fmt=args.fmt,
            tz_from=args.tz_from,
            tz_to=args.tz_to,
            drop_original_date_time=args.drop_original,
            keep_columns=args.keep_columns,
            use_default_columns=args.use_default_columns,
            floor_to_hour=args.floor_to_hour,
        )
        for o in outs:
            print(f"[OK] Exportado: {o}")
    else:
        df = process_file(
            targets[0],
            date_col=args.date_col,
            time_col=args.time_col,
            created_col=args.created_col,
            skiprows=args.skiprows,
            header=args.header,
            tz_from=args.tz_from,
            tz_to=args.tz_to,
            drop_original_date_time=args.drop_original,
            keep_columns=args.keep_columns,
            use_default_columns=args.use_default_columns,
            floor_to_hour=args.floor_to_hour,
        )
        print(df.head())
