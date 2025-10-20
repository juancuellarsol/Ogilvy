#!/usr/bin/env python3
"""
unify_sources.py (MEJORADO)

Unifica exports de Sprinklr, Tubular y YouScan en un único .xlsx con:
 - Una hoja por fuente presente (sprinklr, tubular, youscan)
 - Una hoja "combined" con todas las filas unificadas y columnas canonizadas
 - Soporte para skiprows DIFERENTES por cada fuente

Características:
 - Acepta 0..N archivos por cada fuente
 - Usa las funciones process_file existentes en sprinklr.py, tubular.py y youscan.py
 - Mapea nombres de columnas habituales a un conjunto canónico
 - Guarda un único .xlsx con hojas por fuente y la hoja "combined"
 - **NUEVO**: Parámetros separados de skiprows/header para cada fuente

Ejemplo de uso en línea de comandos:
  python unify_sources.py \\
    --sprinklr XandNewsCO.xlsx \\
    --tubular tubular1.xlsx \\
    --youscan youscan.csv \\
    --skiprows-sprinklr 2 \\
    --skiprows-tubular 0 \\
    --skiprows-youscan 1 \\
    -o unified.xlsx

Ejemplo de uso programático en Python/Colab:
  from unify_sources import unify_files
  result = unify_files(
      sprinklr_files=["archivo1.xlsx"],
      tubular_files=["tubular.csv"],
      youscan_files=["youscan.xlsx"],
      skiprows_sprinklr=2,
      skiprows_tubular=0,
      skiprows_youscan=1,
      out_path="unified_output.xlsx"
  )
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Union
import sys
import pandas as pd

# Intentamos importar los procesadores locales
try:
    from sprinklr import process_file as process_sprinklr  # type: ignore
except Exception:
    process_sprinklr = None  # type: ignore

try:
    from tubular import process_file as process_tubular  # type: ignore
except Exception:
    process_tubular = None  # type: ignore

try:
    from youscan import process_file as process_youscan  # type: ignore
except Exception:
    process_youscan = None  # type: ignore

# CANONICAL_COLUMNS: columnas estándar en la salida unificada
CANONICAL_COLUMNS = [
    "source",          # plataforma: sprinklr / tubular / youscan
    "original_file",   # nombre de archivo origen
    "date",            # fecha
    "hora",            # hora
    "hora_original",   # cuando exista (youscan)
    # metadatos comunes
    "platform", "author", "creator",
    "text", "title",
    "url", "video_url",
    # métricas comunes
    "likes", "comments", "shares", "views", "reach", "engagement", "total_engagements",
    # otros
    "sentiment", "saved_at"
]

# MAP_VARIANTS: mapea variantes comunes a nombre canónico
_MAP_VARIANTS = {
    # authors / creators
    "author": "author",
    "author name": "author",
    "creator": "creator",
    "creator name": "creator",
    "sender profile": "author",

    # platform / source
    "platform": "platform",
    "source": "platform",

    # date / time
    "date": "date",
    "created time": "date",
    "created_time": "date",
    "published_date": "date",
    "published date": "date",
    "timestamp": "date",

    "time": "hora",
    "hora": "hora",
    "time_of_day": "hora",

    # title / text
    "text": "text",
    "text snippet": "text",
    "message": "text",
    "body": "text",
    "video_title": "title",
    "title": "title",

    # urls
    "video_url": "video_url",
    "video url": "video_url",
    "url": "url",
    "link": "url",

    # metrics
    "likes": "likes",
    "like_count": "likes",
    "comments": "comments",
    "comment_count": "comments",
    "shares": "shares",
    "share_count": "shares",
    "views": "views",
    "reach": "reach",
    "engagement": "engagement",
    "total_engagements": "total_engagements",

    # others
    "sentiment": "sentiment",
    "saved at": "saved_at",
    "saved_at": "saved_at",
}

def _normalize_col_name(col: str) -> str:
    """Normaliza a versión simple (lower, strip, collapse espacios y guiones/underscores)."""
    s = str(col).strip().lower()
    s = s.replace("-", " ").replace("_", " ").replace(".", " ")
    s = " ".join(s.split())
    return s

def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas del dataframe a nombres canónicos cuando sea posible."""
    renames = {}
    for c in df.columns:
        key = _normalize_col_name(c)
        if key in _MAP_VARIANTS:
            renames[c] = _MAP_VARIANTS[key]
    if renames:
        df = df.rename(columns=renames)
    return df

def _ensure_minimal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Garantiza que existan las columnas 'source' y 'original_file'."""
    df = df.copy()
    if "source" not in df.columns:
        df["source"] = None
    if "original_file" not in df.columns:
        df["original_file"] = None
    return df

def _order_and_fill(df: pd.DataFrame, canonical: List[str]) -> pd.DataFrame:
    """Reindexa columnas para incluir las canónicas primero."""
    existing = [c for c in canonical if c in df.columns]
    others = [c for c in df.columns if c not in existing]
    return df.reindex(columns=(existing + others))

def _process_for_source(
    files: List[Path],
    source_name: str,
    processor,
    created_col: Optional[str],
    skiprows: Optional[int],
    header: Optional[int],
    tz_from: Optional[str],
    tz_to: Optional[str],
    drop_original_created: bool,
    extra_args: Optional[Dict] = None
) -> Optional[pd.DataFrame]:
    """Procesa una lista de archivos con el processor proporcionado."""
    if not files:
        return None
    if processor is None:
        raise RuntimeError(f"Processor for {source_name} not available (module missing).")
    frames = []
    for f in files:
        try:
            kwargs = dict(
                file_path=f,
                created_col=created_col,
                skiprows=skiprows,
                header=header,
                tz_from=tz_from,
                tz_to=tz_to,
                drop_original_created=drop_original_created
            )
            if extra_args:
                kwargs.update(extra_args)
            
            try:
                df = processor(**kwargs)  # type: ignore
            except TypeError:
                # fallback para procesadores con diferentes argumentos
                df = processor(f, skiprows=skiprows, header=header)  # type: ignore

            df = df.copy()
            df["source"] = source_name
            df["original_file"] = Path(f).name
            frames.append(df)
        except Exception as e:
            print(f"[WARN] Al procesar {f} ({source_name}): {e}", file=sys.stderr)
    
    if not frames:
        return None
    
    concat = pd.concat(frames, ignore_index=True, sort=False)
    concat = _map_columns(concat)
    concat = _ensure_minimal_columns(concat)
    return concat

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
    drop_original_created: bool,
) -> Path:
    """
    Procesa los archivos por plataforma y escribe un xlsx con hojas individuales y combined.
    Ahora con skiprows SEPARADOS por fuente.
    """
    sheets: Dict[str, pd.DataFrame] = {}
    
    # Sprinklr
    if sprinklr_files:
        df_s = _process_for_source(
            sprinklr_files, "sprinklr", process_sprinklr,
            created_col=created_col_spr, skiprows=skiprows_spr, header=header,
            tz_from=tz_from, tz_to=tz_to, drop_original_created=drop_original_created
        )
        if df_s is not None:
            sheets["sprinklr"] = df_s

    # Tubular
    if tubular_files:
        df_t = _process_for_source(
            tubular_files, "tubular", process_tubular,
            created_col=created_col_tub, skiprows=skiprows_tub, header=header,
            tz_from=tz_from, tz_to=tz_to, drop_original_created=drop_original_created
        )
        if df_t is not None:
            sheets["tubular"] = df_t

    # YouScan
    if youscan_files:
        extra = {"date_col": date_col_you, "time_col": time_col_you}
        df_y = _process_for_source(
            youscan_files, "youscan", process_youscan,
            created_col=None, skiprows=skiprows_you, header=header,
            tz_from=tz_from, tz_to=tz_to, drop_original_created=drop_original_created,
            extra_args=extra
        )
        if df_y is not None:
            sheets["youscan"] = df_y

    if not sheets:
        raise RuntimeError("No hubo hojas para exportar (no se procesó ningún archivo).")

    # Creamos combined
    all_frames = []
    for name, df in sheets.items():
        df = _map_columns(df)
        df = _ensure_minimal_columns(df)
        all_frames.append(df)
    combined = pd.concat(all_frames, ignore_index=True, sort=False)
    combined = _order_and_fill(combined, CANONICAL_COLUMNS)

    # Reordenamos hojas individuales
    for k in list(sheets.keys()):
        sheets[k] = _order_and_fill(sheets[k], CANONICAL_COLUMNS)

    # Escribimos .xlsx
    out_path = Path(out_path)
    if out_path.suffix.lower() != ".xlsx":
        out_path = out_path.with_suffix(".xlsx")

    try:
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            for name, df in sheets.items():
                sheet_name = name[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            combined.to_excel(writer, sheet_name="combined", index=False)
    except Exception:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for name, df in sheets.items():
                df.to_excel(writer, sheet_name=name[:31], index=False)
            combined.to_excel(writer, sheet_name="combined", index=False)

    return out_path

def _expand_paths(paths: Optional[List[Union[str, Path]]]) -> List[Path]:
    """Normaliza argumentos string->Path y expande globs si es necesario."""
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

# ========== FUNCIÓN PRINCIPAL PARA USO PROGRAMÁTICO ==========
def unify_files(
    sprinklr_files: Optional[List[Union[str, Path]]] = None,
    tubular_files: Optional[List[Union[str, Path]]] = None,
    youscan_files: Optional[List[Union[str, Path]]] = None,
    out_path: Union[str, Path] = "unified.xlsx",
    created_col_spr: str = "Created Time",
    created_col_tub: str = "Published_Date",
    date_col_you: str = "Date",
    time_col_you: str = "Time",
    skiprows_sprinklr: Optional[int] = None,
    skiprows_tubular: Optional[int] = None,
    skiprows_youscan: Optional[int] = None,
    header: int = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
    keep_created: bool = False
) -> Path:
    """
    Función principal para unificar archivos programáticamente (desde Python/Colab).
    
    Args:
        sprinklr_files: Lista de archivos Sprinklr (paths o strings)
        tubular_files: Lista de archivos Tubular
        youscan_files: Lista de archivos YouScan
        out_path: Ruta del archivo de salida .xlsx
        created_col_spr: Nombre de columna de fecha en Sprinklr
        created_col_tub: Nombre de columna de fecha en Tubular
        date_col_you: Nombre de columna de fecha en YouScan
        time_col_you: Nombre de columna de hora en YouScan
        skiprows_sprinklr: Número de filas a saltar en Sprinklr (default 0)
        skiprows_tubular: Número de filas a saltar en Tubular (default 0)
        skiprows_youscan: Número de filas a saltar en YouScan (default 0)
        header: Fila del encabezado (default 0)
        tz_from: Zona horaria origen (opcional)
        tz_to: Zona horaria destino (opcional)
        keep_created: Si True, no elimina columnas originales de fecha
    
    Returns:
        Path del archivo generado
    
    Examples:
        >>> # Ejemplo 1: Unificar los tres orígenes con skiprows diferentes
        >>> result = unify_files(
        ...     sprinklr_files=["sprinklr.xlsx"],
        ...     tubular_files=["tubular.csv"],
        ...     youscan_files=["youscan.xlsx"],
        ...     skiprows_sprinklr=2,
        ...     skiprows_tubular=0,
        ...     skiprows_youscan=1,
        ...     out_path="unified.xlsx"
        ... )
        >>> print(f"Archivo creado: {result}")
        
        >>> # Ejemplo 2: Solo Sprinklr y Tubular
        >>> result = unify_files(
        ...     sprinklr_files=["file1.xlsx", "file2.xlsx"],
        ...     tubular_files=["tubular.csv"],
        ...     skiprows_sprinklr=2,
        ...     out_path="output.xlsx"
        ... )
    """
    spr_paths = _expand_paths(sprinklr_files) if sprinklr_files else []
    tub_paths = _expand_paths(tubular_files) if tubular_files else []
    you_paths = _expand_paths(youscan_files) if youscan_files else []

    # Validar módulos
    if spr_paths and process_sprinklr is None:
        raise RuntimeError("sprinklr.py no está disponible. Descárgalo primero.")
    if tub_paths and process_tubular is None:
        raise RuntimeError("tubular.py no está disponible. Descárgalo primero.")
    if you_paths and process_youscan is None:
        raise RuntimeError("youscan.py no está disponible. Descárgalo primero.")

    return unify_and_export(
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
        drop_original_created=(not keep_created),
    )

# ========== CLI ==========
def main(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(
        description="Unifica Sprinklr/Tubular/YouScan en un único .xlsx con skiprows separados"
    )
    parser.add_argument("--sprinklr", nargs="*", default=[], 
                       help="Archivo(s) Sprinklr")
    parser.add_argument("--tubular", nargs="*", default=[], 
                       help="Archivo(s) Tubular")
    parser.add_argument("--youscan", nargs="*", default=[], 
                       help="Archivo(s) YouScan")
    parser.add_argument("-o", "--out", default="unified.xlsx", 
                       help="Ruta de salida .xlsx")
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
    parser.add_argument("--keep-created", action="store_true",
                       help="Mantener columnas originales de fecha")
    
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
            keep_created=args.keep_created,
        )
        print(f"[OK] Exportado: {result}")
        print(f"\nArchivo unificado creado exitosamente:")
        print(f"  Ubicación: {result}")
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
