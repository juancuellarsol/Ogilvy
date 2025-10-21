#!/usr/bin/env python3
"""
Unify.py (VERSIÓN MEJORADA CON sprinklr_fields, tubular_fields, youscan_fields)

Unifica exports de Sprinklr, Tubular y YouScan en un único .xlsx con:
 - Una hoja por fuente presente (sprinklr, tubular, youscan)
 - Una hoja "combined" con todas las filas unificadas y columnas canonizadas
 - Soporte para skiprows DIFERENTES por cada fuente

Características:
 - Acepta 0..N archivos por cada fuente
 - Usa los nuevos módulos independientes: sprinklr_fields, tubular_fields, youscan_fields
 - Mapea nombres de columnas habituales a un conjunto canónico
 - Guarda un único .xlsx con hojas por fuente y la hoja "combined"
 - Parámetros separados de skiprows/header para cada fuente

Ejemplo de uso en Google Colab:
  # Subir archivos y ejecutar:
  from Unify import unify_files
  
  result = unify_files(
      sprinklr_files=["XandNewsCO.xlsx"],
      tubular_files=["tubular1.xlsx"],
      youscan_files=["youscan.csv"],
      skiprows_sprinklr=2,
      skiprows_tubular=0,
      skiprows_youscan=1,
      out_path="unified.xlsx"
  )
  
  # Descargar archivo resultante
  from google.colab import files
  files.download(str(result))

Ejemplo de uso en línea de comandos:
  python Unify.py \\
    --sprinklr XandNewsCO.xlsx \\
    --tubular tubular1.xlsx \\
    --youscan youscan.csv \\
    --skiprows-sprinklr 2 \\
    --skiprows-tubular 0 \\
    --skiprows-youscan 1 \\
    -o unified.xlsx
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Union
import sys
import pandas as pd

# ===== IMPORTAR LOS NUEVOS MÓDULOS *_fields.py =====
try:
    from sprinklr_fields import process_file as process_sprinklr_file
    SPRINKLR_AVAILABLE = True
except ImportError:
    SPRINKLR_AVAILABLE = False
    process_sprinklr_file = None

try:
    from tubular_fields import process_file as process_tubular_file
    TUBULAR_AVAILABLE = True
except ImportError:
    TUBULAR_AVAILABLE = False
    process_tubular_file = None

try:
    from youscan_fields import process_file as process_youscan_file
    YOUSCAN_AVAILABLE = True
except ImportError:
    YOUSCAN_AVAILABLE = False
    process_youscan_file = None


# CANONICAL_COLUMNS: columnas estándar en la salida unificada
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


def _normalize_col_name(col: str) -> str:
    """Normaliza a versión simple (lower, strip, collapse espacios)."""
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
    """Reindexa columnas para incluir las canónicas primero."""
    existing = [c for c in canonical if c in df.columns]
    others = [c for c in df.columns if c not in existing]
    return df.reindex(columns=(existing + others))


def _process_sprinklr(
    files: List[Path],
    created_col: Optional[str] = "Created Time",
    skiprows: Optional[int] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Procesa archivos Sprinklr usando sprinklr_fields.process_file"""
    if not files or not SPRINKLR_AVAILABLE:
        return None
    
    frames = []
    for f in files:
        try:
            df = process_sprinklr_file(
                file_path=f,
                created_col=created_col,
                skiprows=skiprows,
                header=header,
                tz_from=tz_from,
                tz_to=tz_to,
                drop_original_created=True
            )
            df = df.copy()
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


def _process_tubular(
    files: List[Path],
    created_col: Optional[str] = "Published_Date",
    skiprows: Optional[int] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Procesa archivos Tubular usando tubular_fields.process_file"""
    if not files or not TUBULAR_AVAILABLE:
        return None
    
    frames = []
    for f in files:
        try:
            df = process_tubular_file(
                file_path=f,
                created_col=created_col,
                skiprows=skiprows,
                header=header,
                tz_from=tz_from,
                tz_to=tz_to,
                drop_original_created=True,
                add_mentions=True
            )
            df = df.copy()
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


def _process_youscan(
    files: List[Path],
    date_col: str = "Date",
    time_col: str = "Time",
    skiprows: Optional[int] = None,
    header: Optional[int] = 0,
    tz_from: Optional[str] = None,
    tz_to: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Procesa archivos YouScan usando youscan_fields.process_file"""
    if not files or not YOUSCAN_AVAILABLE:
        return None
    
    frames = []
    for f in files:
        try:
            df = process_youscan_file(
                file_path=f,
                date_col=date_col,
                time_col=time_col,
                skiprows=skiprows,
                header=header,
                tz_from=tz_from,
                tz_to=tz_to,
                drop_original_date_time=True,
                add_mentions=True
            )
            df = df.copy()
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
    Procesa los archivos por plataforma y escribe un xlsx con hojas individuales y combined.
    Usa los nuevos módulos *_fields.py
    """
    sheets: Dict[str, pd.DataFrame] = {}
    
    print("\n=== PROCESANDO FUENTES ===\n")
    
    # Sprinklr
    if sprinklr_files:
        if not SPRINKLR_AVAILABLE:
            print("[ERROR] sprinklr_fields no disponible. Instálalo o cópialo al directorio.", file=sys.stderr)
        else:
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
        if not TUBULAR_AVAILABLE:
            print("[ERROR] tubular_fields no disponible. Instálalo o cópialo al directorio.", file=sys.stderr)
        else:
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
        if not YOUSCAN_AVAILABLE:
            print("[ERROR] youscan_fields no disponible. Instálalo o cópialo al directorio.", file=sys.stderr)
        else:
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

    # Creamos combined
    print("\n=== COMBINANDO DATOS ===\n")
    all_frames = list(sheets.values())
    combined = pd.concat(all_frames, ignore_index=True, sort=False)
    combined = _order_and_fill(combined, CANONICAL_COLUMNS)
    print(f"[OK] Combined: {len(combined)} filas totales")

    # Reordenamos hojas individuales
    for k in list(sheets.keys()):
        sheets[k] = _order_and_fill(sheets[k], CANONICAL_COLUMNS)

    # Escribimos .xlsx
    out_path = Path(out_path)
    if out_path.suffix.lower() != ".xlsx":
        out_path = out_path.with_suffix(".xlsx")

    print(f"\n=== EXPORTANDO ===\n")
    try:
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
            for name, df in sheets.items():
                sheet_name = name[:31]  # Excel límite de 31 caracteres
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


# ========== FUNCIÓN PRINCIPAL PARA USO PROGRAMÁTICO (GOOGLE COLAB) ==========
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
    Función principal para unificar archivos programáticamente (desde Python/Colab).
    
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
        >>> from google.colab import files
        >>> uploaded = files.upload()  # Sube tus archivos
        >>> 
        >>> from Unify import unify_files
        >>> result = unify_files(
        ...     sprinklr_files=["XandNewsCO.xlsx"],
        ...     tubular_files=["tubular1.xlsx"],
        ...     youscan_files=["youscan.csv"],
        ...     skiprows_sprinklr=2,
        ...     skiprows_tubular=0,
        ...     skiprows_youscan=1,
        ...     out_path="unified_output.xlsx"
        ... )
        >>> print(f"✓ Archivo creado: {result}")
        >>> files.download(str(result))
        
        >>> # Ejemplo 2: Solo Sprinklr y Tubular con zona horaria
        >>> result = unify_files(
        ...     sprinklr_files=["file1.xlsx", "file2.xlsx"],
        ...     tubular_files=["tubular.csv"],
        ...     skiprows_sprinklr=2,
        ...     tz_from="UTC",
        ...     tz_to="America/Bogota",
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


# ========== CLI ==========
def main(argv: Optional[List[str]] = None):
    """Interfaz de línea de comandos para Unify.py"""
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
