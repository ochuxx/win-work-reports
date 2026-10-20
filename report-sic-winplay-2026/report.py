import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from IPython.display import display

    return mo, pl


@app.cell
def _(mo, pl):
    import os

    # Directorio de los particionados mensuales (misma ruta que ya funcionaba para 2026-01)
    _base_dir = './win-work-reports/streaming_data/'

    # Rutas de 2026-01 (enero) a 2026-10 (octubre)
    _rutas = [os.path.join(_base_dir, f"LIVE_2026-{m:02d}.parquet") for m in range(1, 11)]
    _existentes = [r for r in _rutas if os.path.exists(r)]
    _faltantes = [os.path.basename(r) for r in _rutas if r not in _existentes]

    # ── MERGE LAZY ──────────────────────────────────────────────────────────
    # scan_parquet NO lee los datos: construye un plan (LazyFrame).
    # Nada ocupa RAM hasta .collect(), y polars procesa en chunks (streaming).
    df = pl.concat(
        [
            pl.scan_parquet(_r).with_columns(
                pl.lit(os.path.basename(_r)[5:12]).alias("fuente_mes")  # ej. "2026-01"
            )
            for _r in _existentes
        ],
        how="vertical_relaxed",
    )

    # Filas por mes DESDE METADATOS (sin leer ninguna fila)
    _conteo = pl.DataFrame({
        "mes": [os.path.basename(_r)[5:12] for _r in _existentes],
        "filas": [pl.scan_parquet(_r).select(pl.len()).collect().item() for _r in _existentes],
    })

    _resumen = mo.md(
        f"**Merge lazy 2026-01 → 2026-10:** {len(_existentes)} meses · "
        f"{_conteo['filas'].sum():,} filas totales · {len(df.columns)} columnas.\n\n"
        "⚙️ `df` es un **LazyFrame**: no consume RAM hasta que hagas `.collect()`. "
        "Encadena filtros/proyecciones/`.head()` antes de recolectar.\n\n"
        + ("⚠️ Faltan: " + ", ".join(_faltantes) if _faltantes else "✅ Todos los meses presentes.")
    )

    mo.vstack([_resumen, _conteo])
    return (df,)


@app.cell
def _(df):
    # Revisión de full dataframe

    df.head(10)
    return


if __name__ == "__main__":
    app.run()
