import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import datetime as dt
    import os
    import sys
    from pathlib import Path

    import marimo as mo
    import matplotlib.pyplot as plt
    import plotly.graph_objects as go
    import polars as pl
    from IPython.display import display

    return Path, mo, os, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 🏟️ Win Play · Vistas por señal para SIC

    **Qué mide este reporte**

    1. **Vistas por señal** — réplica de la metodología de `dsports.py`: las *visualizaciones*
       (sesiones de reproducción con corte de inactividad) se cuentan por día y por señal
       (canal lineal) y luego se **reescalan** para que la suma de las señales de cada día
       sea exactamente el total real de vistas medido ese día (método del residuo mayor).
    """)
    return


@app.cell
def _(Path, os):
    # ══════════════════════ CONFIGURACIÓN ══════════════════════
    def _find_repo_root() -> Path:
        """Ubica la raíz del repo (donde vive `streaming_data/`)."""
        cands: list[Path] = []
        try:
            cands.append(Path(__file__).resolve().parent)
        except NameError:
            pass
        cands += [Path.cwd(), Path.cwd() / "report-sic-winplay-2026"]
        for c in cands:
            for p in [c, *c.parents]:
                if (p / "streaming_data").is_dir():
                    return p
        return Path.cwd()

    REPO_ROOT = _find_repo_root()
    DATA_DIR = REPO_ROOT / "streaming_data"
    GRID_XLSX = Path(
        os.environ.get(
            "WINPLAY_GRID",
            "/home/ochux/Documentos/Work/Reportes/Industria_Comercio_Vistas_WinPlay/"
            "Ejemplo info Win Play.xlsx",
        )
    )
    MONTHS = [f"2026-{m:02d}" for m in range(1, 11)]

    WINDOW_MINUTES = 25   # ventana rodante de concurrencia (metodología del proveedor)
    SESSION_MINUTES = 20  # corte de inactividad que define una visualización
    PLAYBACK_DAYS = 7     # ventana de diferido tras la emisión
    CO_ONLY = True        # solo Colombia

    SIGNALS = [
        "WIN+FUTBOL",
        "WIN SPORTS",
        "WIN LITE",
        "WIN AUDIO",
        "DSPORTS",
        "DSPORTS 2",
        "WIN PLAY EVENTOS",
    ]
    return (
        CO_ONLY,
        DATA_DIR,
        GRID_XLSX,
        MONTHS,
        PLAYBACK_DAYS,
        SESSION_MINUTES,
        SIGNALS,
        WINDOW_MINUTES,
    )


@app.cell
def _():
    # ══════════════════════ MOTOR (módulo local) ══════════════════════
    import winplay_engine as we

    return (we,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1 · Parrilla de programación (Excel)

    Hoja `Hoja1` del Excel de ejemplo: 716 renglones = la programación de FPC de
    **Win Sports** y **Win Sports Premium** (enero–octubre 2026).

    Los renglones **A–K** son la grilla (dimensiones) y los **L–U** las 10 métricas de
    negocio que calcula este reporte.
    """)
    return


@app.cell
def _(GRID_XLSX, we):
    grid = we.load_grid(GRID_XLSX)
    programs = we.grid_programs(grid)
    grid
    return (grid,)


@app.cell
def _(grid, mo, pl):
    _canales = grid.group_by("Canal").agg(pl.len().alias("Renglones")).sort(
        "Renglones", descending=True
    )
    mo.vstack(
        [
            mo.md(
                f"**Parrilla cargada:** {grid.height} renglones · "
                f"{grid['Programa'].n_unique()} programas · "
                f"{grid['Canal'].n_unique()} canales · "
                f"{grid['Fecha'].min():%d/%m/%Y} → {grid['Fecha'].max():%d/%m/%Y}"
            ),
            _canales,
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2 · Carga y medición (lazy, mes a mes)

    | paso | qué hace |
    |---|---|
    | `presence_for_month` | colapsa los eventos crudos a **presencia minuto a minuto** por *stream* (`SubscriberID + Title + SeriesTitle + deviceDescription`) y asigna la **sesión de visualización** (corte de inactividad de 20 min, por día) |
    | `title_catalog_raw` | catálogo por título (usuarios, hora pico, duración del asset) para **emparejar la parrilla con el OTT** |
    | `concurrency_series` | **concurrencia** con ventana rodante de 25 min (misma matemática que `dsports.py`) |
    | `grid_metrics_for_month` | las 10 métricas de la parrilla, programa por programa |

    La cola de diferido de un programa emitido a fin de mes se resuelve con la presencia
    del mes siguiente: en memoria coexisten **dos meses**, nunca los diez.
    """)
    return


@app.cell
def _(
    CO_ONLY,
    DATA_DIR,
    GRID_XLSX,
    MONTHS,
    PLAYBACK_DAYS,
    SESSION_MINUTES,
    SIGNALS,
    WINDOW_MINUTES,
    mo,
    we,
):
    @mo.cache
    def _cargar(
        data_dir,
        grid_xlsx,
        months,
        window_minutes,
        session_minutes,
        playback_days,
        co_only,
        signals,
        engine_version,
    ):
        return we.procesar(
            data_dir,
            grid_xlsx,
            months,
            window_minutes,
            session_minutes,
            playback_days,
            co_only,
            signals,
        )

    res = _cargar(
        DATA_DIR,
        GRID_XLSX,
        MONTHS,
        WINDOW_MINUTES,
        SESSION_MINUTES,
        PLAYBACK_DAYS,
        CO_ONLY,
        SIGNALS,
        we.engine_version(),
    )
    return (res,)


@app.cell
def _(mo, res):
    _matched = sum(1 for m in res["grid_metrics"] if m["n_titulos"])
    _total = len(res["grid_metrics"])
    mo.md(
        f"**Procesado en {res['elapsed']:.0f}s** · {len(res['paths'])} meses · "
        f"{len(res['catalog']):,} títulos OTT catalogados · "
        f"**{_matched}/{_total}** renglones de la parrilla emparejados con su título OTT."
    )
    return


@app.cell
def _(pl, res):
    # ══════════════════════ DATOS PARA LA SECCIÓN DE SEÑALES ══════════════════════
    df_views_day = res["views_day"].rename({"_total": "Total views"})
    df_views_scaler = res["views_scaler"]
    df_concurrency = res["concurrency"]
    df_signal_concurrency = res["signal_concurrency"]
    df_signal_pct = res["signal_pct"]

    df_kpi_senales = (
        df_views_scaler.group_by("senal")
        .agg(
            pl.col("Total views (escalado)").sum().alias("Total vistas"),
            pl.col("day").min().alias("Fecha min"),
            pl.col("day").max().alias("Fecha max"),
        )
        .sort("Total vistas", descending=True)
    )
    return df_views_day, df_views_scaler


@app.cell
def _(pl, res):
    # ══════════════════════ DATOS PARA LA SECCIÓN DE PARRILLA ══════════════════════
    df_tabla = res["tabla"]
    df_conciliacion = res["conciliacion"]

    df_mes = (
        df_tabla.with_columns(pl.col("Fecha").dt.strftime("%Y-%m").alias("Mes"))
        .group_by("Mes")
        .agg(
            pl.len().alias("Programas"),
            pl.col("Stream").sum().alias("Stream"),
            pl.col("Play back").sum().alias("Play back"),
            (pl.col("Time (minutos)").sum()).round(0).alias("Time (minutos)"),
            pl.col("Activos").sum().alias("Activos"),
            pl.col("Peak WSO").max().alias("Peak WSO"),
            pl.col("% de consumo").mean().round(2).alias("% consumo medio"),
        )
        .sort("Mes")
    )
    df_cobertura = (
        df_tabla.group_by("Estado match")
        .agg(
            pl.len().alias("Renglones"),
            pl.col("Stream").sum().alias("Stream"),
            (pl.col("Time (minutos)").sum()).round(0).alias("Time (minutos)"),
        )
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3 · Señales: vistas reales y reescalado

    `Total views` por día es el **total real medido** (todas las señales + contenido VOD).
    El reescalado reparte ese total entre las señales según su peso relativo del día
    (`Porcent`), con el método del **residuo mayor**: la suma diaria cuadra exacta.
    """)
    return


@app.cell
def _(df_views_day, df_views_scaler, mo, pl):
    _ejemplo = df_views_scaler.filter(pl.col("day") == df_views_scaler["day"].min()).head(8)
    mo.vstack(
        [
            mo.md(
                f"**Total real del periodo:** {df_views_day['Total views'].sum():,} vistas · "
                f"**{df_views_scaler['day'].n_unique()} días** con actividad · "
                f"**{df_views_scaler['senal'].n_unique()} señales** detectadas."
            ),
            _ejemplo,
        ]
    )
    return


if __name__ == "__main__":
    app.run()
