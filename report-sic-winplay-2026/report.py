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
    return df_signal_concurrency, df_signal_pct, df_views_day, df_views_scaler


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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Configuración de señales cada 15 minutos con los helpers
    """)
    return


@app.cell
def _(Path, pl):
    # ══════════════════════ 15 min · helpers optimizados ══════════════════════

    BUCKET_MINUTES = 15
    BUCKET_EVERY = f"{BUCKET_MINUTES}m"

    VALUE_COL_CANDIDATES = ("active_streams", "Concurrencia", "concurrencia", "active")


    def agregar_eventos_15m_por_senal(
        senal: str,
        serie: pl.DataFrame,
        value_col: str | None = None,
    ) -> pl.DataFrame:
        """
        Agrega una serie por señal a bloques de 15 minutos.

        La columna de valor (streams concurrentes) se detecta automáticamente
        (`active_streams`, `Concurrencia`, ...) o se pasa explícitamente con
        `value_col`. Se procesa una señal por vez y se devuelve una tabla
        pequeña: esto evita concatenar todas las señales crudas en memoria.
        """
        if value_col is None:
            for _cand in VALUE_COL_CANDIDATES:
                if _cand in serie.columns:
                    value_col = _cand
                    break
            else:
                raise ValueError(
                    f"La serie de '{senal}' no tiene columna de valor "
                    f"(esperaba alguna de {VALUE_COL_CANDIDATES}); "
                    f"columnas disponibles: {serie.columns}"
                )

        return (
            serie.lazy()
            .select(
                pl.col("ts").cast(pl.Datetime("us")).alias("ts"),
                pl.col(value_col).cast(pl.Float64).fill_null(0.0).alias("streams"),
            )
            .filter(pl.col("ts").is_not_null() & (pl.col("streams") > 0))
            .with_columns(
                pl.col("ts").dt.truncate(BUCKET_EVERY).alias("time_15"),
                pl.col("ts").dt.date().alias("day"),
            )
            .group_by(["day", "time_15"])
            .agg(
                pl.col("streams")
                .sum()
                .round(0)
                .cast(pl.Int64)
                .alias("eventos_15m")
            )
            .collect()
            .with_columns(pl.lit(senal).alias("senal"))
            .select(["day", "senal", "time_15", "eventos_15m"])
            .sort(["day", "senal", "time_15"])
        )


    def repartir_views_por_15m(
        eventos_15m: pl.LazyFrame,
        vistas_dia_senal: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """
        Reparte vistas escaladas por día/señal en bloques de 15m.

        Espera:
            eventos_15m:
                day, senal, time_15, eventos_15m

            vistas_dia_senal:
                day, senal, vistas_dia_senal

        Devuelve un LazyFrame para que el notebook controle cuándo
        materializar el resultado.
        """
        return (
            eventos_15m
            .join(
                vistas_dia_senal,
                on=["day", "senal"],
                how="left",
            )
            .filter(pl.col("vistas_dia_senal") > 0)
            .with_columns(
                pl.col("eventos_15m")
                .sum()
                .over(["day", "senal"])
                .alias("eventos_dia_senal")
            )
            .with_columns(
                pl.when(pl.col("eventos_dia_senal") > 0)
                .then(
                    pl.col("vistas_dia_senal")
                    * pl.col("eventos_15m")
                    / pl.col("eventos_dia_senal")
                )
                .otherwise(0.0)
                .alias("vistas_15m_proporcionales")
            )
            .with_columns(
                pl.col("vistas_15m_proporcionales")
                .floor()
                .cast(pl.Int64)
                .alias("_entero")
            )
            .with_columns(
                (
                    pl.col("vistas_dia_senal")
                    - pl.col("_entero")
                    .sum()
                    .over(["day", "senal"])
                )
                .cast(pl.Int64)
                .alias("_restantes")
            )
            .with_columns(
                pl.when(pl.col("_restantes") < 0)
                .then(pl.lit(0, dtype=pl.Int64))
                .otherwise(pl.col("_restantes"))
                .alias("_restantes")
            )
            .with_columns(
                pl.len()
                .over(["day", "senal"])
                .cast(pl.Int64)
                .alias("_grupo_rows")
            )
            .with_columns(
                pl.min_horizontal(
                    pl.col("_restantes"),
                    pl.col("_grupo_rows"),
                )
                .cast(pl.Int64)
                .alias("_restantes")
            )
            .drop("_grupo_rows")
            .with_columns(
                pl.col("vistas_15m_proporcionales")
                .rank(method="ordinal", descending=True)
                .over(["day", "senal"])
                .cast(pl.Int64)
                .alias("_rank")
            )
            .with_columns(
                pl.when(pl.col("_rank") <= pl.col("_restantes"))
                .then(pl.col("_entero") + 1)
                .otherwise(pl.col("_entero"))
                .cast(pl.Int64)
                .alias("vistas_15m_escaladas")
            )
            .with_columns(
                (
                    100.0
                    * pl.col("eventos_15m")
                    / pl.col("eventos_dia_senal")
                )
                .round(3)
                .alias("peso_eventos_%")
            )
            .with_columns(
                (
                    100.0
                    * pl.col("vistas_15m_escaladas")
                    / pl.col("vistas_dia_senal")
                )
                .round(3)
                .alias("peso_vistas_%")
            )
            .drop(["_entero", "_restantes", "_rank"])
            .sort(["day", "senal", "time_15"])
        )


    def eventos_15m_desde_parquet(
        paths: list[Path],
        ts_col: str,
        senal_col: str,
        co_col=None,
    ) -> pl.DataFrame:
        """
        Variante para eventos originales en parquet sin cargarlos completos.

        Lee archivo por archivo o carpeta por carpeta, agrega a bloques
        de 15m y devuelve solo la tabla agregada pequeña.
        """
        partes = []

        for p in paths:
            source = p / "**/*.parquet" if p.is_dir() else p
            ts_expr = pl.col(ts_col).cast(pl.Datetime("us"))

            lf = pl.scan_parquet(str(source)).filter(ts_expr.is_not_null())

            if co_col is not None:
                lf = lf.filter(pl.col(co_col) == "CO")

            part = (
                lf.with_columns(
                    ts_expr.dt.truncate(BUCKET_EVERY).alias("time_15"),
                    ts_expr.dt.date().alias("day"),
                )
                .group_by(["day", senal_col, "time_15"])
                .agg(pl.len().alias("eventos_15m"))
                .rename({senal_col: "senal"})
                .collect()
            )

            partes.append(part)

        return (
            pl.concat(partes, rechunk=False)
            .sort(["day", "senal", "time_15"])
        )

    return agregar_eventos_15m_por_senal, repartir_views_por_15m


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Eventos / presencia por bloques 15 mins
    """)
    return


@app.cell
def _(
    agregar_eventos_15m_por_senal,
    df_signal_concurrency,
    df_views_scaler,
    pl,
    repartir_views_por_15m,
):
    _parts_15m = []

    for _senal, _serie in df_signal_concurrency.items():
        _parts_15m.append(agregar_eventos_15m_por_senal(_senal, _serie))

    df_eventos_15min = (
        pl.concat(_parts_15m, rechunk=False)
        .sort(["day", "senal", "time_15"])
    )

    # Vistas escaladas distribuidas
    df_views_15min = (
        repartir_views_por_15m(
            df_eventos_15min.lazy(),
            df_views_scaler.lazy().select(
                "day",
                "senal",
                pl.col("Total views (escalado)").alias("vistas_dia_senal"),
            ),
        )
        .collect()
    )

    # Total agregado por día y bloque de 15min, con desglose por señal
    _senales_15m = (
        df_views_15min.group_by("senal")
        .agg(pl.col("vistas_15m_escaladas").sum().alias("_vistas"))
        .sort("_vistas", descending=True)["senal"]
        .to_list()
    )

    df_views_15min_total = (
        df_views_15min.lazy()
        .group_by(["day", "time_15"])
        .agg(
            pl.col("vistas_15m_escaladas").sum().alias("vistas_15m"),
            pl.col("eventos_15m").sum().alias("eventos_15m"),
            *[
                pl.col("vistas_15m_escaladas")
                .filter(pl.col("senal") == _s)
                .sum()
                .alias(f"vistas_15m · {_s}")
                for _s in _senales_15m
            ],
        )
        .collect()
        .sort(["day", "time_15"])
    )

    df_views_15min_total
    return df_eventos_15min, df_views_15min_total


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Comparación contra vistas reales
    """)
    return


@app.cell
def _(df_views_15min_total, df_views_day, pl):
    df_15min_check_day = (
        df_views_15min_total.lazy()
        .group_by("day")
        .agg(pl.col("vistas_15m").sum().alias("vistas_15m_dia"))
        .join(df_views_day.lazy(), on="day", how="full", coalesce=True)
        .with_columns(
            pl.col("vistas_15m_dia").fill_null(0).cast(pl.Int64),
            pl.col("Total views").fill_null(0).cast(pl.Int64),
        )
        .with_columns(
            (pl.col("vistas_15m_dia") - pl.col("Total views")).alias("diferencia")
        )
        .collect()
        .sort(["diferencia", "day"])
    )

    df_15min_check_day.filter(pl.col("diferencia") != 0).head(20)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    (Opcional) Vistas escaladas sin bloques de 15 minutos
    """)
    return


@app.cell
def _(df_eventos_15min, df_views_scaler, pl):
    df_views_15min_faltantes = (
        df_views_scaler.lazy()
        .filter(pl.col("Total views (escalado)") > 0)
        .join(
            df_eventos_15min.lazy()
            .select(["day", "senal"])
            .unique(),
            on=["day", "senal"],
            how="anti",
        )
        .collect()
    )

    df_views_15min_faltantes
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Validación de conteo de eventos cada 15 min
    """)
    return


@app.cell
def _(df_eventos_15min, df_signal_pct, pl):
    df_15m_eventos_check = (
        df_eventos_15min.lazy()
        .group_by(["day", "senal"])
        .agg(pl.col("eventos_15m").sum().alias("eventos_15m_sum"))
        .join(
            df_signal_pct.lazy().select(["day", "senal", "eventos"]),
            on=["day", "senal"],
            how="full",
            coalesce=True,
        )
        .with_columns(
            pl.col("eventos_15m_sum").fill_null(0).cast(pl.Int64),
            pl.col("eventos").fill_null(0).cast(pl.Int64),
        )
        .with_columns(
            (pl.col("eventos_15m_sum") - pl.col("eventos")).alias("diferencia")
        )
        .collect()
        .sort(["diferencia", "day", "senal"])
    )

    df_15m_eventos_check.filter(pl.col("diferencia") != 0).head(50)
    return


if __name__ == "__main__":
    app.run()
