import marimo

__generated_with = "0.25.2"
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

    return Path, dt, mo, os, pl


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
            "/mnt/sda1/WinWork/Reports/SIC_2026/"
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

    df_tabla.head(20)
    return (df_tabla,)


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

    return (
        BUCKET_EVERY,
        BUCKET_MINUTES,
        VALUE_COL_CANDIDATES,
        agregar_eventos_15m_por_senal,
        repartir_views_por_15m,
    )


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
    return df_eventos_15min, df_views_15min, df_views_15min_total


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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Agregar información a reporte SIC
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Play back por programa (bloques de 15 min)

    Se llena el campo **`Play back`** de la parrilla con la suma de las vistas de 15 min
    (`vistas_15m_escaladas`) de las señales que alimentan cada canal:

    | Canal en la parrilla | Señales que se suman |
    |---|---|
    | `Win Sports Premium` | `WIN+FUTBOL` + `WIN LITE` + `WIN AUDIO` |
    | `Win Sports` | `WIN SPORTS` |

    **Regla de redondeo:** la `Hora Inicio` y la `Hora Final` se llevan al bloque de 15
    minutos **más cercano** y se suman todos los bloques del intervalo, incluidos ambos
    extremos, en el día de `Fecha` (si la emisión cruza medianoche, el fin pasa al día
    siguiente).

    > Ejemplo · 15/01/2026 · `Win Sports Premium` · 19:21 → 21:38
    > 19:21 → **19:15** y 21:38 → **21:45**, así que se suman los bloques 19:15, 19:30, …, 21:45
    > (11 bloques) de `WIN+FUTBOL`, `WIN LITE` y `WIN AUDIO`.
    """)
    return


@app.cell
def _(BUCKET_MINUTES, pl):
    # ══════════════════════ PLAY BACK DESDE BLOQUES DE 15 MIN ══════════════════════
    MINUTOS_DIA = 24 * 60

    # Canal de la parrilla → señales del OTT que lo alimentan
    CANAL_SENALES: dict[str, list[str]] = {
        "Win Sports Premium": ["WIN+FUTBOL", "WIN LITE", "WIN AUDIO"],
        "Win Sports": ["WIN SPORTS"],
    }


    def minutos_absolutos(fecha: str, hora: str) -> pl.Expr:
        """(fecha, hora) → minutos absolutos: días desde epoch × 1440 + minuto del día."""
        return (
            pl.col(fecha).dt.date().cast(pl.Int32) * MINUTOS_DIA
            # dt.hour()/dt.minute() devuelven Int8: hay que castear o desborda
            + pl.col(hora).dt.hour().cast(pl.Int32) * 60
            + pl.col(hora).dt.minute().cast(pl.Int32)
            + pl.col(hora).dt.second().cast(pl.Int32) // 30
        )


    def bloque_15m(minutos: pl.Expr) -> pl.Expr:
        """Minutos absolutos → bloque de 15 min más cercano (19:21 → 19:15 · 21:38 → 21:45)."""
        return (((minutos + BUCKET_MINUTES // 2) // BUCKET_MINUTES) * BUCKET_MINUTES).cast(
            pl.Int64
        )


    def play_back_desde_15m(
        parrilla: pl.DataFrame,
        vistas_15m: pl.DataFrame,
        mapa_senales: dict[str, list[str]] | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Llena `Play back` sumando las vistas de 15 min de las señales de cada canal.

        Regla de redondeo: la hora de inicio y la hora final se llevan al bloque de
        15 min **más cercano** y se suman todos los bloques del intervalo, incluidos
        ambos extremos (día de `Fecha`; si cruza medianoche se suma un día al fin).

            Win Sports Premium → WIN+FUTBOL + WIN LITE + WIN AUDIO
            Win Sports         → WIN SPORTS

        Devuelve (detalle fila × señal × bloque, parrilla original con `Play back`).
        """
        mapa = mapa_senales or CANAL_SENALES
        canal_senal = pl.DataFrame(
            {
                "Canal": [canal for canal, ss in mapa.items() for _ in ss],
                "senal": [senal for ss in mapa.values() for senal in ss],
            }
        )

        vistas_bloque = vistas_15m.select(
            "day",
            "senal",
            (
                pl.col("time_15").dt.hour().cast(pl.Int32) * 60
                + pl.col("time_15").dt.minute().cast(pl.Int32)
            ).alias("minuto_bloque"),
            pl.col("vistas_15m_escaladas").cast(pl.Int64).alias("vistas_15m"),
        )

        detalle = (
            parrilla.with_row_index("_fila")
            .select(
                "_fila",
                "Fecha",
                "Canal",
                "Programa",
                pl.col("Hora Inicio").alias("hora_inicio"),
                pl.col("Hora Final").alias("hora_final"),
            )
            .with_columns(
                bloque_15m(minutos_absolutos("Fecha", "hora_inicio")).alias("_b_ini"),
                bloque_15m(minutos_absolutos("Fecha", "hora_final")).alias("_b_fin"),
            )
            # emisión que termina después de medianoche
            .with_columns(
                pl.when(pl.col("_b_fin") < pl.col("_b_ini"))
                .then(pl.col("_b_fin") + MINUTOS_DIA)
                .otherwise(pl.col("_b_fin"))
                .alias("_b_fin")
            )
            .with_columns(
                (
                    (pl.col("_b_fin") - pl.col("_b_ini")) // BUCKET_MINUTES + 1
                ).alias("_n_bloques")
            )
            .with_columns(
                (
                    pl.col("_b_ini")
                    + pl.int_ranges(0, pl.col("_n_bloques")) * BUCKET_MINUTES
                ).alias("_min_bloque")
            )
            .explode("_min_bloque")
            .with_columns(
                (pl.col("_min_bloque") // MINUTOS_DIA).cast(pl.Date).alias("day"),
                (pl.col("_min_bloque") % MINUTOS_DIA).alias("minuto_bloque"),
            )
            .join(canal_senal, on="Canal", how="inner")
            .join(vistas_bloque, on=["day", "senal", "minuto_bloque"], how="left")
            .with_columns(pl.col("vistas_15m").fill_null(0).cast(pl.Int64))
            .sort(["_fila", "senal", "minuto_bloque"])
        )

        play_back = detalle.group_by("_fila").agg(
            pl.col("vistas_15m").sum().alias("Play back"),
            pl.col("minuto_bloque").n_unique().alias("Bloques 15m"),
            (pl.col("vistas_15m") > 0).sum().alias("Bloques-señal con vistas"),
        )

        parrilla_playback = (
            parrilla.with_row_index("_fila")
            .drop("Play back")
            .join(play_back, on="_fila", how="left")
            .drop("_fila")
            .with_columns(
                pl.col("Play back").fill_null(0).cast(pl.Int64),
                pl.col("Bloques 15m").fill_null(0).cast(pl.Int64),
                pl.col("Bloques-señal con vistas").fill_null(0).cast(pl.Int64),
            )
            .select([*parrilla.columns, "Bloques 15m", "Bloques-señal con vistas"])
        )

        return detalle, parrilla_playback


    return (
        CANAL_SENALES,
        MINUTOS_DIA,
        bloque_15m,
        minutos_absolutos,
        play_back_desde_15m,
    )


@app.cell
def _(df_views_15min, grid, play_back_desde_15m):
    df_playback_detalle, grid_playback = play_back_desde_15m(grid, df_views_15min)
    grid_playback
    return df_playback_detalle, grid_playback


@app.cell
def _(
    CANAL_SENALES: dict[str, list[str]],
    df_tabla,
    df_views_15min,
    df_views_day,
    grid,
    grid_playback,
    mo,
    pl,
):
    _senales_parrilla = {s for ss in CANAL_SENALES.values() for s in ss}
    _senales_con_datos = set(df_views_15min["senal"].unique().to_list())
    _senales_sin_datos = sorted(_senales_parrilla - _senales_con_datos)
    _canales_sin_mapa = sorted(set(grid["Canal"].unique().to_list()) - set(CANAL_SENALES))
    _total_real = int(df_views_day["Total views"].sum())
    _total_pb = int(grid_playback["Play back"].sum())


    df_playback_canal = (
        grid_playback.group_by("Canal")
        .agg(
            pl.len().alias("Programas"),
            pl.col("Play back").sum().alias("Play back (15m)"),
            pl.col("Play back").mean().round(0).cast(pl.Int64).alias("Media por programa"),
            pl.col("Play back").max().alias("Máximo"),
            (pl.col("Play back") > 0).sum().alias("Programas con vistas"),
        )
        .join(
            df_tabla.group_by("Canal").agg(
                pl.col("Play back").sum().alias("Play back (OTT, referencia)")
            ),
            on="Canal",
            how="left",
        )
        .sort("Play back (15m)", descending=True)
    )

    mo.vstack(
        [
            mo.md(
                f"**Play back estimado con bloques de 15 min:** "
                f"{_total_pb:,} vistas · "
                f"{grid_playback.height:,} programas · "
                f"{(grid_playback['Play back'] > 0).sum():,} con al menos un bloque con vistas  \n"
                "Regla: hora inicio y hora final se llevan al bloque de 15 min más cercano y se "
                "suman todos los bloques del intervalo (extremos incluidos), según las señales del canal."
            ),
            df_playback_canal,
            mo.md(
                "**Señales del mapa sin datos en `vistas_15m`:** "
                + (", ".join(f"`{s}`" for s in _senales_sin_datos) or "ninguna")
                + "  \n**Canales sin señales asignadas:** "
                + (", ".join(f"`{c}`" for c in _canales_sin_mapa) or "ninguno")
                + f"  \n_Nota:_ la suma por programa **no** es comparable 1:1 con las vistas reales "
                f"({_total_real:,} en el periodo, {_total_pb / max(_total_real, 1):.0%} de eso): un "
                "mismo partido se emite en Premium y en Win Sports (se cuenta dos veces) y cada "
                "señal tiene visualizaciones fuera de la ventana del programa."
            ),
        ]
    )
    return


@app.cell
def _(df_playback_detalle, mo, pl):
    _fila_ejemplo = 0  # 15/01/2026 · Win Sports Premium · 19:21 → 21:38

    _det_ejemplo = df_playback_detalle.filter(pl.col("_fila") == _fila_ejemplo)

    df_pb_ejemplo = (
        _det_ejemplo.with_columns(
            pl.format(
                "{}:{}",
                (pl.col("minuto_bloque") // 60).cast(pl.Utf8).str.zfill(2),
                (pl.col("minuto_bloque") % 60).cast(pl.Utf8).str.zfill(2),
            ).alias("bloque")
        )
        .group_by("senal")
        .agg(
            pl.len().alias("Bloques"),
            pl.col("bloque").str.join(", ").alias("Bloques 15m usados"),
            pl.col("vistas_15m").sum().alias("Vistas sumadas"),
        )
        .sort("Vistas sumadas", descending=True)
    )

    _meta = _det_ejemplo.row(0, named=True)
    _hora_ini = f"{_meta['hora_inicio']:%H:%M}"
    _hora_fin = f"{_meta['hora_final']:%H:%M}"

    mo.vstack(
        [
            mo.md(
                f"**Ejemplo · {_meta['Fecha']:%d/%m/%Y} · {_meta['Canal']}** — "
                f"{_meta['Programa']}  \n"
                f"Intervalo original {_hora_ini} → {_hora_fin}; se suman los bloques de 15 min "
                f"del intervalo redondeado (extremos incluidos) de las señales del canal.  \n"
                f"**Play back = {df_pb_ejemplo['Vistas sumadas'].sum():,}**"
            ),
            df_pb_ejemplo,
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Stream por programa (bloques de 15 min con SubscriberID)

    La columna **`Stream`** sigue la misma mecánica de ventana temporal que `Play back`,
    pero en lugar de sumar vistas reescaladas usa **`SubscriberID` únicos**.

    Para cada bloque de 15 minutos y por **canal de la parrilla**:

    1. se leen las tablas mensuales concatenadas (`res["paths"]`),
    2. se filtran eventos de Colombia si `CO_ONLY=True` (`CountryCode == "CO"`),
    3. se normaliza la señal desde `Title`,
    4. se asocia la señal al `Canal` usando `CANAL_SENALES`,
    5. se deduplica `SubscriberID` dentro de `day + Canal + time_15`.

    Después se llena el intervalo del programa con la misma regla de redondeo que
    `Play back`: `Hora Inicio` y `Hora Final` se llevan al bloque de 15 minutos más
    cercano y se suman todos los bloques incluidos ambos extremos.

    > Nota: esta métrica deduplica usuarios **dentro de cada bloque de 15 min**.
    > Si un mismo `SubscriberID` aparece en dos bloques distintos, aparece una vez por
    > bloque. Para “usuarios únicos de todo el programa” habría que deduplicar
    > `SubscriberID` sobre todo el intervalo, no bloque a bloque.
    """)
    return


@app.cell
def _(
    BUCKET_EVERY,
    BUCKET_MINUTES,
    MINUTOS_DIA,
    bloque_15m,
    minutos_absolutos,
    pl,
):
    # ══════════════════════ STREAM DESDE BLOQUES DE 15 MIN ══════════════════════
    STREAM_VERSION = "v3"


    def senal_desde_title_expr(col_title: str = "Title") -> pl.Expr:
        """Normaliza `Title` a señal para los canales usados en la parrilla."""
        up = pl.col(col_title).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
        return (
            pl.when(up.str.starts_with("WIN PLAY EVENTOS"))
            .then(pl.lit("WIN PLAY EVENTOS"))
            .otherwise(up)
            .alias("senal")
        )


    def usuarios_15min_por_canal_desde_parquet(
        paths,
        mapa_senales: dict[str, list[str]],
        co_only: bool = True,
    ) -> pl.DataFrame:
        """
        Tabla `day + Canal + time_15` con SubscriberID únicos por bloque.

        Se recorre cada parquet mensual y se deduplica `SubscriberID` dentro del
        bloque de 15 minutos del canal (que puede estar alimentado por varias
        señales). Esto evita contar dos veces el mismo suscriptor si aparece en
        más de una señal del mismo canal.
        """
        senal_canal = pl.DataFrame(
            {
                "Canal": [canal for canal, ss in mapa_senales.items() for _ in ss],
                "senal": [senal for ss in mapa_senales.values() for senal in ss],
            }
        ).unique(["Canal", "senal"])

        senales_mapa = senal_canal["senal"].unique().to_list()
        partes: list[pl.DataFrame] = []

        for _month, path in paths:
            lf = pl.scan_parquet(str(path))

            if co_only:
                lf = lf.filter(pl.col("CountryCode") == "CO")

            part = (
                lf.select(
                    pl.col("Date")
                    .cast(pl.Utf8)
                    .str.to_datetime(strict=False)
                    .alias("ts"),
                    pl.col("SubscriberID").cast(pl.Utf8).alias("SubscriberID"),
                    senal_desde_title_expr("Title"),
                )
                .filter(
                    pl.col("ts").is_not_null()
                    & pl.col("SubscriberID").is_not_null()
                    & (pl.col("SubscriberID") != "")
                    & pl.col("senal").is_in(senales_mapa)
                )
                .join(senal_canal.lazy(), on="senal", how="inner")
                .with_columns(
                    pl.col("ts").dt.truncate(BUCKET_EVERY).alias("time_15"),
                    pl.col("ts").dt.date().alias("day"),
                )
                .unique(subset=["day", "Canal", "time_15", "SubscriberID"])
                .group_by(["day", "Canal", "time_15"])
                .agg(pl.len().alias("usuarios_15m"))
                .collect()
            )

            if part.height:
                partes.append(part)

        if not partes:
            return pl.DataFrame(
                schema={
                    "day": pl.Date,
                    "Canal": pl.Utf8,
                    "time_15": pl.Datetime("us"),
                    "usuarios_15m": pl.Int64,
                }
            )

        return pl.concat(partes, rechunk=False).sort(["day", "Canal", "time_15"])


    def stream_por_canal_desde_15m(
        parrilla: pl.DataFrame,
        usuarios_15m: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Llena `Stream` con usuarios únicos por bloque de 15 min.

        Usa la misma ventana de bloques redondeados que `play_back_desde_15m`:
        inicio y fin al bloque de 15 min más cercano, extremos incluidos.

        Devuelve (detalle fila × bloque, parrilla con `Stream`).
        """
        usuarios_bloque = usuarios_15m.select(
            "day",
            "Canal",
            (
                pl.col("time_15").dt.hour().cast(pl.Int32) * 60
                + pl.col("time_15").dt.minute().cast(pl.Int32)
            ).alias("minuto_bloque"),
            pl.col("usuarios_15m").cast(pl.Int64),
        )

        detalle = (
            parrilla.with_row_index("_fila")
            .select(
                "_fila",
                "Fecha",
                "Canal",
                "Programa",
                pl.col("Hora Inicio").alias("hora_inicio"),
                pl.col("Hora Final").alias("hora_final"),
            )
            .with_columns(
                bloque_15m(minutos_absolutos("Fecha", "hora_inicio")).alias("_b_ini"),
                bloque_15m(minutos_absolutos("Fecha", "hora_final")).alias("_b_fin"),
            )
            .with_columns(
                pl.when(pl.col("_b_fin") < pl.col("_b_ini"))
                .then(pl.col("_b_fin") + MINUTOS_DIA)
                .otherwise(pl.col("_b_fin"))
                .alias("_b_fin")
            )
            .with_columns(
                (
                    (pl.col("_b_fin") - pl.col("_b_ini")) // BUCKET_MINUTES + 1
                ).alias("_n_bloques")
            )
            .with_columns(
                (
                    pl.col("_b_ini")
                    + pl.int_ranges(0, pl.col("_n_bloques")) * BUCKET_MINUTES
                ).alias("_min_bloque")
            )
            .explode("_min_bloque")
            .with_columns(
                (pl.col("_min_bloque") // MINUTOS_DIA).cast(pl.Date).alias("day"),
                (pl.col("_min_bloque") % MINUTOS_DIA).alias("minuto_bloque"),
            )
            .join(usuarios_bloque, on=["day", "Canal", "minuto_bloque"], how="left")
            .with_columns(pl.col("usuarios_15m").fill_null(0).cast(pl.Int64))
            .sort(["_fila", "minuto_bloque"])
        )

        stream = detalle.group_by("_fila").agg(
            pl.col("usuarios_15m").sum().alias("Stream"),
            pl.col("minuto_bloque").n_unique().alias("Bloques 15m Stream"),
            (pl.col("usuarios_15m") > 0).sum().alias("Bloques con usuarios"),
        )

        orig_cols = parrilla.columns
        nuevos_cols = [c for c in stream.columns if c != "_fila" and c not in orig_cols]

        parrilla_stream = (
            parrilla.with_row_index("_fila")
            .drop("Stream")
            .join(stream, on="_fila", how="left")
            .drop("_fila")
            .with_columns(
                pl.col("Stream").fill_null(0).cast(pl.Int64),
                *(
                    [
                        pl.col(c).fill_null(0).cast(pl.Int64)
                        for c in nuevos_cols
                    ]
                ),
            )
            .select([*orig_cols, *nuevos_cols])
        )

        return detalle, parrilla_stream


    return (
        STREAM_VERSION,
        stream_por_canal_desde_15m,
        usuarios_15min_por_canal_desde_parquet,
    )


@app.cell
def _(
    BUCKET_MINUTES,
    CANAL_SENALES: dict[str, list[str]],
    CO_ONLY,
    Path,
    STREAM_VERSION,
    grid_playback,
    mo,
    res,
    stream_por_canal_desde_15m,
    usuarios_15min_por_canal_desde_parquet,
):
    # Cálculo Stream: usuarios únicos por bloque de 15 min y canal
    paths_stream = tuple((month, str(path)) for month, path in res["paths"])
    canal_senales_tup = tuple(
        sorted((canal, tuple(senales)) for canal, senales in CANAL_SENALES.items())
    )


    @mo.cache
    def _usuarios_15min_cached(
        paths_tup,
        mapa_tup,
        co_only: bool,
        bucket_minutes: int,
        stream_version: str,
    ):
        mapa = {canal: list(senales) for canal, senales in mapa_tup}
        return usuarios_15min_por_canal_desde_parquet(
            [(month, Path(path_str)) for month, path_str in paths_tup],
            mapa,
            co_only=co_only,
        )


    df_usuarios_15min = _usuarios_15min_cached(
        paths_stream,
        canal_senales_tup,
        CO_ONLY,
        BUCKET_MINUTES,
        STREAM_VERSION,
    )

    df_stream_detalle, grid_stream = stream_por_canal_desde_15m(
        grid_playback,
        df_usuarios_15min,
    )

    grid_stream

    return df_usuarios_15min, grid_stream


@app.cell
def _(df_tabla, df_usuarios_15min, grid_stream, mo, pl):
    df_stream_porcanal = (
        grid_stream.group_by("Canal")
        .agg(
            pl.len().alias("Programas"),
            pl.col("Stream").sum().alias("Stream (15m)"),
            pl.col("Stream").mean().round(0).cast(pl.Int64).alias("Media por programa"),
            pl.col("Stream").max().alias("Máximo"),
            (pl.col("Stream") > 0).sum().alias("Programas con usuarios"),
        )
        .sort("Stream (15m)", descending=True)
    )

    stream_total = int(grid_stream["Stream"].sum())
    usuarios_15min_total = int(df_usuarios_15min["usuarios_15m"].sum())

    df_stream_vs_referencia = (
        grid_stream.with_row_index("_fila")
        .join(
            df_tabla.with_row_index("_fila")
            .select(
                "_fila",
                pl.col("Stream").alias("Stream OTT (referencia)"),
                "Estado match",
            ),
            on="_fila",
            how="left",
        )
        .drop("_fila")
        .select(
            "Fecha",
            "Canal",
            "Programa",
            "Hora Inicio",
            "Hora Final",
            "Stream",
            "Play back",
            "Stream OTT (referencia)",
            "Estado match",
        )
        .sort("Fecha")
    )

    mo.vstack(
        [
            mo.md(
                f"**Stream estimado con bloques de 15 min:** {stream_total:,} usuarios-bloque · "
                f"{df_stream_porcanal['Programas'].sum():,} programas · "
                f"{(grid_stream['Stream'] > 0).sum():,} con al menos un bloque con usuarios  \n"
                f"`df_usuarios_15min` tiene {df_usuarios_15min.height:,} filas y {usuarios_15min_total:,} usuarios-bloque únicos."
            ),
            df_stream_porcanal,
            df_stream_vs_referencia,
        ]
    )

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Métricas por programa basadas en scripts de concurrencia y tiempo

    Se toman los campos que quedaron pendientes y se llenan usando la misma ventana
    redondeada de 15 minutos que `Play back` y `Stream`, pero ahora con la concurrencia
    por minuto ya calculada por señal (`df_signal_concurrency`).

    Definiciones aplicadas:

    - `Time (minutos)`: sumatoria de la concurrencia minuto a minuto en los bloques del
      evento. Es el total de minutos-vista consumidos dentro del lapso.
    - `Time Averange (minutos)`: `Time (minutos) / Play back`.
    - `Peak WSO`: máximo de usuarios/streams concurrentes observado en los lapsos de 5
      minutos dentro del evento.
    - `Activos`: copia de `Play back`.
    - `Peso% Stream`: `Stream / Play back`, expresado como porcentaje y recortado a
      `100%` cuando por métricas superpuestas resulte mayor.
    - `peso% peak`: `Peak WSO / Play back`, expresado como porcentaje y recortado a
      `100%`.
    - `Duracion_minutos`: minutos completos entre `Hora Inicio` y `Hora Final` de la
      parrilla, con ajuste si el evento cruza medianoche.
    - `% de consumo`: `Time (minutos) / (Play back × Duracion_minutos)`, como porcentaje
      del consumo ideal si todas las vistas hubieran visto el evento completo. Se limita
      a `100%`.
    """)

    return


@app.cell
def _(
    BUCKET_EVERY,
    BUCKET_MINUTES,
    CANAL_SENALES: dict[str, list[str]],
    MINUTOS_DIA,
    VALUE_COL_CANDIDATES,
    bloque_15m,
    df_signal_concurrency,
    minutos_absolutos,
    pl,
):
    # ══════════════════════ MÉTRICAS DE CONSUMO / PICO / PESOS ══════════════════════
    SENALES_METRICAS = sorted({s for ss in CANAL_SENALES.values() for s in ss})
    METRICS_VERSION = "v1"


    def df_conc_15m_por_canal_desde_concurrencia() -> pl.DataFrame:
        """
        Construye una tabla por día/canal/bloque 15m con tiempo y pico de concurrencia.

        Parte de `df_signal_concurrency`, que ya aplica la metodología del script 1:
        streams activos en una ventana rodante de concurrencia. Para cada canal:

        - `Time (minutos)`: suma de la concurrencia por minuto dentro del bloque.
        - `Peak WSO`: máximo de la concurrencia por minuto dentro del bloque.
        """
        canal_senal = pl.DataFrame(
            {
                "Canal": [canal for canal, ss in CANAL_SENALES.items() for _ in ss],
                "senal": [senal for ss in CANAL_SENALES.values() for senal in ss],
            }
        ).unique(["Canal", "senal"])

        partes: list[pl.DataFrame] = []

        for _senal, _serie in df_signal_concurrency.items():
            if _senal not in SENALES_METRICAS:
                continue

            _value_col = next(
                (_c for _c in VALUE_COL_CANDIDATES if _c in _serie.columns),
                None,
            )
            if _value_col is None:
                continue

            partes.append(
                _serie.lazy()
                .select(
                    pl.col("ts").cast(pl.Datetime("us")).alias("ts"),
                    pl.col(_value_col)
                    .cast(pl.Float64)
                    .fill_null(0.0)
                    .alias("streams"),
                )
                .filter(pl.col("ts").is_not_null() & (pl.col("streams") > 0))
                .with_columns(pl.lit(_senal).alias("senal"))
                .collect()
            )

        if not partes:
            return pl.DataFrame(
                schema={
                    "day": pl.Date,
                    "Canal": pl.Utf8,
                    "time_15": pl.Datetime("us"),
                    "Time (minutos)": pl.Int64,
                    "Peak WSO": pl.Int64,
                }
            )

        df_conc_senales = pl.concat(partes, rechunk=False).join(
            canal_senal,
            on="senal",
            how="inner",
        )

        if df_conc_senales.height == 0:
            return pl.DataFrame(
                schema={
                    "day": pl.Date,
                    "Canal": pl.Utf8,
                    "time_15": pl.Datetime("us"),
                    "Time (minutos)": pl.Int64,
                    "Peak WSO": pl.Int64,
                }
            )

        df_conc_1m = (
            df_conc_senales.with_columns(
                pl.col("ts").dt.date().alias("day"),
                pl.col("ts").dt.truncate("1m").alias("minute"),
            )
            .group_by(["day", "Canal", "minute"])
            .agg(
                pl.col("streams")
                .sum()
                .round(0)
                .cast(pl.Int64)
                .alias("active_streams")
            )
        )

        return (
            df_conc_1m.with_columns(
                pl.col("minute").dt.truncate(BUCKET_EVERY).alias("time_15"),
            )
            .group_by(["day", "Canal", "time_15"])
            .agg(
                pl.col("active_streams").sum().cast(pl.Int64).alias("Time (minutos)"),
                pl.col("active_streams").max().cast(pl.Int64).alias("Peak WSO"),
            )
            .sort(["day", "Canal", "time_15"])
        )


    def metricas_por_programa_desde_15m(
        parrilla: pl.DataFrame,
        tiempo_pico_15m: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Llena Time, Peak WSO, Activos, pesos, duración y consumo usando la misma
        ventana redondeada de 15 minutos que `Play back` y `Stream`.
        """
        tiempo_bloque = tiempo_pico_15m.select(
            "day",
            "Canal",
            (
                pl.col("time_15").dt.hour().cast(pl.Int32) * 60
                + pl.col("time_15").dt.minute().cast(pl.Int32)
            ).alias("minuto_bloque"),
            pl.col("Time (minutos)").cast(pl.Int64).alias("_time_minutos"),
            pl.col("Peak WSO").cast(pl.Int64).alias("_peak"),
        )

        detalle = (
            parrilla.with_row_index("_fila")
            .select(
                "_fila",
                "Fecha",
                "Canal",
                "Programa",
                pl.col("Hora Inicio").alias("hora_inicio"),
                pl.col("Hora Final").alias("hora_final"),
                "Play back",
                "Stream",
            )
            .with_columns(
                bloque_15m(minutos_absolutos("Fecha", "hora_inicio")).alias("_b_ini"),
                bloque_15m(minutos_absolutos("Fecha", "hora_final")).alias("_b_fin"),
            )
            .with_columns(
                pl.when(pl.col("_b_fin") < pl.col("_b_ini"))
                .then(pl.col("_b_fin") + MINUTOS_DIA)
                .otherwise(pl.col("_b_fin"))
                .alias("_b_fin")
            )
            .with_columns(
                (
                    (pl.col("_b_fin") - pl.col("_b_ini")) // BUCKET_MINUTES + 1
                ).alias("_n_bloques")
            )
            .with_columns(
                (
                    pl.col("_b_ini")
                    + pl.int_ranges(0, pl.col("_n_bloques")) * BUCKET_MINUTES
                ).alias("_min_bloque")
            )
            .explode("_min_bloque")
            .with_columns(
                (pl.col("_min_bloque") // MINUTOS_DIA).cast(pl.Date).alias("day"),
                (pl.col("_min_bloque") % MINUTOS_DIA).alias("minuto_bloque"),
            )
            .join(tiempo_bloque, on=["day", "Canal", "minuto_bloque"], how="left")
            .with_columns(
                pl.col("_time_minutos").fill_null(0).cast(pl.Int64),
                pl.col("_peak").fill_null(0).cast(pl.Int64),
                pl.col("Play back").fill_null(0).cast(pl.Int64),
                pl.col("Stream").fill_null(0).cast(pl.Int64),
            )
            .sort(["_fila", "minuto_bloque"])
        )

        metrics = (
            detalle.group_by("_fila")
            .agg(
                pl.col("_time_minutos").sum().cast(pl.Int64).alias("_time_minutos"),
                pl.col("_peak").max().cast(pl.Int64).alias("_peak"),
                pl.col("minuto_bloque").n_unique().alias("_bloques_metricas"),
            )
        )

        base = (
            parrilla.with_row_index("_fila")
            .join(metrics, on="_fila", how="left")
            .with_columns(
                pl.col("_time_minutos").fill_null(0).cast(pl.Int64),
                pl.col("_peak").fill_null(0).cast(pl.Int64),
                pl.col("_bloques_metricas").fill_null(0).cast(pl.Int64),
                pl.col("Play back").fill_null(0).cast(pl.Int64),
                pl.col("Stream").fill_null(0).cast(pl.Int64),
            )
        )

        ini_sec = (
            pl.col("Hora Inicio")
            .dt.hour()
            .cast(pl.Int64)
            * 3600
            + pl.col("Hora Inicio").dt.minute().cast(pl.Int64) * 60
            + pl.col("Hora Inicio").dt.second().cast(pl.Int64)
        )
        fin_sec = (
            pl.col("Hora Final")
            .dt.hour()
            .cast(pl.Int64)
            * 3600
            + pl.col("Hora Final").dt.minute().cast(pl.Int64) * 60
            + pl.col("Hora Final").dt.second().cast(pl.Int64)
        )
        dur_sec = fin_sec - ini_sec

        duracion_min = (
            pl.when(dur_sec < 0)
            .then(dur_sec + 86400)
            .otherwise(dur_sec)
            .cast(pl.Float64)
            / 60.0
        ).floor().cast(pl.Int64).alias("Duracion_minutos")

        base = base.with_columns(
            pl.col("_time_minutos").alias("Time (minutos)"),
            pl.col("_peak").alias("Peak WSO"),
            pl.col("Play back").alias("Activos"),
            duracion_min,
        )

        base = base.with_columns(
            pl.when(pl.col("Play back") > 0)
            .then(pl.col("_time_minutos") / pl.col("Play back"))
            .otherwise(0.0)
            .round(2)
            .alias("Time Averange (minutos)"),
        )

        base = base.with_columns(
            pl.when(pl.col("Play back") > 0)
            .then(100.0 * pl.col("Stream") / pl.col("Play back"))
            .otherwise(0.0)
            .clip(0.0, 100.0)
            .round(2)
            .alias("Peso% Stream"),
            pl.when(pl.col("Play back") > 0)
            .then(100.0 * pl.col("_peak") / pl.col("Play back"))
            .otherwise(0.0)
            .clip(0.0, 100.0)
            .round(2)
            .alias("peso% peak"),
        )

        ideal_minutos = pl.col("Play back") * pl.col("Duracion_minutos")

        base = base.with_columns(
            pl.when((pl.col("Play back") > 0) & (ideal_minutos > 0))
            .then(100.0 * pl.col("_time_minutos") / ideal_minutos)
            .otherwise(0.0)
            .clip(0.0, 100.0)
            .round(2)
            .alias("% de consumo"),
        )

        final = base.drop(["_fila", "_time_minutos", "_peak", "_bloques_metricas"])

        return detalle, final


    return (
        df_conc_15m_por_canal_desde_concurrencia,
        metricas_por_programa_desde_15m,
    )


@app.cell
def _(
    df_conc_15m_por_canal_desde_concurrencia,
    grid_stream,
    metricas_por_programa_desde_15m,
):
    df_conc_metrics_15m = df_conc_15m_por_canal_desde_concurrencia()

    df_metricas_detalle, grid_report = metricas_por_programa_desde_15m(
        grid_stream,
        df_conc_metrics_15m,
    )

    grid_report

    return (grid_report,)


@app.cell
def _(grid_report, mo, pl):
    df_report_summary = (
        grid_report.group_by("Canal")
        .agg(
            pl.len().alias("Programas"),
            pl.col("Stream").sum().alias("Stream"),
            pl.col("Play back").sum().alias("Play back"),
            pl.col("Time (minutos)").sum().alias("Time (minutos)"),
            pl.col("Peak WSO").max().alias("Peak WSO máx"),
            pl.col("% de consumo").mean().round(2).alias("% consumo medio"),
        )
        .sort("Time (minutos)", descending=True)
    )

    _df_report_nulls = grid_report.filter(
        pl.col("Time (minutos)").is_null()
        | pl.col("Peak WSO").is_null()
        | pl.col("Activos").is_null()
        | pl.col("Duracion_minutos").is_null()
    )

    _df_report_sample = grid_report.select(
        "Fecha",
        "Canal",
        "Programa",
        "Hora Inicio",
        "Hora Final",
        "Stream",
        "Play back",
        "Time (minutos)",
        "Time Averange (minutos)",
        "Peak WSO",
        "Activos",
        "Peso% Stream",
        "peso% peak",
        "Duracion_minutos",
        "% de consumo",
    ).head(20)

    mo.vstack(
        [
            mo.md(
                f"**Reporte con métricas completas:** {grid_report.height:,} programas · "
                f"{grid_report.filter(pl.col('Time (minutos)') > 0).height:,} con Time &gt; 0 · "
                f"{grid_report.filter(pl.col('Peak WSO') > 0).height:,} con Peak &gt; 0"
            ),
            df_report_summary,
            _df_report_sample,
            mo.md(f"**Filas con campos principales nulos:** {_df_report_nulls.height}"),
        ]
    )

    return


@app.cell
def _(dt, grid_report, mo):
    import base64
    import io

    report_filename = f"report_sic_winplay_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    report_csv_buffer = io.StringIO()
    grid_report.write_csv(report_csv_buffer)
    report_csv_bytes = report_csv_buffer.getvalue().encode("utf-8-sig")

    report_csv_b64 = base64.b64encode(report_csv_bytes).decode("ascii")
    report_csv_uri = f"data:text/csv;base64,{report_csv_b64}"

    mo.Html(
        f"""
        <div style="display:flex;align-items:center;gap:10px;margin-top:8px;">
          <a
            href="{report_csv_uri}"
            download="{report_filename}"
            style="
              display:inline-block;
              padding:8px 14px;
              background:#ff5500;
              color:white;
              text-decoration:none;
              border-radius:6px;
              font-weight:600;
            "
          >
            ⬇ Descargar reporte CSV
          </a>
          <span style="opacity:0.7;">
            {grid_report.height:,} filas · {len(grid_report.columns)} columnas
          </span>
        </div>
        """
    )

    return


if __name__ == "__main__":
    app.run()
