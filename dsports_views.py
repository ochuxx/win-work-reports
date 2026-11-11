import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo  # Relevante para usar .py como .ipynb
    import polars as pl
    import datetime as dt
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from IPython.display import display

    return display, dt, mo, pl, plt


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Reporte de Views por señal

    Este reporte replica la lógica de **Total views** del panel de Streaming:
    una *view* es un bloque de eventos de un mismo
    `SubscriberID + Title + SeriesTitle + deviceDescription` sin huecos mayores
    a **20 minutos** (LIVE). El total de views es la cantidad de filas del frame
    deduplicado, y cada view se asigna al día de su primer evento.

    A diferencia de `dsports.py` (concurrencia), aquí toda la visualización se
    hace con **gráficos de barras apiladas** de views por señal. El reescalado
    diario se mantiene: las views de las señales se reparten con el método del
    residuo mayor para que la suma diaria sea exactamente el total real del día.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Importar datos
    """)
    return


@app.cell
def _(dt):
    # Columnas que definen una visualización única.
    DEVICE_UNIQUE_COLS = [
        'Date',
        'SubscriberID',
        'Title',
        'SeriesTitle',
        'DeviceType',
        'deviceDescription',
    ]

    # Periodo del reporte (cambios manuales).
    DATE_REPORT_INI = dt.date(2026, 7, 1)
    DATE_REPORT_END = dt.date(2026, 10, 31)

    # Ventana de inactividad para separar una view de otra.
    # El panel usa 20 minutos para LIVE (24 h para VOD).
    LIVE_GAP_MINUTES = 20
    LIVE_GAP_HOURS = LIVE_GAP_MINUTES / 60

    # Señales que se reportan.
    SIGNALS = ['WIN AUDIO', 'WIN SPORTS', 'WIN+FUTBOL', 'DSPORTS', 'DSPORTS 2', 'TYC']

    # Mismos colores que el reporte de concurrencia.
    SENAL_COLORS = {
        'WIN+FUTBOL': '#ff5500',
        'WIN SPORTS': '#d839ff',
        'WIN AUDIO': '#6e8680',
        'DSPORTS': '#0ba4e5',
        'DSPORTS 2': '#1070a0',
        'TYC': '#86c186',
    }

    # Abreviaturas de meses en español para los ejes.
    MESES_ABREV = {
        1: 'ene', 2: 'feb', 3: 'mar', 4: 'abr', 5: 'may', 6: 'jun',
        7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dic',
    }
    return (
        DATE_REPORT_END,
        DATE_REPORT_INI,
        DEVICE_UNIQUE_COLS,
        LIVE_GAP_HOURS,
        MESES_ABREV,
        SENAL_COLORS,
        SIGNALS,
    )


@app.cell
def _(DATE_REPORT_END, DATE_REPORT_INI, DEVICE_UNIQUE_COLS, dt, pl):
    from pathlib import Path

    # Solo se leen los días del reporte y las columnas necesarias: evita cargar
    # los meses completos (octubre tiene ~12 M de eventos).
    # Se buscan las rutas en las ubicaciones habituales para que el notebook
    # funcione tanto desde la raíz del repo como desde su carpeta padre.
    _candidatos = []
    try:
        _candidatos.append(Path(__file__).resolve().parent / 'streaming_data')
    except NameError:
        pass
    # Meses que cubre el periodo (soporta rangos de uno o varios meses).
    _meses = []
    _year, _month = DATE_REPORT_INI.year, DATE_REPORT_INI.month
    while (_year, _month) <= (DATE_REPORT_END.year, DATE_REPORT_END.month):
        _meses.append(f'{_year:04d}-{_month:02d}')
        _month += 1
        if _month > 12:
            _month = 1
            _year += 1
    _primer_archivo = f'LIVE_{_meses[0]}.parquet'

    _candidatos += [
        Path.cwd() / 'streaming_data',
        Path.cwd() / 'win-work-reports' / 'streaming_data',
        Path('./win-work-reports/streaming_data'),
    ]
    _data_dir = next(
        (
            _carpeta
            for _carpeta in _candidatos
            if (_carpeta / _primer_archivo).exists()
        ),
        None,
    )
    if _data_dir is None:
        raise FileNotFoundError(
            f'No se encontró streaming_data/{_primer_archivo}. '
            'Ejecuta el notebook desde la raíz del repo o desde su carpeta padre.'
        )

    _paths = [_data_dir / f'LIVE_{_mes}.parquet' for _mes in _meses]
    _start = DATE_REPORT_INI.isoformat()
    _end_excl = (DATE_REPORT_END + dt.timedelta(days=1)).isoformat()

    _lf = pl.concat(
        [pl.scan_parquet(_path).select(DEVICE_UNIQUE_COLS) for _path in _paths],
        how='vertical',
    ).filter((pl.col('Date') >= _start) & (pl.col('Date') < _end_excl))

    # Compatible con Date String o Datetime.
    if _lf.collect_schema()['Date'] == pl.String:
        _lf = _lf.with_columns(pl.col('Date').str.to_datetime(strict=False))
    else:
        _lf = _lf.with_columns(pl.col('Date').cast(pl.Datetime, strict=False))

    df_events = _lf.collect(engine='streaming')
    return (df_events,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Verificación

    Se muestran los primeros eventos del periodo y el volumen cargado.
    """)
    return


@app.cell
def _(df_events, display):
    print(f'Eventos del periodo: {df_events.height:,}')
    print(f'Rango: {df_events["Date"].min()} → {df_events["Date"].max()}')
    display(df_events.head())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Normalizar información (Views)

    ### Función para reconocer usuarios y views

    Se deduplica por la llave
    `SubscriberID + Title + SeriesTitle + deviceDescription`.
    Si la misma llave reaparece después de **20 minutos** (LIVE), se considera
    otra view. El frame resultante tiene una fila por view.
    """)
    return


@app.cell
def _(LIVE_GAP_HOURS, pl):
    def _empty_pl_df(columns: list[str]) -> pl.DataFrame:
        return pl.DataFrame({col: [] for col in columns})


    def device_unique_per_user(
        df: pl.DataFrame,
        cols: list[str],
        hours_diff: float = LIVE_GAP_HOURS,
    ) -> pl.DataFrame:
        """Deduplica events por view (misma definición del panel de Streaming).

        - Una view se identifica por SubscriberID + Title + SeriesTitle +
          deviceDescription.
        - Si la misma llave reaparece después de `hours_diff`, es otra view.
        - Devuelve una fila por view, con `id_unique_visual_by_user` y
          `UserUniqueID` (primer stream histórico del usuario).
        """
        expected_base_cols = [c for c in cols if isinstance(c, str) and c.strip()]
        empty = _empty_pl_df(
            expected_base_cols + ['id_unique_visual_by_user', 'UserUniqueID']
        )
        if df is None or df.is_empty():
            return empty

        try:
            missing_cols = [c for c in expected_base_cols if c not in df.columns]
            if missing_cols:
                print(f'Error al procesar la información: faltan columnas requeridas: {missing_cols}')
                return empty

            df = df.select(expected_base_cols)
            df = df.with_columns(
                pl.when(
                    pl.col('SeriesTitle').is_null()
                    | (pl.col('SeriesTitle').cast(pl.Utf8, strict=False).str.strip_chars() == '')
                )
                .then(pl.lit('Sin programa'))
                .otherwise(pl.col('SeriesTitle').cast(pl.Utf8, strict=False).str.strip_chars())
                .alias('SeriesTitle'),
                pl.col('deviceDescription').fill_null('UnknownDevice'),
            )

            # Normaliza la fecha para calcular la ventana de inactividad.
            if df.schema['Date'] == pl.String:
                df = df.with_columns(pl.col('Date').str.to_datetime(strict=False))
            else:
                df = df.with_columns(pl.col('Date').cast(pl.Datetime, strict=False))
            df = df.filter(pl.col('Date').is_not_null())

            # Clave numérica de la view: evita concatenar cuatro textos por fila.
            df = df.with_columns(
                pl.struct(
                    [
                        pl.col('SubscriberID').cast(pl.Utf8, strict=False),
                        pl.col('Title').cast(pl.Utf8, strict=False),
                        pl.col('SeriesTitle').cast(pl.Utf8, strict=False),
                        pl.col('deviceDescription').cast(pl.Utf8, strict=False),
                    ]
                )
                .hash(seed=0)
                .alias('_visual_key')
            )

            df = df.sort(['_visual_key', 'Date'])

            # Nueva view si cambia la llave o si pasó más de la ventana.
            id_changed = (pl.col('_visual_key') != pl.col('_visual_key').shift(1)).fill_null(True)
            elapsed = pl.col('Date').diff()
            is_new_visualization = id_changed | (
                elapsed > pl.duration(hours=hours_diff)
            ).fill_null(False)

            df = df.with_columns(
                is_new_visualization.cast(pl.Int64).cum_sum().alias('_visualization_session')
            )

            # Una fila por (llave, sesión): ignora eventos repetidos como
            # pause/resume dentro de la misma view.
            df_unique = df.unique(
                subset=['_visual_key', '_visualization_session'],
                keep='first',
            )

            df_unique = df_unique.with_columns(
                pl.concat_str(
                    [
                        pl.col('SubscriberID').cast(pl.Utf8, strict=False).fill_null('nan'),
                        pl.col('Title').cast(pl.Utf8, strict=False).fill_null('nan'),
                        pl.col('SeriesTitle').cast(pl.Utf8, strict=False).fill_null('Sin programa'),
                        pl.col('deviceDescription').cast(pl.Utf8, strict=False).fill_null('UnknownDevice'),
                    ],
                    separator='_',
                ).alias('id_unique_visual_by_user')
            ).drop(['_visual_key', '_visualization_session'])

            # Primer stream histórico de cada usuario.
            df_unique = df_unique.sort(['SubscriberID', 'Date'])
            df_unique = df_unique.with_columns(
                pl.col('SubscriberID').cum_count().over('SubscriberID').alias('_user_rank')
            )
            df_unique = df_unique.with_columns(
                pl.when(pl.col('_user_rank') == 1)
                .then(
                    pl.concat_str(
                        [
                            pl.col('SubscriberID').cast(pl.Utf8, strict=False).fill_null('nan'),
                            pl.lit('--1'),
                        ],
                        separator='',
                    )
                )
                .otherwise(None)
                .alias('UserUniqueID')
            ).drop('_user_rank')

        except Exception as e:
            print(f'Error al procesar la información: {e}')
            return empty

        return df_unique

    return (device_unique_per_user,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Total views por día y por señal

    La deduplicación se hace sobre **todo el periodo del reporte** y cada view
    se asigna al día de su primer evento. Así, la suma de las barras diarias
    coincide exactamente con el total de views del periodo (sin duplicar
    sesiones que cruzan medianoche).
    """)
    return


@app.cell
def _(DEVICE_UNIQUE_COLS, LIVE_GAP_HOURS, device_unique_per_user, pl):
    def compute_views_tables(
        events_df: pl.DataFrame,
        hours_diff: float = LIVE_GAP_HOURS,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Calcula (total views por día, total views por día y Title).

        - `events_df`: base cruda de eventos del periodo.
        - `hours_diff`: ventana de inactividad. 20/60 para LIVE.

        Devuelve:
          - ['Date', 'Total views']
          - ['Date', 'Title', 'Total views']
        """
        empty_total = pl.DataFrame(schema={'Date': pl.Date, 'Total views': pl.Int64})
        empty_title = pl.DataFrame(
            schema={'Date': pl.Date, 'Title': pl.String, 'Total views': pl.Int64}
        )
        if events_df is None or events_df.is_empty():
            return empty_total, empty_title

        missing = [c for c in DEVICE_UNIQUE_COLS if c not in events_df.columns]
        if missing:
            raise ValueError(f'La base no tiene las columnas requeridas: {missing}')

        df = events_df.select(DEVICE_UNIQUE_COLS)
        if df.schema['Date'] == pl.String:
            df = df.with_columns(pl.col('Date').str.to_datetime(strict=False))
        else:
            df = df.with_columns(pl.col('Date').cast(pl.Datetime, strict=False))
        df = df.filter(pl.col('Date').is_not_null())

        unique_views = device_unique_per_user(
            df,
            DEVICE_UNIQUE_COLS,
            hours_diff=hours_diff,
        )
        if unique_views.is_empty():
            return empty_total, empty_title

        # Cada view queda en el día de su primer evento.
        unique_views = unique_views.with_columns(
            pl.col('Date').dt.date().alias('Date')
        )

        total_views = (
            unique_views
            .group_by('Date')
            .agg(pl.len().cast(pl.Int64).alias('Total views'))
            .sort('Date')
        )

        total_views_title = (
            unique_views
            .group_by(['Date', 'Title'])
            .agg(pl.len().cast(pl.Int64).alias('Total views'))
            .sort(['Date', 'Title'])
        )

        return total_views, total_views_title

    return (compute_views_tables,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Ahora se verifica la información de views de usuarios.
    """)
    return


@app.cell
def _(compute_views_tables, df_events, display):
    df_views, df_views_title = compute_views_tables(df_events)

    print(f'Total views del periodo: {int(df_views["Total views"].sum()):,}')
    display(df_views)
    print('-' * 25)
    display(df_views_title.head(10))
    return df_views, df_views_title


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Reescalado de visualizaciones

    Se filtran las fechas del reporte; **solo las señales** se usan en el
    dataset con títulos y se calcula el porcentaje diario de cada señal.
    """)
    return


@app.cell
def _(
    DATE_REPORT_END,
    DATE_REPORT_INI,
    SIGNALS,
    df_views,
    df_views_title,
    display,
    pl,
):
    # Filtrar fechas para el reporte.
    df_views_filt = df_views.filter(
        pl.col('Date').is_between(DATE_REPORT_INI, DATE_REPORT_END)
    )
    df_views_title_filt = df_views_title.filter(
        pl.col('Date').is_between(DATE_REPORT_INI, DATE_REPORT_END)
    )

    # Solo señales en el dataset con títulos (luego se reescala con el total real).
    df_views_title_filt = df_views_title_filt.filter(pl.col('Title').is_in(SIGNALS))

    # Porcentaje por día: cada señal representa un % de las views de las señales
    # de ese día (suma 100% por día).
    df_views_title_filt = df_views_title_filt.with_columns(
        (pl.col('Total views') / pl.col('Total views').sum().over('Date') * 100).alias('Porcent')
    )

    display(df_views_filt)
    display(df_views_title_filt.head())
    return df_views_filt, df_views_title_filt


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Reescalación a valores reales repartidos por señales según su porcentaje.

    Se usa el **método del residuo mayor** para que la suma por día sea
    exactamente el total real de views del día.
    """)
    return


@app.cell
def _(df_views_filt, df_views_title_filt, display, pl):
    df_views_scaler = (
        df_views_title_filt
        .join(
            df_views_filt.select('Date', pl.col('Total views').alias('_total_real_dia')),
            on='Date',
            how='left',
        )
        .with_columns(
            (pl.col('Porcent') / 100 * pl.col('_total_real_dia').fill_null(0)).alias('_raw')
        )
        .with_columns(
            pl.col('_raw').floor().cast(pl.Int64).alias('_base')
        )
        .with_columns(
            (pl.col('_total_real_dia').fill_null(0) - pl.col('_base').sum().over('Date')).alias('_residuo')
        )
        .with_columns(
            (pl.col('_raw') - pl.col('_raw').floor()).alias('_frac')
        )
        .with_columns(
            pl.col('_frac').rank(method='ordinal', descending=True).over('Date').alias('_rank')
        )
        .with_columns(
            (pl.col('_base') + (pl.col('_rank') <= pl.col('_residuo')).cast(pl.Int64)).alias('Total views')
        )
        .select('Date', 'Title', 'Total views', 'Porcent')
        .sort(['Date', 'Title'])
    )

    df_views_scaler = df_views_scaler.drop(['Porcent'])
    df_views_scaler = df_views_scaler.rename({'Total views': 'Total views (escalado)'})

    display(df_views_scaler)
    return (df_views_scaler,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Conciliación diaria

    Comprobación de que el reescalado no pierde views: la suma de las señales
    de cada día debe ser igual al total real del día.
    """)
    return


@app.cell
def _(df_views_filt, df_views_scaler, display, pl):
    df_conciliacion = (
        df_views_scaler
        .group_by('Date')
        .agg(pl.col('Total views (escalado)').sum().alias('Suma escalada'))
        .join(
            df_views_filt.select('Date', pl.col('Total views').alias('Total real')),
            on='Date',
            how='full',
            coalesce=True,
        )
        .with_columns(
            pl.col('Suma escalada').fill_null(0),
            pl.col('Total real').fill_null(0),
        )
        .with_columns(
            (pl.col('Suma escalada') - pl.col('Total real')).alias('Diferencia')
        )
        .sort('Date')
    )

    print(f'Diferencia absoluta total: {int(df_conciliacion["Diferencia"].abs().sum()):,}')
    display(df_conciliacion)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Graficar toda la información
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    KPIs de views totales por señal (tarjetas que acompañan a cada gráfico de
    barras).
    """)
    return


@app.cell
def _(df_views_scaler, mo, pl):
    df_kpi_por_senal = (
        df_views_scaler
        .group_by('Title')
        .agg(
            pl.col('Total views (escalado)').sum().alias('Total vistas'),
            pl.col('Date').min().alias('Fecha min'),
            pl.col('Date').max().alias('Fecha max'),
        )
    )


    def kpi_card_senal(signal, total, fecha_min, fecha_max, color):
        """Tarjeta KPI con el total de views reescaladas de una señal."""
        if fecha_min is not None and fecha_max is not None:
            rango_fechas = (
                f'Vistas escaladas desde {fecha_min:%d/%m/%Y} hasta {fecha_max:%d/%m/%Y}'
            )
        else:
            rango_fechas = 'Sin vistas en el periodo del reporte'

        return mo.Html(
            f"""
            <div style="
                height: 100%;
                display: flex;
                flex-direction: column;
                justify-content: center;
                gap: 6px;
                padding: 20px 18px;
                border-radius: 14px;
                background: rgba(127, 127, 127, 0.08);
                border-left: 6px solid {color};
                box-shadow: 0 4px 14px rgba(0, 0, 0, 0.10);
            ">
                <div style="
                    font-size: 0.85rem;
                    font-weight: 700;
                    letter-spacing: 1.5px;
                    text-transform: uppercase;
                    color: {color};
                ">
                    {signal}
                </div>
                <div style="font-size: 2.2rem; font-weight: 800; line-height: 1.05; color: {color};">
                    {total:,}
                </div>
                <div style="
                    font-size: 0.7rem;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    opacity: 0.7;
                ">
                    Total views
                </div>
                <div style="font-size: 0.7rem; opacity: 0.7;">
                    {rango_fechas}
                </div>
            </div>
            """
        )

    return df_kpi_por_senal, kpi_card_senal


@app.cell
def _(DATE_REPORT_END, DATE_REPORT_INI, df_views_filt, df_views_scaler, mo):
    _total = int(df_views_filt['Total views'].sum())
    _dias = df_views_filt.height
    _senales = df_views_scaler['Title'].n_unique()

    mo.md(
        f'**Periodo:** {DATE_REPORT_INI:%d/%m/%Y} – {DATE_REPORT_END:%d/%m/%Y} · '
        f'**Total views:** {_total:,} · '
        f'**Días con datos:** {_dias} · '
        f'**Señales:** {_senales}'
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gráfico 1

    Views diarias por señal (barras) junto al total de views escaladas de la
    señal (tarjeta KPI).
    """)
    return


@app.cell
def _(
    DATE_REPORT_END,
    DATE_REPORT_INI,
    MESES_ABREV,
    SENAL_COLORS,
    SIGNALS,
    dt,
    pl,
    plt,
):
    def fmt_fecha_esp(fecha):
        """Formatea una fecha/datetime como '3 oct', '4 sep', '15 ene', etc."""
        if fecha is None:
            return ''
        if isinstance(fecha, str):
            fecha = dt.date.fromisoformat(fecha[:10])
        if isinstance(fecha, dt.datetime):
            fecha = fecha.date()
        return f'{fecha.day} {MESES_ABREV[fecha.month]}'


    def formatear_eje_fechas(ax, fechas):
        """Coloca las fechas del eje X con abreviaturas en español.

        Para periodos largos se muestran ~12 etiquetas (las barras se mantienen).
        """
        if not fechas:
            return
        n = len(fechas)
        if n <= 20:
            ticks = list(fechas)
        else:
            step = max(1, n // 12)
            ticks = list(fechas[::step])
            if ticks[-1] != fechas[-1]:
                ticks.append(fechas[-1])
        rotacion = 0 if n <= 12 else 45
        ax.set_xticks(ticks)
        ax.set_xticklabels(
            [fmt_fecha_esp(fecha) for fecha in ticks],
            rotation=rotacion,
            ha='center' if rotacion == 0 else 'right',
        )


    def serie_por_senal(
        df_scaler: pl.DataFrame,
        signal: str,
        date_ini=DATE_REPORT_INI,
        date_end=DATE_REPORT_END,
    ) -> pl.DataFrame:
        """Serie diaria completa de una señal (días sin views = 0)."""
        fechas = [
            date_ini + dt.timedelta(days=i)
            for i in range((date_end - date_ini).days + 1)
        ]
        base = pl.DataFrame({'Date': fechas})
        valores = (
            df_scaler
            .filter(pl.col('Title') == signal)
            .select('Date', pl.col('Total views (escalado)'))
        )
        return (
            base
            .join(valores, on='Date', how='left')
            .with_columns(
                pl.col('Total views (escalado)').fill_null(0).cast(pl.Int64)
            )
            .sort('Date')
        )


    def plot_views_senal(
        senal_df: pl.DataFrame,
        title: str,
        color: str = '#ff5500',
        figsize: tuple = (12, 4),
    ):
        """Barras diarias de views de una señal."""
        fechas = senal_df['Date'].to_list()
        valores = senal_df['Total views (escalado)'].to_list()

        fig, ax = plt.subplots(figsize=figsize)
        fig.patch.set_alpha(0)
        ax.patch.set_alpha(0)
        barras = ax.bar(
            fechas,
            valores,
            color=color,
            width=0.72,
            alpha=0.9,
            edgecolor='white',
            linewidth=0.6,
        )
        ax.set_title(title, color=color, fontweight='bold')
        ax.set_ylabel('Vistas')
        ax.grid(True, alpha=0.2, axis='y')
        ax.set_axisbelow(True)
        formatear_eje_fechas(ax, fechas)

        if len(fechas) <= 20:
            for barra, valor in zip(barras, valores):
                if valor:
                    ax.annotate(
                        f'{int(valor):,}',
                        (barra.get_x() + barra.get_width() / 2, barra.get_height()),
                        ha='center',
                        va='bottom',
                        fontsize=7,
                        color=color,
                        xytext=(0, 2),
                        textcoords='offset points',
                    )

        fig.tight_layout()
        for spine in ax.spines.values():
            spine.set_visible(False)
        return fig


    def plot_views_apiladas(
        df_scaler: pl.DataFrame,
        title: str = 'Vistas escaladas apiladas por señal',
        figsize: tuple = (13, 5),
        date_ini=DATE_REPORT_INI,
        date_end=DATE_REPORT_END,
        mostrar_totales: bool = True,
    ):
        """Barras apiladas: un día por barra y una señal por segmento.

        La altura total de cada barra es el total real de views del día.
        """
        fechas = [
            date_ini + dt.timedelta(days=i)
            for i in range((date_end - date_ini).days + 1)
        ]
        lookup = {
            (row['Date'], row['Title']): int(row['Total views (escalado)'] or 0)
            for row in df_scaler.iter_rows(named=True)
        }
        valores = [
            [lookup.get((fecha, signal), 0) for fecha in fechas]
            for signal in SIGNALS
        ]

        x = list(range(len(fechas)))
        bottoms = [0] * len(fechas)

        fig, ax = plt.subplots(figsize=figsize)
        fig.patch.set_alpha(0)
        ax.patch.set_alpha(0)

        for signal, vals in zip(SIGNALS, valores):
            ax.bar(
                x,
                vals,
                bottom=bottoms,
                label=signal,
                color=SENAL_COLORS[signal],
                width=0.72,
                edgecolor='white',
                linewidth=0.5,
            )
            bottoms = [bottom + valor for bottom, valor in zip(bottoms, vals)]

        if len(fechas) <= 20:
            tick_positions = x
        else:
            step = max(1, len(fechas) // 12)
            tick_positions = x[::step]
            if tick_positions[-1] != x[-1]:
                tick_positions.append(x[-1])
        rotacion = 0 if len(fechas) <= 12 else 45
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(
            [fmt_fecha_esp(fechas[i]) for i in tick_positions],
            rotation=rotacion,
            ha='center' if rotacion == 0 else 'right',
        )
        ax.set_title(title, fontweight='bold')
        ax.set_xlabel(
            f'Días desde {fmt_fecha_esp(date_ini)} hasta {fmt_fecha_esp(date_end)}',
            fontweight='bold',
        )
        ax.set_ylabel('Vistas')
        ax.grid(True, alpha=0.2, axis='y')
        ax.set_axisbelow(True)
        ax.legend(loc='upper right', frameon=False, ncol=2, fontsize=9)

        if mostrar_totales and len(fechas) <= 20:
            for xi, total in zip(x, bottoms):
                if total:
                    ax.text(
                        xi,
                        total,
                        f'{int(total):,}',
                        ha='center',
                        va='bottom',
                        fontsize=8,
                        fontweight='bold',
                    )

        fig.tight_layout()
        for spine in ax.spines.values():
            spine.set_visible(False)
        return fig

    return (
        formatear_eje_fechas,
        plot_views_apiladas,
        plot_views_senal,
        serie_por_senal,
    )


@app.cell(expand_output=True)
def _(
    SENAL_COLORS,
    SIGNALS,
    df_kpi_por_senal,
    df_views_scaler,
    kpi_card_senal,
    mo,
    pl,
    plot_views_senal,
    serie_por_senal,
):
    _filas_senales = []
    for _signal in SIGNALS:
        _senal_df = serie_por_senal(df_views_scaler, _signal)
        _color = SENAL_COLORS[_signal]
        _fila_kpi = df_kpi_por_senal.filter(pl.col('Title') == _signal)
        _total = int(_fila_kpi['Total vistas'][0]) if _fila_kpi.height else 0
        _fmin = _fila_kpi['Fecha min'][0] if _fila_kpi.height else None
        _fmax = _fila_kpi['Fecha max'][0] if _fila_kpi.height else None
        _filas_senales.append(
            mo.hstack(
                [
                    plot_views_senal(_senal_df, _signal, color=_color),
                    kpi_card_senal(_signal, _total, _fmin, _fmax, _color),
                ],
                gap=2,
                align='stretch',
                widths=[3, 1],
            )
        )

    mo.vstack(_filas_senales, gap=2.5)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gráfico 2

    Views del periodo segmentadas por señal: **barras apiladas** por día.
    La altura total de cada barra coincide con el total real de views del día.
    """)
    return


@app.cell
def _(df_views_scaler, plot_views_apiladas):
    plot_views_apiladas(df_views_scaler)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Gráfico 3

    Consolidado del mes: las views agrupadas por **mes calendario** (rótulo
    `MMM-YY`, por ejemplo `OCT-26`) en lugar de por día. Se imprimen por separado:

    - **Consolidado mensual apilado por señal**: una barra por mes con un segmento
      por señal. El número encima de la barra es el total de views del mes.
    - **Views por señal y mes**: una barra por mes para cada señal, para comparar
      señal contra señal.

    Las vistas de cada mes se deduplican **de forma independiente**, igual que al
    filtrar el panel mes a mes: una sesión que empieza el último día del mes y
    sigue en el mes siguiente cuenta como una view en cada mes. Con la serie
    diaria del reporte (Gráfico 2), esa sesión sólo cuenta en el día donde empezó,
    así que el consolidado mensual es levemente mayor. La diferencia real por mes
    se imprime en la tabla `df_ajuste_mes`.
    """)
    return


@app.cell
def _(
    DATE_REPORT_END,
    DATE_REPORT_INI,
    LIVE_GAP_HOURS,
    MESES_ABREV,
    SIGNALS,
    compute_views_tables,
    df_events,
    df_views_filt,
    df_views_scaler,
    dt,
    pl,
):
    def etiqueta_mes_esp(fecha):
        """Rótulo del mes en formato MMM-YY (ej. OCT-26)."""
        if isinstance(fecha, str):
            fecha = dt.date.fromisoformat(fecha[:10])
        if isinstance(fecha, dt.datetime):
            fecha = fecha.date()
        return f'{MESES_ABREV[fecha.month].upper()}-{fecha.year % 100:02d}'


    # Meses del consolidado: todos los que caen en el periodo del reporte (un mes
    # aparece aunque alguna señal no tenga vistas en él) más los de los datos.
    _meses_vistos = set()
    _anio, _mes = DATE_REPORT_INI.year, DATE_REPORT_INI.month
    while (_anio, _mes) <= (DATE_REPORT_END.year, DATE_REPORT_END.month):
        _meses_vistos.add(f'{_anio:04d}-{_mes:02d}')
        _mes += 1
        if _mes > 12:
            _mes, _anio = 1, _anio + 1
    _meses_vistos.update(
        f'{_fecha.year:04d}-{_fecha.month:02d}'
        for _fecha in df_views_scaler['Date'].to_list()
    )
    _claves_mes = sorted(_meses_vistos)

    _marco_mes = pl.DataFrame(
        {
            '_clave_mes': _claves_mes,
            'Mes': [
                etiqueta_mes_esp(dt.date(int(_clave[:4]), int(_clave[5:7]), 1))
                for _clave in _claves_mes
            ],
            'Orden mes': list(range(1, len(_claves_mes) + 1)),
        }
    )


    def compute_views_por_mes(
        events_df: pl.DataFrame,
        senales: list[str],
        marco: pl.DataFrame,
        hours_diff: float = LIVE_GAP_HOURS,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Consolidado mensual: deduplica cada mes por separado y reparte el total.

        El total de un mes se obtiene filtrando sólo los eventos de ese mes, como
        al consultar el panel mes a mes. Una sesión que cruza el límite entre meses
        cuenta entonces una view en cada mes; deduplicando todo el periodo junto,
        esa sesión contaría únicamente en el mes donde empezó (por eso el
        consolidado mensual es un poco mayor que la suma de la serie diaria).

        Dentro de cada mes se aplica la ventana de inactividad (`hours_diff`) y el
        total real del mes se reparte entre las señales con el método del residuo
        mayor, de modo que la suma de las señales sea exactamente ese total.

        Devuelve:
          - señales: ['Orden mes', '_clave_mes', 'Mes', 'Title', 'Views']
          - totales: ['Orden mes', '_clave_mes', 'Mes', 'Total señales',
                      'Total real del mes', 'Días con datos']
        """
        filas_senal = []
        filas_total = []

        for _marco_fila in marco.iter_rows(named=True):
            _clave = _marco_fila['_clave_mes']
            _ini = dt.datetime(int(_clave[:4]), int(_clave[5:7]), 1)
            _fin = (
                _ini.replace(year=_ini.year + 1, month=1)
                if _ini.month == 12
                else _ini.replace(month=_ini.month + 1)
            )

            # Sólo los eventos del mes: las sesiones se cortan en el límite.
            _ev_mes = events_df.filter(
                (pl.col('Date') >= _ini) & (pl.col('Date') < _fin)
            )
            _total_dia, _titulos_mes = compute_views_tables(
                _ev_mes, hours_diff=hours_diff
            )

            _real = int(_total_dia['Total views'].sum()) if _total_dia.height else 0
            _por_senal = (
                _titulos_mes
                .filter(pl.col('Title').is_in(senales))
                .group_by('Title')
                .agg(pl.col('Total views').sum().alias('Views'))
            )
            _mapa = {
                _fila['Title']: int(_fila['Views'])
                for _fila in _por_senal.iter_rows(named=True)
            }
            _crudos = [_mapa.get(_senal, 0) for _senal in senales]
            _suma_cruda = sum(_crudos)

            if _real and _suma_cruda:
                # Residuo mayor: el total real del mes se reparte entre señales.
                _exactos = [_real * _v / _suma_cruda for _v in _crudos]
                _base = [int(_e) for _e in _exactos]
                _sobrante = _real - sum(_base)
                _mayores = sorted(
                    range(len(_exactos)),
                    key=lambda _i: _exactos[_i] - _base[_i],
                    reverse=True,
                )
                for _k in range(_sobrante):
                    _base[_mayores[_k]] += 1
                _escalados = _base
            else:
                _escalados = [0] * len(senales)

            for _senal, _vistas in zip(senales, _escalados):
                filas_senal.append(
                    {
                        'Orden mes': _marco_fila['Orden mes'],
                        '_clave_mes': _clave,
                        'Mes': _marco_fila['Mes'],
                        'Title': _senal,
                        'Views': _vistas,
                    }
                )

            filas_total.append(
                {
                    'Orden mes': _marco_fila['Orden mes'],
                    '_clave_mes': _clave,
                    'Mes': _marco_fila['Mes'],
                    'Total señales': sum(_escalados),
                    'Total real del mes': _real,
                    'Días con datos': int(_total_dia.height),
                }
            )

        senales_mes = (
            pl.DataFrame(filas_senal)
            .with_columns(pl.col('Views').cast(pl.Int64))
            .sort(['Orden mes', 'Title'])
        )
        totales_mes = pl.DataFrame(filas_total).sort('Orden mes')
        return senales_mes, totales_mes


    # Las vistas mensuales no salen de sumar la serie diaria: se recalculan con
    # los eventos de cada mes para reproducir el total del panel mes a mes.
    df_views_mensual, _totales_brutos = compute_views_por_mes(
        df_events, SIGNALS, _marco_mes
    )

    df_totales_mes = (
        _totales_brutos
        .with_columns(
            pl.when(pl.col('Total real del mes') > 0)
            .then((pl.col('Total señales') / pl.col('Total real del mes') * 100).round(1))
            .otherwise(0.0)
            .alias('% señales sobre total')
        )
        .drop('_clave_mes', 'Orden mes')
    )

    # Matriz mes x señal: una fila por mes con las views de cada señal y el total.
    df_consolidado_mes = (
        df_views_mensual
        .pivot(
            on='Title',
            index='Mes',
            values='Views',
            aggregate_function='sum',
        )
        .join(_marco_mes.select('Mes', 'Orden mes'), on='Mes', how='left')
        .sort('Orden mes')
        .select(['Mes', *SIGNALS])
        .with_columns(
            pl.sum_horizontal([pl.col(_senal) for _senal in SIGNALS]).alias(
                'Total señales'
            )
        )
    )

    # Diagnóstico del ajuste: la serie diaria (Gráfico 2) asigna cada sesión a un
    # solo día; el consolidado deduplica mes a mes. La diferencia son las sesiones
    # que empiezan el último día del mes y continúan en el mes siguiente, por eso
    # el primer mes del periodo nunca difiere y los demás suman un poco más.
    df_ajuste_mes = (
        df_views_filt
        .with_columns(pl.col('Date').dt.strftime('%Y-%m').alias('_clave_mes'))
        .group_by('_clave_mes')
        .agg(pl.col('Total views').sum().alias('Total serie diaria'))
        .join(_marco_mes, on='_clave_mes', how='right')
        .join(
            _totales_brutos.select('_clave_mes', 'Total real del mes'),
            on='_clave_mes',
            how='left',
        )
        .with_columns(pl.col('Total serie diaria').fill_null(0))
        .with_columns(
            (pl.col('Total real del mes') - pl.col('Total serie diaria')).alias(
                'Cruces de mes'
            )
        )
        .sort('Orden mes')
        .select('Mes', 'Total serie diaria', 'Total real del mes', 'Cruces de mes')
    )
    return df_ajuste_mes, df_consolidado_mes, df_totales_mes, df_views_mensual


@app.cell
def _(SENAL_COLORS, SIGNALS, pl, plt):
    MESES_PALETA = (
        '#4c72b0',
        '#dd8452',
        '#55a868',
        '#c44e52',
        '#8172b2',
        '#937860',
        '#da8bc3',
        '#8c8c8c',
    )


    def _meses_ordenados(df_mensual: pl.DataFrame) -> list[str]:
        """Rótulos de mes (MMM-YY) en orden cronológico."""
        return (
            df_mensual.sort('Orden mes')
            .get_column('Mes')
            .unique(maintain_order=True)
            .to_list()
        )


    def _vista_mensual(df_mensual: pl.DataFrame) -> dict:
        """Diccionario (Mes, Title) -> views."""
        return {
            (fila['Mes'], fila['Title']): int(fila['Views'])
            for fila in df_mensual.iter_rows(named=True)
        }


    def plot_consolidado_mensual(
        df_mensual: pl.DataFrame,
        totales: pl.DataFrame | None = None,
        figsize: tuple = (10, 5),
    ):
        """Una barra por mes (MMM-YY) apilada por señal.

        La altura de cada barra es el total real de views del mes y el número
        encima de la barra lo etiqueta; las vistas por señal se ven en la tabla.
        """
        senales = list(SIGNALS)
        meses = _meses_ordenados(df_mensual)
        lookup = _vista_mensual(df_mensual)
        dias_por_mes = {}
        if totales is not None and 'Días con datos' in totales.columns:
            dias_por_mes = {
                fila['Mes']: int(fila['Días con datos'])
                for fila in totales.iter_rows(named=True)
            }

        fig, ax = plt.subplots(figsize=figsize)
        fig.patch.set_alpha(0)
        ax.patch.set_alpha(0)

        x = list(range(len(meses)))
        bottoms = [0] * len(meses)
        for signal in senales:
            valores = [lookup.get((mes, signal), 0) for mes in meses]
            ax.bar(
                x,
                valores,
                bottom=bottoms,
                label=signal,
                color=SENAL_COLORS[signal],
                width=0.55,
                edgecolor='white',
                linewidth=0.6,
            )
            bottoms = [b + v for b, v in zip(bottoms, valores)]

        # Única etiqueta por barra: el total del mes (sin totales por segmento).
        for xi, total in zip(x, bottoms):
            if total:
                ax.text(
                    xi,
                    total,
                    f'{total:,}',
                    ha='center',
                    va='bottom',
                    fontsize=11,
                    fontweight='bold',
                )

        techo = max(bottoms) if bottoms else 0
        ax.set_ylim(0, techo * 1.5 if techo else 1)
        ax.set_xticks(x)
        ax.set_xticklabels(
            [
                f'{mes}\n{dias_por_mes[mes]} días' if mes in dias_por_mes else mes
                for mes in meses
            ],
            fontweight='bold',
        )
        ax.set_title('Consolidado mensual apilado por señal', fontweight='bold')
        ax.set_ylabel('Vistas')
        ax.grid(True, alpha=0.2, axis='y')
        ax.set_axisbelow(True)
        ax.legend(
            loc='upper right',
            frameon=False,
            ncol=2,
            fontsize=8,
            title='Señal',
            title_fontsize=8,
        )

        fig.tight_layout()
        for spine in ax.spines.values():
            spine.set_visible(False)
        return fig


    def plot_views_senal_por_mes(
        df_mensual: pl.DataFrame,
        figsize: tuple = (12.5, 5),
    ):
        """Una barra por mes para cada señal, sin etiquetas de valores."""
        senales = list(SIGNALS)
        meses = _meses_ordenados(df_mensual)
        lookup = _vista_mensual(df_mensual)

        fig, ax = plt.subplots(figsize=figsize)
        fig.patch.set_alpha(0)
        ax.patch.set_alpha(0)

        n_meses = max(len(meses), 1)
        ancho_barra = 0.82 / n_meses
        posiciones = list(range(len(senales)))
        techo = 0
        for i, mes in enumerate(meses):
            color = MESES_PALETA[i % len(MESES_PALETA)]
            valores = [lookup.get((mes, signal), 0) for signal in senales]
            techo = max([techo, *valores])
            desplaz = (i - (n_meses - 1) / 2) * ancho_barra
            ax.bar(
                [p + desplaz for p in posiciones],
                valores,
                width=ancho_barra * 0.86,
                label=mes,
                color=color,
                edgecolor='white',
                linewidth=0.6,
            )

        ax.set_ylim(0, techo * 1.35 if techo else 1)
        ax.set_title('Views por señal y mes', fontweight='bold')
        ax.set_ylabel('Vistas')
        ax.set_xticks(posiciones)
        ax.set_xticklabels(senales, rotation=20, ha='right', fontsize=9)
        ax.grid(True, alpha=0.2, axis='y')
        ax.set_axisbelow(True)
        ax.legend(
            loc='upper right',
            frameon=False,
            ncol=min(len(meses), 4),
            fontsize=8.5,
            title='Mes',
            title_fontsize=8.5,
        )

        fig.tight_layout()
        for spine in ax.spines.values():
            spine.set_visible(False)
        return fig

    return plot_consolidado_mensual, plot_views_senal_por_mes


@app.cell
def _(df_totales_mes, df_views_mensual, plot_consolidado_mensual):
    plot_consolidado_mensual(df_views_mensual, df_totales_mes)
    return


@app.cell
def _(df_views_mensual, plot_views_senal_por_mes):
    plot_views_senal_por_mes(df_views_mensual)
    return


@app.cell
def _(df_consolidado_mes):
    df_consolidado_mes
    return


@app.cell
def _(df_totales_mes):
    df_totales_mes
    return


@app.cell
def _(mo):
    mo.md(r"""
    #### Conciliación mensual

    La serie diaria del reporte deduplica todo el periodo de una sola vez y asigna
    cada sesión al día donde empezó; el consolidado mensual deduplica mes a mes,
    como el panel al filtrar por mes. La columna `Cruces de mes` son las sesiones
    que empiezan el último día del mes y siguen en el siguiente (se cuentan en
    ambos meses), y explica el pequeño sobreconteo del consolidado. El primer mes
    del periodo nunca tiene diferencia.
    """)
    return


@app.cell
def _(df_ajuste_mes):
    df_ajuste_mes
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Descarga de gráficas

    Cuatro botones para bajar un PNG (200 dpi, fondo blanco) de cada gráfico del
    reporte:

    - **Comparación señales (views)**: el Gráfico 1, con las barras diarias de
      cada señal junto a su tarjeta KPI de views escaladas.
    - **Vistas apiladas**: el Gráfico 2, con el total diario segmentado por
      señal.
    - **Consolidado mensual**: el primer gráfico del Gráfico 3, con las views
      agregadas por mes (`MMM-YY`) apiladas por señal.
    - **Señal por mes**: el segundo gráfico del Gráfico 3, con la comparación de
      señales mes a mes.

    Los PNG se reconstruyen con los mismos datos del notebook, así que se
    actualizan al re-ejecutarlo.
    """)
    return


@app.cell
def _(
    SENAL_COLORS,
    SIGNALS,
    df_kpi_por_senal,
    df_views_scaler,
    formatear_eje_fechas,
    pl,
    plt,
    serie_por_senal,
):
    import io
    from matplotlib.patches import FancyBboxPatch, Rectangle
    from matplotlib.colors import to_rgba

    # Series diarias por señal con su color (los mismos de los gráficos).
    SENALES_SERIES = [
        (_signal, serie_por_senal(df_views_scaler, _signal), SENAL_COLORS[_signal])
        for _signal in SIGNALS
    ]


    def render_senal_on_ax(ax, senal_df, title, color):
        """Dibuja las barras diarias de una señal con el estilo del Gráfico 1."""
        fechas = senal_df['Date'].to_list()
        valores = senal_df['Total views (escalado)'].to_list()
        ax.patch.set_alpha(0)
        barras = ax.bar(
            fechas,
            valores,
            color=color,
            width=0.72,
            alpha=0.9,
            edgecolor='white',
            linewidth=0.6,
        )
        ax.set_title(title, color=color, fontweight='bold')
        ax.set_ylabel('Vistas')
        ax.grid(True, alpha=0.2, axis='y')
        ax.set_axisbelow(True)
        formatear_eje_fechas(ax, fechas)

        if len(fechas) <= 20:
            for barra, valor in zip(barras, valores):
                if valor:
                    ax.annotate(
                        f'{int(valor):,}',
                        (barra.get_x() + barra.get_width() / 2, barra.get_height()),
                        ha='center',
                        va='bottom',
                        fontsize=7,
                        color=color,
                        xytext=(0, 2),
                        textcoords='offset points',
                    )

        for spine in ax.spines.values():
            spine.set_visible(False)


    def render_kpi_on_ax(ax, signal, total, fecha_min, fecha_max, color):
        """Dibuja la tarjeta KPI (equivalente a `kpi_card_senal`) sobre un axes."""
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')

        ax.add_patch(FancyBboxPatch(
            (0.03, 0.04), 0.94, 0.92,
            boxstyle='round,pad=0.004,rounding_size=0.05',
            facecolor='0.55', alpha=0.15, edgecolor='none', clip_on=False,
        ))
        ax.add_patch(Rectangle(
            (0.03, 0.04), 0.045, 0.92,
            facecolor=color, edgecolor='none', clip_on=False,
        ))

        if fecha_min is not None and fecha_max is not None:
            rango = f'Vistas escaladas\ndesde {fecha_min:%d/%m/%Y}\nhasta {fecha_max:%d/%m/%Y}'
        else:
            rango = 'Sin vistas en el\nperiodo del reporte'

        ax.text(0.13, 0.82, signal.upper(), fontsize=9, fontweight='bold', color=color, va='top')
        ax.text(0.13, 0.63, f'{total:,}', fontsize=20, fontweight='bold', color=color, va='top')
        ax.text(0.13, 0.42, 'TOTAL VIEWS', fontsize=7.5, color='#666666', va='top')
        ax.text(0.13, 0.30, rango, fontsize=7.5, color='#666666', va='top', linespacing=1.4)


    def build_figura_comparacion():
        """Reconstruye el Gráfico 1 (señales + KPI) en una sola figura."""
        filas = len(SENALES_SERIES)
        fig, axes = plt.subplots(
            filas, 2,
            figsize=(15, 3.4 * filas),
            gridspec_kw={'width_ratios': [3, 1], 'hspace': 0.45, 'wspace': 0.05},
        )
        if filas == 1:
            axes = axes.reshape(1, -1)

        for i, (signal, senal_df, color) in enumerate(SENALES_SERIES):
            fila_kpi = df_kpi_por_senal.filter(pl.col('Title') == signal)
            total = int(fila_kpi['Total vistas'][0]) if fila_kpi.height else 0
            fmin = fila_kpi['Fecha min'][0] if fila_kpi.height else None
            fmax = fila_kpi['Fecha max'][0] if fila_kpi.height else None
            render_senal_on_ax(axes[i, 0], senal_df, signal, color)
            render_kpi_on_ax(axes[i, 1], signal, total, fmin, fmax, color)

        # Sin autofmt_xdate: las etiquetas ya están formateadas en español y
        # rotarlas cortaría la primera fecha. Se ajustan márgenes a mano porque
        # los axes de las tarjetas KPI no son compatibles con tight_layout.
        fig.subplots_adjust(left=0.075, right=0.97, top=0.97, bottom=0.04)
        return fig


    # --- Exportación a PNG (tema claro) -----------------------------------
    # marimo, en modo oscuro, deja matplotlib con texto, ejes, ticks y rejilla en
    # blanco. Las figuras del notebook se dibujan con fondo transparente, así que
    # en pantalla se ven bien; al volcarlas a un PNG de fondo blanco los textos
    # blancos desaparecerían. Por eso la exportación usa siempre este tema claro.
    EXPORT_TEXTO = '#1a1a1a'
    EXPORT_RC = {
        'figure.facecolor': 'white',
        'savefig.facecolor': 'white',
        'axes.facecolor': 'white',
        'text.color': EXPORT_TEXTO,
        'axes.labelcolor': EXPORT_TEXTO,
        'axes.titlecolor': EXPORT_TEXTO,
        'xtick.color': EXPORT_TEXTO,
        'ytick.color': EXPORT_TEXTO,
        'axes.edgecolor': EXPORT_TEXTO,
        'grid.color': EXPORT_TEXTO,
        'legend.labelcolor': EXPORT_TEXTO,
        'legend.edgecolor': EXPORT_TEXTO,
    }


    def _es_color_claro(color):
        """True si el color es casi blanco (texto heredado del tema oscuro)."""
        try:
            r, g, b, _ = to_rgba(color)
        except (ValueError, TypeError):
            return False
        return (r + g + b) / 3 >= 0.85


    def tema_claro_figura(fig):
        """Fondo blanco y textos oscuros en una figura ya construida."""
        fig.patch.set_facecolor('white')
        fig.patch.set_alpha(1)
        for ax in fig.get_axes():
            ax.set_facecolor('white')
            ax.patch.set_alpha(0)
            if _es_color_claro(ax.title.get_color()):
                ax.title.set_color(EXPORT_TEXTO)
            for eje in (ax.xaxis, ax.yaxis):
                if _es_color_claro(eje.label.get_color()):
                    eje.label.set_color(EXPORT_TEXTO)
                for tick in eje.get_major_ticks() + eje.get_minor_ticks():
                    tick.label1.set_color(EXPORT_TEXTO)
                    tick.label2.set_color(EXPORT_TEXTO)
            leg = ax.get_legend()
            if leg is not None:
                for txt in leg.get_texts():
                    if _es_color_claro(txt.get_color()):
                        txt.set_color(EXPORT_TEXTO)
                marco = leg.get_frame()
                marco.set_edgecolor(EXPORT_TEXTO)
                if _es_color_claro(marco.get_facecolor()):
                    marco.set_facecolor('white')
        return fig


    def figura_a_png_bytes(fig_o_builder, dpi=200):
        """Serializa a PNG una figura o una función que la construye."""
        with plt.rc_context(EXPORT_RC):
            fig = fig_o_builder() if callable(fig_o_builder) else fig_o_builder
            fig = tema_claro_figura(fig)
            buffer = io.BytesIO()
            fig.savefig(
                buffer,
                format='png',
                dpi=dpi,
                facecolor='white',
                bbox_inches='tight',
            )
            plt.close(fig)
        return buffer.getvalue()

    return build_figura_comparacion, figura_a_png_bytes


@app.cell
def _(
    DATE_REPORT_END,
    DATE_REPORT_INI,
    build_figura_comparacion,
    df_totales_mes,
    df_views_mensual,
    df_views_scaler,
    figura_a_png_bytes,
    mo,
    plot_consolidado_mensual,
    plot_views_apiladas,
    plot_views_senal_por_mes,
):
    _periodo = mo.md(
        f'PNG del reporte: **{DATE_REPORT_INI:%d/%m/%Y} – {DATE_REPORT_END:%d/%m/%Y}**'
    )
    _sufijo = f'{DATE_REPORT_INI:%Y%m%d}-{DATE_REPORT_END:%Y%m%d}'

    # Comparación señales: mismo render que el Gráfico 1.
    _img_comparacion = figura_a_png_bytes(build_figura_comparacion)

    # Vistas apiladas: mismo render que el Gráfico 2.
    _img_apilada = figura_a_png_bytes(
        lambda: plot_views_apiladas(df_views_scaler)
    )

    # Consolidado mensual apilado: mismo render que el Gráfico 3 (primero).
    _img_consolidado = figura_a_png_bytes(
        lambda: plot_consolidado_mensual(df_views_mensual, df_totales_mes)
    )

    # Views por señal y mes: mismo render que el Gráfico 3 (segundo).
    _img_senal_mes = figura_a_png_bytes(
        lambda: plot_views_senal_por_mes(df_views_mensual)
    )

    btn_comparacion = mo.download(
        _img_comparacion,
        filename=f'comparacion_senales_views_{_sufijo}.png',
        mimetype='image/png',
        label='Comparación señales (views)',
    )

    btn_apilada = mo.download(
        _img_apilada,
        filename=f'vistas_apiladas_{_sufijo}.png',
        mimetype='image/png',
        label='Vistas apiladas',
    )

    btn_consolidado = mo.download(
        _img_consolidado,
        filename=f'consolidado_mensual_{_sufijo}.png',
        mimetype='image/png',
        label='Consolidado mensual',
    )

    btn_senal_mes = mo.download(
        _img_senal_mes,
        filename=f'views_senal_por_mes_{_sufijo}.png',
        mimetype='image/png',
        label='Señal por mes',
    )

    mo.vstack([
        _periodo,
        mo.hstack([btn_comparacion, btn_apilada, btn_consolidado], gap=1),
        mo.hstack([btn_senal_mes], gap=1),
    ])
    return


if __name__ == "__main__":
    app.run()
