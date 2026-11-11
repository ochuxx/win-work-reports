import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo # Relevante para usar .py como .ipynb
    import polars as pl
    import datetime as dt
    import matplotlib.pyplot as plt
    from IPython.display import display

    return display, dt, mo, pl, plt


@app.cell
def _(dt):
    # Columnas que definen una visualización única.
    # DeviceType no forma parte del id de deduplicación y no se usa en el reporte.
    DEVICE_UNIQUE_COLS = [
      'Date',
      'SubscriberID',
      'Title',
      'SeriesTitle',
      'deviceDescription',
      'TYPE_NATION',
    ]

    # Tipos de nación y colores usados en las gráficas.
    NATIONS = ['NACIONAL', 'INTERNACIONAL']
    NATION_COLORS = {
      'NACIONAL': '#ff5500',
      'INTERNACIONAL': '#0ba4e5',
    }

    # Constantes -> Cambios manuales
    # Afecta también a la concurrencia de reproducción
    DATE_REPORT_INI = dt.date(2026, 10, 1)
    DATE_REPORT_END = dt.date(2026, 10, 31)
    return (
        DATE_REPORT_END,
        DATE_REPORT_INI,
        DEVICE_UNIQUE_COLS,
        NATIONS,
        NATION_COLORS,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Importar datos
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Se leen únicamente las columnas necesarias, se filtra el periodo al vuelo y
    se crea el campo **TYPE_NATION** a partir de `CountryCode`:

    - `CO` → **NACIONAL**
    - cualquier otro valor → **INTERNACIONAL**

    No se retienen los dos parquet crudos ni columnas que el reporte no usa.
    """)
    return


@app.cell
def _(DATE_REPORT_END, DATE_REPORT_INI, pl):
    # Columnas mínimas para views y concurrencia.
    _COLS = [
      'Date',
      'CountryCode',
      'SubscriberID',
      'Title',
      'SeriesTitle',
      'deviceDescription',
    ]

    df = (
      pl.concat(
        [
          pl.scan_parquet('./win-work-reports/streaming_data/LIVE_2026-09.parquet'),
          pl.scan_parquet('./win-work-reports/streaming_data/LIVE_2026-10.parquet'),
        ],
        how='vertical',
      )
      .select(_COLS)
      .with_columns(
        # Date se normaliza una sola vez para todo el reporte.
        pl.col('Date')
        .str.to_datetime(strict=False)
        .dt.replace_time_zone('UTC'),
        pl.when(
          pl.col('CountryCode')
          .cast(pl.Utf8, strict=False)
          .str.strip_chars()
          .str.to_uppercase()
          == 'CO'
        )
        .then(pl.lit('NACIONAL'))
        .otherwise(pl.lit('INTERNACIONAL'))
        .alias('TYPE_NATION'),
      )
      .with_columns(pl.col('Date').dt.date().alias('_day'))
      .filter(
        pl.col('Date').is_not_null()
        & pl.col('_day').is_between(DATE_REPORT_INI, DATE_REPORT_END)
      )
      .drop('CountryCode')
      .collect(engine='streaming')
    )
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Verificación
    """)
    return


@app.cell
def _(df, display, pl):
    display(df.head())
    print('-'*25)
    display(
        df.group_by('TYPE_NATION')
        .agg(pl.len().alias('Registros'))
        .sort('TYPE_NATION')
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Normalizar información (Vistas por TYPE_NATION)

    Se normaliza la info para guardar correctamente el conteo de views y la concurrencia de reproducción.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Función para reconocer usuarios
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Inicialmente las funciones para extraer la información única por usuario.
    """)
    return


@app.cell
def _(pl):
    def _empty_pl_df(columns: list[str]) -> pl.DataFrame:
      return pl.DataFrame({col: [] for col in columns})


    def device_unique_per_user(df: pl.DataFrame, cols: list[str], hours_diff: int = 24) -> pl.DataFrame:
      expected_base_cols = [c for c in cols if isinstance(c, str) and c.strip()]
      if df is None or df.is_empty():
        return _empty_pl_df(expected_base_cols + ['id_unique_visual_by_user', 'UserUniqueID'])

      try:
        missing_cols = [c for c in expected_base_cols if c not in df.columns]
        if missing_cols:
          print(f"Error al procesar la información: faltan columnas requeridas: {missing_cols}")
          return _empty_pl_df(expected_base_cols + ['id_unique_visual_by_user', 'UserUniqueID'])

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

        # ===== START DEPURING UNIQUES DEVICES =====
        df = df.with_columns(
          pl.concat_str(
            [
              pl.col('SubscriberID').cast(pl.Utf8, strict=False).fill_null('nan'),
              pl.col('Title').cast(pl.Utf8, strict=False).fill_null('nan'),
              pl.col('SeriesTitle').cast(pl.Utf8, strict=False).fill_null('Sin programa'),
              pl.col('deviceDescription').cast(pl.Utf8, strict=False).fill_null('UnknownDevice'),
            ],
            separator='_',
          ).alias('id_unique_visual_by_user')
        )

        # Normaliza fecha para calcular ventanas de 24 horas por visualizacion.
        # La base del reporte ya llega como Datetime UTC, así que no se vuelve a castear.
        dtype = df.schema['Date']
        if dtype == pl.String:
          df = df.with_columns(
            pl.col('Date').str.to_datetime(strict=False)
          )
        elif not isinstance(dtype, pl.Datetime):
          df = df.with_columns(
            pl.col('Date').cast(pl.Datetime, strict=False)
          )
        df = df.filter(pl.col('Date').is_not_null())
        df = df.sort(['id_unique_visual_by_user', 'Date'])

        # Si el mismo id_unique_visual_by_user reaparece despues de 24h,
        # se considera una nueva visualizacion.
        time_diff = pl.col('Date').diff().over('id_unique_visual_by_user')
        is_new_visualization = time_diff.is_null() | (time_diff > pl.duration(hours=hours_diff))

        df = df.with_columns(
          is_new_visualization.alias('is_new_visualization')
        )
        df = df.with_columns(
          pl.col('is_new_visualization')
          .cast(pl.Int64)
          .cum_sum()
          .over('id_unique_visual_by_user')
          .alias('visualization_session')
        )
        df = df.with_columns(
          pl.concat_str(
            [
              pl.col('id_unique_visual_by_user'),
              pl.col('visualization_session').cast(pl.Utf8, strict=False),
            ],
            separator='_',
          ).alias('visualization_id_24h')
        )

        # Registros unicos por visualizacion (ignora eventos repetidos como pause/resume).
        df_unique = df.unique(subset=['visualization_id_24h'], keep='first')
        df_unique = df_unique.drop(['is_new_visualization', 'visualization_session', 'visualization_id_24h'])

        # ===== START IDENTIFYING HISTORICAL FIRST STREAM =====
        # 1. Ordenar por usuario y fecha para garantizar el orden cronológico
        df_unique = df_unique.sort(['SubscriberID', 'Date'])

        # 2. Identificar la primera aparición de cada usuario
        df_unique = df_unique.with_columns(
          pl.col('SubscriberID')
          .cum_count()
          .over('SubscriberID')
          .alias('_user_rank')
        )

        # 3. Inicializar la columna como nula y asignar '--1' SOLO a los primeros streams
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
        # ===== END IDENTIFYING HISTORICAL FIRST STREAM =====

        # ===== END DEPURING UNIQUES DEVICES =====

      except Exception as e:
        print(f"Error al procesar la información: {e}")
        return _empty_pl_df(expected_base_cols + ['id_unique_visual_by_user', 'UserUniqueID'])  # Retorna estructura esperada

      return df_unique

    return (device_unique_per_user,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Función para views per user (por día y TYPE_NATION)
    """)
    return


@app.cell
def _(DEVICE_UNIQUE_COLS, device_unique_per_user, pl):
    def total_views_per_day_nation(
        events_df: pl.DataFrame | pl.LazyFrame,
        hours_diff: float = 24,
    ) -> pl.DataFrame:
        """Total views por día y TYPE_NATION, con el mismo criterio que el KPI
        del panel (filtro de fechas puesto día a día sobre la base de referencia).

        La visualización se deduplica por día y luego se clasifica con el
        TYPE_NATION de su primer evento; no se aplica ningún reescalado.

        - events_df: base cruda de eventos (1 fila = 1 evento) con las columnas
          de DEVICE_UNIQUE_COLS. Acepta DataFrame o LazyFrame.
        - hours_diff: ventana de sesión. 24 para VOD, 20/60 (20 min) para LIVE.

        Devuelve ['Date', 'TYPE_NATION', 'Total views']: un registro por día y
        tipo de nación, ordenado por fecha y TYPE_NATION.
        """
        if isinstance(events_df, pl.LazyFrame):
            events_df = events_df.collect(engine='streaming')
        empty = pl.DataFrame({'Date': [], 'TYPE_NATION': [], 'Total views': []})
        if events_df is None or events_df.is_empty():
            return empty

        missing = [c for c in DEVICE_UNIQUE_COLS if c not in events_df.columns]
        if missing:
            raise ValueError(f'La base no tiene las columnas requeridas: {missing}')

        # La base del reporte ya trae Date como Datetime UTC y la columna _day.
        # Si se pasa otra base, se prepara aquí sin duplicar el frame completo.
        if '_day' in events_df.columns:
            df = events_df
        else:
            df = events_df.select(DEVICE_UNIQUE_COLS)
            dtype = df.schema['Date']
            if dtype == pl.String:
                df = df.with_columns(pl.col('Date').str.to_datetime(strict=False))
            elif not isinstance(dtype, pl.Datetime):
                df = df.with_columns(pl.col('Date').cast(pl.Datetime, strict=False))
            df = df.filter(pl.col('Date').is_not_null())
            df = df.with_columns(pl.col('Date').dt.date().alias('_day'))

        rows = []
        # Un día a la vez: evita materializar todas las particiones a la vez.
        for day in df.get_column('_day').unique().sort().to_list():
            day_df = df.filter(pl.col('_day') == day)
            df_unique = device_unique_per_user(day_df, DEVICE_UNIQUE_COLS, hours_diff=hours_diff)
            if df_unique.is_empty():
                continue
            counts = (
                df_unique
                .group_by('TYPE_NATION')
                .agg(pl.len().cast(pl.Int64).alias('Total views'))
                .with_columns(pl.lit(day).alias('Date'))
                .select(['Date', 'TYPE_NATION', 'Total views'])
            )
            rows.append(counts)

        if not rows:
            return empty
        return pl.concat(rows).sort(['Date', 'TYPE_NATION'])

    return (total_views_per_day_nation,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Ahora verificar información de views de usuarios.
    """)
    return


@app.cell
def _(df, display, pl, total_views_per_day_nation):
    df_views_nation = total_views_per_day_nation(events_df=df, hours_diff=20 /60)
    display(df_views_nation.head())
    print('-'*25)
    display(
        df_views_nation
        .group_by('TYPE_NATION')
        .agg(pl.col('Total views').sum().alias('Total vistas'))
        .sort('TYPE_NATION')
    )
    return (df_views_nation,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Views por TYPE_NATION (sin reescalado)

    La base ya viene filtrada por el periodo del reporte. A diferencia de
    `dsports.py`, aquí no se reparten las views: cada visualización se cuenta
    directamente en el TYPE_NATION de su primer evento.
    """)
    return


@app.cell
def _(df_views_nation, display):
    display(df_views_nation)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Normalizar información (Concurrencia de reproducción)

    Se normaliza la info para guardar correctamente el conteo de views y la concurrencia de reproducción.
    """)
    return


@app.cell
def _():
    WINDOW_MINUTES = 25
    return (WINDOW_MINUTES,)


@app.cell
def _(plt):
    def plot_concurrencia_serie(serie_df, title, color='#ff5500', figsize=(12, 4)):
        """Dibuja una serie [ts, Concurrencia] con el estilo del reporte."""
        ts = serie_df['ts'].to_list()
        valores = serie_df['Concurrencia'].to_list()
        fig, ax = plt.subplots(figsize=figsize)
        fig.patch.set_alpha(0)
        ax.patch.set_alpha(0)
        ax.plot(ts, valores, color=color, linewidth=1.5)
        ax.fill_between(ts, valores, 0, color=color, alpha=0.2)
        ax.set_title(title, color=color, fontweight='bold')
        ax.set_ylabel(f'Reproducciones')
        ax.grid(True, alpha=0.2, axis='y')
        fig.autofmt_xdate()
        fig.tight_layout()
        for spine in ax.spines.values():
            spine.set_visible(False)
        return fig

    return (plot_concurrencia_serie,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Se calculan los pares únicos (minuto, stream) una sola vez y se reutilizan
    para la concurrencia general y para cada TYPE_NATION. No hay reescalado:
    cada serie se calcula sobre los eventos de su tipo de nación.
    """)
    return


@app.cell
def _(WINDOW_MINUTES, df, dt, pl):
    def _freq_to_seconds(freq: str) -> int:
      digits = ''.join(c for c in freq if c.isdigit()) or '1'
      unit = freq[-1].lower() if freq and freq[-1].isalpha() else 'm'
      return int(digits) * {'m': 60, 't': 60, 'h': 3600, 'd': 86400, 'w': 604800}.get(unit, 60)


    def _prepare_pairs(events_df: pl.DataFrame) -> pl.DataFrame:
      """Pares únicos (minuto, stream, nación).

      El stream se representa con un hash de 64 bits de
      (SubscriberID, Title, deviceDescription), más liviano que un struct para
      los `n_unique` de la ventana rodante. La nación se guarda como booleano
      (1 byte) en lugar de la cadena completa.
      """
      return (
        events_df
        .select(
          pl.col('Date').dt.truncate('1m').alias('_minute'),
          pl.struct(['SubscriberID', 'Title', 'deviceDescription']).hash().alias('_stream'),
          (pl.col('TYPE_NATION') == 'NACIONAL').alias('_is_nacional'),
        )
        .unique()
      )


    def _serie_concurrencia(
      pairs: pl.DataFrame,
      freq: str = '5T',
      window_minutes: int = WINDOW_MINUTES,
    ) -> pl.DataFrame:
      """Serie [ts, Concurrencia] a partir de pares ya únicos."""
      if pairs.is_empty():
        return pl.DataFrame({'ts': [], 'Concurrencia': []})

      start_utc = pairs.select(pl.col('_minute').min()).item()
      end_utc = pairs.select(pl.col('_minute').max()).item()

      active_per_minute = (
        pairs
        .with_columns(
          (pl.col('_minute') + pl.duration(minutes=window_minutes - 1)).alias('_window_time')
        )
        .sort('_window_time')
        .group_by_dynamic(
          index_column='_window_time',
          every='1m',
          period=f'{window_minutes}m',
          offset=f'-{window_minutes - 1}m',
          closed='left',
          label='left',
        )
        .agg(pl.col('_stream').n_unique().cast(pl.Int64).alias('active_streams'))
        .rename({'_window_time': 'ts'})
        .filter((pl.col('ts') >= pl.lit(start_utc)) & (pl.col('ts') <= pl.lit(end_utc)))
        .sort('ts')
      )

      # Agregar al bucket de salida (e.g. 5T o H) tomando el max dentro de cada
      # bucket para preservar los picos reales.
      bucket_seconds = _freq_to_seconds(freq)
      if bucket_seconds >= 60:
        bucket_minutes = max(1, bucket_seconds // 60)
        bucket_label = (
          f'{bucket_minutes // 60}h'
          if bucket_minutes >= 60 and bucket_minutes % 60 == 0
          else f'{bucket_minutes}m'
        )
        active_per_minute = (
          active_per_minute
          .with_columns(pl.col('ts').dt.truncate(bucket_label).alias('_bucket'))
          .group_by('_bucket')
          .agg(pl.col('active_streams').max().alias('active_streams'))
          .rename({'_bucket': 'ts'})
          .sort('ts')
        )

      return (
        active_per_minute
        .rename({'active_streams': 'Concurrencia'})
        .select('ts', 'Concurrencia')
      )


    def _series_concurrencia(
      pairs: pl.DataFrame,
      freq: str = '5T',
      window_minutes: int = WINDOW_MINUTES,
    ):
      """Series (general, NACIONAL, INTERNACIONAL) calculadas día a día.

      Se procesa un día a la vez con 24 minutos de solape para que la ventana
      rodante vea los eventos del día anterior. Así `group_by_dynamic` nunca
      expande los ~19M de pares completos y el pico de memoria se mantiene bajo.
      """
      empty = pl.DataFrame({'ts': [], 'Concurrencia': []})
      if pairs.is_empty():
        return empty, empty, empty

      days = (
        pairs
        .select(pl.col('_minute').dt.date().alias('_d'))
        .get_column('_d')
        .unique()
        .sort()
        .to_list()
      )
      general_frames, nacional_frames, internacional_frames = [], [], []
      overlap = dt.timedelta(minutes=window_minutes - 1)

      for day in days:
        day_start = dt.datetime.combine(day, dt.time.min, tzinfo=dt.timezone.utc)
        day_end = day_start + dt.timedelta(days=1)
        chunk = pairs.filter(
          (pl.col('_minute') >= day_start - overlap)
          & (pl.col('_minute') < day_end)
        )
        if chunk.is_empty():
          continue

        subsets = (
          (chunk, general_frames),
          (chunk.filter(pl.col('_is_nacional')), nacional_frames),
          (chunk.filter(~pl.col('_is_nacional')), internacional_frames),
        )
        for subset, frames in subsets:
          if subset.is_empty():
            continue
          serie = _serie_concurrencia(subset, freq=freq, window_minutes=window_minutes)
          serie = serie.filter(
            (pl.col('ts') >= pl.lit(day_start))
            & (pl.col('ts') < pl.lit(day_end))
          )
          if not serie.is_empty():
            frames.append(serie)

      def _concat(frames):
        return pl.concat(frames).sort('ts') if frames else empty

      general = _concat(general_frames)
      nacional = (
        _concat(nacional_frames)
        .with_columns(pl.lit('NACIONAL').alias('TYPE_NATION'))
        .select('ts', 'TYPE_NATION', 'Concurrencia')
      )
      internacional = (
        _concat(internacional_frames)
        .with_columns(pl.lit('INTERNACIONAL').alias('TYPE_NATION'))
        .select('ts', 'TYPE_NATION', 'Concurrencia')
      )
      return general, nacional, internacional


    # Los pares se preparan una sola vez y se recorren por día.
    _pairs_all = _prepare_pairs(df)
    (
      df_concurrencia_general,
      df_concurrencia_nacional,
      df_concurrencia_internacional,
    ) = _series_concurrencia(_pairs_all)
    df_concurrencia_nacional.head()
    return (
        df_concurrencia_general,
        df_concurrencia_internacional,
        df_concurrencia_nacional,
    )


@app.cell
def _(df_concurrencia_general, plot_concurrencia_serie):
    plot_concurrencia_serie(
        df_concurrencia_general,
        title='Concurrencia de reproducción',
        color='#ff5500',
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Separación por TYPE_NATION

    Cantidad de registros por día y por tipo de nación (verificación del split y **descarga**).
    """)
    return


@app.cell
def _(df, pl):
    # Cantidad de registros por día y por TYPE_NATION.
    df_nation_day = (
        df
        .group_by(['_day', 'TYPE_NATION'])
        .agg(pl.len().alias('Registros'))
        .sort(['_day', 'TYPE_NATION'])
    )

    df_nation_day
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
    KPIs para cada total de visualizaciones por TYPE_NATION (tarjetas que se adjuntan al lado del gráfico de concurrencia)
    """)
    return


@app.cell
def _(df_views_nation, mo, pl):
    # Totales de vistas por TYPE_NATION (sumando todos los días, sin reescalado)
    df_kpi_por_nacion = (
        df_views_nation
        .group_by('TYPE_NATION')
        .agg(
            pl.col('Total views').sum().alias('Total vistas'),
            pl.col('Date').min().alias('Fecha min'),
            pl.col('Date').max().alias('Fecha max'),
        )
        .sort('TYPE_NATION')
    )


    def kpi_card_nacion(nation, total, fecha_min, fecha_max, color):
        """Tarjeta estilo KPI con el total de vistas de un TYPE_NATION.

        Tolera tipos de nación sin filas en `df_kpi_por_nacion` (fechas None): en
        ese caso muestra un aviso en lugar de fallar al formatear la fecha.
        """
        if fecha_min is not None and fecha_max is not None:
            rango_fechas = (
                f'Vistas totales desde {fecha_min:%d/%m/%Y} hasta {fecha_max:%d/%m/%Y}'
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
                    {nation}
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

    return df_kpi_por_nacion, kpi_card_nacion


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gráfico 1
    Gráficos de concurrencia por TYPE_NATION separados junto al total de visualizaciones
    """)
    return


@app.cell(expand_output=True)
def _(
    NATIONS,
    NATION_COLORS,
    df_concurrencia_internacional,
    df_concurrencia_nacional,
    df_kpi_por_nacion,
    kpi_card_nacion,
    mo,
    pl,
    plot_concurrencia_serie,
):
    _series_por_nacion = {
        'NACIONAL': df_concurrencia_nacional,
        'INTERNACIONAL': df_concurrencia_internacional,
    }
    _NACIONES_PLOTS = [
        (_nation, _series_por_nacion[_nation], NATION_COLORS[_nation])
        for _nation in NATIONS
    ]

    _filas_naciones = []
    for _nation, _nacion_df, _color in _NACIONES_PLOTS:
        _fila_kpi = df_kpi_por_nacion.filter(pl.col('TYPE_NATION') == _nation)
        _total = int(_fila_kpi['Total vistas'][0]) if _fila_kpi.height else 0
        _fmin = _fila_kpi['Fecha min'][0] if _fila_kpi.height else None
        _fmax = _fila_kpi['Fecha max'][0] if _fila_kpi.height else None
        _filas_naciones.append(
            mo.hstack(
                [
                    plot_concurrencia_serie(_nacion_df, _nation, color=_color),
                    kpi_card_nacion(_nation, _total, _fmin, _fmax, _color),
                ],
                gap=2,
                align='stretch',
                widths=[3, 1],
            )
        )

    mo.vstack(_filas_naciones, gap=2.5)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gráfico 2

    Gráfico de concurrencia general segmentado por TYPE_NATION.
    """)
    return


@app.cell
def _(dt, pl, plt):
    # Abreviaturas de meses en español para el título del eje X
    _MESES_ABREV = {
        1: 'ene', 2: 'feb', 3: 'mar', 4: 'abr', 5: 'may', 6: 'jun',
        7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dic',
    }


    def _fmt_fecha_esp(fecha):
        """Formatea una fecha/datetime como '3 oct', '4 sep', '15 ene', etc."""
        if fecha is None:
            return ''
        if isinstance(fecha, str):
            fecha = dt.date.fromisoformat(fecha[:10])
        if isinstance(fecha, dt.datetime):
            fecha = fecha.date()
        return f'{fecha.day} {_MESES_ABREV[fecha.month]}'


    def plot_concurrencia_multi_nacion(
        series,
        title='Concurrencia de reproducción apilada por TYPE_NATION',
        figsize=(13, 5),
        fill_alpha=0.9,
        date_ini=None,
        date_end=None,
    ):
        """Concurrencia por TYPE_NATION apilada.

        Cada tipo de nación se acumula sobre el anterior. A diferencia de
        `dsports.py`, las series se calculan de forma independiente sobre los
        eventos de cada TYPE_NATION (sin reescalado), por lo que la altura total
        equivale a la concurrencia de ambos tipos en cada instante.

        Donde un tipo de nación no tuvo streams, su aporte es 0.

        series: lista de tuplas (label, DataFrame con columnas ['ts', 'Concurrencia'], color).
        date_ini / date_end: rango mostrado en el eje X. Si son None se toma el
        mínimo/máximo de las marcas de tiempo de `series`.
        """
        labels = [label for label, _, _ in series]
        colors = [color for _, _, color in series]

        # Alinea todos los tipos de nación por su marca de tiempo.
        frame = None
        for label, nacion_df, _ in series:
            bloque = nacion_df.select(
                'ts',
                pl.col('Concurrencia').fill_null(0).cast(pl.Float64).alias(label),
            )
            frame = bloque if frame is None else frame.join(bloque, on='ts', how='full', coalesce=True)
        frame = frame.sort('ts')
        frame = frame.with_columns([pl.col(label).fill_null(0) for label in labels])

        ts = frame['ts'].to_list()
        values = [frame[label].to_list() for label in labels]

        # Rango de fechas para el título del eje X ('3 oct', '4 sep', etc.)
        if date_ini is None:
            date_ini = frame['ts'].min()
        if date_end is None:
            date_end = frame['ts'].max()
        xlabel = f'Tiempo desde {_fmt_fecha_esp(date_ini)} hasta {_fmt_fecha_esp(date_end)}'

        fig, ax = plt.subplots(figsize=figsize)
        fig.patch.set_alpha(0)
        ax.patch.set_alpha(0)

        ax.stackplot(ts, *values, labels=labels, colors=colors, alpha=fill_alpha, linewidth=0)

        ax.set_title(title, fontweight='bold')
        ax.set_xlabel(xlabel, fontweight='bold')
        ax.set_ylabel('Reproducciones')
        ax.grid(True, alpha=0.2, axis='y')
        ax.legend(loc='upper left', frameon=False, ncol=2, fontsize=9)
        fig.autofmt_xdate()
        fig.tight_layout()
        for spine in ax.spines.values():
            spine.set_visible(False)
        return fig

    return (plot_concurrencia_multi_nacion,)


@app.cell
def _(
    NATIONS,
    NATION_COLORS,
    df_concurrencia_internacional,
    df_concurrencia_nacional,
    plot_concurrencia_multi_nacion,
):
    _series_por_nacion = {
        'NACIONAL': df_concurrencia_nacional,
        'INTERNACIONAL': df_concurrencia_internacional,
    }
    _series_naciones_all = [
        (_nation, _series_por_nacion[_nation], NATION_COLORS[_nation])
        for _nation in NATIONS
    ]

    plot_concurrencia_multi_nacion(_series_naciones_all)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Descarga de gráficas

    Dos botones para bajar un PNG (200 dpi, fondo blanco) de cada gráfico del reporte:

    - **Comparación naciones**: el Gráfico 1 tal cual, 2 filas con la concurrencia de cada TYPE_NATION junto a su tarjeta KPI de vistas totales.
    - **Concurrencia apilada**: el Gráfico 2, concurrencia segmentada por TYPE_NATION.

    Los PNG se reconstruyen con los mismos datos del notebook, así que se actualizan al re-ejecutarlo.
    """)
    return


@app.cell
def _(
    NATIONS,
    NATION_COLORS,
    df_concurrencia_internacional,
    df_concurrencia_nacional,
    df_kpi_por_nacion,
    pl,
    plt,
):
    import io
    from matplotlib.patches import FancyBboxPatch, Rectangle
    from matplotlib.colors import to_rgba

    # Series de concurrencia por TYPE_NATION con su color (los mismos que en los gráficos).
    _series_por_nacion = {
        'NACIONAL': df_concurrencia_nacional,
        'INTERNACIONAL': df_concurrencia_internacional,
    }
    NACIONES_SERIES = [
        (_nation, _series_por_nacion[_nation], NATION_COLORS[_nation])
        for _nation in NATIONS
    ]


    def render_nacion_on_ax(ax, nacion_df, title, color):
        """Dibuja la concurrencia de un TYPE_NATION con el estilo del gráfico 1."""
        ts = nacion_df['ts'].to_list()
        valores = nacion_df['Concurrencia'].to_list()
        ax.patch.set_alpha(0)
        ax.plot(ts, valores, color=color, linewidth=1.5)
        ax.fill_between(ts, valores, 0, color=color, alpha=0.2)
        ax.set_title(title, color=color, fontweight='bold')
        ax.set_ylabel('Reproducciones')
        ax.grid(True, alpha=0.2, axis='y')
        for spine in ax.spines.values():
            spine.set_visible(False)


    def render_kpi_on_ax(ax, nation, total, fecha_min, fecha_max, color):
        """Dibuja la tarjeta KPI (equivalente a `kpi_card_nacion`) sobre un axes."""
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
            rango = f'Vistas totales\ndesde {fecha_min:%d/%m/%Y}\nhasta {fecha_max:%d/%m/%Y}'
        else:
            rango = 'Sin vistas en el\nperiodo del reporte'

        ax.text(0.13, 0.82, nation.upper(), fontsize=9, fontweight='bold', color=color, va='top')
        ax.text(0.13, 0.63, f'{total:,}', fontsize=20, fontweight='bold', color=color, va='top')
        ax.text(0.13, 0.42, 'TOTAL VIEWS', fontsize=7.5, color='#666666', va='top')
        ax.text(0.13, 0.30, rango, fontsize=7.5, color='#666666', va='top', linespacing=1.4)


    def build_figura_comparacion():
        """Reconstruye el gráfico 1 (2 filas: concurrencia + KPI) en una sola figura."""
        filas = len(NACIONES_SERIES)
        fig, axes = plt.subplots(
            filas, 2,
            figsize=(15, 3.4 * filas),
            gridspec_kw={'width_ratios': [3, 1], 'hspace': 0.45, 'wspace': 0.05},
        )
        if filas == 1:
            axes = axes.reshape(1, -1)

        for i, (nation, nacion_df, color) in enumerate(NACIONES_SERIES):
            fila_kpi = df_kpi_por_nacion.filter(pl.col('TYPE_NATION') == nation)
            total = int(fila_kpi['Total vistas'][0]) if fila_kpi.height else 0
            fmin = fila_kpi['Fecha min'][0] if fila_kpi.height else None
            fmax = fila_kpi['Fecha max'][0] if fila_kpi.height else None
            render_nacion_on_ax(axes[i, 0], nacion_df, nation, color)
            render_kpi_on_ax(axes[i, 1], nation, total, fmin, fmax, color)

        fig.autofmt_xdate()
        return fig


    # --- Exportación a PNG (tema claro) -----------------------------------
    # marimo, en modo oscuro, deja matplotlib con texto, ejes, ticks y rejilla en
    # blanco (`text.color`, `axes.labelcolor`, `xtick.color`, `grid.color`).
    # Las figuras del notebook se dibujan con fondo transparente, así que en
    # pantalla se ven bien, pero al volcarlas a un PNG de fondo blanco los títulos,
    # etiquetas, ticks y leyendas (blancos) desaparecían. Por eso la exportación se
    # hace siempre con este tema claro.
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
        """Serializa a PNG una figura o una función que la construye.

        Conviene pasar la función (`build_figura_comparacion`, `lambda: ...`) para
        que la figura se cree ya dentro del tema claro y todos los textos nazcan
        con color oscuro. También se acepta una figura ya construida: en ese caso se
        recolorean sus textos claros y se fuerza el fondo blanco.
        """
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


    return NACIONES_SERIES, build_figura_comparacion, figura_a_png_bytes


@app.cell
def _(
    DATE_REPORT_END,
    DATE_REPORT_INI,
    NACIONES_SERIES,
    build_figura_comparacion,
    figura_a_png_bytes,
    mo,
    plot_concurrencia_multi_nacion,
):
    _periodo = mo.md(
        f'PNG del reporte: **{DATE_REPORT_INI:%d/%m/%Y} – {DATE_REPORT_END:%d/%m/%Y}**'
    )
    _sufijo = f'{DATE_REPORT_INI:%Y%m%d}-{DATE_REPORT_END:%Y%m%d}'

    # Comparación naciones: mismo render que `mo.vstack(_filas_naciones)`
    # (2 filas de concurrencia + su tarjeta KPI). Se pasa el *builder* para que la
    # figura se cree dentro del tema claro de exportación.
    _img_comparacion = figura_a_png_bytes(build_figura_comparacion)

    # Concurrencia apilada: mismo render que el Gráfico 2.
    _img_apilada = figura_a_png_bytes(
        lambda: plot_concurrencia_multi_nacion(NACIONES_SERIES)
    )

    btn_comparacion = mo.download(
        _img_comparacion,
        filename=f'comparacion_naciones_{_sufijo}.png',
        mimetype='image/png',
        label='Comparación naciones',
    )

    btn_apilada = mo.download(
        _img_apilada,
        filename=f'concurrencia_apilada_naciones_{_sufijo}.png',
        mimetype='image/png',
        label='Concurrencia apilada',
    )

    mo.vstack([
        _periodo,
        mo.hstack([btn_comparacion, btn_apilada], gap=1),
    ])
    return


if __name__ == "__main__":
    app.run()
