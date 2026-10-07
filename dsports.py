import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo # Relevante para usar .py como .ipynb
    import polars as pl
    import datetime as dt
    import seaborn as sns
    import matplotlib.pyplot as plt
    import plotly.graph_objects as go
    from IPython.display import display

    return display, dt, mo, pl, plt


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Importar datos
    """)
    return


@app.cell
def _(pl):
    df_sep = pl.read_parquet('./win-work-reports/streaming_data/LIVE_2026-09.parquet')
    df_oct = pl.read_parquet('./win-work-reports/streaming_data/LIVE_2026-10.parquet')
    return df_oct, df_sep


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Mergear información de ambos meses
    """)
    return


@app.cell
def _(df_oct, df_sep, pl):
    df = pl.concat([df_sep, df_oct], how='vertical')
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Verificación
    """)
    return


@app.cell
def _(df, display):
    display(df.head())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Normalizar información (Vistas por señal)

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
def _():
    DEVICE_UNIQUE_COLS = [
      'Date',
      'SubscriberID',
      'Title',
      'SeriesTitle',
      'DeviceType',
      'deviceDescription',
    ]
    return (DEVICE_UNIQUE_COLS,)


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
        if df.schema['Date'] == pl.String:
          df = df.with_columns(
            pl.col('Date').str.to_datetime(strict=False)
          )
        else:
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
    ### Función para views per user (por día y serie)
    """)
    return


@app.cell
def _(DEVICE_UNIQUE_COLS, device_unique_per_user, pl):
    def total_views_per_day(
        events_df: pl.DataFrame | pl.LazyFrame,
        hours_diff: float = 24,
    ) -> pl.DataFrame:
        """Total views por día, con el mismo criterio que el KPI del panel
        (filtro de fechas puesto día a día sobre la base de referencia).

        - events_df: base cruda de eventos (1 fila = 1 evento) con las columnas
          de DEVICE_UNIQUE_COLS. Acepta DataFrame o LazyFrame.
        - hours_diff: ventana de sesión. 24 para VOD, 20/60 (20 min) para LIVE.

        Devuelve ['Date', 'Total views']: un registro por día con eventos,
        ordenado cronológicamente.
        """
        if isinstance(events_df, pl.LazyFrame):
            events_df = events_df.collect(engine='streaming')
        if events_df is None or events_df.is_empty():
            return pl.DataFrame({'Date': [], 'Total views': []})

        missing = [c for c in DEVICE_UNIQUE_COLS if c not in events_df.columns]
        if missing:
            raise ValueError(f'La base no tiene las columnas requeridas: {missing}')

        df = events_df.select(DEVICE_UNIQUE_COLS)
        # Compatible Polars 1.x/2.x: String -> Datetime sin cast (removido en 2.0)
        if df.schema['Date'] == pl.String:
            df = df.with_columns(pl.col('Date').str.to_datetime(strict=False))
        else:
            df = df.with_columns(pl.col('Date').cast(pl.Datetime, strict=False))
        df = df.filter(pl.col('Date').is_not_null())
        df = df.with_columns(pl.col('Date').dt.date().alias('_day'))

        rows = []
        for key, day_df in df.partition_by('_day', as_dict=True).items():
            day = key[0] if isinstance(key, tuple) else key
            df_unique = device_unique_per_user(day_df, DEVICE_UNIQUE_COLS, hours_diff=hours_diff)
            total = (
                int(df_unique.select(pl.col('id_unique_visual_by_user').count()).item())
                if 'id_unique_visual_by_user' in df_unique.columns
                else df_unique.height
            )
            rows.append({'Date': day, 'Total views': total})

        if not rows:
            return pl.DataFrame({'Date': [], 'Total views': []})
        return pl.DataFrame(rows).sort('Date')

    def total_views_per_day_series(
        events_df: pl.DataFrame | pl.LazyFrame,
        hours_diff: float = 24,
    ) -> pl.DataFrame:
        """Total views por día y Title, con el mismo criterio que el KPI del panel
        (filtro de fechas puesto día a día sobre la base de referencia).

        - events_df: base cruda de eventos (1 fila = 1 evento) con las columnas
          de DEVICE_UNIQUE_COLS. Acepta DataFrame o LazyFrame.
        - hours_diff: ventana de sesión. 24 para VOD, 20/60 (20 min) para LIVE.

        Devuelve ['Date', 'Title', 'Total views']: un registro por día y título
        con visualizaciones, ordenado por fecha y luego título.
        """
        if isinstance(events_df, pl.LazyFrame):
            events_df = events_df.collect(engine='streaming')
        empty = pl.DataFrame({'Date': [], 'Title': [], 'Total views': []})
        if events_df is None or events_df.is_empty():
            return empty

        missing = [c for c in DEVICE_UNIQUE_COLS if c not in events_df.columns]
        if missing:
            raise ValueError(f'La base no tiene las columnas requeridas: {missing}')

        df = events_df.select(DEVICE_UNIQUE_COLS)
        # Compatible Polars 1.x/2.x: String -> Datetime sin cast (removido en 2.0)
        if df.schema['Date'] == pl.String:
            df = df.with_columns(pl.col('Date').str.to_datetime(strict=False))
        else:
            df = df.with_columns(pl.col('Date').cast(pl.Datetime, strict=False))
        df = df.filter(pl.col('Date').is_not_null())
        df = df.with_columns(pl.col('Date').dt.date().alias('_day'))

        rows = []
        for key, day_df in df.partition_by('_day', as_dict=True).items():
            day = key[0] if isinstance(key, tuple) else key
            df_unique = device_unique_per_user(day_df, DEVICE_UNIQUE_COLS, hours_diff=hours_diff)
            if df_unique.is_empty():
                continue
            counts = (
                df_unique
                .group_by('Title')
                .agg(pl.len().cast(pl.Int64).alias('Total views'))
                .with_columns(pl.lit(day).alias('Date'))
                .select(['Date', 'Title', 'Total views'])
            )
            rows.append(counts)

        if not rows:
            return empty
        return pl.concat(rows).sort(['Date', 'Title'])

    return total_views_per_day, total_views_per_day_series


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Ahora verificar información de views de usuarios.
    """)
    return


@app.cell
def _(df, display, total_views_per_day, total_views_per_day_series):
    df_views = total_views_per_day(events_df=df, hours_diff=20 /60)
    df_views_title = total_views_per_day_series(events_df=df, hours_diff=20 /60)
    display(df_views.head())
    print('-'*25)
    display(df_views_title.head())
    return df_views, df_views_title


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Reescalado de visualizaciones

    Filtrar por fechas específicas; **solo señales en los programas** y porcentaje de equivalencia para escalarlo.
    """)
    return


@app.cell
def _():
    SIGNALS = ['WIN AUDIO', 'WIN SPORTS', 'WIN+FUTBOL', 'DSPORTS', 'DSPORTS 2']
    return (SIGNALS,)


@app.cell
def _(SIGNALS, df_views, df_views_title, display, dt, pl):
    # Filtrar fechas para reporte
    df_views_filt = df_views.filter(pl.col('Date').dt.date().is_between(dt.date(2026, 9, 26), dt.date(2026, 10, 6)))
    df_views_title_filt = df_views_title.filter(pl.col('Date').dt.date().is_between(dt.date(2026, 9, 26), dt.date(2026, 10, 6)))

    # Filtrar solo señales para el dataset con títulos (después se reescalan con las views totales del día)
    df_views_title_filt = df_views_title_filt.filter(pl.col('Title').is_in(SIGNALS))

    # Porcentaje por día: cada título representa un % del total de views de ese día (suma 100% por día)
    df_views_title_filt = df_views_title_filt.with_columns(
        (pl.col('Total views') / pl.col('Total views').sum().over('Date') * 100).alias('Porcent')
    )

    # Verificar información
    display(df_views_filt.head())
    display(df_views_title_filt.head())
    return df_views_filt, df_views_title_filt


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Reescalación a valores reales repartidos por señales según su porcentaje
    """)
    return


@app.cell
def _(df_views_filt, df_views_title_filt, display, pl):
    # Escalado de visualizaciones para que coincidan con el total por día
    # Se reparte el total real del día (df_views_filt) según el Porcent de cada señal.
    # Se usa el método de residuo mayor para que la suma por día sea EXACTAMENTE el total real.
    df_views_scaler = (
        df_views_title_filt
        .join(
            df_views_filt.select('Date', pl.col('Total views').alias('_total_real_dia')),
            on='Date',
            how='left',
        )
        .with_columns(
            (pl.col('Porcent') / 100 * pl.col('_total_real_dia')).alias('_raw')
        )
        .with_columns(
            pl.col('_raw').floor().cast(pl.Int64).alias('_base')
        )
        .with_columns(
            (pl.col('_total_real_dia') - pl.col('_base').sum().over('Date')).alias('_residuo')
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

    display(df_views_scaler.head(6))
    return (df_views_scaler,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Normalizar información (Concurrencia de reproducción)

    Se normaliza la info para guardar correctamente el conteo de views y la concurrencia de reproducción.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Inicialmente se crea el plot para graficar la concurrencia verificando con el dataset original entre los meses seleccionados inicialmente en el `df`.
    """)
    return


@app.cell
def _():
    WINDOW_MINUTES = 25
    return (WINDOW_MINUTES,)


@app.cell
def _(WINDOW_MINUTES, df, dt, pl, plt):
    def _to_utc_datetime(df: pl.DataFrame, col: str) -> pl.DataFrame:
      dtype = df.schema.get(col)
      if dtype is None or not isinstance(dtype, pl.Datetime):
        return df.with_columns(
          pl.col(col)
          .cast(pl.Utf8, strict=False)
          .str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S', strict=False)
          .dt.replace_time_zone('UTC')
          .alias(col)
        )
      if getattr(dtype, 'time_zone', None) is None:
        return df.with_columns(pl.col(col).dt.replace_time_zone('UTC').alias(col))
      return df


    def _coerce_to_utc(value):
      if isinstance(value, pl.Expr):
        return value.dt.replace_time_zone('UTC')
      return pl.lit(value).dt.replace_time_zone('UTC')


    def _freq_to_seconds(freq: str) -> int:
      digits = ''.join(c for c in freq if c.isdigit()) or '1'
      unit = freq[-1].lower() if freq and freq[-1].isalpha() else 'm'
      return int(digits) * {'m': 60, 't': 60, 'h': 3600, 'd': 86400, 'w': 604800}.get(unit, 60)


    def plot_concurrent_users_chart(
      events_df: pl.DataFrame,
      freq: str = '5T',
      title: str = 'Concurrencia de streams',
      line_color: str = '#ff5500',
      fill_alpha: float = 0.2,
      start_datetime=None,
      end_datetime=None,
      window_minutes: int = WINDOW_MINUTES,
      figsize: tuple = (12, 4),
      show: bool = True,
    ):
      """Grafica concurrencia de streams usando ventana rodante (matplotlib).

      Un stream (SubscriberID + Title + deviceDescription) se considera activo en
      el instante t si tuvo cualquier evento en [t - window_minutes, t].

      Metodologia alineada con el proveedor (ventana de 25-26 min).

      Devuelve (fig, ax), o (None, None) si no hay datos.
      """
      if events_df is None or events_df.is_empty():
        print('No hay datos de eventos para mostrar la concurrencia.')
        return None, None

      required = ['Date', 'SubscriberID', 'Title', 'deviceDescription']
      missing = [c for c in required if c not in events_df.columns]
      if missing:
        print(f'Faltan columnas requeridas para calcular la concurrencia: {missing}')
        return None, None

      events_df = _to_utc_datetime(events_df, 'Date')

      if start_datetime is None:
        start_datetime = events_df.select(pl.col('Date').min()).item()
      if end_datetime is None:
        end_datetime = events_df.select(pl.col('Date').max()).item()

      start_utc = _coerce_to_utc(start_datetime)
      end_utc = _coerce_to_utc(end_datetime)

      # Para que el conteo en el borde izquierdo sea correcto, necesitamos
      # eventos de hasta `window_minutes` antes de start_datetime.
      extended_start_utc = start_utc - pl.duration(minutes=window_minutes)
      events_in_range = events_df.filter(
        (pl.col('Date') >= extended_start_utc) & (pl.col('Date') <= end_utc)
      )

      if events_in_range.is_empty():
        print('No hay eventos en el rango seleccionado.')
        return None, None

      # 1) Pares unicos (minuto, stream). Un "stream" se identifica por la triada
      #    (SubscriberID, Title, deviceDescription). Si hay multiples eventos del
      #    mismo stream en el mismo minuto, solo cuentan una vez.
      unique_pairs = (
        events_in_range
        .with_columns(pl.col('Date').dt.truncate('1m').alias('_minute'))
        .select([
          '_minute',
          pl.struct(['SubscriberID', 'Title', 'deviceDescription']).alias('_stream'),
        ])
        .unique()
      )

      if unique_pairs.is_empty():
        print('No hay eventos validos para calcular la concurrencia.')
        return None, None

      # 2) Cuenta las ventanas activas sin expandir cada par a N x 25 filas.
      #    Al desplazar cada evento al final de su ventana, un grupo dinamico
      #    [t, t + window) equivale a contar eventos en [t - window + 1, t].
      #    Esto mantiene la semantica anterior sin el cross join que disparaba el
      #    consumo de memoria para datasets grandes.
      active_per_minute = (
        unique_pairs
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
        .filter((pl.col('ts') >= start_utc) & (pl.col('ts') <= end_utc))
        .sort('ts')
      )

      if active_per_minute.is_empty():
        print('No hay minutos con streams activos en el rango seleccionado.')
        return None, None

      # 3) Agregar al bucket de salida (e.g. 5T o H) tomando el max dentro de cada
      #    bucket para preservar los picos reales.
      bucket_seconds = _freq_to_seconds(freq)
      if bucket_seconds >= 60:
        bucket_minutes = max(1, bucket_seconds // 60)
        bucket_label = (
          f'{bucket_minutes // 60}h' if bucket_minutes >= 60 and bucket_minutes % 60 == 0
          else f'{bucket_minutes}m'
        )
        active_per_minute = (
          active_per_minute
          .with_columns(
            pl.col('ts').dt.truncate(bucket_label).alias('_bucket')
          )
          .group_by('_bucket')
          .agg(pl.col('active_streams').max().alias('active_streams'))
          .rename({'_bucket': 'ts'})
          .sort('ts')
        )

      ts = active_per_minute.get_column('ts').to_list()
      values = active_per_minute.get_column('active_streams').to_list()

      fig, ax = plt.subplots(figsize=figsize)
      fig.patch.set_alpha(0)
      ax.patch.set_alpha(0)
      ax.plot(ts, values, color=line_color, linewidth=1.5)
      ax.fill_between(ts, values, 0, color=line_color, alpha=fill_alpha)
      ax.set_title(title)
      ax.set_xlabel('Tiempo')
      ax.set_ylabel(f'Reproducciones')
      ax.grid(True, alpha=0.2, axis='y')
      fig.autofmt_xdate()
      fig.tight_layout()
      for spine in ax.spines.values():
        spine.set_visible(False)

      return fig, ax

    df_dates_filter = df.filter(
      pl.col('Date').str.to_datetime(strict=False).dt.date().is_between(dt.date(2026, 9, 26), dt.date(2026, 10, 6))
    )

    general_streaming_plot = plot_concurrent_users_chart(
        events_df=df_dates_filter,
        title='Concurrencia de reproducción'
    )

    general_streaming_plot
    return (df_dates_filter,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Separación por señales

    Ahora se procede a separar las concurrencias generales por señales repartidas (y escaladas como en el caso anterior) para reconocer el impacto de cada una.
    """)
    return


@app.cell
def _(SIGNALS, df_dates_filter, dt, pl):
    REPORT_START = dt.date(2026, 9, 26)
    REPORT_END = dt.date(2026, 10, 4)

    # Cantidad de registros por día y por señal, y su porcentaje relativo diario
    # (entre señales). Es la base del reescalado de la concurrencia.
    df_signals_day = (
        df_dates_filter
        .with_columns(pl.col('Date').str.to_datetime(strict=False).dt.date().alias('_day'))
        .filter(pl.col('Title').is_in(SIGNALS))
        .group_by(['_day', 'Title'])
        .agg(pl.len().alias('Registros'))
        .with_columns(pl.col('Registros').sum().over('_day').alias('Total registros dia'))
        .with_columns(
            (pl.col('Registros') / pl.col('Total registros dia') * 100).alias('Porcentaje')
        )
        .sort(['_day', 'Title'])
    )

    df_signals_day
    return (df_signals_day,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Reescalado de las señales
    """)
    return


@app.cell
def _(WINDOW_MINUTES, df_dates_filter, df_signals_day, pl):
    def _serie_to_utc(frame, col):
        dtype = frame.schema.get(col)
        if dtype is None or not isinstance(dtype, pl.Datetime):
            return frame.with_columns(
                pl.col(col)
                .cast(pl.Utf8, strict=False)
                .str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S', strict=False)
                .dt.replace_time_zone('UTC')
                .alias(col)
            )
        if getattr(dtype, 'time_zone', None) is None:
            return frame.with_columns(pl.col(col).dt.replace_time_zone('UTC').alias(col))
        return frame


    def _serie_as_utc(value):
        if isinstance(value, pl.Expr):
            return value.dt.replace_time_zone('UTC')
        return pl.lit(value).dt.replace_time_zone('UTC')


    def _serie_freq_to_seconds(freq):
        digits = ''.join(c for c in freq if c.isdigit()) or '1'
        unit = freq[-1].lower() if freq and freq[-1].isalpha() else 'm'
        return int(digits) * {'m': 60, 't': 60, 'h': 3600, 'd': 86400, 'w': 604800}.get(unit, 60)


    def concurrency_series(events_df, freq='5T', window_minutes=WINDOW_MINUTES):
        """Serie de concurrencia [ts, active_streams] con la misma metodología de
        `plot_concurrent_users_chart` (ventana rodante de `window_minutes`)."""
        if events_df is None or events_df.is_empty():
            return pl.DataFrame({'ts': [], 'active_streams': []})
        events_df = _serie_to_utc(events_df, 'Date')
        start_utc = _serie_as_utc(events_df.select(pl.col('Date').min()).item())
        end_utc = _serie_as_utc(events_df.select(pl.col('Date').max()).item())
        extended_start_utc = start_utc - pl.duration(minutes=window_minutes)
        events_in_range = events_df.filter(
            (pl.col('Date') >= extended_start_utc) & (pl.col('Date') <= end_utc)
        )
        unique_pairs = (
            events_in_range
            .with_columns(pl.col('Date').dt.truncate('1m').alias('_minute'))
            .select(
                '_minute',
                pl.struct(['SubscriberID', 'Title', 'deviceDescription']).alias('_stream'),
            )
            .unique()
        )
        active_per_minute = (
            unique_pairs
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
            .filter((pl.col('ts') >= start_utc) & (pl.col('ts') <= end_utc))
            .sort('ts')
        )
        bucket_seconds = _serie_freq_to_seconds(freq)
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
        return active_per_minute


    general_concurrency = concurrency_series(df_dates_filter)


    def reescalar_concurrencia(signal, serie, pct):
        """Reparte la concurrencia general (todos los eventos) entre las señales
        multiplicándola por el porcentaje diario de registros de cada señal."""
        pesos = (
            pct.filter(pl.col('Title') == signal)
            .select('_day', (pl.col('Porcentaje') / 100).alias('_peso'))
        )
        return (
            serie
            .with_columns(pl.col('ts').dt.date().alias('_day'))
            .join(pesos, on='_day', how='left')
            .with_columns(
                (pl.col('active_streams') * pl.col('_peso').fill_null(0))
                .round()
                .cast(pl.Int64)
                .alias('Concurrencia')
            )
            .select('ts', 'Concurrencia')
        )


    df_WIN_FUTBOL = reescalar_concurrencia('WIN+FUTBOL', general_concurrency, df_signals_day)
    df_WIN_SPORTS = reescalar_concurrencia('WIN SPORTS', general_concurrency, df_signals_day)
    df_WIN_AUDIO = reescalar_concurrencia('WIN AUDIO', general_concurrency, df_signals_day)
    df_DSPORTS = reescalar_concurrencia('DSPORTS', general_concurrency, df_signals_day)
    df_DSPORTS_2 = reescalar_concurrencia('DSPORTS 2', general_concurrency, df_signals_day)
    df_WIN_FUTBOL.head()
    return df_DSPORTS, df_DSPORTS_2, df_WIN_AUDIO, df_WIN_FUTBOL, df_WIN_SPORTS


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Función para crear plot de concurrencia por señal seleccionada
    """)
    return


@app.cell
def _(plt):
    def plot_concurrencia_senal(senal_df, title, color='#ff5500', figsize=(12, 4)):
        ts = senal_df['ts'].to_list()
        valores = senal_df['Concurrencia'].to_list()
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

    return (plot_concurrencia_senal,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Graficar toda la información
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    KPIs para cada total de visualizaciones por señal (tarjetas que se adjuntan al lado del gráfico de concurrencia)
    """)
    return


@app.cell
def _(df_views_scaler, mo, pl):
    # Totales de vistas reescaladas por señal (sumando todos los días)
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
        """Tarjeta estilo KPI con el total de vistas reescaladas de una señal."""
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
                    Total reproducciones
                </div>
                <div style="font-size: 0.7rem; opacity: 0.7;">
                    Vistas totales desde {fecha_min:%d/%m/%Y} hasta {fecha_max:%d/%m/%Y}
                </div>
            </div>
            """
        )

    return df_kpi_por_senal, kpi_card_senal


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Gráfico 1
    Gráficos de concurrencia por señal separados junto al total de visualiz
    aciones (ambos reescalados)
    """)
    return


@app.cell
def _(
    df_DSPORTS,
    df_DSPORTS_2,
    df_WIN_AUDIO,
    df_WIN_FUTBOL,
    df_WIN_SPORTS,
    df_kpi_por_senal,
    kpi_card_senal,
    mo,
    pl,
    plot_concurrencia_senal,
):
    _SENALES_PLOTS = [
        ('WIN+FUTBOL', df_WIN_FUTBOL, '#ff5500'),
        ('WIN SPORTS', df_WIN_SPORTS, '#d839ff'),
        ('WIN AUDIO', df_WIN_AUDIO, '#6e8680'),
        ('DSPORTS', df_DSPORTS, '#0ba4e5'),
        ('DSPORTS 2', df_DSPORTS_2, '#1070a0'),
    ]

    _filas_senales = []
    for _signal, _senal_df, _color in _SENALES_PLOTS:
        _fila_kpi = df_kpi_por_senal.filter(pl.col('Title') == _signal)
        _total = int(_fila_kpi['Total vistas'][0]) if _fila_kpi.height else 0
        _fmin = _fila_kpi['Fecha min'][0] if _fila_kpi.height else None
        _fmax = _fila_kpi['Fecha max'][0] if _fila_kpi.height else None
        _filas_senales.append(
            mo.hstack(
                [
                    plot_concurrencia_senal(_senal_df, _signal, color=_color),
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

    Gráfico de concurrencia general segmentado por señal.
    """)
    return


@app.cell
def _(pl, plt):
    def plot_concurrencia_multi_senal(
        series,
        title='Concurrencia de reproducción apilada por señal',
        figsize=(13, 5),
        fill_alpha=0.9,
    ):
        """Concurrencia apilada (escalado total) por señal.

        Cada señal se acumula sobre las anteriores, de modo que la altura total en
        cada instante equivale a la concurrencia total de todos los eventos,
        coincidiendo con los picos del gráfico general (`general_streaming_plot`).

        Donde una señal no existió, su aporte es 0, por lo que su color no aparece
        hasta que vuelve a estar presente.

        series: lista de tuplas (label, DataFrame con columnas ['ts', 'Concurrencia'], color).
        """
        labels = [label for label, _, _ in series]
        colors = [color for _, _, color in series]

        # Alinea todas las señales por su marca de tiempo.
        frame = None
        for label, senal_df, _ in series:
            bloque = senal_df.select(
                'ts',
                pl.col('Concurrencia').fill_null(0).cast(pl.Float64).alias(label),
            )
            frame = bloque if frame is None else frame.join(bloque, on='ts', how='inner')
        frame = frame.sort('ts')

        ts = frame['ts'].to_list()
        values = [frame[label].to_list() for label in labels]

        fig, ax = plt.subplots(figsize=figsize)
        fig.patch.set_alpha(0)
        ax.patch.set_alpha(0)

        ax.stackplot(ts, *values, labels=labels, colors=colors, alpha=fill_alpha, linewidth=0)

        ax.set_title(title)
        ax.set_xlabel('Tiempo')
        ax.set_ylabel('Reproducciones')
        ax.grid(True, alpha=0.2, axis='y')
        ax.legend(loc='upper left', frameon=False, ncol=2, fontsize=9)
        fig.autofmt_xdate()
        fig.tight_layout()
        for spine in ax.spines.values():
            spine.set_visible(False)
        return fig

    return (plot_concurrencia_multi_senal,)


@app.cell
def _(
    df_DSPORTS,
    df_DSPORTS_2,
    df_WIN_AUDIO,
    df_WIN_FUTBOL,
    df_WIN_SPORTS,
    plot_concurrencia_multi_senal,
):
    _series_senales_all = [
        ('WIN+FUTBOL', df_WIN_FUTBOL, '#ff5500'),
        ('WIN SPORTS', df_WIN_SPORTS, '#d839ff'),
        ('WIN AUDIO', df_WIN_AUDIO, '#6e8680'),
        ('DSPORTS', df_DSPORTS, '#0ba4e5'),
        ('DSPORTS 2', df_DSPORTS_2, '#1070a0'),
    ]

    plot_concurrencia_multi_senal(_series_senales_all)
    return


if __name__ == "__main__":
    app.run()
