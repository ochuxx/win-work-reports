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
    from IPython.display import display

    return display, dt, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Importar datos
    """)
    return


@app.cell
def _(pl):
    df_sep = pl.read_parquet('./streaming_data/LIVE_2026-09.parquet')
    df_oct = pl.read_parquet('./streaming_data/LIVE_2026-10.parquet')
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
    Agrupación de Title's
    """)
    return


@app.cell
def _(pl):
    df = df.with_columns(
        pl.col('Title').replace(
            ['DESPIERTA WIN', 'GRAN PREVIO', ''],
            ['', '']
        )
    )
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Normalizar información

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
    Filtrar por fechas específicas; **solo señales en los programas** y porcentaje de equivalencia para escalarlo.
    """)
    return


@app.cell
def _(df_views, df_views_title, display, dt, pl):
    # Filtrar fechas para reporte
    df_views_filt = df_views.filter(pl.col('Date').dt.date().is_between(dt.date(2026, 9, 26), dt.date(2026, 10, 4)))
    df_views_title_filt = df_views_title.filter(pl.col('Date').dt.date().is_between(dt.date(2026, 9, 26), dt.date(2026, 10, 4)))

    # Filtrar solo señales para el dataset con títulos (después se reescalan con las views totales del día)
    df_views_title_filt = df_views_title_filt.filter(pl.col('Title').is_in(['WIN AUDIO', 'WIN SPORTS', 'WIN + FUTBOL', 'DSPORTS', 'DSPORTS 2']))

    # Extraer porcentajes de cada día equivalente


    # Verificar información
    display(df_views_filt.head(10))
    print('-'*25)
    display(df_views_title_filt.head(20))
    return


if __name__ == "__main__":
    app.run()
