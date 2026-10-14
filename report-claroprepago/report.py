import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import IPython.display as display
    import calendar

    return mo, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Carga de información por parte del encargado de facturación.
    """)
    return


@app.cell
def _(pd):
    df_fact = pd.read_excel('./win-work-reports/data/elena-reports/sep26.xlsx', header=5)
    df_fact = df_fact.drop(['Unnamed: 0'], axis=1)

    # Corta el reporte en el primer subscriberId nulo: se eliminan todos de allí en adelante
    _null_rows = df_fact.index[df_fact['subscriberId'].isna()]

    if len(_null_rows) > 0:
        _cut_pos = df_fact.index.get_loc(_null_rows[0])
        print(
            f"Primer subscriberId nulo en la posición {_cut_pos}. "
            f"Se descartan {len(df_fact) - _cut_pos} filas desde ahí hacia abajo."
        )
        df_fact = df_fact.iloc[:_cut_pos]
    else:
        print("No se detectaron subscriberId nulos: se conservan todas las filas.")

    df_fact.tail(15)
    return (df_fact,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Carga de información de reporte **"CLARO PREPAGO - Monthly"** descargado desde ClickHouse.
    """)
    return


@app.cell
def _(pd):
    df_mails = pd.read_csv('./win-work-reports/data/elena-reports/sep26_mails.csv')

    df_mails.head()
    return (df_mails,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Cruzar información de correo a través del campo **subscriberId**
    """)
    return


@app.cell
def _(df_fact, df_mails, pd):
    df = pd.merge(df_fact, df_mails, left_on='subscriberId', right_on='Subscriber_ID', how='left')
    df = df[['subscriberId', 'sku', 'fecha_inicio', 'email']]

    df.head(15)
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Sección de descaraga ✅

    Descargar lista de todos los correos en el orden por defecto del dataset de facturación.
    """)
    return


@app.cell
def _(df):
    df[['email']]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Verificar completitud de los correos cruzados.
    """)
    return


@app.cell
def _(df):
    df.info()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Revisar posibles registros que quedaron sin correo.
    """)
    return


@app.cell
def _(df):
    _df_no_emails = df[df['email'].isna()]

    _df_no_emails
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Finalmente, aquellos registros sin correos encontrados (sea el caso) se completarán a través de **encontrarlo en clickhouse por aparte** y completar esa información en el reporte final.
    """)
    return


if __name__ == "__main__":
    app.run()
