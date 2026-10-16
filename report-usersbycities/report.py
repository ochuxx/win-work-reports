import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from IPython.display import display

    return display, mo, pd


@app.cell
def _(pd):
    df = pd.read_csv('./win-work-reports/data/2026-10-15-cities.csv')

    df.head()
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Filtrar para conteo
    """)
    return


@app.cell
def _(df, display):
    df_filt = df.copy()
    df_filt['department'] = df_filt['department'].str.upper()
    df_filt = df_filt[(df_filt['department'] == 'SANTANDER') & (df_filt['Esta_Activo'] == 'ACTIVE')]

    display(df_filt['city'].value_counts())
    display(df_filt['department'].value_counts())
    return


if __name__ == "__main__":
    app.run()
