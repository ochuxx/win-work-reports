import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo # Relevante para usar .py como .ipynb
    import polars as pl
    from IPython.display import display

    return display, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Importar datos
    """)
    return


@app.cell
def _(pl):
    df_sep = pl.read_parquet('./streaming_data/LIVE_2026-09.parquet')
    df_oct = pl.read_parquet('./streaming_data/LIVE_2026-10.parquet')
    return (df_sep,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Verificación
    """)
    return


@app.cell
def _(df_sep, display):
    display(df_sep.head())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Normalizar información

    Se normaliza la info para guardar correctamente el conteo de views y la concurrencia de reproducción.
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
