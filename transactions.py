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
def _(display, pd):
    df_t = pd.read_csv('./win-work-reports/transactions_data/11-12oct.csv')
    df_c = pd.read_csv('./win-work-reports/transactions_data/userInformation.csv')
    df_t['customer_correlative_id'] = ""

    display(df_t.head(10))
    display(df_c.head())
    return df_c, df_t


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Merging information
    """)
    return


@app.cell
def _(df_c, df_t, display):
    # Join: traer nombre, apellido y correo a través de Subscriber_ID.
    # Del lado de usuarios (df_c) solo tomamos las columnas de contacto,
    # deduplicando por subscriber_id para no multiplicar las filas de transacciones.
    _user_cols = df_c[["subscriber_id", "name", "lastname", "email"]].drop_duplicates(subset="subscriber_id")

    df_ct = (
        df_t.merge(
            _user_cols,
            left_on="Subscriber_ID",
            right_on="subscriber_id",
            how="left",
        )
        .drop(columns=["subscriber_id"])
        .rename(columns={"name": "customer_name", "lastname": "customer_lastname", "email": "customer_email"})
    )

    display(df_ct.head())
    return (df_ct,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Seleccionar información relevante para cruzar con ClickHousee
    """)
    return


@app.cell
def _(df_ct):
    _df = df_ct[['Subscriber_ID', 'customer_correlative_id', 'customer_name', 'customer_lastname', 'customer_email', 'Transaction_Cycle_ID', 'Payment_Processor', 'Transaction_Type']]

    _df.head(10)
    return


@app.cell
def _(df_ct, display):
    display(df_ct['Transaction_Type'].value_counts())
    display(df_ct[df_ct['Transaction_Type'] == 'SUBSCRIPTION_ADDON'])
    display(df_ct[df_ct['Transaction_Type'] == 'SUBSCRIPTION_TRIAL'])
    return


if __name__ == "__main__":
    app.run()
