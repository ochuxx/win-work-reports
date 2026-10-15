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
    df_t['purchasepayment_product_type'] = ""
    df_t['purchasepayment_invoice_number'] = ""
    df_t['coupon_amount'] = ""
    df_t['customer_integrator_id'] = ""
    df_t['customerpurchase_reseller'] = ""

    display(df_t.head(10))
    display(df_c.head())
    return df_c, df_t


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Merging & cleaning information
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

    # Combinar información de los tipos productos adquiridos
    df_ct['customerpurchase_product_id'] = df_ct['Plan_SKU'].combine_first(df_ct['AddOn_SKU01'])
    df_ct['customerpurchase_product_name'] = df_ct['Plan_SKU_Name'].combine_first(df_ct['AddOn_SKU01_Name'])

    # Combinar fecha de expiración
    df_ct['customerpurchase_valid_until'] = df_ct['Plan_SKU_Expiration'].combine_first(df_ct['AddOn_SKU01_Expiration'])

    # Dejar precios null en ceros
    df_ct['Local_Amount'] = df_ct['Local_Amount'].astype(float)
    df_ct['Local_Amount'] = df_ct['Local_Amount'].fillna(0)

    # Columna refuned y su verificación
    df_ct['purchasepayment_partial_refund'] = ''
    df_ct.loc[df_ct['Transaction_Status'] == 'REFUNDED', 'purchasepayment_partial_refund'] = 'REFUNDED'

    # customerpurchase_id basado únicamente en tipo SUSCRIPTION o SUSCRIPTION_TRIAL
    df_ct['customerpurchase_id'] = ""
    df_ct.loc[((df_ct['Transaction_Type'] == 'SUBSCRIPTION') | (df_ct['Transaction_Type'] == 'SUBSCRIPTION_TRIAL')), 'customerpurchase_id'] = df_ct['Transaction_Cycle_ID']

    # purchasepayment_date_created basado únicamente en tipo SUSCRIPTION o SUSCRIPTION_TRIAL
    df_ct['purchasepayment_date_created'] = ""
    df_ct.loc[((df_ct['Transaction_Type'] == 'SUBSCRIPTION') | (df_ct['Transaction_Type'] == 'SUBSCRIPTION_TRIAL')), 'purchasepayment_date_created'] = df_ct['Datetime']


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
    _df = df_ct[['Subscriber_ID', 'customer_correlative_id', 'customer_name', 'customer_lastname', 'customer_email', 'customerpurchase_id', 'Payment_Processor', 'Transaction_Type', 'purchasepayment_date_created', 'Country_Code', 'purchasepayment_product_type', 'customerpurchase_product_id', 'customerpurchase_product_name', 'Local_Amount', 'purchasepayment_partial_refund', 'Local_Currency', 'Transaction_Status', 'purchasepayment_invoice_number', 'Transaction_Cycle_ID', 'Transaction_External_ID', 'Payment_Method_ID', 'Coupon_Promotion_Name', 'Coupon_Code', 'Coupon_Discount_Type', 'coupon_amount', 'Datetime', 'customerpurchase_valid_until', 'customer_integrator_id', 'customerpurchase_reseller']]

    _df.head(10)

    df_adjust_t = _df.copy()

    df_adjust_t.head(5)
    return (df_adjust_t,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Transforming information
    """)
    return


@app.cell
def _(df_adjust_t, pd):
    # Renombrar algunas columnas
    df_clean_t = df_adjust_t.copy()

    df_clean_t = df_clean_t.rename(
        columns={
            'Subscriber_ID': 'customer_id',
            'customer_name': 'customer_first_name',
            'customer_lastname': 'customer_last_name',
            'Payment_Processor': 'customerpurchase_gateway',
            'Transaction_Type': 'customerpurchase_type',
            'purchasepayment_date_created': 'customerpurchase_date_created',
            'Country_Code': 'customerpurchase_country_code',
            'Local_Amount': 'purchasepayment_amount',
            'Local_Currency': 'purchasepayment_currency',
            'Transaction_Status': 'purchasepayment_status',
            'Transaction_Cycle_ID': 'purchasepayment_id',
            'Transaction_External_ID': 'purchasepayment_transaction_id',
            'Payment_Method_ID': 'purchasepayment_payment_method',
            'Coupon_Promotion_Name': 'coupongroup_name',
            'Coupon_Code': 'coupon_code',
            'Coupon_Discount_Type': 'coupon_percent_or_day',
            'Datetime': 'purchasepayment_date_created'
        }
    )

    # Cambiar nombre de valores
    df_clean_t.loc[df_clean_t['customerpurchase_gateway'] == 'affiliates', 'customerpurchase_gateway'] = 'platform'
    df_clean_t.loc[df_clean_t['customerpurchase_gateway'] == 'freepass', 'customerpurchase_gateway'] = 'coupon'

    # Convertir fechas str a datetime
    df_clean_t["customerpurchase_date_created"] = df_clean_t["customerpurchase_date_created"].str[:-5]
    df_clean_t["purchasepayment_date_created"] = df_clean_t["purchasepayment_date_created"].str[:-5]
    df_clean_t["customerpurchase_valid_until"] = df_clean_t["customerpurchase_valid_until"].str[:-5]
    df_clean_t["customerpurchase_date_created"] = pd.to_datetime(df_clean_t["customerpurchase_date_created"])
    df_clean_t["purchasepayment_date_created"] = pd.to_datetime(df_clean_t["purchasepayment_date_created"])
    df_clean_t["customerpurchase_valid_until"] = pd.to_datetime(df_clean_t["customerpurchase_valid_until"])

    df_clean_t.head(20)
    return (df_clean_t,)


@app.cell
def _(df_clean_t, display):
    # display(df_clean_t['customerpurchase_type'].value_counts())
    # display(df_clean_t['purchasepayment_transaction_id'].value_counts())
    # display(df_clean_t['purchasepayment_partial_refund'].value_counts())
    # display(df_clean_t['customerpurchase_gateway'].value_counts())
    # df_count = pd.DataFrame(df_clean_t['purchasepayment_amount'].value_counts())
    # df_count = df_count.sort_values('purchasepayment_amount')
    # display(df_count)
    display(df_clean_t[df_clean_t['customerpurchase_gateway'] == 'access'])

    display(df_clean_t[df_clean_t['customer_id'] == '67b106de199c1a00081096d4'])

    print(df_clean_t.info())
    print(df_clean_t['purchasepayment_date_created'].min())
    print(df_clean_t['purchasepayment_date_created'].max())
    return


if __name__ == "__main__":
    app.run()
