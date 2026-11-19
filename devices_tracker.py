import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from IPython.display import display

    return display, pl


@app.cell
def _(display, pl):
    df = pl.read_csv('./win-work-reports/data/clicks/02-08-nov.csv')

    display(df.head())
    return (df,)


@app.cell
def _(df, pl):
    _df_baduser = df.filter(pl.col('deviceDescription').str.contains('python'))

    df_baduser_clean = (
        _df_baduser
        .with_columns(
            pl.col('date')
            .str.to_datetime(time_zone='UTC')
            .dt.strftime('%Y-%m-%d %H:%M:%S')
        )
        .select(['date', 'network', 'subscriberId', 'deviceDescription'])
    )

    df_baduser_clean
    return


if __name__ == "__main__":
    app.run()
