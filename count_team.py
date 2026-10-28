import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import polars as pl
    import unicodedata

    return mo, pd, pl, unicodedata


@app.cell
def _(pd):
    df = pd.read_csv('win-work-reports/data/crm/2026-10-27.csv')

    df.head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Conteo de equipos favoritos en total de usuarios segmentado por usuarios activos contra inactivos
    """)
    return


@app.cell
def _(unicodedata):
    RUTA_CRM = "win-work-reports/data/crm/2026-10-27.csv"

    ETIQUETA_SIN_EQUIPO = "Sin equipo"

    ESTADOS = ["Activo", "Inactivo"]

    # Valores que no identifican un equipo real
    _VALORES_NO_EQUIPO = {"", "#nd", "none", "null", "nan", "n/a", "na", "-"}

    # Sinónimos a fusionar (clave = nombre normalizado en minúsculas y sin acentos)
    ALIAS_EQUIPOS = {
        "junior de barranquilla": "junior",
        "junior fc": "junior",
        "millonarios fc": "millonarios",
        "cucuta": "cucuta deportivo",
        "nacional": "atletico nacional",
        "nac": "atletico nacional",
        "internacional fc": "internacional bogota",
    }

    # Ortografía oficial a mostrar, cuando conviene fijarla a mano
    NOMBRE_PREFERIDO = {
        "atletico nacional": "Atlético Nacional",
        "america de cali": "América de Cali",
        "atletico bucaramanga": "Atlético Bucaramanga",
        "atletico fc": "Atlético FC",
        "atletico huila": "Atlético Huila",
        "independiente medellin": "Independiente Medellín",
        "cucuta deportivo": "Cúcuta Deportivo",
        "bogota fc": "Bogotá FC",
        "aguilas doradas": "Águilas Doradas",
        "jaguares de cordoba": "Jaguares de Córdoba",
        "boyaca chico": "Boyacá Chicó",
        "union magdalena": "Unión Magdalena",
        "patriotas boyaca": "Patriotas Boyacá",
        "deportes quindio": "Deportes Quindío",
    }


    def normalizar_equipo(valor) -> str:
        """Devuelve la clave canónica de un equipo favorito.

        Quita espacios sobrantes, acentos y mayúsculas, fusiona sinónimos de
        ALIAS_EQUIPOS y devuelve una cadena vacía cuando el valor no es un equipo.
        """
        if valor is None:
            return ""
        texto = "".join(
            c
            for c in unicodedata.normalize("NFKD", str(valor).strip().lower())
            if not unicodedata.combining(c)
        )
        texto = " ".join(texto.split())
        if texto in _VALORES_NO_EQUIPO:
            return ""
        return ALIAS_EQUIPOS.get(texto, texto)

    return (
        ESTADOS,
        ETIQUETA_SIN_EQUIPO,
        NOMBRE_PREFERIDO,
        RUTA_CRM,
        normalizar_equipo,
    )


@app.cell
def _(
    ESTADOS,
    ETIQUETA_SIN_EQUIPO,
    NOMBRE_PREFERIDO,
    RUTA_CRM,
    mo,
    normalizar_equipo,
    pl,
):
    _crm = pl.read_csv(
        RUTA_CRM,
        columns=["Subscriber_ID", "Equipo_Favorito", "Esta_Activo"],
        schema_overrides={
            "Subscriber_ID": pl.String,
            "Equipo_Favorito": pl.String,
            "Esta_Activo": pl.String,
        },
    ).unique(subset=["Subscriber_ID"], keep="last")

    # 1 fila == 1 usuario; se normaliza el equipo y se traducen los estados
    _claves = [normalizar_equipo(v) for v in _crm["Equipo_Favorito"].to_list()]
    _crudos = [(v or "").strip() for v in _crm["Equipo_Favorito"].to_list()]

    # Nombre a mostrar: el preferido si existe, si no la grafía más frecuente
    _variantes: dict[str, dict[str, int]] = {}
    for _clave, _crudo in zip(_claves, _crudos):
        _grupo = _variantes.setdefault(_clave, {})
        _grupo[_crudo] = _grupo.get(_crudo, 0) + 1

    _nombre_equipo = {
        _clave: (
            ETIQUETA_SIN_EQUIPO
            if _clave == ""
            else NOMBRE_PREFERIDO.get(_clave, max(_grupo, key=_grupo.get))
        )
        for _clave, _grupo in _variantes.items()
    }

    _mapa_equipos = pl.DataFrame(
        {"clave_equipo": list(_nombre_equipo), "Equipo": list(_nombre_equipo.values())}
    )

    usuarios = (
        _crm.with_columns(pl.Series("clave_equipo", _claves, dtype=pl.String))
        .join(_mapa_equipos, on="clave_equipo", how="left")
        .with_columns(
            pl.Series("Equipo_crudo", _crudos, dtype=pl.String),
            pl.when(pl.col("Esta_Activo").str.strip_chars().str.to_lowercase() == "active")
            .then(pl.lit(ESTADOS[0]))
            .when(pl.col("Esta_Activo").str.strip_chars().str.to_lowercase() == "inactive")
            .then(pl.lit(ESTADOS[1]))
            .otherwise(pl.lit("Desconocido"))
            .alias("Estado"),
        )
        .select(["Subscriber_ID", "Equipo_crudo", "Equipo", "Estado"])
    )

    mo.md(
        f"""
    **Base de usuarios** (`{RUTA_CRM}`)

    - Usuarios: **{usuarios.height:,}** (1 fila por `Subscriber_ID`)
    - Grafías crudas de equipo: **{usuarios['Equipo_crudo'].n_unique()}** → normalizadas a **{usuarios['Equipo'].n_unique()}**
    - Estados: **{', '.join(sorted(usuarios['Estado'].unique().to_list()))}**
    """
    )
    usuarios.head(8)
    return (usuarios,)


@app.cell
def _(ESTADOS, pl, usuarios):
    # Conteo de usuarios por equipo favorito, segmentado por estado
    _largo = usuarios.group_by(["Equipo", "Estado"]).agg(pl.len().alias("Usuarios"))

    _cruzado = _largo.pivot(
        on="Estado",
        index="Equipo",
        values="Usuarios",
        aggregate_function="sum",
    )

    # Garantiza que existan las dos columnas de estado aunque falte uno
    for _estado in ESTADOS:
        if _estado not in _cruzado.columns:
            _cruzado = _cruzado.with_columns(pl.lit(0, dtype=pl.Int64).alias(_estado))

    _total_usuarios = usuarios.height

    # Cast en un paso aparte: dentro del mismo with_columns la suma usaría
    # los dtypes originales del pivot (UInt32) y rompería el concat final
    _con_base = (
        _cruzado.with_columns(pl.col(ESTADOS).cast(pl.Int64).fill_null(0))
        .with_columns((pl.col(ESTADOS[0]) + pl.col(ESTADOS[1])).alias("Total"))
        .sort("Total", descending=True)
    )

    conteo_equipos = _con_base.with_columns(
        (pl.col(ESTADOS[0]) / pl.col("Total") * 100).round(2).alias("% Activo"),
        (pl.col(ESTADOS[1]) / pl.col("Total") * 100).round(2).alias("% Inactivo"),
        (pl.col("Total") / _total_usuarios * 100).round(2).alias("% del total"),
    ).select(["Equipo", ESTADOS[0], ESTADOS[1], "Total", "% Activo", "% Inactivo", "% del total"])

    # Fila de cierre con el consolidado general
    _fin_activo = int(conteo_equipos[ESTADOS[0]].sum())
    _fin_inactivo = int(conteo_equipos[ESTADOS[1]].sum())
    _fin_total = _fin_activo + _fin_inactivo

    _fila_total = pl.DataFrame(
        {
            "Equipo": ["TOTAL"],
            ESTADOS[0]: [_fin_activo],
            ESTADOS[1]: [_fin_inactivo],
            "Total": [_fin_total],
            "% Activo": [round(_fin_activo / _fin_total * 100, 2)],
            "% Inactivo": [round(_fin_inactivo / _fin_total * 100, 2)],
            "% del total": [100.0],
        }
    )

    conteo_equipos = pl.concat([conteo_equipos, _fila_total], how="vertical")

    conteo_equipos
    return (conteo_equipos,)


@app.cell
def _(ESTADOS, conteo_equipos, mo, pl, usuarios):
    # Reconciliación: el consolidado debe cuadrar exactamente con la base de usuarios
    _cuerpo = conteo_equipos.filter(pl.col("Equipo") != "TOTAL")

    _suma_act = int(_cuerpo[ESTADOS[0]].sum())
    _suma_inact = int(_cuerpo[ESTADOS[1]].sum())

    _por_estado = usuarios["Estado"].value_counts()
    _esperado = {fila["Estado"]: int(fila["count"]) for fila in _por_estado.to_dicts()}

    # invariantes reales: todo usuario tiene equipo y estado asignados
    _sin_equipo_final = usuarios.filter(pl.col("Equipo").is_null()).height
    _sin_estado = usuarios.filter(pl.col("Estado") == "Desconocido").height

    _verificacion = pl.DataFrame(
        {
            "Chequeo": [
                "Activos + Inactivos == usuarios",
                "Suma de Total por equipo == usuarios",
                "Columna Activo == conteo directo",
                "Columna Inactivo == conteo directo",
                "% del total suma 100",
                "Usuarios sin estado reconocido",
                "Usuarios sin Equipo tras el join",
            ],
            "Esperado": [
                float(usuarios.height),
                float(usuarios.height),
                float(_esperado.get(ESTADOS[0], 0)),
                float(_esperado.get(ESTADOS[1], 0)),
                100.0,
                0.0,
                0.0,
            ],
            "Obtenido": [
                float(_suma_act + _suma_inact),
                float(_cuerpo["Total"].sum()),
                float(_suma_act),
                float(_suma_inact),
                round(float(_cuerpo["% del total"].sum()), 2),
                float(_sin_estado),
                float(_sin_equipo_final),
            ],
        }
    ).with_columns(
        (pl.col("Esperado") == pl.col("Obtenido")).alias("OK"),
        (pl.col("Obtenido") - pl.col("Esperado")).round(2).alias("Diferencia"),
    ).select(["Chequeo", "Esperado", "Obtenido", "Diferencia", "OK"])

    mo.md(
        f"""
    _Dato informativo:_ la normalización fusionó **{usuarios['Equipo_crudo'].n_unique()}**
    grafías distintas del CRM en **{usuarios['Equipo'].n_unique()}** equipos finales.
    """
    )
    _verificacion
    return


@app.cell
def _(mo, pl, usuarios):
    # Auditoría: qué grafía cruda cayó en cada equipo final (permite revisar el criterio)
    auditoria_normalizacion = (
        usuarios.group_by(["Equipo", "Equipo_crudo"])
        .agg(pl.len().alias("Usuarios"))
        .filter(pl.col("Equipo_crudo") != pl.col("Equipo"))
        .sort(["Equipo", "Usuarios"], descending=[False, True])
        .rename({"Equipo_crudo": "Grafía_en_el_CRM"})
    )

    mo.md(
        f"""
    **Normalización aplicada:** {usuarios['Equipo_crudo'].n_unique()} grafías distintas →
    {usuarios['Equipo'].n_unique()} equipos.
    Solo se listan las grafías que **no** coinciden con el nombre final
    (acentos corregidos + alias fusionados).
    """
    )
    auditoria_normalizacion
    return


@app.cell
def _(conteo_equipos, mo, pl):
    # Barras apiladas Activo/Inactivo para los equipos más grandes.
    # HTML puro porque altair no está instalado en este entorno.
    _top = conteo_equipos.filter(pl.col("Equipo") != "TOTAL").head(15).to_dicts()
    _tope = max(int(f["Total"]) for f in _top) if _top else 1

    _plantilla = (
        "<div style='margin:6px 0'>"
        "<div style='display:flex;justify-content:space-between;font-size:12px'>"
        "<span><b>{equipo}</b></span>"
        "<span>{total:,} us · {pct:.1f}% activos</span>"
        "</div>"
        "<div style='display:flex;height:14px;background:#f2f2f2;border-radius:3px;overflow:hidden'>"
        "<div title='Activo: {activo:,}' style='width:{ancho_a:.2f}%;background:#1b9e77'></div>"
        "<div title='Inactivo: {inactivo:,}' style='width:{ancho_i:.2f}%;background:#d95f02'></div>"
        "</div></div>"
    )

    _barras = []
    for _f in _top:
        _barras.append(
            _plantilla.format(
                equipo=_f["Equipo"],
                total=int(_f["Total"]),
                pct=float(_f["% Activo"]),
                activo=int(_f["Activo"]),
                inactivo=int(_f["Inactivo"]),
                ancho_a=100 * int(_f["Activo"]) / _tope,
                ancho_i=100 * int(_f["Inactivo"]) / _tope,
            )
        )

    _grafico = mo.Html(
        "<div style='max-width:760px'>"
        "<div style='font-size:13px;margin-bottom:8px'>"
        "<span style='color:#1b9e77'>&#9632;</span> Activo &nbsp;&nbsp;"
        "<span style='color:#d95f02'>&#9632;</span> Inactivo"
        "</div>"
        + "".join(_barras)
        + "<div style='font-size:11px;color:#777;margin-top:6px'>"
        "Ancho relativo al equipo con más usuarios.</div></div>"
    )

    mo.md("### Top 15 equipos favoritos · activos vs inactivos")
    _grafico
    return


@app.cell
def _(ESTADOS, ETIQUETA_SIN_EQUIPO, conteo_equipos, mo, pl):
    # Resumen ejecutivo leído de la fila TOTAL del conteo
    _total = conteo_equipos.filter(pl.col("Equipo") == "TOTAL").to_dicts()[0]

    _top5_df = conteo_equipos.filter(pl.col("Equipo") != "TOTAL").head(5)
    _top5 = [
        f"{r['Equipo']} — {int(r['Total']):,} usuarios ({r['% Activo']}% activos)"
        for r in _top5_df.to_dicts()
    ]

    _con_equipo = conteo_equipos.filter(
        (pl.col("Equipo") != "TOTAL") & (pl.col("Equipo") != ETIQUETA_SIN_EQUIPO)
    )
    _sin_equipo = conteo_equipos.filter(pl.col("Equipo") == ETIQUETA_SIN_EQUIPO)

    _pct_sin_equipo = float(_sin_equipo["% Activo"][0]) if _sin_equipo.height else 0.0
    _pct_con_equipo = round(
        100 * int(_con_equipo[ESTADOS[0]].sum()) / int(_con_equipo["Total"].sum()), 2
    )

    mo.md(
        f"""
    ### Resultado: equipos favoritos por estado del usuario

    | Indicador | Valor |
    | --- | --- |
    | Usuarios analizados | **{_total["Total"]:,}** |
    | Activos | **{_total["Activo"]:,}** ({_total["% Activo"]}%) |
    | Inactivos | **{_total["Inactivo"]:,}** ({_total["% Inactivo"]}%) |
    | Equipos reportados | **{conteo_equipos.height - 1}** |
    | Tasa de actividad · con equipo | **{_pct_con_equipo}%** |
    | Tasa de actividad · sin equipo | **{_pct_sin_equipo}%** |

    **Top 5 por volumen**

    {chr(10).join(f"{i}. {t}" for i, t in enumerate(_top5, start=1))}

    _Lectura:_ el segmento que **no** registra equipo favorito es bastante más activo
    ({_pct_sin_equipo}% de activos frente a {_pct_con_equipo}% entre quienes sí lo registran).
    El campo `Equipo_Favorito` se captura mejor en el flujo de altas activas que en la
    base dormida, así que el ranking por equipo está dominado por usuarios inactivos.
    """
    )
    return


if __name__ == "__main__":
    app.run()
