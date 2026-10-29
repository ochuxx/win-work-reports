import marimo

__generated_with = "0.25.2"
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
    return (df,)


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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Conteo por plan · operadores y suscripciones
    """)
    return


@app.cell
def _(unicodedata):
    # --- Conteo por plan: configuración y normalización de Nombre_Plan ---

    ETIQUETA_SIN_PLAN = "Sin plan"
    ETIQUETA_EVENTO = "Eventos y partidos (PPV)"
    ETIQUETA_SIN_CLASIFICAR = "Sin clasificar"
    FAMILIA_PLAN_OTROS = "Otros planes de suscripción"

    # Valores del CRM que no identifican un plan real
    _VALORES_NO_PLAN = {"", "#nd", "none", "null", "nan", "n/a", "na", "-"}

    # Códigos del CRM → (Plan visible, Familia de negocio)
    PLANES_CONOCIDOS = {
        "sku_win_cla_prepago_7d": ("Claro prepago 7 días", "Claro prepago (por días)"),
        "sku_win_cla_prepago_10d": ("Claro prepago 10 días", "Claro prepago (por días)"),
        "sku_win_cla_prepago_15d": ("Claro prepago 15 días", "Claro prepago (por días)"),
        "sku_win_cla_prepago_20d": ("Claro prepago 20 días", "Claro prepago (por días)"),
        "sku_win_cla_prepago_30d": ("Claro prepago 30 días", "Claro prepago (por días)"),
        "sku_appwin_premium_tigo": ("App Win Premium Tigo", "App Win por operador"),
        "sku_appwin_claro": ("App Win Claro", "App Win por operador"),
        "sku_appwin_tigo": ("App Win Tigo", "App Win por operador"),
        "sku_appwin_etb": ("App Win ETB", "App Win por operador"),
        "sku_appwin_prensa": ("App Win Prensa", "App Win por operador"),
        "sku_movistar_winplay_7d": ("Movistar Winplay 7 días", "Movistar Winplay"),
        "sku_movistar_winplay_30d": ("Movistar Winplay 30 días", "Movistar Winplay"),
        "winplay_incluido_full": ("Winplay Incluido Full", "Winplay incluido por operador"),
        "winplay_incluido_lite": ("Winplay Incluido Lite", "Winplay incluido por operador"),
        "winplay_standalone_full": ("Winplay Standalone Full", "Winplay directo (standalone)"),
        "contenido gratuito": ("Contenido Gratuito", "Contenido gratuito"),
    }

    # Nivel del plan para los planes propios ("Plan ...")
    FAMILIAS_PLAN = (
        ("estandar", "Plan Estándar"),
        ("win lite", "Plan Win Lite"),
        ("lite", "Plan Win Lite"),
        ("familiar", "Plan Familiar"),
    )

    # Palabras que delatan una compra de evento suelto y no una suscripción
    _MARCAS_EVENTO = {
        "liga", "betplay", "dimayor", "copa", "superliga", "supercopa", "recopa",
        "final", "semifinal", "cuartos", "octavos", "ronda", "grupo", "fecha",
        "jornada", "ida", "vuelta", "vs", "mundial", "clubes", "torneo",
        "eliminatorias", "amistoso", "clasico", "playoff",
    }


    def normalizar_plan(valor) -> tuple[str, str]:
        """Devuelve (Plan, Familia) para un Nombre_Plan del CRM.

        Los códigos SKU se traducen con PLANES_CONOCIDOS, los planes propios se
        agrupan por nivel y el resto de nombres con vocabulario deportivo se
        consideran compras de evento suelto (PPV).
        """
        crudo = " ".join(str(valor or "").split())
        clave = "".join(
            c
            for c in unicodedata.normalize("NFKD", crudo.lower())
            if not unicodedata.combining(c)
        )
        if clave in _VALORES_NO_PLAN:
            return (ETIQUETA_SIN_PLAN, ETIQUETA_SIN_PLAN)
        if clave in PLANES_CONOCIDOS:
            return PLANES_CONOCIDOS[clave]
        if clave.startswith("plan"):
            for _token, _familia in FAMILIAS_PLAN:
                if _token in clave:
                    return (crudo, _familia)
            return (crudo, FAMILIA_PLAN_OTROS)
        fichas = set("".join(c if c.isalnum() else " " for c in clave).split())
        if fichas & _MARCAS_EVENTO:
            return (crudo, ETIQUETA_EVENTO)
        return (crudo, ETIQUETA_SIN_CLASIFICAR)

    return (
        ETIQUETA_EVENTO,
        ETIQUETA_SIN_CLASIFICAR,
        ETIQUETA_SIN_PLAN,
        normalizar_plan,
    )


@app.cell
def _(ESTADOS, pl):
    def conteo_por_segmento(base: pl.DataFrame, columna: str) -> pl.DataFrame:
        """Cruza `columna` con el estado del usuario (Activo/Inactivo).

        Devuelve una tabla ordenada por Total con porcentajes de actividad,
        participación sobre la base y una última fila TOTAL con el consolidado.
        """
        _largo = base.group_by([columna, "Estado"]).agg(pl.len().alias("Usuarios"))

        _cruzado = _largo.pivot(
            on="Estado",
            index=columna,
            values="Usuarios",
            aggregate_function="sum",
        )

        # Garantiza que existan las dos columnas de estado aunque falte uno
        for _estado in ESTADOS:
            if _estado not in _cruzado.columns:
                _cruzado = _cruzado.with_columns(pl.lit(0, dtype=pl.Int64).alias(_estado))

        # Cast en un paso aparte: dentro del mismo with_columns la suma usaría
        # los dtypes originales del pivot (UInt32) y rompería el concat final
        _cuerpo = (
            _cruzado.with_columns(pl.col(ESTADOS).cast(pl.Int64).fill_null(0))
            .with_columns((pl.col(ESTADOS[0]) + pl.col(ESTADOS[1])).alias("Total"))
            .sort("Total", descending=True)
        )

        _cuerpo = _cuerpo.with_columns(
            (pl.col(ESTADOS[0]) / pl.col("Total") * 100).round(2).alias("% Activo"),
            (pl.col(ESTADOS[1]) / pl.col("Total") * 100).round(2).alias("% Inactivo"),
            (pl.col("Total") / base.height * 100).round(2).alias("% del total"),
        ).select(
            [columna, ESTADOS[0], ESTADOS[1], "Total", "% Activo", "% Inactivo", "% del total"]
        )

        _fin_act = int(_cuerpo[ESTADOS[0]].sum())
        _fin_inact = int(_cuerpo[ESTADOS[1]].sum())
        _fin_total = _fin_act + _fin_inact

        _fila_total = pl.DataFrame(
            {
                columna: ["TOTAL"],
                ESTADOS[0]: [_fin_act],
                ESTADOS[1]: [_fin_inact],
                "Total": [_fin_total],
                "% Activo": [round(_fin_act / _fin_total * 100, 2)],
                "% Inactivo": [round(_fin_inact / _fin_total * 100, 2)],
                "% del total": [100.0],
            }
        )

        return pl.concat([_cuerpo, _fila_total], how="vertical")

    return (conteo_por_segmento,)


@app.cell
def _(ETIQUETA_EVENTO, RUTA_CRM, mo, normalizar_plan, pl, usuarios):
    _planes_crudos = pl.read_csv(
        RUTA_CRM,
        columns=["Subscriber_ID", "Nombre_Plan"],
        schema_overrides={"Subscriber_ID": pl.String, "Nombre_Plan": pl.String},
    ).unique(subset=["Subscriber_ID"], keep="last")

    # 1 fila == 1 usuario; el Estado se toma de `usuarios` para que ambos
    # análisis (equipos y planes) partan del mismo universo
    _pares_plan = [normalizar_plan(v) for v in _planes_crudos["Nombre_Plan"].to_list()]

    usuarios_planes = (
        _planes_crudos.rename({"Nombre_Plan": "Plan_crudo"})
        .with_columns(
            pl.Series("Plan", [p for p, _ in _pares_plan], dtype=pl.String),
            pl.Series("Familia", [f for _, f in _pares_plan], dtype=pl.String),
        )
        .join(usuarios.select(["Subscriber_ID", "Estado"]), on="Subscriber_ID", how="left")
        .with_columns(pl.col("Estado").fill_null("Desconocido"))
        .select(["Subscriber_ID", "Plan_crudo", "Plan", "Familia", "Estado"])
    )

    # Separar suscripciones (pocos nombres) de eventos sueltos (cola larga de PPV)
    _es_ppv = pl.col("Familia") == ETIQUETA_EVENTO
    _n_eventos = usuarios_planes.filter(_es_ppv)["Plan"].n_unique()
    _n_suscripciones = usuarios_planes.filter(~_es_ppv)["Plan"].n_unique()
    _us_eventos = usuarios_planes.filter(_es_ppv).height

    mo.vstack(
        [
            mo.md(
                f"""
    ## Conteo por `Nombre_Plan` · usuarios Activos vs Inactivos

    - Usuarios: **{usuarios_planes.height:,}** (1 fila por `Subscriber_ID`)
    - Valores crudos del CRM: **{usuarios_planes['Plan_crudo'].n_unique()}**, que caen en
      **{_n_suscripciones}** planes de suscripción y **{_n_eventos}** eventos sueltos (PPV)
    - Esas {usuarios_planes['Plan_crudo'].n_unique()} grafías se consolidan en
      **{usuarios_planes['Familia'].n_unique()}** familias de plan / operador
    - Compradores de eventos: **{_us_eventos:,}** usuarios
    """
            ),
            usuarios_planes.head(8),
        ]
    )
    return (usuarios_planes,)


@app.cell
def _(conteo_por_segmento, mo, usuarios_planes):
    # Conteo de usuarios por Nombre_Plan (nivel de detalle)
    conteo_planes = conteo_por_segmento(usuarios_planes, "Plan")

    mo.vstack(
        [
            mo.md(
                f"""
    ### Detalle por plan

    **{conteo_planes.height - 1}** planes (incluye cada compra de evento suelto).
    La columna **% Activo** es la tasa de usuarios activos dentro del plan.
    """
            ),
            conteo_planes,
        ]
    )
    return (conteo_planes,)


@app.cell
def _(conteo_por_segmento, mo, usuarios_planes):
    # Consolidado por familia de plan (opera como "conteo de operadores")
    conteo_familias = conteo_por_segmento(usuarios_planes, "Familia")

    mo.vstack(
        [
            mo.md(
                f"""
    ### Consolidado por familia de plan / operador

    {conteo_familias.height - 1} familias. **% del total** mide el peso de la familia
    sobre los {usuarios_planes.height:,} usuarios; **% Activo**, su tasa de actividad.
    """
            ),
            conteo_familias,
        ]
    )
    return (conteo_familias,)


@app.cell
def _(
    ESTADOS,
    ETIQUETA_SIN_CLASIFICAR,
    conteo_familias,
    conteo_planes,
    mo,
    pl,
    usuarios,
    usuarios_planes,
):
    # Reconciliación: el conteo por plan debe cuadrar con la base y con el análisis de equipos
    _cuerpo_detalle = conteo_planes.filter(pl.col("Plan") != "TOTAL")
    _cuerpo_familias = conteo_familias.filter(pl.col("Familia") != "TOTAL")

    _esperado_estado = {
        fila["Estado"]: int(fila["count"])
        for fila in usuarios_planes["Estado"].value_counts().to_dicts()
    }

    _sin_clasificar = usuarios_planes.filter(pl.col("Familia") == ETIQUETA_SIN_CLASIFICAR).height
    _sin_estado_plan = usuarios_planes.filter(pl.col("Estado") == "Desconocido").height
    _desvio_pct = round(abs(float(_cuerpo_detalle["% del total"].sum()) - 100.0), 2)

    verificacion_planes = pl.DataFrame(
        {
            "Chequeo": [
                "Activos + Inactivos == usuarios",
                "Suma Total del detalle == usuarios",
                "Suma Total por familia == usuarios",
                "Detalle == Familia (mismos usuarios)",
                "Columna Activo == conteo directo",
                "Columna Inactivo == conteo directo",
                "Base de planes == base de equipos",
                "Usuarios sin Estado conocido",
                "Valores crudos sin clasificar",
                "% del total suma 100 (redondeo)",
            ],
            "Esperado": [
                float(usuarios_planes.height),
                float(usuarios_planes.height),
                float(usuarios_planes.height),
                float(_cuerpo_detalle["Total"].sum()),
                float(_esperado_estado.get(ESTADOS[0], 0)),
                float(_esperado_estado.get(ESTADOS[1], 0)),
                float(usuarios.height),
                0.0,
                0.0,
                100.0,
            ],
            "Obtenido": [
                float(int(_cuerpo_detalle[ESTADOS[0]].sum()) + int(_cuerpo_detalle[ESTADOS[1]].sum())),
                float(_cuerpo_detalle["Total"].sum()),
                float(_cuerpo_familias["Total"].sum()),
                float(_cuerpo_familias["Total"].sum()),
                float(_cuerpo_detalle[ESTADOS[0]].sum()),
                float(_cuerpo_detalle[ESTADOS[1]].sum()),
                float(usuarios_planes.height),
                float(_sin_estado_plan),
                float(_sin_clasificar),
                round(float(_cuerpo_detalle["% del total"].sum()), 2),
            ],
            "Tolerancia": [
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                # con 374 filas el %.2f acumula hasta ~0.35 pp de pérdida
                0.5,
            ],
        }
    ).with_columns(
        (pl.col("Obtenido") - pl.col("Esperado")).round(2).alias("Diferencia"),
    ).with_columns(
        (pl.col("Diferencia").abs() <= pl.col("Tolerancia")).alias("OK"),
    ).select(["Chequeo", "Esperado", "Obtenido", "Diferencia", "Tolerancia", "OK"])

    mo.vstack(
        [
            mo.md(
                f"""
    _Todos los chequeos deben estar en `true`. El único tolerado con desvío es la suma de
    **% del total** ({_desvio_pct} pp por redondeo de 2 decimales en {_cuerpo_detalle.height} filas).
    """
            ),
            verificacion_planes,
        ]
    )
    return


@app.cell
def _(ETIQUETA_SIN_CLASIFICAR, mo, pl, usuarios_planes):
    # Auditoría: qué valor crudo del CRM cayó en cada plan final
    auditoria_planes = (
        usuarios_planes.group_by(["Familia", "Plan", "Plan_crudo"])
        .agg(pl.len().alias("Usuarios"))
        .filter(
            (pl.col("Plan_crudo") != pl.col("Plan"))
            | (pl.col("Familia") == ETIQUETA_SIN_CLASIFICAR)
        )
        .sort(["Usuarios"], descending=True)
        .rename({"Plan_crudo": "Valor_en_el_CRM"})
    )

    # Compresión de nombres por familia (374 valores crudos → pocas categorías)
    _resumen_familias = (
        usuarios_planes.group_by("Familia")
        .agg(
            pl.len().alias("Usuarios"),
            pl.col("Plan_crudo").n_unique().alias("Valores_en_el_CRM"),
            pl.col("Plan").n_unique().alias("Planes_mostrados"),
        )
        .sort("Usuarios", descending=True)
    )

    mo.vstack(
        [
            mo.md(
                f"""
    **Traducciones aplicadas:** solo se listan los valores del CRM que cambiaron de nombre
    o que no se reconocieron como plan. Los eventos (PPV) se agrupan en una familia aparte.
    """
            ),
            _resumen_familias,
            auditoria_planes,
        ]
    )
    return


@app.cell
def _(conteo_familias, mo, pl):
    # Barras apiladas Activo/Inactivo por familia de plan (HTML puro: altair no está instalado)
    _familias_plot = conteo_familias.filter(pl.col("Familia") != "TOTAL").to_dicts()
    _tope_fam = max(int(f["Total"]) for f in _familias_plot) if _familias_plot else 1

    _plantilla_fam = (
        "<div style='margin:6px 0'>"
        "<div style='display:flex;justify-content:space-between;font-size:12px'>"
        "<span><b>{familia}</b></span>"
        "<span>{total:,} us · {pct:.2f}% activos</span>"
        "</div>"
        "<div style='display:flex;height:14px;background:#f2f2f2;border-radius:3px;overflow:hidden'>"
        "<div title='Activo: {activo:,}' style='width:{ancho_a:.2f}%;background:#1b9e77'></div>"
        "<div title='Inactivo: {inactivo:,}' style='width:{ancho_i:.2f}%;background:#d95f02'></div>"
        "</div></div>"
    )

    _barras_fam = [
        _plantilla_fam.format(
            familia=_f["Familia"],
            total=int(_f["Total"]),
            pct=float(_f["% Activo"]),
            activo=int(_f["Activo"]),
            inactivo=int(_f["Inactivo"]),
            ancho_a=100 * int(_f["Activo"]) / _tope_fam,
            ancho_i=100 * int(_f["Inactivo"]) / _tope_fam,
        )
        for _f in _familias_plot
    ]

    _grafico_planes = mo.Html(
        "<div style='max-width:760px'>"
        "<div style='font-size:13px;margin-bottom:8px'>"
        "<span style='color:#1b9e77'>&#9632;</span> Activo &nbsp;&nbsp;"
        "<span style='color:#d95f02'>&#9632;</span> Inactivo"
        "</div>"
        + "".join(_barras_fam)
        + "<div style='font-size:11px;color:#777;margin-top:6px'>"
        "Ancho relativo a la familia con más usuarios.</div></div>"
    )

    mo.md("### Familias de plan · activos vs inactivos")
    _grafico_planes
    return


@app.cell
def _(
    ESTADOS,
    ETIQUETA_EVENTO,
    ETIQUETA_SIN_PLAN,
    conteo_familias,
    conteo_planes,
    mo,
    pl,
):
    # Resumen ejecutivo del análisis por plan (cifras leídas de las tablas, sin texto fijo)
    _tot_plan = conteo_familias.filter(pl.col("Familia") == "TOTAL").to_dicts()[0]

    _top5_plan = [
        f"{_r['Plan']} — {int(_r['Total']):,} usuarios ({_r['% Activo']}% activos)"
        for _r in conteo_planes.filter(pl.col("Plan") != "TOTAL").head(5).to_dicts()
    ]

    # Familias comparables: se excluye el consolidado, "Sin plan" y el PPV (no son suscripciones)
    _fam_suscripcion = conteo_familias.filter(
        ~pl.col("Familia").is_in(["TOTAL", ETIQUETA_SIN_PLAN, ETIQUETA_EVENTO])
    )
    _fam_por_tasa = _fam_suscripcion.filter(pl.col("Total") >= 1000).sort("% Activo", descending=True)
    _mas_activo = _fam_por_tasa.to_dicts()[0]
    _menos_activo = _fam_por_tasa.to_dicts()[-1]

    _sin_plan = conteo_familias.filter(pl.col("Familia") == ETIQUETA_SIN_PLAN)
    _con_plan = conteo_familias.filter(~pl.col("Familia").is_in(["TOTAL", ETIQUETA_SIN_PLAN]))

    _pct_con_plan = round(
        100 * int(_con_plan[ESTADOS[0]].sum()) / int(_con_plan["Total"].sum()), 2
    )
    _pct_sin_plan = float(_sin_plan["% Activo"][0]) if _sin_plan.height else 0.0
    _ppv = conteo_familias.filter(pl.col("Familia") == ETIQUETA_EVENTO).to_dicts()[0]

    mo.md(
        f"""
    ### Resultado: planes por estado del usuario

    | Indicador | Valor |
    | --- | --- |
    | Usuarios analizados | **{_tot_plan['Total']:,}** |
    | Planes detectados (detalle) | **{conteo_planes.height - 1}** |
    | Familias de plan | **{conteo_familias.height - 1}** |
    | Con plan registrado | **{int(_con_plan['Total'].sum()):,}** usuarios ({_pct_con_plan}% activos) |
    | Sin plan registrado | **{int(_sin_plan['Total'][0]):,}** usuarios ({_pct_sin_plan}% activos) |
    | Compran solo eventos (PPV) | **{int(_ppv['Total']):,}** usuarios ({_ppv['% Activo']}% activos) |
    | Familia más grande | **{conteo_familias.filter(pl.col('Familia') != 'TOTAL')['Familia'][0]}** |
    | Mayor tasa de actividad | **{_mas_activo['Familia']}** ({_mas_activo['% Activo']}%) |
    | Menor tasa de actividad | **{_menos_activo['Familia']}** ({_menos_activo['% Activo']}%) |

    **Top 5 planes por volumen**

    {chr(10).join(f"{_i}. {_t}" for _i, _t in enumerate(_top5_plan, start=1))}

    _Lectura:_ la cartera inactiva se concentra en **{_menos_activo['Familia']}**
    ({_menos_activo['% Activo']}% activos), mientras que
    **{_mas_activo['Familia']}** es la familia con mejor retención
    ({_mas_activo['% Activo']}% activos). El grupo **Sin plan**
    ({int(_sin_plan['Total'][0]):,} usuarios) está prácticamente todo inactivo
    ({_pct_sin_plan}%): son altas sin suscripción vigente, no una cartera recuperable vía plan.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Normal consolidado operadores
    """)
    return


@app.cell
def _(df):
    _filter = (df['Esta_Activo'] == 'ACTIVE')
    _df_actives = df[_filter]
    _df_actives['Nombre_Plan'].value_counts()
    return


if __name__ == "__main__":
    app.run()
