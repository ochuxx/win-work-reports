"""Motor de medición de Win Play (Win Sports / Win Sports Premium) — 2026.

Replica la metodología de `dsports.py` (vistas por señal + reescalado + concurrencia
con ventana rodante) y la aplica a la **parrilla de programación SIC** para calcular
las 10 métricas de negocio por programa.

Diseño
------
* Todo el escaneo de los 10 parquet (~67M eventos) es **lazy y por mes**:
  `scan_parquet` → filtros/proyecciones → `collect()` de frames *pequeños*.
  Nunca se materializa el dataset completo; en el peor caso coexisten **dos meses**
  de presencia (el propio y el siguiente, para la cola de diferido).
* La concurrencia usa la **ventana rodante de 25 min** del proveedor:
  un stream está activo en `t` si tuvo algún evento en `[t-24 min, t]`.
* Una **visualización** (view) es una sesión de reproducción: mismo
  `SubscriberID + Title + SeriesTitle + deviceDescription` con inactividad
  mayor a `session_minutes` (20 min) marca una sesión nueva. El corte es **por día**,
  igual que en `dsports.py`.
* El **reescalado** reparte el total real de vistas de cada día entre las señales
  (o entre los programas de la parrilla) según su porcentaje, con el método del
  **residuo mayor**: la suma diaria es exactamente el total real medido.
"""

from __future__ import annotations

import datetime as dt
import re
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

import polars as pl

__all__ = [
    "procesar",
    "load_grid",
    "grid_programs",
    "norm_text",
    "canon_team",
    "parse_title",
    "jornada_num",
    "with_signal",
    "engine_version",
]


def engine_version() -> str:
    """Hash del propio módulo.

    Sirve como clave de caché: si el motor cambia, el `mo.cache` del notebook
    se invalida y los datos se recalculan (marimo hashea el *nombre* del módulo,
    no su contenido).
    """
    import hashlib

    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]

# ─────────────────────────────── configuración ───────────────────────────────
DEFAULT_SIGNALS = [
    "WIN+FUTBOL",
    "WIN SPORTS",
    "WIN LITE",
    "WIN AUDIO",
    "DSPORTS",
    "DSPORTS 2",
    "WIN PLAY EVENTOS",
]

SIGNAL_ALIASES = {
    "WIN + FUTBOL": "WIN+FUTBOL",
    "WIN+FUTBOL": "WIN+FUTBOL",
    "WIN SPORTS": "WIN SPORTS",
    "WIN LITE": "WIN LITE",
    "WIN AUDIO": "WIN AUDIO",
    "DSPORTS": "DSPORTS",
    "DSPORTS 2": "DSPORTS 2",
}

# Alias de equipos: el Excel y el catálogo OTT no usan el mismo nombre.
TEAM_ALIASES = {
    "AGUILAS D": "AGUILAS DORADAS",
    "AGUILAS DOARADAS": "AGUILAS DORADAS",
    "RIONEGRO AGUILAS DORADAS": "AGUILAS DORADAS",
    "ALIANZA": "ALIANZA VALLEDUPAR",
    "ALIANZA FC": "ALIANZA VALLEDUPAR",
    "BOCA JUNIOR": "BOCA JUNIORS",
    "BOCA JUNIORS CALI": "BOCA JUNIORS",
    "CUCUTA DEPORTIVO": "CUCUTA",
    "ATL BUCARAMANGA": "BUCARAMANGA",
    "INDEPENDIENTE YUMBO": "IND YUMBO",
    "INDEPENDIENTE VALLE": "IND VALLE",
    "IND VALLE DEL CAUCA": "IND VALLE",
    "DEPORTIVO PASTO": "PASTO",
    "DEPORTIVO PEREIRA": "PEREIRA",
}

JORNADA_MAP = {"CU": 8, "CUAD": 8, "CUARTOS": 8, "SEMIFINAL": 4, "FINAL": 2}

GRID_METRIC_COLS = [
    "Stream",
    "Play back",
    "Time (minutos)",
    "Time Averange (minutos)",
    "Peak WSO",
    "Activos",
    "Peso% Stream",
    "peso% peak",
    "Duracion_minutos",
    "% de consumo",
]


# ───────────────────────── normalización de textos / equipos ─────────────────────────
def norm_text(s) -> str:
    """Mayúsculas, sin acentos ni puntuación."""
    if s is None:
        return ""
    s = str(s).upper()
    s = "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )
    s = s.replace("&", " Y ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def canon_team(t) -> str:
    """Nombre canónico de un equipo (alias + prefijos otorgados)."""
    t = norm_text(t)
    t = re.sub(r"\b(DE|DEL|LA|EL|LOS|LAS|FC|CF|CD)\b", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return TEAM_ALIASES.get(t, t)


def canon_tournament(t) -> str:
    """Nombre canónico del torneo (BPD/BETPLAY, DIMAYOR/LIGA, numerales romanos)."""
    t = norm_text(t).replace("BPD", "BETPLAY")
    t = re.sub(r"\bDIMAYOR\b", "LIGA", t)
    t = re.sub(r"\bI\b", "1", t)
    t = re.sub(r"\bII\b", "2", t)
    t = re.sub(r"\bIII\b", "3", t)
    return re.sub(r"\s+", " ", t).strip()


def parse_title(raw: str) -> dict:
    """Extrae torneo, par de equipos y fecha (jornada) de un título OTT."""
    t = str(raw).strip()
    rep = bool(re.match(r"^[\(\s]*REPETICION", t, re.I))
    t = re.sub(r"^[\(\s]*REPETICION[\)\s]*", "", t, flags=re.I)
    m = re.search(r"FECHA\s*(\d+)", t, re.I)
    fecha = int(m.group(1)) if m else None
    head, rest = (t.split(":", 1) + [""])[:2] if ":" in t else ("", t)
    rest = re.sub(r"\([^)]*\)", " ", rest)
    rest = norm_text(rest)
    parts = re.split(r"\s+VS\s+", rest)
    teams = None
    if len(parts) == 2:
        teams = tuple(sorted([canon_team(parts[0]), canon_team(parts[1])]))
    return {
        "repeticion": rep,
        "fecha": fecha,
        "torneo": canon_tournament(head),
        "teams": teams,
        "raw": str(raw),
    }


def jornada_num(j) -> int | None:
    """Jornada de la parrilla → número de fecha del torneo."""
    j = (j or "").upper().strip()
    if not j:
        return None
    return JORNADA_MAP.get(j, int(j) if j.isdigit() else None)


def with_signal(df: pl.DataFrame, col_title: str = "Title") -> pl.DataFrame:
    """Añade la columna `senal`: canal lineal normalizado (vacío si no es señal)."""
    up = pl.col(col_title).str.to_uppercase()
    return df.with_columns(
        pl.when(up.str.starts_with("WIN PLAY EVENTOS"))
        .then(pl.lit("WIN PLAY EVENTOS"))
        .otherwise(up.replace(SIGNAL_ALIASES))
        .alias("senal")
    )


# ─────────────────────────────── parrilla (Excel) ───────────────────────────────
def load_grid(path) -> pl.DataFrame:
    """Lee la hoja `Hoja1` del Excel de programación (21 columnas)."""
    import openpyxl

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontró la parrilla de programación en: {path}\n"
            "Ajusta la variable de entorno WINPLAY_GRID con la ruta correcta."
        )
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb["Hoja1"]
    header = [ws.cell(row=1, column=c).value for c in range(1, 22)]
    rows = [[ws.cell(row=r, column=c).value for c in range(1, 22)] for r in range(2, ws.max_row + 1)]
    return pl.DataFrame(rows, schema=[str(h).strip() for h in header], orient="row")


def grid_programs(grid: pl.DataFrame) -> list[dict]:
    """Normaliza la parrilla a programas con ventana temporal explícita."""
    out = []
    for i, r in enumerate(grid.iter_rows(named=True)):
        fecha, hi, hf = r["Fecha"], r["Hora Inicio"], r["Hora Final"]
        if fecha is None or hi is None or hf is None:
            continue
        start = dt.datetime.combine(fecha, hi)
        end = dt.datetime.combine(fecha, hf)
        if end <= start:
            end += dt.timedelta(days=1)
        out.append(
            {
                "row": i,
                "fecha": fecha,
                "mes": f"{fecha.year}-{fecha.month:02d}",
                "canal": r["Canal"],
                "programa": r["Programa"],
                "tipo": r["Tipo Programa"],
                "jornada": str(r["JORNADA"]).strip() if r["JORNADA"] else "",
                "local": r["Local"],
                "visitante": r["Visitante"],
                "start": start,
                "end": end,
                "duracion_min": (end - start).total_seconds() / 60,
                "teams": tuple(sorted([canon_team(r["Local"]), canon_team(r["Visitante"])])),
            }
        )
    return out


# ─────────────────────────── presencia y catálogo (por mes) ───────────────────────────
def presence_for_month(
    path, session_minutes: int = 20, co_only: bool = True
) -> pl.DataFrame:
    """Presencia minuto a minuto por stream + nº de sesión de visualización.

    Columnas: `day`, `Title`, `sh` (hash del stream), `minute`, `sess` (sesión del día).
    """
    lf = (
        pl.scan_parquet(path)
        .filter(pl.col("CountryCode") == "CO" if co_only else pl.lit(True))
        .select(["Date", "SubscriberID", "Title", "SeriesTitle", "deviceDescription"])
        .with_columns(
            pl.col("Date").str.to_datetime(strict=False).alias("ts"),
            pl.col("SeriesTitle").fill_null("").str.strip_chars().alias("_series"),
            pl.col("deviceDescription").fill_null("UnknownDevice").alias("_dev"),
        )
        .filter(pl.col("ts").is_not_null())
        .with_columns(
            pl.col("ts").dt.truncate("1m").alias("minute"),
            pl.col("ts").dt.date().alias("day"),
            pl.struct(["SubscriberID", "Title", "_series", "_dev"]).hash(seed=42).alias("sh"),
        )
        .select(["day", "Title", "sh", "minute"])
        .unique(subset=["day", "Title", "sh", "minute"])
    )
    pres = lf.collect()
    return (
        pres.sort(["sh", "day", "minute"])
        .with_columns(pl.col("minute").diff().over(["sh", "day"]).alias("gap"))
        .with_columns(
            (
                pl.col("gap").is_null()
                | (pl.col("gap") > pl.duration(minutes=session_minutes))
            )
            .cast(pl.Int64)
            .alias("_new")
        )
        .with_columns(pl.col("_new").cum_sum().over(["sh", "day"]).alias("sess"))
        .drop("_new")
    )


def title_catalog_raw(path, co_only: bool = True) -> pl.DataFrame:
    """Agregación directa (sin presencia) para catalogar los títulos OTT."""
    lf = (
        pl.scan_parquet(path)
        .filter(pl.col("CountryCode") == "CO" if co_only else pl.lit(True))
        .select(
            ["Date", "SubscriberID", "Title", "SeriesTitle", "deviceDescription", "Duration"]
        )
        .with_columns(
            pl.col("Date").str.to_datetime(strict=False).alias("ts"),
            pl.col("SeriesTitle").fill_null("").str.strip_chars().alias("_series"),
            pl.col("deviceDescription").fill_null("UnknownDevice").alias("_dev"),
        )
        .with_columns(
            pl.struct(["SubscriberID", "Title", "_series", "_dev"]).hash(seed=42).alias("sh")
        )
        .filter(pl.col("ts").is_not_null())
    )
    per_title = (
        lf.group_by("Title")
        .agg(
            pl.col("sh").n_unique().alias("users"),
            pl.col("Duration").max().alias("asset_sec"),
            pl.col("ts").min().alias("first_min"),
            pl.col("ts").max().alias("last_min"),
        )
        .collect()
    )
    per_hour = (
        lf.with_columns(pl.col("ts").dt.truncate("1h").alias("hour"))
        .group_by(["Title", "hour"])
        .agg(pl.col("sh").n_unique().alias("u"), pl.len().alias("ev"))
        .collect()
    )
    peak = (
        per_hour.sort(["Title", "u"], descending=[False, True])
        .group_by("Title")
        .agg(pl.col("hour").first().alias("peak_hour"))
    )
    ct = (
        pl.scan_parquet(path)
        .filter(pl.col("CountryCode") == "CO" if co_only else pl.lit(True))
        .group_by("Title")
        .agg(
            (pl.col("ContentType") == "SPORT").sum().alias("n_sport"),
            (pl.col("ContentType") == "BROADCAST").sum().alias("n_bcast"),
        )
        .collect()
    )
    return per_title.join(peak, on="Title", how="left").join(ct, on="Title", how="left")


def catalog_to_dicts(cat: pl.DataFrame) -> list[dict]:
    """Catálogo → lista de dicts con la metadata de parseo del título."""
    out = []
    for r in cat.iter_rows(named=True):
        meta = parse_title(r["Title"])
        out.append(
            {
                "raw": r["Title"],
                "teams": meta["teams"],
                "fecha": meta["fecha"],
                "torneo": meta["torneo"],
                "repeticion": meta["repeticion"],
                "users": r["users"],
                "peak_dt": r["peak_hour"],
                "first_min": r["first_min"],
                "last_min": r["last_min"],
                "asset_min": (r["asset_sec"] or 0) / 60,
                "es_sport": (r["n_sport"] or 0) >= (r["n_bcast"] or 0),
            }
        )
    return out


def match_programs(catalog: list[dict], programs: list[dict]) -> list[dict]:
    """Empareja cada programa de la parrilla con su(s) título(s) OTT.

    Clave: par de equipos canónicos (orden-insensible) + fecha/jornada;
    desempate por hora pico dentro de la ventana de emisión.
    """
    by_teams: dict[tuple, list[dict]] = defaultdict(list)
    for c in catalog:
        if c["teams"]:
            by_teams[c["teams"]].append(c)
    for p in programs:
        p["match_titles"] = []
        p["match_meta"] = None
        cands = by_teams.get(p["teams"], [])
        if not cands:
            continue
        jn = jornada_num(p["jornada"])
        pool = [c for c in cands if c["fecha"] == jn] if jn is not None else []
        if not pool:
            pool = [c for c in cands if c["fecha"] is None] or cands
        lo = p["start"] - dt.timedelta(hours=3)
        hi = p["end"] + dt.timedelta(hours=3)
        # el título debe tener actividad que solape la ventana de emisión
        win = [
            c
            for c in pool
            if lo <= c["peak_dt"] <= hi
            and c["first_min"] is not None
            and c["last_min"] is not None
            and c["first_min"] <= p["end"]
            and c["last_min"] >= p["start"]
        ]
        if not win:
            win = [c for c in pool if p["start"].date() == c["peak_dt"].date()]
        if not win:
            continue
        best = max(win, key=lambda c: c["users"])
        variants = [
            c
            for c in cands
            if c["fecha"] == best["fecha"]
            and c["torneo"] == best["torneo"]
            and abs((c["peak_dt"] - best["peak_dt"]).total_seconds()) <= 6 * 3600
        ]
        p["match_titles"] = [c["raw"] for c in variants]
        p["match_meta"] = best
    return programs


# ─────────────────────────────────── métricas ───────────────────────────────────
def peak_concurrency(rows: pl.DataFrame, window_minutes: int = 25) -> int:
    """Pico de usuarios simultáneos (ventana rodante) sobre (minute, sh).

    Fusiona los minutos de presencia de cada stream en intervalos activos
    `[inicio, último+24]` y calcula el máximo solapamiento con un barrido.
    Devuelve el mismo valor que la expansión de la ventana, sin multiplicar filas.
    """
    if rows.is_empty():
        return 0
    runs = (
        rows.sort(["sh", "minute"])
        .with_columns(pl.col("minute").diff().over("sh").alias("gap"))
        .with_columns(
            (
                pl.col("gap").is_null()
                | (pl.col("gap") > pl.duration(minutes=window_minutes))
            )
            .cast(pl.Int64)
            .alias("_new")
        )
        .with_columns(pl.col("_new").cum_sum().over("sh").alias("run"))
    )
    iv = (
        runs.group_by("sh", "run")
        .agg(pl.col("minute").min().alias("t0"), pl.col("minute").max().alias("t1"))
        .with_columns((pl.col("t1") + pl.duration(minutes=window_minutes - 1)).alias("tend"))
        .select(["t0", "tend"])
    )
    ev = iv.select(pl.col("t0").alias("t"), pl.lit(1).alias("d")).vstack(
        iv.select((pl.col("tend") + pl.duration(minutes=1)).alias("t"), pl.lit(-1).alias("d"))
    )
    sweep = ev.sort(["t", "d"]).with_columns(pl.col("d").cum_sum().alias("c"))
    return int(sweep["c"].max() or 0)


def concurrency_series(pres: pl.DataFrame, window_minutes: int = 25, freq: str = "5T") -> pl.DataFrame:
    """Concurrencia global con ventana rodante (misma matemática que `dsports.py`)."""
    pairs = pres.select("minute", "sh").unique()
    if pairs.is_empty():
        return pl.DataFrame({"ts": [], "active_streams": []})
    out = (
        pairs.with_columns((pl.col("minute") + pl.duration(minutes=window_minutes - 1)).alias("_wt"))
        .sort("_wt")
        .group_by_dynamic(
            index_column="_wt",
            every="1m",
            period=f"{window_minutes}m",
            offset=f"-{window_minutes - 1}m",
            closed="left",
            label="left",
        )
        .agg(pl.col("sh").n_unique().cast(pl.Int64).alias("active_streams"))
        .rename({"_wt": "ts"})
        .sort("ts")
    )
    m = re.search(r"(\d+)\s*([mhd])", freq)
    if m:
        n = int(m.group(1))
        label = {1: "1m", 5: "5m", 15: "15m", 30: "30m"}.get(n, "1h")
        out = (
            out.with_columns(pl.col("ts").dt.truncate(label).alias("_b"))
            .group_by("_b")
            .agg(pl.col("active_streams").max().alias("active_streams"))
            .rename({"_b": "ts"})
            .sort("ts")
        )
    return out


def event_counts_by_day_title(path, co_only: bool = True) -> pl.DataFrame:
    """Conteo de eventos por día y título (base del % diario de las señales)."""
    return (
        pl.scan_parquet(path)
        .filter(pl.col("CountryCode") == "CO" if co_only else pl.lit(True))
        .with_columns(pl.col("Date").str.to_datetime(strict=False).dt.date().alias("day"))
        .group_by(["day", "Title"])
        .agg(pl.len().alias("eventos"))
        .collect()
    )


def grid_metrics_for_month(
    pres: pl.DataFrame,
    programs: list[dict],
    tail_pres: pl.DataFrame | None,
    playback_days: int = 7,
    window_minutes: int = 25,
) -> list[dict]:
    """Las 10 métricas de la parrilla para los programas de un mes.

    `pres`      : presencia del mes del programa.
    `tail_pres` : presencia del mes siguiente (cola de diferido).
    """
    out = []
    for p in programs:
        titles = p.get("match_titles") or []
        rec = {
            "row": p["row"],
            "titulo_ott": " | ".join(titles) if titles else None,
            "n_titulos": len(titles),
            "Stream": 0,
            "Play back": 0,
            "Time (minutos)": 0.0,
            "Time Averange (minutos)": 0.0,
            "Peak WSO": 0,
            "Activos": 0,
            "usuarios_diferido": 0,
            "minutos_diferido": 0.0,
            "usuarios_totales": 0,
            "minutos_totales": 0.0,
            "asset_min": (p.get("match_meta") or {}).get("asset_min", 0.0),
        }
        if titles:
            tl = list(titles)
            sub = pres.filter(pl.col("Title").is_in(tl))
            tail = (
                tail_pres.filter(pl.col("Title").is_in(tl))
                if tail_pres is not None and not tail_pres.is_empty()
                else None
            )
            win = sub.filter(
                (pl.col("minute") >= pl.lit(p["start"]))
                & (pl.col("minute") <= pl.lit(p["end"]))
            )
            after = sub.filter(pl.col("minute") > pl.lit(p["end"]))
            if tail is not None:
                after = pl.concat([after, tail], how="vertical")
            after = after.filter(
                pl.col("minute") <= pl.lit(p["end"]) + pl.duration(days=playback_days)
            )
            if not win.is_empty():
                rec["Stream"] = win.select(pl.struct(["sh", "sess"]).n_unique()).item()
                rec["Activos"] = win["sh"].n_unique()
                rec["Time (minutos)"] = float(win.height)
                rec["Peak WSO"] = peak_concurrency(
                    win.select(["sh", "minute"]), window_minutes
                )
            if not after.is_empty():
                rec["Play back"] = after.select(pl.struct(["sh", "sess"]).n_unique()).item()
                rec["usuarios_diferido"] = after["sh"].n_unique()
                rec["minutos_diferido"] = float(after.height)
                ambos = pl.concat([win, after], how="vertical")
                rec["usuarios_totales"] = ambos["sh"].n_unique()
                rec["minutos_totales"] = float(ambos.height)
            if rec["Activos"]:
                rec["Time Averange (minutos)"] = rec["Time (minutos)"] / rec["Activos"]
        out.append(rec)
    return out


# ───────────────────────────────── reescalado ─────────────────────────────────
def rescale_to_total(
    parts: pl.DataFrame, key: str, value: str, total: pl.DataFrame, pct_col: str = "Porcent"
) -> pl.DataFrame:
    """Reparte el total real de cada grupo según el % de cada parte (residuo mayor).

    La suma por grupo es EXACTAMENTE el total real medido.
    """
    if parts.is_empty():
        return parts.with_columns(pl.lit(0, dtype=pl.Int64).alias("Total views (escalado)"))
    return (
        parts.join(total, on=key, how="left")
        .with_columns((pl.col(pct_col) / 100 * pl.col("_total")).alias("_raw"))
        .with_columns(pl.col("_raw").floor().cast(pl.Int64).alias("_base"))
        .with_columns((pl.col("_total") - pl.col("_base").sum().over(key)).alias("_residuo"))
        .with_columns((pl.col("_raw") - pl.col("_raw").floor()).alias("_frac"))
        .with_columns(pl.col("_frac").rank(method="ordinal", descending=True).over(key).alias("_rank"))
        .with_columns(
            (pl.col("_base") + (pl.col("_rank") <= pl.col("_residuo")).cast(pl.Int64)).alias(
                "Total views (escalado)"
            )
        )
        .select([key, value, "Porcent", "Total views (escalado)"])
        .sort([key, value])
    )


def build_signal_views(
    views_day_title: pl.DataFrame, signals: list[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(vistas reales por día, vistas reescaladas por día y señal)."""
    views_day = views_day_title.group_by("day").agg(pl.col("views").sum().alias("_total"))
    sig = with_signal(views_day_title).filter(pl.col("senal").is_in(signals))
    sig = sig.group_by(["day", "senal"]).agg(pl.col("views").sum().alias("views"))
    sig = sig.with_columns((pl.col("views") / pl.col("views").sum().over("day") * 100).alias("Porcent"))
    return views_day, rescale_to_total(sig, "day", "senal", views_day)


def build_signal_event_pct(ev_day_title: pl.DataFrame, signals: list[str]) -> pl.DataFrame:
    """% diario de eventos por señal (base del reescalado de la concurrencia)."""
    ev = with_signal(ev_day_title).filter(pl.col("senal").is_in(signals))
    ev = ev.group_by(["day", "senal"]).agg(pl.col("eventos").sum().alias("eventos"))
    return ev.with_columns(
        (pl.col("eventos") / pl.col("eventos").sum().over("day") * 100).alias("Porcentaje")
    ).sort(["day", "senal"])


def rescale_concurrency(signal: str, serie: pl.DataFrame, pct: pl.DataFrame) -> pl.DataFrame:
    """Reparte la concurrencia general entre las señales según su % diario."""
    pesos = pct.filter(pl.col("senal") == signal).select(
        "day", (pl.col("Porcentaje") / 100).alias("_peso")
    )
    return (
        serie.with_columns(pl.col("ts").dt.date().alias("day"))
        .join(pesos, on="day", how="left")
        .with_columns(
            (pl.col("active_streams") * pl.col("_peso").fill_null(0)).round().cast(pl.Int64).alias("Concurrencia")
        )
        .select("ts", "Concurrencia")
    )


# ─────────────────────────── tabla final de la parrilla ───────────────────────────
def build_grid_table(
    grid: pl.DataFrame, programs: list[dict], grid_metrics: list[dict]
) -> pl.DataFrame:
    """Parrilla (21 columnas) + 10 métricas + columnas de soporte."""
    metrics = {m["row"]: m for m in grid_metrics}
    rows = []
    for p in programs:
        g = grid.row(p["row"], named=True)
        m = metrics.get(p["row"], {})
        dur = p["duracion_min"]
        avg = m.get("Time Averange (minutos)", 0.0)
        rows.append(
            {
                "Fecha": g["Fecha"],
                "Mes": g["Mes"],
                "Calendario": g["Calendario"],
                "Canal": g["Canal"],
                "Programa": g["Programa"],
                "Hora Inicio": g["Hora Inicio"],
                "Hora Final": g["Hora Final"],
                "Tipo Programa": g["Tipo Programa"],
                "JORNADA": p["jornada"],
                "Local": g["Local"],
                "Visitante": g["Visitante"],
                "Stream": m.get("Stream", 0),
                "Play back": m.get("Play back", 0),
                "Time (minutos)": round(m.get("Time (minutos)", 0.0), 1),
                "Time Averange (minutos)": round(avg, 2),
                "Peak WSO": m.get("Peak WSO", 0),
                "Activos": m.get("Activos", 0),
                "Peso% Stream": None,
                "peso% peak": None,
                "Duracion_minutos": round(dur, 1),
                "% de consumo": round(100 * avg / dur, 2) if dur else None,
                # columnas de soporte
                "Titulo OTT": m.get("titulo_ott"),
                "Vistas OTP": m.get("n_titulos", 0),
                "Usuarios diferido": m.get("usuarios_diferido", 0),
                "Minutos diferido": round(m.get("minutos_diferido", 0.0), 1),
                "Usuarios totales": m.get("usuarios_totales", 0),
                "Minutos totales": round(m.get("minutos_totales", 0.0), 1),
                "Asset min (OTT)": round(m.get("asset_min", 0.0) or 0.0, 1),
                "Estado match": (
                    "match"
                    if m.get("n_titulos") and m.get("Activos")
                    else ("match sin actividad" if m.get("n_titulos") else "sin match")
                ),
            }
        )
    return pl.DataFrame(rows)


def apply_grid_weights(tabla: pl.DataFrame) -> pl.DataFrame:
    """`Peso% Stream` y `peso% peak` contra los totales del mes."""
    mes = tabla.with_columns(pl.col("Fecha").dt.strftime("%Y-%m").alias("_mes"))
    out = mes.with_columns(
        (pl.col("Time (minutos)") / pl.col("Time (minutos)").sum().over("_mes") * 100).alias(
            "Peso% Stream"
        )
    ).with_columns(
        (
            pl.col("Peak WSO")
            / pl.max_horizontal(pl.col("Peak WSO").max().over("_mes"), pl.lit(1))
            * 100
        ).alias("peso% peak")
    )
    return out.drop("_mes")


def rescale_grid_views(
    tabla: pl.DataFrame, views_day_title: pl.DataFrame, sport_titles: list[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Reescala el total real de vistas SPORT del día entre los programas de la parrilla.

    Devuelve (tabla con 'Vistas (escaladas)', conciliación diaria).
    """
    sp = views_day_title.filter(pl.col("Title").is_in(sport_titles))
    real_dia = sp.group_by("day").agg(pl.col("views").sum().alias("_total"))
    # un renglón por programa (el canal no se discierne): máximo entre canales
    prog = (
        tabla.with_columns(pl.col("Fecha").dt.date().alias("day"))
        .group_by(["day", "Titulo OTT", "Programa"])
        .agg(pl.col("Stream").max().alias("views"))
        .filter(pl.col("Titulo OTT").is_not_null())
    )
    prog = prog.with_columns(
        (pl.col("views") / pl.col("views").sum().over("day") * 100).alias("Porcent")
    )
    esc = rescale_to_total(prog, "day", "Programa", real_dia)
    conc = (
        real_dia.rename({"_total": "_real_dia"})
        .join(
            esc.group_by("day").agg(
                pl.col("Total views (escalado)").sum().alias("_suma_escalada")
            ),
            on="day",
            how="left",
        )
        .with_columns((pl.col("_suma_escalada") - pl.col("_real_dia")).alias("dif"))
        .with_columns(
            pl.col("_suma_escalada").fill_null(0).cast(pl.Int64),
            pl.col("dif").fill_null(0).cast(pl.Int64),
        )
        .select(["day", "_real_dia", "_suma_escalada", "dif"])
        .rename(
            {
                "day": "Fecha",
                "_real_dia": "Vistas reales (día)",
                "_suma_escalada": "Vistas reescaladas (parrilla)",
                "dif": "Diferencia",
            }
        )
    )
    tabla = (
        tabla.with_columns(pl.col("Fecha").dt.date().alias("_day"))
        .join(
            esc.select(
                [
                    pl.col("day").alias("_day"),
                    "Programa",
                    "Total views (escalado)",
                ]
            ),
            on=["_day", "Programa"],
            how="left",
        )
        .drop("_day")
    )
    return tabla, conc


# ───────────────────────────────── orquestador ─────────────────────────────────
def procesar(
    data_dir,
    grid_xlsx,
    months: list[str],
    window_minutes: int = 25,
    session_minutes: int = 20,
    playback_days: int = 7,
    co_only: bool = True,
    signals: list[str] | None = None,
    verbose: bool = True,
) -> dict:
    """Ejecuta el pipeline completo y devuelve todos los dataframes del reporte."""
    import gc

    signals = signals or DEFAULT_SIGNALS
    t_all = time.time()
    paths = [
        (m, Path(data_dir) / f"LIVE_{m}.parquet")
        for m in months
        if (Path(data_dir) / f"LIVE_{m}.parquet").exists()
    ]

    grid = load_grid(grid_xlsx)
    programs = grid_programs(grid)

    # 1) catálogo + emparejamiento con la parrilla
    cats: list[dict] = []
    for _month, path in paths:
        cats.extend(catalog_to_dicts(title_catalog_raw(path, co_only)))
    match_programs(cats, programs)
    by_month: dict[str, list[dict]] = defaultdict(list)
    for p in programs:
        by_month[p["mes"]].append(p)

    # 2) presencia mes a mes (nunca más de dos meses en memoria)
    vdt_parts, ev_parts, conc_parts, grid_rows = [], [], [], []
    pres = presence_for_month(paths[0][1], session_minutes, co_only)
    for i, (month, path) in enumerate(paths):
        t0 = time.time()
        pres_next = (
            presence_for_month(paths[i + 1][1], session_minutes, co_only)
            if i + 1 < len(paths)
            else None
        )
        vdt_parts.append(
            pres.group_by(["day", "Title"]).agg(
                pl.struct(["sh", "sess"]).n_unique().alias("views")
            )
        )
        ev_parts.append(event_counts_by_day_title(path, co_only))
        conc_parts.append(concurrency_series(pres, window_minutes))
        grid_rows.extend(
            grid_metrics_for_month(
                pres, by_month.get(month, []), pres_next, playback_days, window_minutes
            )
        )
        pres = pres_next
        gc.collect()
        if pres is None:
            break
        if verbose:
            print(f"  {month}: {time.time() - t0:.1f}s")

    views_day_title = pl.concat(vdt_parts, how="vertical")
    ev_day_title = pl.concat(ev_parts, how="vertical")
    concurrency = pl.concat(conc_parts, how="vertical").sort("ts")
    sport_titles = sorted({c["raw"] for c in cats if c.get("es_sport")})

    # 3) señales: vistas reales, vistas reescaladas, % de eventos
    views_day, views_scaler = build_signal_views(views_day_title, signals)
    signal_pct = build_signal_event_pct(ev_day_title, signals)
    signal_concurrency = {
        s: rescale_concurrency(s, concurrency, signal_pct)
        for s in signals
        if s in signal_pct["senal"].unique().to_list()
    }

    # 4) parrilla: tabla final con métricas, pesos y vistas reescaladas
    tabla = build_grid_table(grid, programs, grid_rows)
    tabla = apply_grid_weights(tabla)
    tabla, conciliacion = rescale_grid_views(tabla, views_day_title, sport_titles)

    return {
        "paths": paths,
        "grid": grid,
        "programs": programs,
        "catalog": cats,
        "views_day_title": views_day_title,
        "ev_day_title": ev_day_title,
        "views_day": views_day,
        "views_scaler": views_scaler,
        "signal_pct": signal_pct,
        "concurrency": concurrency,
        "signal_concurrency": signal_concurrency,
        "grid_metrics": grid_rows,
        "tabla": tabla,
        "conciliacion": conciliacion,
        "sport_titles": sport_titles,
        "elapsed": time.time() - t_all,
    }
