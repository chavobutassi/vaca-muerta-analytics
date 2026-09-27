"""
=============================================================
  VACA MUERTA — Validación del registro de activos (pozos)
  Autor: Claudio Butassi
  Versión: 1.0
=============================================================

¿QUÉ ES ESTO?
    Todos los meses las operadoras entregan a la Secretaría de Energía la
    información de cada pozo (Capítulo IV). Además de la producción, esa
    entrega trae los ATRIBUTOS DEL ACTIVO: sigla, estado, tipo de pozo,
    sistema de extracción, profundidad, formación, yacimiento, coordenadas.

    Ese conjunto es, en la práctica, el registro de activos del sector: un
    modelo de información del activo sin geometría 3D. Este módulo valida la
    calidad de esa entrega con el mismo enfoque que se usa para validar datos
    BIM según IRAM-ISO 19650: requisitos explícitos → chequeo → hallazgos →
    veredicto sobre la entrega.

    A diferencia de bim_validacion.py (modelo IFC sintético), acá los datos
    son 100% reales y públicos.

ENTRADA:
    Los CSV que vaca_muerta_pipeline.py ya descargó en ./cache_csv/
    (se reutilizan sus funciones de lectura, normalización y filtro VM,
    así el universo de pozos es exactamente el mismo que el del dashboard).

REGLAS:
    A01  Identificador único        cada idpozo tiene una sola sigla y viceversa
    A02  Atributos obligatorios     el registro vigente del pozo tiene todos sus campos
    A03  Dominio de valores         profundidad plausible, producción ≥ 0, TEF ≤ días del mes
    A04  Coordenadas                presentes y dentro de la Cuenca Neuquina
    A05  Estado vs. producción      un pozo inactivo no declara producción,
                                    uno en extracción efectiva no declara todo en cero
    A06  Estabilidad de atributos   profundidad y yacimiento no cambian mes a mes
    A07  Consistencia entre entregas un mismo pozo-mes informado en dos archivos
                                    con valores distintos (rectificación)
    A08  Continuidad de reporte     pozos en extracción efectiva sin meses faltantes

SEVERIDAD:
    CRITICA  el dato no se puede usar sin corregirlo
    MAYOR    distorsiona análisis (producción, estados, rankings)
    MENOR    de forma o con explicación operativa posible

USO:
    python vaca_muerta_pipeline.py      # primero: descarga y cachea la fuente
    python validacion_activos.py

OUTPUTS:
    output/11_activos_registro.csv        registro vigente (último estado de cada pozo)
    output/11_activos_resumen.csv         cumplimiento por regla
    output/11_activos_hallazgos.csv       muestra de hallazgos (hasta 300 por regla)
    output/completo/11_activos_hallazgos_completo.csv   todos los hallazgos
    output/11_activos_meta.json           veredicto de la entrega y trazabilidad
    (los 4 primeros se copian a docs/data/ para el dashboard)
=============================================================
"""

from __future__ import annotations

import json
import shutil
import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import vaca_muerta_pipeline as vm   # misma lectura, normalización y filtro VM

log = logging.getLogger(__name__)

CARPETA_CACHE  = vm.CARPETA_CACHE
CARPETA_OUTPUT = vm.CARPETA_OUTPUT
CARPETA_DOCS   = vm.CARPETA_DOCS

# ─── REQUISITOS DE INFORMACIÓN ───────────────────────────────────────────────
# Qué debe traer el registro de cada pozo. En términos de IRAM-ISO 19650, esto
# son los requisitos de información del activo que fija quien recibe la entrega.

ATRIBUTOS_OBLIGATORIOS = [
    "empresa", "sigla", "tipoestado", "tipopozo", "tipoextraccion",
    "profundidad", "yacimiento", "formacion", "clasificacion",
]

PROFUNDIDAD_MIN_M = 300      # por debajo no es un pozo productor de VM
PROFUNDIDAD_MAX_M = 9_000    # rama lateral larga en profundidad medida; más es implausible

# Recuadro aproximado de la Cuenca Neuquina (grados decimales, WGS84)
LAT_MIN, LAT_MAX = -41.5, -34.5
LON_MIN, LON_MAX = -71.5, -66.0

ESTADOS_INACTIVOS = {
    "ABANDONADO", "A ABANDONAR", "PARADO TRANSITORIAMENTE",
    "EN ESPERA DE REPARACIÓN", "EN ESPERA DE REPARACION",
    "OTRAS SITUACIÓN INACTIVO", "OTRAS SITUACION INACTIVO",
}
ESTADO_PRODUCIENDO = "EXTRACCIÓN EFECTIVA"

TOLERANCIA_RECTIFICACION = 0.005   # 0,5 % de diferencia entre entregas
MUESTRA_POR_REGLA = 300

REGLAS: dict[str, tuple[str, str]] = {
    "A01": ("Identificador único",         "CRITICA"),
    "A02": ("Atributos obligatorios",      "MAYOR"),
    "A03": ("Dominio de valores",          "MAYOR"),
    "A04": ("Coordenadas",                 "MAYOR"),
    "A05": ("Estado vs. producción",       "MAYOR"),
    "A06": ("Estabilidad de atributos",    "MENOR"),
    "A07": ("Consistencia entre entregas", "MAYOR"),
    "A08": ("Continuidad de reporte",      "MENOR"),
}

COLS = [
    "pozo_id", "sigla", "fecha", "empresa", "yacimiento", "formacion",
    "tipoestado", "tipopozo", "tipoextraccion", "clasificacion", "profundidad",
    "coordenadax", "coordenaday", "petroleo_m3", "gas_mm3", "agua_m3", "tef",
]

# ─── CARGA ───────────────────────────────────────────────────────────────────

def _vacio(serie: pd.Series) -> pd.Series:
    s = serie.astype(str).str.strip().str.upper()
    return serie.isna() | s.isin(["", "NAN", "NONE", "NULL", "<NA>"])


def cargar() -> pd.DataFrame:
    """Lee cada archivo del cache, aplica el mismo filtro VM del pipeline y
    conserva el origen (fuente) de cada registro."""
    frames = []
    for ruta in sorted(CARPETA_CACHE.glob("*.csv")):
        try:
            df = vm.filtrar_vm(vm.normalizar(vm.leer_csv(ruta)))
        except Exception as exc:
            log.warning("  ⚠ No se pudo leer %s: %s", ruta.name, exc)
            continue
        if df.empty:
            continue
        for c in COLS:
            if c not in df.columns:
                df[c] = np.nan
        df = df[COLS].copy()
        df["fuente"] = ruta.stem
        frames.append(df)
        log.info("  %-22s %8s registros VM", ruta.stem, f"{len(df):,}")
    if not frames:
        raise SystemExit("No hay archivos en cache_csv/. Correr primero vaca_muerta_pipeline.py")

    d = pd.concat(frames, ignore_index=True)
    d = d[d["pozo_id"].notna() & d["fecha"].notna()].copy()
    d["pozo_id"] = d["pozo_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    for c in ["profundidad", "coordenadax", "coordenaday", "petroleo_m3", "gas_mm3", "agua_m3", "tef"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    for c in ["tipoestado", "tipopozo", "tipoextraccion", "clasificacion", "sigla"]:
        d[c] = d[c].where(_vacio(d[c]) == False, np.nan)
        d[c] = d[c].astype("string").str.strip()
    d["estado_u"] = d["tipoestado"].str.upper()
    return d


def registro_vigente(d: pd.DataFrame) -> pd.DataFrame:
    """Último registro de cada pozo = su estado actual en el registro de activos."""
    return (d.sort_values(["pozo_id", "fecha"])
             .groupby("pozo_id", as_index=False).tail(1)
             .reset_index(drop=True))

# ─── REGLAS ──────────────────────────────────────────────────────────────────
# Cada regla devuelve (evaluados, DataFrame de hallazgos con pozo_id + detalle, nota)

def _h(df: pd.DataFrame, detalle: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({"pozo_id": df["pozo_id"].values, "detalle": detalle.values})


def a01_identificador(d, reg):
    con_sigla = d[d["sigla"].notna()]
    siglas_x_pozo = con_sigla.groupby("pozo_id")["sigla"].nunique()
    pozos_x_sigla = con_sigla.groupby("sigla")["pozo_id"].nunique()
    malos1 = siglas_x_pozo[siglas_x_pozo > 1]
    h1 = pd.DataFrame({"pozo_id": malos1.index,
                       "detalle": [f"El pozo figura con {n} siglas distintas" for n in malos1]})
    sig_dup = pozos_x_sigla[pozos_x_sigla > 1].index
    dup = con_sigla[con_sigla["sigla"].isin(sig_dup)].drop_duplicates(["sigla", "pozo_id"])
    h2 = pd.DataFrame({"pozo_id": dup["pozo_id"].values,
                       "detalle": ["Sigla '" + s + "' compartida con otro idpozo" for s in dup["sigla"]]})
    return reg["pozo_id"].nunique(), pd.concat([h1, h2], ignore_index=True), ""


def a02_obligatorios(d, reg):
    faltan = pd.DataFrame({c: _vacio(reg[c]) for c in ATRIBUTOS_OBLIGATORIOS})
    faltan["profundidad"] |= reg["profundidad"].fillna(0) <= 0
    malos = faltan.any(axis=1)
    detalle = faltan[malos].apply(lambda r: "Faltan: " + ", ".join(r.index[r]), axis=1)
    return len(reg), _h(reg[malos], detalle), ""


def a03_dominio(d, reg):
    prof = d["profundidad"]
    fuera = prof.notna() & (prof > 0) & ((prof < PROFUNDIDAD_MIN_M) | (prof > PROFUNDIDAD_MAX_M))
    g_prof = d[fuera].groupby("pozo_id")["profundidad"].agg(["size", "first"])
    h1 = pd.DataFrame({"pozo_id": g_prof.index,
                       "detalle": [f"Profundidad {v:,.0f} m fuera de rango plausible "
                                   f"({PROFUNDIDAD_MIN_M}–{PROFUNDIDAD_MAX_M:,} m) en {n} mes(es)"
                                   for n, v in zip(g_prof["size"], g_prof["first"])]})
    neg = (d[["petroleo_m3", "gas_mm3", "agua_m3"]] < 0).any(axis=1)
    g_neg = d[neg].groupby("pozo_id").size()
    h2 = pd.DataFrame({"pozo_id": g_neg.index, "detalle": [f"{n} mes(es) con producción negativa" for n in g_neg]})
    dias = d["fecha"].dt.days_in_month
    tef_mal = d["tef"].notna() & ((d["tef"] < 0) | (d["tef"] > dias))
    g_tef = d[tef_mal].groupby("pozo_id").size()
    h3 = pd.DataFrame({"pozo_id": g_tef.index,
                       "detalle": [f"{n} mes(es) con días efectivos (TEF) mayores a los días del mes" for n in g_tef]})
    return len(reg), pd.concat([h1, h2, h3], ignore_index=True), ""


def a04_coordenadas(d, reg):
    con = d[d["coordenadax"].notna() | d["coordenaday"].notna()]
    fuentes_sin = sorted(set(d["fuente"]) - set(con["fuente"]))
    nota = ("Las entregas " + ", ".join(fuentes_sin) + " no incluyen coordenadas: "
            "solo se evalúan pozos presentes en archivos que sí las traen.") if fuentes_sin else ""
    if con.empty:
        return 0, pd.DataFrame(columns=["pozo_id", "detalle"]), nota
    ult = con.sort_values("fecha").groupby("pozo_id").tail(1)
    x, y = ult["coordenadax"], ult["coordenaday"]
    # La documentación dice x = latitud; se detecta la orientación real con los datos
    x_es_lat = x.between(LAT_MIN, LAT_MAX).mean() >= y.between(LAT_MIN, LAT_MAX).mean()
    lat, lon = (x, y) if x_es_lat else (y, x)
    nota += (" " if nota else "") + f"Orientación detectada: coordenada{'x' if x_es_lat else 'y'} = latitud."
    falta = lat.isna() | lon.isna() | (lat == 0) | (lon == 0)
    fuera = ~falta & ~(lat.between(LAT_MIN, LAT_MAX) & lon.between(LON_MIN, LON_MAX))
    det = pd.Series("", index=ult.index)
    det[falta] = "Coordenadas faltantes o en cero"
    det[fuera] = ("Punto fuera de la Cuenca Neuquina (" + lat[fuera].round(4).astype(str)
                  + ", " + lon[fuera].round(4).astype(str) + ")")
    malos = falta | fuera
    return len(ult), _h(ult[malos], det[malos]), nota


def a05_estado_produccion(d, reg):
    hc = d["petroleo_m3"].fillna(0) + d["gas_mm3"].fillna(0)
    total = hc + d["agua_m3"].fillna(0)
    inactivo_prod = d["estado_u"].isin(ESTADOS_INACTIVOS) & (hc > 0)
    activo_cero = (d["estado_u"] == ESTADO_PRODUCIENDO) & (total == 0) & (d["tef"].fillna(0) == 0)
    g1 = d[inactivo_prod].groupby("pozo_id")["tipoestado"].agg(["size", "first"])
    h1 = pd.DataFrame({"pozo_id": g1.index,
                       "detalle": [f"{n} mes(es) en estado '{e}' declarando producción" for n, e in zip(g1["size"], g1["first"])]})
    g2 = d[activo_cero].groupby("pozo_id").size()
    h2 = pd.DataFrame({"pozo_id": g2.index,
                       "detalle": [f"{n} mes(es) en 'Extracción Efectiva' con producción y TEF en cero" for n in g2]})
    return d["pozo_id"].nunique(), pd.concat([h1, h2], ignore_index=True), ""


def a06_estabilidad(d, reg):
    prof = d[d["profundidad"] > 0].groupby("pozo_id")["profundidad"].nunique()
    yac  = d[~_vacio(d["yacimiento"])].groupby("pozo_id")["yacimiento"].nunique()
    h1 = pd.DataFrame({"pozo_id": prof[prof > 1].index,
                       "detalle": [f"Profundidad informada con {n} valores distintos" for n in prof[prof > 1]]})
    h2 = pd.DataFrame({"pozo_id": yac[yac > 1].index,
                       "detalle": [f"Asignado a {n} yacimientos distintos" for n in yac[yac > 1]]})
    return d["pozo_id"].nunique(), pd.concat([h1, h2], ignore_index=True), \
        "Puede tener explicación operativa (reentrada, profundización, reasignación de área)."


def a07_entre_entregas(d, reg):
    k = ["pozo_id", "fecha"]
    multi = d[d.duplicated(k, keep=False)]
    if multi.empty:
        return 0, pd.DataFrame(columns=["pozo_id", "detalle"]), "No hay pozo-mes informado en más de un archivo."
    g = multi.groupby(k).agg(pmin=("petroleo_m3", "min"), pmax=("petroleo_m3", "max"),
                             gmin=("gas_mm3", "min"), gmax=("gas_mm3", "max"),
                             fuentes=("fuente", lambda s: " / ".join(sorted(set(s))))).reset_index()
    def difiere(a, b):
        base = np.maximum(b.abs(), 1e-9)
        return (b - a).abs() / base > TOLERANCIA_RECTIFICACION
    g["mal"] = difiere(g["pmin"], g["pmax"]) | difiere(g["gmin"], g["gmax"])
    malos = g[g["mal"]]
    por_pozo = malos.groupby("pozo_id").agg(n=("fecha", "size"), fuentes=("fuentes", "first"))
    h = pd.DataFrame({"pozo_id": por_pozo.index,
                      "detalle": [f"{n} mes(es) con producción distinta entre {f}" for n, f in zip(por_pozo["n"], por_pozo["fuentes"])]})
    return g["pozo_id"].nunique(), h, \
        "El pipeline conserva el primer valor leído; si difieren, cuál es el vigente queda sin trazabilidad."


def a08_continuidad(d, reg):
    activos = reg[reg["estado_u"] == ESTADO_PRODUCIENDO]["pozo_id"]
    sub = d[d["pozo_id"].isin(activos)]
    g = sub.groupby("pozo_id")["fecha"].agg(["min", "max", "nunique"])
    esperados = (g["max"].dt.year - g["min"].dt.year) * 12 + (g["max"].dt.month - g["min"].dt.month) + 1
    faltan = esperados - g["nunique"]
    malos = faltan[faltan > 0]
    h = pd.DataFrame({"pozo_id": malos.index,
                      "detalle": [f"{int(n)} mes(es) sin informar entre su primer y último reporte" for n in malos]})
    return len(g), h, ""


FUNCIONES = {
    "A01": a01_identificador, "A02": a02_obligatorios, "A03": a03_dominio,
    "A04": a04_coordenadas,   "A05": a05_estado_produccion, "A06": a06_estabilidad,
    "A07": a07_entre_entregas, "A08": a08_continuidad,
}

# ─── EJECUCIÓN ───────────────────────────────────────────────────────────────

def validar(d: pd.DataFrame, reg: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    hallazgos, resumen = [], []
    info = reg.set_index("pozo_id")[["sigla", "empresa", "yacimiento", "tipoestado"]]
    for rid, fn in FUNCIONES.items():
        nombre, sev = REGLAS[rid]
        evaluados, h, nota = fn(d, reg)
        pozos_mal = h["pozo_id"].nunique() if len(h) else 0
        if len(h):
            h = h.join(info, on="pozo_id")
            h.insert(0, "regla_id", rid); h.insert(1, "regla", nombre); h.insert(2, "severidad", sev)
            hallazgos.append(h)
        resumen.append({
            "regla_id": rid, "regla": nombre, "severidad": sev,
            "evaluados": int(evaluados), "pozos_con_hallazgo": int(pozos_mal),
            "cumplimiento_pct": round((evaluados - pozos_mal) / evaluados * 100, 1) if evaluados else None,
            "nota": nota,
        })
        log.info("  %s %-28s %6s / %-6s pozos con hallazgo", rid, nombre, f"{pozos_mal:,}", f"{evaluados:,}")
    cols = ["regla_id", "regla", "severidad", "pozo_id", "sigla", "empresa", "yacimiento", "tipoestado", "detalle"]
    todos = pd.concat(hallazgos, ignore_index=True)[cols] if hallazgos else pd.DataFrame(columns=cols)
    return todos, pd.DataFrame(resumen)


def veredicto(resumen: pd.DataFrame) -> dict:
    """Decisión sobre la entrega, siguiendo el flujo del entorno común de datos
    de IRAM-ISO 19650: si no cumple, no pasa a 'publicado' y vuelve al originador."""
    crit = resumen[(resumen["severidad"] == "CRITICA") & (resumen["pozos_con_hallazgo"] > 0)]
    may  = resumen[(resumen["severidad"] == "MAYOR") & (resumen["pozos_con_hallazgo"] > 0)]
    if len(crit):
        estado, texto = "NO ACEPTADA", "Hallazgos críticos: la información vuelve al originador para corrección antes de publicarse."
    elif len(may):
        estado, texto = "ACEPTADA CON OBSERVACIONES", "Utilizable con salvedades: los hallazgos mayores deben corregirse en la próxima entrega."
    else:
        estado, texto = "ACEPTADA", "Cumple los requisitos de información definidos."
    return {"estado": estado, "texto": texto,
            "reglas_criticas_con_hallazgo": crit["regla_id"].tolist(),
            "reglas_mayores_con_hallazgo": may["regla_id"].tolist()}


def guardar(df: pd.DataFrame, nombre: str, publicar: bool = True) -> None:
    CARPETA_OUTPUT.mkdir(exist_ok=True)
    ruta = CARPETA_OUTPUT / nombre
    ruta.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    if publicar:
        CARPETA_DOCS.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ruta, CARPETA_DOCS / Path(nombre).name)
    log.info("✓ %s  (%s filas)", nombre, f"{len(df):,}")


def main() -> None:
    log.info("=" * 60)
    log.info("  VACA MUERTA — Validación del registro de activos (pozos)")
    log.info("=" * 60)
    log.info("[1/4] Cargando entregas desde %s/", CARPETA_CACHE)
    d = cargar()
    reg = registro_vigente(d)
    log.info("  %s pozos · %s registros pozo-mes", f"{len(reg):,}", f"{len(d):,}")

    log.info("[2/4] Validando")
    hallazgos, resumen = validar(d, reg)
    ver = veredicto(resumen)
    log.info("  Veredicto de la entrega: %s", ver["estado"])

    log.info("[3/4] Exportando")
    reg_out = reg.drop(columns=["estado_u"]).rename(columns={"fecha": "ultimo_reporte"})
    reg_out["ultimo_reporte"] = reg_out["ultimo_reporte"].dt.strftime("%Y-%m")
    guardar(reg_out, "11_activos_registro.csv")
    guardar(resumen, "11_activos_resumen.csv")
    muestra = hallazgos.groupby("regla_id", group_keys=False).head(MUESTRA_POR_REGLA)
    guardar(muestra, "11_activos_hallazgos.csv")
    guardar(hallazgos, "completo/11_activos_hallazgos_completo.csv", publicar=False)

    log.info("[4/4] Metadata")
    meta = {
        "ejecutado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "pozos": int(len(reg)),
        "registros_pozo_mes": int(len(d)),
        "periodo_desde": d["fecha"].min().strftime("%Y-%m"),
        "periodo_hasta": d["fecha"].max().strftime("%Y-%m"),
        "entregas": sorted(d["fuente"].unique().tolist()),
        "pozos_con_algun_hallazgo": int(hallazgos["pozo_id"].nunique()) if len(hallazgos) else 0,
        "hallazgos_totales": int(len(hallazgos)),
        "hallazgos_publicados": int(len(muestra)),
        "veredicto": ver,
        "marco": "Requisitos de información y flujo de entrega según IRAM-ISO 19650-1/-2",
        "fuente": "Secretaría de Energía — Capítulo IV (datos.energia.gob.ar)",
    }
    ruta = CARPETA_OUTPUT / "11_activos_meta.json"
    ruta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    shutil.copy2(ruta, CARPETA_DOCS / ruta.name)
    log.info("✓ 11_activos_meta.json")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
