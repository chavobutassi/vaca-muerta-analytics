"""
=============================================================
  VACA MUERTA — Pipeline de datos para dashboard Power BI
  Fuente: Secretaría de Energía — datos.energia.gob.ar
  Autor: Claudio Butassi
  Versión: 2.2
=============================================================

DESCRIPCIÓN:
    Pipeline ETL que descarga, normaliza, filtra y transforma
    datos de producción de pozos no convencionales de Vaca Muerta,
    exportando 8 tablas analíticas listas para Power BI.

REQUISITOS:
    pip install pandas requests tqdm openpyxl scipy numpy

OUTPUTS (carpeta ./output/):
    01_vm_produccion_mensual.csv   → producción mensual por empresa
    02_vm_por_yacimiento.csv       → producción por yacimiento y año
    03_vm_top_pozos.csv            → ranking de pozos por producción acumulada
    04_vm_eficiencia_pozos.csv     → water cut y GOR por pozo
    05_vm_market_share.csv         → participación de mercado por empresa
    06_vm_nuevos_pozos.csv         → pozos nuevos por mes (proxy de perforación)
    07_vm_raw_filtrado.csv         → dataset completo Vaca Muerta
    08_vm_declinacion.csv          → curvas de declinación de Arps + EUR por pozo

CAMBIOS v2.2:
    • Análisis de declinación (Arps hiperbólica modificada) con EUR por pozo,
      R² del ajuste y validación de calidad del fit.
    • CORRECCIÓN DE UNIDADES: el gas del Capítulo IV viene en MILES de m3.
      El factor BOE del gas pasa de 5886 a 5.886 boe/(mil m3). Antes el aporte
      de gas al BOE estaba inflado ~1000×, distorsionando market share y rankings.
=============================================================
"""

from __future__ import annotations

import os
import sys
import shutil
import logging
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional
from scipy.optimize import curve_fit
from tqdm import tqdm

# ─── CONFIGURACIÓN ───────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE = "http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c/resource"

FUENTES: dict[str, str] = {
    "no_conv_historico": f"{BASE}/b5b58cdc-9e07-41f9-b392-fb9ec68b0725/download/produccin-de-pozos-de-gas-y-petrleo-no-convencional.csv",
    "prod_2024":         f"{BASE}/43a09dce-1742-44d0-bc13-f193deaab563/download/produccin-de-pozos-de-gas-y-petrleo-2024.csv",
    "prod_2025":         f"{BASE}/d774b5d7-0756-48fe-88f2-8729b57b22da/download/produccin-de-pozos-de-gas-y-petrleo-2025.csv",
    "prod_2026":         f"{BASE}/fb7a47a0-cba9-4667-a004-6f6c1c346c23/download/produccin-de-pozos-de-gas-y-petrleo-2026.csv",
}

CARPETA_CACHE  = Path("cache_csv")
CARPETA_OUTPUT = Path("output")
CARPETA_DOCS   = Path("docs/data")   # GitHub Pages sirve desde acá

# ─── FACTORES DE EQUIVALENCIA ENERGÉTICA (BOE) ───────────────────────────────
# Fuente del dato: Secretaría de Energía, Capítulo IV.
#   Petróleo → [m3]   |   Gas → [MILES de m3]   |   Agua → [m3]
# Base: 1 boe ≈ 6.000 pie3 de gas (6 Mscf/boe) y 1 m3 petróleo ≈ 6.2898 boe.
#   • 1 m3 petróleo         = 6.2898 boe
#   • 1 (mil m3) de gas     = 5.886  boe   (NO 5886: el nombre "mm3" es un
#                                            misnomer, la columna trae miles de m3)
FACTOR_BOE_PETROLEO = 6.2898   # boe por m3
FACTOR_BOE_GAS      = 5.886    # boe por mil m3 (unidad nativa de gas_mm3)

# ─── PARÁMETROS DEL ANÁLISIS DE DECLINACIÓN (ARPS) ───────────────────────────
MIN_MESES_AJUSTE   = 6       # mínimo de puntos post-pico para intentar el ajuste
R2_MIN_BUENO       = 0.70    # R² ≥ → calidad "BUENO"
R2_MIN_REGULAR     = 0.40    # R² < → calidad "DESCARTADO"
B_MIN, B_MAX       = 0.01, 2.0   # rango físico del exponente de Arps
D_TERMINAL_ANUAL   = 0.08    # declinación terminal 8%/año (hiperbólica modificada)
HORIZONTE_MESES    = 360     # tope de pronóstico para el EUR (30 años)
FRAC_ABANDONO      = 0.01    # caudal de abandono = 1% del qi

# Grupos de empresa: clave = nombre canónico, valores = patrones a detectar
GRUPOS: dict[str, list[str]] = {
    "YPF":                 ["YPF", "YSUR"],
    "Shell":               ["SHELL"],
    "TotalEnergies":       ["TOTAL AUSTRAL", "TOTALENERGIES", "TOTAL E&P", "TOTAL S.A"],
    "Vista Energy":        ["VISTA"],
    "Tecpetrol":           ["TECPETROL"],
    "Equinor":             ["EQUINOR", "STATOIL"],
    "Wintershall":         ["WINTERSHALL"],
    "Pan American Energy": ["PAN AMERICAN", "PANAMERICAN"],
    "Pluspetrol":          ["PLUSPETROL"],
    "Pampa Energía":       ["PAMPA"],
    "Chevron":             ["CHEVRON"],
    "ExxonMobil":          ["EXXONMOBIL", "EXXON MOBIL", "ESSO"],
    "Capex":               ["CAPEX"],
    "Geopark":             ["GEOPARK"],
    "Phoenix":             ["PHOENIX"],
    "Kilwer":              ["KILWER"],
    "Petrolera El Trébol": ["PETROLERA EL TREBOL", "EL TREBOL"],
    "Petrobras":           ["PETROBRAS"],
    "O&G Developments":    ["O&G DEVELOPMENTS", "O&G DEV"],
    "Grecoil":             ["GRECOIL"],
    "Medanito":            ["MEDANITO"],
    "Americas Petrogas":   ["AMERICAS PETROGAS", "AMERICAS"],
    "APCO":                ["APCO"],
    "Apache":              ["APACHE"],
    "Madalena":            ["MADALENA"],
    "Quintana":            ["QUINTANA"],
    "Roch":                ["ROCH"],
    "Continental":         ["CONTINENTAL"],
    "Hattrick":            ["HATTRICK"],
    "Argenta":             ["ARGENTA"],
    "Bentia":              ["BENTIA"],
    "Gas y Petróleo Nqn":  ["GAS Y PETROLEO", "G Y P "],
}

# Mapeo canónico de nombres de columna
MAPA_COLUMNAS: dict[str, list[str]] = {
    "empresa":      ["empresa", "operadora"],
    "pozo_id":      ["idpozo", "id_pozo", "sigla", "pozoid"],
    "cuenca":       ["cuenca"],
    "yacimiento":   ["yacimiento", "nombre_yacimiento", "yac", "campo"],
    "formacion":    ["formacion", "formación"],
    "tipo_recurso": ["tiporecurso", "tipo_recurso", "subtipoderecurso", "sub_tipo_recurso"],
    "periodo":      ["periodo", "fecha", "anio_mes", "anomes"],
    "anio":         ["anio", "año", "year"],
    "mes_num":      ["mes", "month"],
    "petroleo_m3":  ["prod_pet", "produccion_petroleo", "petroleo", "pet_m3"],
    "gas_mm3":      ["prod_gas", "produccion_gas", "gas_mm3", "gas"],
    "agua_m3":      ["prod_agua", "produccion_agua", "agua"],
    "provincia":    ["provincia"],
}

# ─── DESCARGA ────────────────────────────────────────────────────────────────

def descargar(url: str, nombre: str) -> Optional[Path]:
    """
    Descarga un archivo CSV con caché local.
    Si ya existe en cache_csv/, lo reutiliza sin volver a descargar.

    Args:
        url: URL del recurso a descargar.
        nombre: Nombre de archivo para guardar en caché.

    Returns:
        Path al archivo local, o None si la descarga falló.
    """
    CARPETA_CACHE.mkdir(exist_ok=True)
    ruta = CARPETA_CACHE / nombre

    if ruta.exists():
        size_mb = ruta.stat().st_size / 1_048_576
        log.info("✓ Caché: %s  (%.1f MB)", nombre, size_mb)
        return ruta

    log.info("↓ Descargando: %s", nombre)
    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(ruta, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=f"  {nombre[:40]}") as bar:
            for chunk in resp.iter_content(chunk_size=65_536):
                f.write(chunk)
                bar.update(len(chunk))
        log.info("✓ Guardado: %s", ruta)
    except requests.RequestException as e:
        log.warning("⚠ Error descargando %s: %s", nombre, e)
        if ruta.exists():
            ruta.unlink()
        return None
    return ruta

# ─── CARGA Y NORMALIZACIÓN ───────────────────────────────────────────────────

def _detectar_separador(ruta: Path) -> str:
    """Detecta el separador de columnas (coma o punto y coma) de un CSV."""
    for sep in [",", ";"]:
        try:
            df = pd.read_csv(ruta, nrows=2, sep=sep, encoding="utf-8", low_memory=False)
            if len(df.columns) > 3:
                return sep
        except Exception:
            pass
    return ","

def leer_csv(ruta: Path) -> pd.DataFrame:
    """
    Lee un CSV detectando automáticamente el separador.
    Normaliza nombres de columna a minúsculas sin espacios.
    """
    sep = _detectar_separador(ruta)
    df = pd.read_csv(ruta, sep=sep, encoding="utf-8", low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]
    log.debug("  Columnas detectadas: %s", ", ".join(df.columns[:12]))
    return df

def normalizar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapea variantes de nombres de columna a nombres canónicos,
    parsea fechas y convierte tipos numéricos.
    """
    cols = list(df.columns)

    # Renombrar columnas según mapa canónico
    for canonico, candidatos in MAPA_COLUMNAS.items():
        for cand in candidatos:
            if cand in cols and canonico not in cols:
                df = df.rename(columns={cand: canonico})
                cols = list(df.columns)
                break

    # Parsear fecha desde distintos formatos
    if "periodo" in df.columns:
        s = df["periodo"].astype(str).str.strip()
        if s.str.match(r"^\d{6}$").all():
            df["fecha"] = pd.to_datetime(s, format="%Y%m", errors="coerce")
        elif s.str.match(r"^\d{4}-\d{2}$").all():
            df["fecha"] = pd.to_datetime(s, format="%Y-%m", errors="coerce")
        else:
            df["fecha"] = pd.to_datetime(s, errors="coerce")
    elif "anio" in df.columns and "mes_num" in df.columns:
        df["fecha"] = pd.to_datetime(
            df["anio"].astype(str) + "-" + df["mes_num"].astype(str).str.zfill(2),
            format="%Y-%m", errors="coerce",
        )

    # Convertir columnas numéricas
    for col in ["petroleo_m3", "gas_mm3", "agua_m3"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Normalizar strings
    for col in ["empresa", "cuenca", "yacimiento", "formacion", "tipo_recurso", "provincia"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    return df

def filtrar_vm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra registros correspondientes a Vaca Muerta:
    cuenca Neuquina + formación Vaca Muerta o tipo_recurso No Convencional.
    """
    mask = pd.Series(True, index=df.index)

    if "cuenca" in df.columns:
        mask &= df["cuenca"].str.contains("NEUQUIN", na=False)

    if "formacion" in df.columns:
        tiene_formacion = df["formacion"].notna() & ~df["formacion"].isin(["NAN", "", "NONE"])
        es_vm = df["formacion"].str.contains("VACA MUERTA", na=False)
        es_nc = df.get("tipo_recurso", pd.Series("", index=df.index)).str.contains("NO CONVENCIONAL", na=False)
        mask &= es_vm | (~tiene_formacion & es_nc)
    elif "tipo_recurso" in df.columns:
        mask &= df["tipo_recurso"].str.contains("NO CONVENCIONAL", na=False)

    return df[mask].copy()

# ─── ENRIQUECIMIENTO ─────────────────────────────────────────────────────────

def _clasificar_empresa(nombre: str) -> str:
    """Agrupa operadoras por holding según patrones de nombre."""
    nombre = str(nombre).upper()
    for grupo, claves in GRUPOS.items():
        if any(c in nombre for c in claves):
            return grupo
    partes = nombre.split()
    return partes[0].capitalize() if partes else "Otra"

def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega columnas calculadas:
    - empresa_grupo: holding de la operadora
    - anio, mes, anio_mes: descomposición temporal
    - boe: barriles de petróleo equivalente
    - water_cut_pct: corte de agua (%)
    - gor: gas-oil ratio
    """
    if "empresa" in df.columns:
        df["empresa_grupo"] = df["empresa"].apply(_clasificar_empresa)

    if "fecha" in df.columns:
        df["anio"]     = df["fecha"].dt.year
        df["mes"]      = df["fecha"].dt.month
        df["anio_mes"] = df["fecha"].dt.to_period("M").astype(str)

    if {"petroleo_m3", "gas_mm3"}.issubset(df.columns):
        # gas_mm3 está en MILES de m3 (Capítulo IV) → factor 5.886, no 5886
        df["boe"] = (
            df["petroleo_m3"] * FACTOR_BOE_PETROLEO
            + df["gas_mm3"] * FACTOR_BOE_GAS
        ).round(0)

    if "gas_mm3" in df.columns:
        df["gas_m3"] = df["gas_mm3"] * 1_000

    if {"agua_m3", "petroleo_m3"}.issubset(df.columns):
        liquido = (df["petroleo_m3"] + df["agua_m3"]).astype(float)
        df["water_cut_pct"] = (
            df["agua_m3"].astype(float) / liquido.where(liquido > 0) * 100
        ).round(2)

    if {"gas_m3", "petroleo_m3"}.issubset(df.columns):
        pet = df["petroleo_m3"].astype(float)
        df["gor"] = (df["gas_m3"].astype(float) / pet.where(pet > 0)).round(1)

    return df

# ─── ANÁLISIS DE DECLINACIÓN (ARPS) ──────────────────────────────────────────

def _arps_hiperbolica(t: np.ndarray, qi: float, di: float, b: float) -> np.ndarray:
    """
    Caudal de Arps hiperbólico.
        q(t) = qi / (1 + b·di·t)^(1/b)
    Con t en meses y di en 1/mes. El caso exponencial (b→0) y el harmónico
    (b=1) quedan cubiertos dentro del rango [B_MIN, B_MAX].
    """
    return qi / np.power(1.0 + b * di * t, 1.0 / b)


def _eur_post_pico(qi: float, di: float, b: float) -> float:
    """
    Integra la curva de Arps HIPERBÓLICA MODIFICADA desde el pico hasta el
    abandono y devuelve el volumen pronosticado (unidad nativa del caudal).

    Modificación terminal: mientras la declinación instantánea nominal de la
    hiperbólica sea mayor que la declinación terminal, se usa hiperbólica;
    cuando cae al terminal, se conmuta a exponencial. Esto evita la clásica
    sobreestimación del EUR de Arps en pozos de shale (b alto).
    """
    d_term_mes = 1.0 - (1.0 - D_TERMINAL_ANUAL) ** (1.0 / 12.0)
    q_ab = FRAC_ABANDONO * qi
    total, q, t = 0.0, qi, 1

    # Tramo hiperbólico
    while t <= HORIZONTE_MESES:
        d_inst = di / (1.0 + b * di * t)
        q = qi / (1.0 + b * di * t) ** (1.0 / b)
        if d_inst <= d_term_mes or q <= q_ab:
            break
        total += q
        t += 1

    # Tramo terminal exponencial
    while t <= HORIZONTE_MESES and q > q_ab:
        q *= (1.0 - d_term_mes)
        total += q
        t += 1

    return total


def _ajustar_pozo(serie: np.ndarray) -> Optional[dict]:
    """
    Ajusta Arps a la serie de caudal mensual de un pozo (ordenada por fecha).

    Aplica la práctica estándar de DCA: ajusta desde el pico de producción
    (descarta la rampa inicial de puesta en marcha). El EUR se compone del
    acumulado real pre-pico + el pronóstico de la curva ajustada.

    Returns:
        dict con qi, di, b, R², EUR y metadatos del fit, o None si no hay
        suficientes puntos post-pico o el ajuste no converge.
    """
    q = np.asarray(serie, dtype=float)
    q = q[~np.isnan(q)]
    if len(q) < MIN_MESES_AJUSTE + 1:
        return None

    # Ajustar desde el pico (descartar rampa de puesta en servicio)
    i_pico = int(np.argmax(q))
    np_pre = float(q[:i_pico].sum())
    q_fit = q[i_pico:]
    if len(q_fit) < MIN_MESES_AJUSTE:
        return None

    t = np.arange(len(q_fit), dtype=float)
    qi0 = max(float(q_fit[0]), 1e-6)
    try:
        popt, _ = curve_fit(
            _arps_hiperbolica, t, q_fit,
            p0=[qi0, 0.10, 1.0],
            bounds=([qi0 * 0.5, 1e-4, B_MIN], [qi0 * 2.0, 1.0, B_MAX]),
            maxfev=8000,
        )
    except (RuntimeError, ValueError):
        return None

    qi, di, b = (float(x) for x in popt)
    q_pred = _arps_hiperbolica(t, qi, di, b)
    ss_res = float(np.sum((q_fit - q_pred) ** 2))
    ss_tot = float(np.sum((q_fit - q_fit.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    eur = np_pre + _eur_post_pico(qi, di, b)
    if not np.isfinite(eur) or eur <= 0:
        return None

    di_anual_pct = (1.0 - (1.0 - di) ** 12) * 100  # declinación efectiva anual %
    calidad = (
        "BUENO"      if r2 >= R2_MIN_BUENO else
        "REGULAR"    if r2 >= R2_MIN_REGULAR else
        "DESCARTADO"
    )
    return {
        "qi": round(qi, 1),
        "di_anual_pct": round(di_anual_pct, 1),
        "b_factor": round(b, 3),
        "r2": round(r2, 4),
        "eur_nativo": round(eur, 1),
        "np_pre_pico": round(np_pre, 1),
        "n_meses_ajuste": int(len(q_fit)),
        "meses_hasta_pico": int(i_pico),
        "calidad_fit": calidad,
    }


def t_declinacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta una curva de declinación de Arps a cada pozo y estima su EUR.

    Para cada pozo se ajusta la FASE DOMINANTE (petróleo o gas, según su aporte
    al BOE), lo que permite comparar EUR de pozos de líquidos y de gas sobre una
    base común (EUR en BOE). Descarta pozos con menos de MIN_MESES_AJUSTE meses
    post-pico y marca la calidad del ajuste por R².

    Alimenta type-curves por cohorte (columna anio_inicio) y el análisis de
    sweet spots (EUR normalizado por yacimiento/formación).
    """
    requeridas = {"pozo_id", "fecha", "petroleo_m3", "gas_mm3"}
    if not requeridas.issubset(df.columns):
        log.warning("  ⚠ Faltan columnas para declinación: %s",
                    requeridas - set(df.columns))
        return pd.DataFrame()

    meta_cols = [c for c in ["empresa_grupo", "yacimiento", "formacion"] if c in df.columns]
    filas: list[dict] = []
    descartados = {"pocos_meses": 0, "sin_fit": 0, "r2_bajo": 0}

    pozos = df.sort_values("fecha").groupby("pozo_id", observed=True)
    for pozo_id, g in tqdm(pozos, desc="  Ajustando declinación", unit="pozo"):
        # Determinar fase dominante por aporte al BOE
        boe_pet = g["petroleo_m3"].sum() * FACTOR_BOE_PETROLEO
        boe_gas = g["gas_mm3"].sum() * FACTOR_BOE_GAS
        if max(boe_pet, boe_gas) <= 0:
            continue
        if boe_pet >= boe_gas:
            fase, col, factor = "PETROLEO", "petroleo_m3", FACTOR_BOE_PETROLEO
        else:
            fase, col, factor = "GAS", "gas_mm3", FACTOR_BOE_GAS

        serie = g[col].to_numpy(dtype=float)
        res = _ajustar_pozo(serie)
        if res is None:
            descartados["pocos_meses" if len(serie) < MIN_MESES_AJUSTE + 1 else "sin_fit"] += 1
            continue
        if res["calidad_fit"] == "DESCARTADO":
            descartados["r2_bajo"] += 1

        np_actual = float(g[col].sum())
        eur_boe = res["eur_nativo"] * factor
        fila = {
            "pozo_id": pozo_id,
            **{c: g[c].iloc[0] for c in meta_cols},
            "fase_ajustada": fase,
            "anio_inicio": int(g["fecha"].min().year),
            "qi": res["qi"],
            "di_anual_pct": res["di_anual_pct"],
            "b_factor": res["b_factor"],
            "eur_nativo": res["eur_nativo"],
            "eur_boe": round(eur_boe, 0),
            "np_actual_nativo": round(np_actual, 1),
            "factor_recup_pct": round(np_actual / res["eur_nativo"] * 100, 1)
                                if res["eur_nativo"] > 0 else None,
            "r2": res["r2"],
            "n_meses_ajuste": res["n_meses_ajuste"],
            "meses_hasta_pico": res["meses_hasta_pico"],
            "calidad_fit": res["calidad_fit"],
        }
        filas.append(fila)

    if not filas:
        log.warning("  ⚠ Ningún pozo ajustable para declinación")
        return pd.DataFrame()

    out = pd.DataFrame(filas).sort_values("eur_boe", ascending=False).reset_index(drop=True)
    n_bueno = (out["calidad_fit"] == "BUENO").sum()
    log.info("  Declinación: %s pozos ajustados (%s BUENO, %s REGULAR, %s DESCARTADO)",
             f"{len(out):,}", f"{n_bueno:,}",
             f"{(out['calidad_fit'] == 'REGULAR').sum():,}",
             f"{(out['calidad_fit'] == 'DESCARTADO').sum():,}")
    log.info("  Excluidos: %s pocos meses, %s sin convergencia",
             f"{descartados['pocos_meses']:,}", f"{descartados['sin_fit']:,}")
    return out


# ─── TABLAS ANALÍTICAS ───────────────────────────────────────────────────────

def t_produccion_mensual(df: pd.DataFrame) -> pd.DataFrame:
    """Producción mensual agregada por empresa. Tabla principal del dashboard."""
    g = df.groupby(["anio_mes", "anio", "mes", "empresa_grupo"], observed=True).agg(
        petroleo_m3=("petroleo_m3", "sum"),
        gas_mm3=("gas_mm3", "sum"),
        agua_m3=("agua_m3", "sum"),
        boe=("boe", "sum"),
        pozos_activos=("pozo_id", "nunique"),
    ).reset_index()
    g[["petroleo_m3", "gas_mm3", "boe"]] = g[["petroleo_m3", "gas_mm3", "boe"]].round(1)
    return g.sort_values(["anio_mes", "empresa_grupo"])

def t_por_yacimiento(df: pd.DataFrame) -> pd.DataFrame:
    """Producción anual por yacimiento y empresa. Alimenta treemaps y barras."""
    dims = [c for c in ["yacimiento", "empresa_grupo", "anio"] if c in df.columns]
    if not dims:
        return pd.DataFrame()
    g = df.groupby(dims, observed=True).agg(
        petroleo_m3=("petroleo_m3", "sum"),
        gas_mm3=("gas_mm3", "sum"),
        boe=("boe", "sum"),
        pozos=("pozo_id", "nunique"),
    ).reset_index()
    return g.sort_values("boe", ascending=False)

def t_top_pozos(df: pd.DataFrame, n: int = 200) -> pd.DataFrame:
    """Ranking de los n pozos con mayor producción acumulada en BOE."""
    id_cols = [c for c in ["pozo_id", "empresa_grupo", "yacimiento", "formacion"] if c in df.columns]
    g = (
        df.groupby(id_cols, observed=True)
        .agg(
            petroleo_total_m3=("petroleo_m3", "sum"),
            gas_total_mm3=("gas_mm3", "sum"),
            boe_acumulado=("boe", "sum"),
            meses_activo=("anio_mes", "nunique"),
            primer_mes=("fecha", "min"),
            ultimo_mes=("fecha", "max"),
        )
        .reset_index()
        .sort_values("boe_acumulado", ascending=False)
        .head(n)
    )
    g.insert(0, "rank", range(1, len(g) + 1))
    return g

def t_eficiencia(df: pd.DataFrame) -> pd.DataFrame:
    """Water cut y GOR promedio por pozo. Identifica candidatos a intervención."""
    id_cols = [c for c in ["pozo_id", "empresa_grupo", "yacimiento"] if c in df.columns]
    g = df[df["petroleo_m3"] > 0].groupby(id_cols, observed=True).agg(
        water_cut_prom=("water_cut_pct", "mean"),
        gor_prom=("gor", "mean"),
        petroleo_prom_m3=("petroleo_m3", "mean"),
        meses=("anio_mes", "nunique"),
    ).reset_index()
    g["water_cut_prom"] = g["water_cut_prom"].round(1)
    g["gor_prom"]       = g["gor_prom"].round(1)
    g["etapa_pozo"] = g["water_cut_prom"].apply(
        lambda x: (
            "Temprano (<30%)"    if x < 30 else
            "Intermedio (30-60%)" if x < 60 else
            "Maduro (>60%)"
        ) if pd.notna(x) else "Sin datos"
    )
    return g

def t_market_share(df: pd.DataFrame) -> pd.DataFrame:
    """Participación de mercado anual por empresa expresada en BOE.
    Si el año más reciente tiene menos de 10 meses de datos, se marca como parcial
    pero se incluye igual — el dashboard lo filtra por año completo.
    """
    a = df.groupby(["anio", "empresa_grupo"], observed=True)["boe"].sum().reset_index()
    total_anio = a.groupby("anio")["boe"].transform("sum")
    a["market_share_pct"] = (a["boe"] / total_anio * 100).round(2)
    # Marcar año parcial (menos de 10 meses de datos en el dataset)
    meses_por_anio = df.groupby("anio")["anio_mes"].nunique()
    a["anio_parcial"] = a["anio"].map(lambda x: meses_por_anio.get(x, 0) < 10)
    return a.sort_values(["anio", "market_share_pct"], ascending=[True, False])

def t_nuevos_pozos(df: pd.DataFrame) -> pd.DataFrame:
    """Pozos nuevos por mes (proxy de actividad de perforación)."""
    agg_dict: dict = {
        "primer_mes":    ("fecha", "min"),
        "empresa_grupo": ("empresa_grupo", "first"),
    }
    if "yacimiento" in df.columns:
        agg_dict["yacimiento"] = ("yacimiento", "first")

    p = df.groupby("pozo_id", observed=True).agg(**agg_dict).reset_index()
    p["anio_mes_inicio"] = p["primer_mes"].dt.to_period("M").astype(str)
    group_cols = [c for c in ["anio_mes_inicio", "empresa_grupo"] if c in p.columns]
    return (
        p.groupby(group_cols, observed=True)
        .size()
        .reset_index(name="pozos_nuevos")
        .sort_values("anio_mes_inicio")
    )

# ─── PERSISTENCIA ────────────────────────────────────────────────────────────

def guardar(df: pd.DataFrame, nombre: str) -> None:
    """Exporta un DataFrame como CSV UTF-8 con BOM (compatible con Excel/PBI)."""
    CARPETA_OUTPUT.mkdir(exist_ok=True)
    ruta = CARPETA_OUTPUT / nombre
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    log.info("✓ %s  (%s filas)", nombre, f"{len(df):,}")

# ─── PUBLICACIÓN GITHUB PAGES ────────────────────────────────────────────────

def copiar_a_docs() -> None:
    """
    Copia todos los CSVs generados a docs/data/ para que
    GitHub Pages los sirva como fuente pública del dashboard HTML.
    """
    CARPETA_DOCS.mkdir(parents=True, exist_ok=True)
    copiados = 0
    for csv in CARPETA_OUTPUT.glob("*.csv"):
        shutil.copy2(csv, CARPETA_DOCS / csv.name)
        copiados += 1
    log.info("✓ %s CSVs copiados a %s/", copiados, CARPETA_DOCS)

# ─── MAIN ────────────────────────────────────────────────────────────────────

TABLAS: list[tuple] = [
    (t_produccion_mensual, "01_vm_produccion_mensual.csv"),
    (t_por_yacimiento,     "02_vm_por_yacimiento.csv"),
    (t_top_pozos,          "03_vm_top_pozos.csv"),
    (t_eficiencia,         "04_vm_eficiencia_pozos.csv"),
    (t_market_share,       "05_vm_market_share.csv"),
    (t_nuevos_pozos,       "06_vm_nuevos_pozos.csv"),
    (t_declinacion,        "08_vm_declinacion.csv"),
]

def main() -> None:
    log.info("=" * 55)
    log.info("  VACA MUERTA — Pipeline de datos")
    log.info("=" * 55)

    # ── 1. Descarga ──────────────────────────────────────────
    log.info("[1/4] Descargando fuentes")
    rutas = {k: descargar(v, f"{k}.csv") for k, v in FUENTES.items()}
    rutas = {k: v for k, v in rutas.items() if v is not None}

    if not rutas:
        log.error("Sin archivos descargados. Abortando.")
        sys.exit(1)

    # ── 2. Carga y filtro ────────────────────────────────────
    log.info("[2/4] Cargando y filtrando Vaca Muerta")
    frames: list[pd.DataFrame] = []

    for nombre, ruta in rutas.items():
        log.info("  → %s", nombre)
        try:
            df_raw  = leer_csv(ruta)
            df_norm = normalizar(df_raw)
            df_vm   = filtrar_vm(df_norm)
            log.info("    Total: %s  →  VM: %s", f"{len(df_raw):,}", f"{len(df_vm):,}")
            if len(df_vm) > 0:
                frames.append(df_vm)
        except Exception as exc:
            log.warning("    ⚠ Error en %s: %s", nombre, exc)

    if not frames:
        log.error("Sin datos de Vaca Muerta tras el filtrado. Abortando.")
        sys.exit(1)

    # Consolidar y deduplicar
    df = pd.concat(frames, ignore_index=True)
    key_cols = [c for c in ["pozo_id", "fecha"] if c in df.columns]
    if len(key_cols) == 2:
        antes = len(df)
        df = df.drop_duplicates(subset=key_cols).reset_index(drop=True)
        log.info("  Duplicados eliminados: %s", f"{antes - len(df):,}")

    # ── 3. Enriquecimiento ───────────────────────────────────
    log.info("[3/4] Enriqueciendo dataset")
    df = enriquecer(df)

    if "fecha" in df.columns:
        log.info("  Período:     %s → %s",
                 df["fecha"].min().strftime("%b %Y"),
                 df["fecha"].max().strftime("%b %Y"))
    log.info("  Empresas:    %s", df["empresa_grupo"].nunique() if "empresa_grupo" in df.columns else "?")
    log.info("  Yacimientos: %s", df["yacimiento"].nunique()   if "yacimiento"     in df.columns else "?")
    log.info("  Pozos:       %s", f"{df['pozo_id'].nunique():,}" if "pozo_id" in df.columns else "?")
    log.info("  Registros:   %s", f"{len(df):,}")

    # ── 4. Exportar ──────────────────────────────────────────
    log.info("[4/4] Exportando tablas para Power BI")
    guardar(df, "07_vm_raw_filtrado.csv")

    for fn, label in TABLAS:
        try:
            guardar(fn(df), label)
        except Exception as exc:
            log.warning("  ⚠ Error generando %s: %s", label, exc)

    log.info("=" * 55)
    log.info("✅ Pipeline completado. Archivos en: ./%s/", CARPETA_OUTPUT)
    log.info("=" * 55)

    # ── 5. Publicar para GitHub Pages ────────────────────────
    log.info("[5/5] Copiando CSVs a docs/data/ para GitHub Pages")
    copiar_a_docs()
    log.info("=" * 55)
    log.info("🌐 Dashboard listo. Commitear y pushear para publicar.")
    log.info("=" * 55)

if __name__ == "__main__":
    main()
