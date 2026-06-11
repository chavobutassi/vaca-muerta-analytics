"""
=============================================================
  VACA MUERTA — Pipeline de datos para dashboard Power BI
  Fuente: Secretaría de Energía — datos.energia.gob.ar
  Autor: Claudio Butassi
  Versión: 2.1
=============================================================

DESCRIPCIÓN:
    Pipeline ETL que descarga, normaliza, filtra y transforma
    datos de producción de pozos no convencionales de Vaca Muerta,
    exportando tablas analíticas listas para Power BI, un reporte
    de calidad de datos y metadatos de trazabilidad del run.

REQUISITOS:
    pip install pandas requests tqdm openpyxl

OUTPUTS (carpeta ./output/):
    01_vm_produccion_mensual.csv     → producción mensual por empresa
    02_vm_por_yacimiento.csv         → producción por yacimiento y año
    03_vm_top_pozos.csv              → ranking de pozos por producción acumulada
    04_vm_eficiencia_pozos.csv       → water cut y GOR por pozo
    05_vm_market_share.csv           → participación de mercado por empresa
    06_vm_nuevos_pozos.csv           → pozos nuevos por mes (proxy de perforación)
    07_vm_raw_filtrado.csv           → dataset completo Vaca Muerta
    08_vm_declinacion_cohortes.csv   → curvas de declinación por cohorte (vintage)
    09_vm_data_quality.csv           → reporte de chequeos de calidad de datos
    _metadata.json                   → trazabilidad del run (fecha, período, filas)
=============================================================
"""

from __future__ import annotations

import os
import sys
import json
import shutil
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
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
        df["boe"] = (df["petroleo_m3"] * 6.29 + df["gas_mm3"] * 5_886).round(0)

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

# ─── VALIDACIÓN DE CALIDAD DE DATOS ──────────────────────────────────────────

def validar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ejecuta chequeos de calidad sobre el dataset consolidado.

    Cada chequeo cuenta cuántos registros violan una regla de negocio.
    No modifica los datos: solo informa. La decisión de qué hacer con
    los registros problemáticos queda explícita y documentada.

    Chequeos implementados:
        1. produccion_negativa  → petróleo, gas o agua < 0 (imposible físicamente)
        2. fecha_futura         → registros con fecha posterior a hoy
        3. fecha_invalida       → registros donde no se pudo parsear la fecha
        4. sin_empresa          → registros sin operadora identificable
        5. sin_pozo_id          → registros sin identificador de pozo
        6. water_cut_invalido   → corte de agua fuera del rango [0, 100]
        7. salto_mensual_brusco → meses donde la producción total varía ±50%
                                  vs. el mes anterior (posible carga incompleta)

    Returns:
        DataFrame con columnas: chequeo, registros_afectados, pct_del_total,
        severidad, descripcion. Se exporta como 09_vm_data_quality.csv.
    """
    n = len(df)
    resultados: list[dict] = []

    def _check(nombre: str, cant: int, severidad: str, desc: str) -> None:
        resultados.append({
            "chequeo": nombre,
            "registros_afectados": int(cant),
            "pct_del_total": round(cant / n * 100, 3) if n else 0.0,
            "severidad": severidad,
            "descripcion": desc,
        })

    # 1. Producción negativa
    neg = 0
    for col in ["petroleo_m3", "gas_mm3", "agua_m3"]:
        if col in df.columns:
            neg += int((df[col] < 0).sum())
    _check("produccion_negativa", neg, "alta",
           "Valores de producción menores a cero (imposible físicamente)")

    # 2 y 3. Fechas
    if "fecha" in df.columns:
        hoy = pd.Timestamp.now().normalize()
        _check("fecha_futura", int((df["fecha"] > hoy).sum()), "alta",
               "Registros con período posterior a la fecha actual")
        _check("fecha_invalida", int(df["fecha"].isna().sum()), "media",
               "Registros donde el período no pudo parsearse a fecha")

    # 4. Sin empresa
    if "empresa" in df.columns:
        sin_emp = int(df["empresa"].isin(["", "NAN", "NONE"]).sum()
                      + df["empresa"].isna().sum())
        _check("sin_empresa", sin_emp, "media",
               "Registros sin operadora identificable (afecta market share)")

    # 5. Sin pozo_id
    if "pozo_id" in df.columns:
        _check("sin_pozo_id", int(df["pozo_id"].isna().sum()), "alta",
               "Registros sin identificador de pozo (afecta deduplicación y rankings)")

    # 6. Water cut fuera de rango
    if "water_cut_pct" in df.columns:
        wc = df["water_cut_pct"]
        _check("water_cut_invalido", int(((wc < 0) | (wc > 100)).sum()), "baja",
               "Corte de agua fuera del rango físico 0-100%")

    # 7. Saltos bruscos en la producción mensual total
    if {"anio_mes", "boe"}.issubset(df.columns):
        serie = df.groupby("anio_mes")["boe"].sum().sort_index()
        var = serie.pct_change().abs()
        saltos = int((var > 0.5).sum())
        _check("salto_mensual_brusco", saltos, "media",
               "Meses cuya producción total varía más de ±50% vs. el mes anterior "
               "(posible mes con carga incompleta en la fuente)")

    reporte = pd.DataFrame(resultados)

    # Log resumido en consola
    problemas = reporte[reporte["registros_afectados"] > 0]
    if problemas.empty:
        log.info("  ✓ Calidad de datos: todos los chequeos OK")
    else:
        for _, r in problemas.iterrows():
            log.warning("  ⚠ %s: %s registros (%s%%)",
                        r["chequeo"], f"{r['registros_afectados']:,}", r["pct_del_total"])

    return reporte

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

def t_declinacion_cohortes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Curvas de declinación tipo (type curves) por cohorte de pozos.

    Concepto:
        Una "cohorte" agrupa todos los pozos que iniciaron producción el mismo
        año (vintage). Para cada pozo se calcula su "mes de vida": el mes 0 es
        su primer mes con producción, el mes 1 el siguiente, etc. Promediando
        la producción de todos los pozos de una cohorte en cada mes de vida se
        obtiene la curva de declinación típica de esa camada.

    Para qué sirve:
        - Comparar cohortes responde la pregunta clave del shale: ¿los pozos
          que se perforan hoy son mejores que los de hace 5 años?
        - Si la curva 2024 está por encima de la 2019 en cada mes de vida,
          hubo mejora real (mejor diseño de fractura, ramas más largas, etc.)
          y no solo más pozos.
        - El pico inicial (mes 0-3) y la velocidad de caída posterior son la
          firma característica de los no convencionales: declinan 60-70% el
          primer año, a diferencia de los convencionales.

    Detalles de implementación:
        - mes_vida se calcula en meses calendario: (año*12+mes) actual menos
          (año*12+mes) del primer mes del pozo.
        - Se promedia por pozo activo (no se suma) para que la curva refleje
          el pozo "típico" y no la cantidad de pozos de la cohorte.
        - Se descartan cohortes con menos de 5 pozos (poca representatividad)
          y meses de vida > 120 (colas con muy pocos pozos generan ruido).

    Returns:
        DataFrame con: cohorte, mes_vida, pozos_en_muestra,
        petroleo_prom_m3, gas_prom_mm3, boe_prom.
        Se exporta como 08_vm_declinacion_cohortes.csv.
    """
    req = {"pozo_id", "fecha", "boe"}
    if not req.issubset(df.columns):
        return pd.DataFrame()

    d = df[df["fecha"].notna()].copy()

    # Primer mes de producción de cada pozo → define su cohorte
    primer = d.groupby("pozo_id")["fecha"].transform("min")
    d["cohorte"] = primer.dt.year

    # Mes de vida del pozo (0 = primer mes con producción)
    d["mes_vida"] = (
        (d["fecha"].dt.year - primer.dt.year) * 12
        + (d["fecha"].dt.month - primer.dt.month)
    )

    g = d.groupby(["cohorte", "mes_vida"], observed=True).agg(
        pozos_en_muestra=("pozo_id", "nunique"),
        petroleo_prom_m3=("petroleo_m3", "mean"),
        gas_prom_mm3=("gas_mm3", "mean"),
        boe_prom=("boe", "mean"),
    ).reset_index()

    # Filtros de representatividad
    g = g[(g["pozos_en_muestra"] >= 5) & (g["mes_vida"] <= 120)]

    for col in ["petroleo_prom_m3", "gas_prom_mm3", "boe_prom"]:
        g[col] = g[col].round(1)

    return g.sort_values(["cohorte", "mes_vida"])

# ─── PERSISTENCIA ────────────────────────────────────────────────────────────

def guardar(df: pd.DataFrame, nombre: str) -> None:
    """Exporta un DataFrame como CSV UTF-8 con BOM (compatible con Excel/PBI)."""
    CARPETA_OUTPUT.mkdir(exist_ok=True)
    ruta = CARPETA_OUTPUT / nombre
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    log.info("✓ %s  (%s filas)", nombre, f"{len(df):,}")

def generar_metadata(df: pd.DataFrame, filas_por_tabla: dict[str, int]) -> None:
    """
    Escribe _metadata.json con la trazabilidad del run.

    Permite que el dashboard muestre "Datos actualizados al ..." y deja
    registro auditable de qué se procesó en cada ejecución:
        - fecha y hora UTC de ejecución
        - período cubierto por los datos (primer y último mes)
        - totales: registros, pozos, empresas, yacimientos
        - cantidad de filas exportadas en cada tabla
    """
    meta = {
        "ejecutado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "periodo_desde": df["fecha"].min().strftime("%Y-%m") if "fecha" in df.columns else None,
        "periodo_hasta": df["fecha"].max().strftime("%Y-%m") if "fecha" in df.columns else None,
        "registros": int(len(df)),
        "pozos": int(df["pozo_id"].nunique()) if "pozo_id" in df.columns else None,
        "empresas": int(df["empresa_grupo"].nunique()) if "empresa_grupo" in df.columns else None,
        "yacimientos": int(df["yacimiento"].nunique()) if "yacimiento" in df.columns else None,
        "tablas": filas_por_tabla,
        "fuente": "Secretaría de Energía — datos.energia.gob.ar",
    }
    ruta = CARPETA_OUTPUT / "_metadata.json"
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    log.info("✓ _metadata.json  (run %s)", meta["ejecutado_utc"])

# ─── PUBLICACIÓN GITHUB PAGES ────────────────────────────────────────────────

def copiar_a_docs() -> None:
    """
    Copia todos los CSVs generados a docs/data/ para que
    GitHub Pages los sirva como fuente pública del dashboard HTML.
    """
    CARPETA_DOCS.mkdir(parents=True, exist_ok=True)
    copiados = 0
    for archivo in list(CARPETA_OUTPUT.glob("*.csv")) + list(CARPETA_OUTPUT.glob("*.json")):
        shutil.copy2(archivo, CARPETA_DOCS / archivo.name)
        copiados += 1
    log.info("✓ %s archivos copiados a %s/", copiados, CARPETA_DOCS)

# ─── MAIN ────────────────────────────────────────────────────────────────────

TABLAS: list[tuple] = [
    (t_produccion_mensual,    "01_vm_produccion_mensual.csv"),
    (t_por_yacimiento,        "02_vm_por_yacimiento.csv"),
    (t_top_pozos,             "03_vm_top_pozos.csv"),
    (t_eficiencia,            "04_vm_eficiencia_pozos.csv"),
    (t_market_share,          "05_vm_market_share.csv"),
    (t_nuevos_pozos,          "06_vm_nuevos_pozos.csv"),
    (t_declinacion_cohortes,  "08_vm_declinacion_cohortes.csv"),
]

def main() -> None:
    log.info("=" * 55)
    log.info("  VACA MUERTA — Pipeline de datos")
    log.info("=" * 55)

    # ── 1. Descarga ──────────────────────────────────────────
    log.info("[1/5] Descargando fuentes")
    rutas = {k: descargar(v, f"{k}.csv") for k, v in FUENTES.items()}
    rutas = {k: v for k, v in rutas.items() if v is not None}

    if not rutas:
        log.error("Sin archivos descargados. Abortando.")
        sys.exit(1)

    # ── 2. Carga y filtro ────────────────────────────────────
    log.info("[2/5] Cargando y filtrando Vaca Muerta")
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
    log.info("[3/5] Enriqueciendo dataset")
    df = enriquecer(df)

    if "fecha" in df.columns:
        log.info("  Período:     %s → %s",
                 df["fecha"].min().strftime("%b %Y"),
                 df["fecha"].max().strftime("%b %Y"))
    log.info("  Empresas:    %s", df["empresa_grupo"].nunique() if "empresa_grupo" in df.columns else "?")
    log.info("  Yacimientos: %s", df["yacimiento"].nunique()   if "yacimiento"     in df.columns else "?")
    log.info("  Pozos:       %s", f"{df['pozo_id'].nunique():,}" if "pozo_id" in df.columns else "?")
    log.info("  Registros:   %s", f"{len(df):,}")

    # ── 3b. Validación de calidad ────────────────────────────
    log.info("[3b]  Validando calidad de datos")
    reporte_calidad = validar(df)

    # ── 4. Exportar ──────────────────────────────────────────
    log.info("[4/5] Exportando tablas para Power BI")
    filas_por_tabla: dict[str, int] = {}

    guardar(df, "07_vm_raw_filtrado.csv")
    filas_por_tabla["07_vm_raw_filtrado.csv"] = len(df)

    for fn, label in TABLAS:
        try:
            tabla = fn(df)
            guardar(tabla, label)
            filas_por_tabla[label] = len(tabla)
        except Exception as exc:
            log.warning("  ⚠ Error generando %s: %s", label, exc)

    guardar(reporte_calidad, "09_vm_data_quality.csv")
    filas_por_tabla["09_vm_data_quality.csv"] = len(reporte_calidad)

    generar_metadata(df, filas_por_tabla)

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
