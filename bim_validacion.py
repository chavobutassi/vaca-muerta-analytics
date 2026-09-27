"""
=============================================================
  VACA MUERTA — Módulo de validación de datos BIM
  Autor: Claudio Butassi
  Versión: 1.0
=============================================================

¿QUÉ ES ESTO?
    BIM (Building Information Modeling) = un modelo 3D de una
    instalación donde cada objeto (tanque, bomba, cañería, cabeza
    de pozo) trae además DATOS: tag, fabricante, estado, presión
    de diseño, etc. El formato abierto estándar es IFC (.ifc).

    Validar datos BIM NO es revisar la geometría: es revisar que
    esos datos estén completos, bien escritos, sean únicos y
    coherentes con otras fuentes. Es el mismo trabajo que hace
    09_vm_data_quality.csv con la producción, aplicado a activos
    físicos.

¿POR QUÉ UN MODELO DEMO?
    No existen modelos BIM públicos de instalaciones de Vaca
    Muerta. Por eso el módulo genera una locación SINTÉTICA
    (un pad con 4 pozos y su batería de producción) y le siembra
    errores a propósito, para mostrar qué detecta cada regla.
    Si se le pasa un .ifc real, valida ese archivo en su lugar.

FLUJO (4 pasos):
    1. MODELO   → genera (o lee) el archivo IFC
    2. EXTRAER  → convierte cada activo en una fila de tabla
    3. VALIDAR  → corre 7 reglas y registra cada falla
    4. EXPORTAR → 3 CSVs para el dashboard (mismo formato que el pipeline)

REGLAS:
    R01  Tag obligatorio          todo activo tiene TagActivo
    R02  Nomenclatura del tag     formato PREFIJO-NNN según tipo (CP-001, TK-002…)
    R03  Tag único                ningún tag se repite en el modelo
    R04  Propiedades obligatorias cada tipo de activo tiene sus campos mínimos
    R05  Dominio de valores       Estado en lista válida; presión/capacidad > 0
    R06  Ubicación espacial       el activo está asignado a un sector/nivel
    R07  Cruce con producción     el PozoId de cada cabeza de pozo existe en
                                  07_vm_raw_filtrado.csv (puente BIM ↔ producción)

ERRORES SEMBRADOS EN EL DEMO (lo que el validador debería encontrar):
    - TK-02        tag mal formado (falta un dígito)              → R02
    - BM-001       tag duplicado en dos bombas                    → R03
    - Separador    sin tag                                        → R01
    - Tanque TK-003 sin CapacidadNominal_m3                       → R04
    - Bomba BM-002 con Estado "OK" (no está en la lista)          → R05
    - Línea LN-002 con presión de diseño negativa                 → R05
    - Bomba BM-003 sin sector asignado                            → R06
    - Cabeza CP-004 con un PozoId que no existe en producción     → R07

USO:
    python bim_validacion.py                → genera el demo y lo valida
    python bim_validacion.py modelo.ifc     → valida un modelo propio

REQUISITOS:
    pip install ifcopenshell pandas

OUTPUTS (carpeta ./output/, copiados a docs/data/):
    10_bim_inventario.csv   → un activo por fila con sus propiedades
    10_bim_hallazgos.csv    → una fila por cada falla detectada
    10_bim_resumen.csv      → % de cumplimiento por regla
=============================================================
"""

from __future__ import annotations

import re
import sys
import shutil
import logging
from pathlib import Path

import pandas as pd
import ifcopenshell
import ifcopenshell.api
import ifcopenshell.util.element as util_el

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── CONFIGURACIÓN ───────────────────────────────────────────────────────────

CARPETA_OUTPUT = Path("output")
CARPETA_DOCS   = Path("docs/data")
CARPETA_MODELO = Path("modelos_bim")
RAW_PRODUCCION = CARPETA_OUTPUT / "07_vm_raw_filtrado.csv"

PSET = "Pset_Activo_VM"   # property set propio donde viven los datos del activo

# Catálogo de tipos de activo. Es la "especificación" contra la que se valida:
# en una empresa esto sale del manual de entregables BIM del proyecto.
TIPOS_ACTIVO: dict[str, dict] = {
    "CABEZA_POZO": {"prefijo": "CP",  "clase_ifc": "IfcBuildingElementProxy",
                    "obligatorias": ["TagActivo", "Estado", "Fabricante", "PozoId", "PresionDiseno_kPa"]},
    "SEPARADOR":   {"prefijo": "SEP", "clase_ifc": "IfcBuildingElementProxy",
                    "obligatorias": ["TagActivo", "Estado", "Fabricante", "PresionDiseno_kPa"]},
    "TANQUE":      {"prefijo": "TK",  "clase_ifc": "IfcTank",
                    "obligatorias": ["TagActivo", "Estado", "Fabricante", "CapacidadNominal_m3"]},
    "BOMBA":       {"prefijo": "BM",  "clase_ifc": "IfcPump",
                    "obligatorias": ["TagActivo", "Estado", "Fabricante", "PresionDiseno_kPa"]},
    "LINEA":       {"prefijo": "LN",  "clase_ifc": "IfcPipeSegment",
                    "obligatorias": ["TagActivo", "Estado", "PresionDiseno_kPa"]},
}

ESTADOS_VALIDOS = {"OPERATIVO", "FUERA_DE_SERVICIO", "PROYECTADO"}

# Severidad: CRITICA impide usar el dato; MAYOR hay que corregirlo; MENOR es de forma.
REGLAS: dict[str, tuple[str, str]] = {
    "R01": ("Tag obligatorio",          "CRITICA"),
    "R02": ("Nomenclatura del tag",     "MENOR"),
    "R03": ("Tag único",                "CRITICA"),
    "R04": ("Propiedades obligatorias", "MAYOR"),
    "R05": ("Dominio de valores",       "MAYOR"),
    "R06": ("Ubicación espacial",       "MAYOR"),
    "R07": ("Cruce con producción",     "CRITICA"),
}

# ─── PASO 1: MODELO DEMO ─────────────────────────────────────────────────────

def _pozos_de_referencia() -> list[str]:
    """
    Toma 3 pozo_id reales del dataset de producción para que el cruce R07
    sea genuino. Si el pipeline todavía no corrió, usa IDs de ejemplo.
    """
    if RAW_PRODUCCION.exists():
        ids = pd.read_csv(RAW_PRODUCCION, usecols=["pozo_id"])["pozo_id"].dropna().astype(str).unique()
        if len(ids) >= 3:
            return list(ids[:3])
    return ["DEMO-0001", "DEMO-0002", "DEMO-0003"]


def generar_modelo_demo(ruta: Path) -> Path:
    """
    Crea un IFC sintético: Proyecto → Sitio (PAD) → Batería → 2 sectores,
    con 13 activos. Incluye los errores sembrados listados arriba.
    """
    api = ifcopenshell.api.run
    m = api("project.create_file", version="IFC4")
    proyecto = api("root.create_entity", m, ifc_class="IfcProject", name="Vaca Muerta — Locación demo")
    api("unit.assign_unit", m)
    api("context.add_context", m, context_type="Model")

    sitio    = api("root.create_entity", m, ifc_class="IfcSite", name="PAD-DEMO-01")
    bateria  = api("root.create_entity", m, ifc_class="IfcBuilding", name="Batería de producción")
    sec_pozos = api("root.create_entity", m, ifc_class="IfcBuildingStorey", name="Sector pozos")
    sec_proc  = api("root.create_entity", m, ifc_class="IfcBuildingStorey", name="Sector proceso")
    api("aggregate.assign_object", m, relating_object=proyecto, products=[sitio])
    api("aggregate.assign_object", m, relating_object=sitio,    products=[bateria])
    api("aggregate.assign_object", m, relating_object=bateria,  products=[sec_pozos, sec_proc])

    pozos = _pozos_de_referencia()

    # (tipo, nombre, sector, propiedades) — los comentarios marcan los errores sembrados
    activos = [
        ("CABEZA_POZO", "Cabeza de pozo 1", sec_pozos, {"TagActivo": "CP-001", "Estado": "OPERATIVO", "Fabricante": "Fab A", "PozoId": pozos[0], "PresionDiseno_kPa": 69000.0}),
        ("CABEZA_POZO", "Cabeza de pozo 2", sec_pozos, {"TagActivo": "CP-002", "Estado": "OPERATIVO", "Fabricante": "Fab A", "PozoId": pozos[1], "PresionDiseno_kPa": 69000.0}),
        ("CABEZA_POZO", "Cabeza de pozo 3", sec_pozos, {"TagActivo": "CP-003", "Estado": "OPERATIVO", "Fabricante": "Fab A", "PozoId": pozos[2], "PresionDiseno_kPa": 69000.0}),
        ("CABEZA_POZO", "Cabeza de pozo 4", sec_pozos, {"TagActivo": "CP-004", "Estado": "PROYECTADO", "Fabricante": "Fab A", "PozoId": "XX-9999", "PresionDiseno_kPa": 69000.0}),  # R07
        ("SEPARADOR",   "Separador trifásico", sec_proc, {"TagActivo": "", "Estado": "OPERATIVO", "Fabricante": "Fab B", "PresionDiseno_kPa": 8600.0}),                    # R01
        ("TANQUE",      "Tanque de petróleo 1", sec_proc, {"TagActivo": "TK-001", "Estado": "OPERATIVO", "Fabricante": "Fab C", "CapacidadNominal_m3": 160.0}),
        ("TANQUE",      "Tanque de petróleo 2", sec_proc, {"TagActivo": "TK-02",  "Estado": "OPERATIVO", "Fabricante": "Fab C", "CapacidadNominal_m3": 160.0}),        # R02
        ("TANQUE",      "Tanque de agua",       sec_proc, {"TagActivo": "TK-003", "Estado": "OPERATIVO", "Fabricante": "Fab C"}),                                     # R04
        ("BOMBA",       "Bomba de transferencia 1", sec_proc, {"TagActivo": "BM-001", "Estado": "OPERATIVO", "Fabricante": "Fab D", "PresionDiseno_kPa": 5000.0}),
        ("BOMBA",       "Bomba de transferencia 2", sec_proc, {"TagActivo": "BM-001", "Estado": "OK", "Fabricante": "Fab D", "PresionDiseno_kPa": 5000.0}),          # R03 + R05
        ("BOMBA",       "Bomba de inyección",       None,     {"TagActivo": "BM-003", "Estado": "OPERATIVO", "Fabricante": "Fab D", "PresionDiseno_kPa": 12000.0}),  # R06
        ("LINEA",       "Línea de conducción 1",    sec_proc, {"TagActivo": "LN-001", "Estado": "OPERATIVO", "PresionDiseno_kPa": 9900.0}),
        ("LINEA",       "Línea de conducción 2",    sec_proc, {"TagActivo": "LN-002", "Estado": "OPERATIVO", "PresionDiseno_kPa": -9900.0}),                  # R05
    ]

    for tipo, nombre, sector, props in activos:
        el = api("root.create_entity", m, ifc_class=TIPOS_ACTIVO[tipo]["clase_ifc"], name=nombre)
        el.ObjectType = tipo
        pset = api("pset.add_pset", m, product=el, name=PSET)
        api("pset.edit_pset", m, pset=pset, properties=props)
        if sector is not None:
            api("spatial.assign_container", m, relating_structure=sector, products=[el])

    ruta.parent.mkdir(parents=True, exist_ok=True)
    m.write(str(ruta))
    log.info("✓ Modelo demo generado: %s  (%s activos)", ruta, len(activos))
    return ruta

# ─── PASO 2: EXTRAER INVENTARIO ──────────────────────────────────────────────

def extraer_inventario(modelo: ifcopenshell.file) -> pd.DataFrame:
    """
    Recorre los activos físicos del modelo y arma una tabla plana:
    una fila por activo, una columna por propiedad. A partir de acá
    todo es pandas, igual que en el pipeline de producción.
    """
    filas = []
    for el in modelo.by_type("IfcElement"):
        props = util_el.get_psets(el).get(PSET, {})
        contenedor = util_el.get_container(el)
        filas.append({
            "global_id": el.GlobalId,
            "clase_ifc": el.is_a(),
            "tipo_activo": el.ObjectType or "",
            "nombre": el.Name or "",
            "sector": contenedor.Name if contenedor else "",
            **{k: v for k, v in props.items() if k != "id"},
        })
    df = pd.DataFrame(filas)
    for col in ["TagActivo", "Estado", "Fabricante", "PozoId", "PresionDiseno_kPa", "CapacidadNominal_m3"]:
        if col not in df.columns:
            df[col] = pd.NA
    return df

# ─── PASO 3: REGLAS ──────────────────────────────────────────────────────────
# Cada regla recibe el inventario y devuelve (evaluados, lista de fallas).
# Una falla es un dict con el índice de la fila y un detalle legible.

def _vacio(v) -> bool:
    return v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == ""


def r01_tag_obligatorio(df):
    fallas = [{"idx": i, "detalle": "El activo no tiene TagActivo"}
              for i, r in df.iterrows() if _vacio(r["TagActivo"])]
    return len(df), fallas


def r02_nomenclatura(df):
    fallas, evaluados = [], 0
    for i, r in df.iterrows():
        if _vacio(r["TagActivo"]) or r["tipo_activo"] not in TIPOS_ACTIVO:
            continue  # sin tag lo reporta R01; tipo desconocido no tiene regla de formato
        evaluados += 1
        prefijo = TIPOS_ACTIVO[r["tipo_activo"]]["prefijo"]
        if not re.fullmatch(rf"{prefijo}-\d{{3}}", str(r["TagActivo"])):
            fallas.append({"idx": i, "detalle": f"'{r['TagActivo']}' no cumple el formato {prefijo}-NNN"})
    return evaluados, fallas


def r03_tag_unico(df):
    con_tag = df[~df["TagActivo"].apply(_vacio)]
    repetidos = con_tag[con_tag.duplicated("TagActivo", keep=False)]
    fallas = [{"idx": i, "detalle": f"Tag '{r['TagActivo']}' repetido en {int((con_tag['TagActivo'] == r['TagActivo']).sum())} activos"}
              for i, r in repetidos.iterrows()]
    return len(con_tag), fallas


def r04_obligatorias(df):
    fallas, evaluados = [], 0
    for i, r in df.iterrows():
        spec = TIPOS_ACTIVO.get(r["tipo_activo"])
        if not spec:
            continue
        evaluados += 1
        faltan = [p for p in spec["obligatorias"] if p != "TagActivo" and _vacio(r.get(p))]
        if faltan:
            fallas.append({"idx": i, "detalle": "Faltan: " + ", ".join(faltan)})
    return evaluados, fallas


def r05_dominio(df):
    fallas = []
    for i, r in df.iterrows():
        problemas = []
        if not _vacio(r["Estado"]) and r["Estado"] not in ESTADOS_VALIDOS:
            problemas.append(f"Estado '{r['Estado']}' no válido (usar {', '.join(sorted(ESTADOS_VALIDOS))})")
        for campo in ["PresionDiseno_kPa", "CapacidadNominal_m3"]:
            v = r.get(campo)
            if not _vacio(v) and float(v) <= 0:
                problemas.append(f"{campo} = {v} (debe ser > 0)")
        if problemas:
            fallas.append({"idx": i, "detalle": "; ".join(problemas)})
    return len(df), fallas


def r06_ubicacion(df):
    fallas = [{"idx": i, "detalle": "El activo no está asignado a ningún sector del modelo"}
              for i, r in df.iterrows() if _vacio(r["sector"])]
    return len(df), fallas


def r07_cruce_produccion(df):
    cabezas = df[(df["tipo_activo"] == "CABEZA_POZO") & ~df["PozoId"].apply(_vacio)]
    if not RAW_PRODUCCION.exists():
        log.warning("  R07: no existe %s — se valida contra los IDs de ejemplo", RAW_PRODUCCION)
        referencia = {"DEMO-0001", "DEMO-0002", "DEMO-0003"}
    else:
        referencia = set(pd.read_csv(RAW_PRODUCCION, usecols=["pozo_id"])["pozo_id"].dropna().astype(str))
    fallas = [{"idx": i, "detalle": f"PozoId '{r['PozoId']}' no existe en el dataset de producción"}
              for i, r in cabezas.iterrows() if str(r["PozoId"]) not in referencia]
    return len(cabezas), fallas


FUNCIONES_REGLA = {
    "R01": r01_tag_obligatorio,
    "R02": r02_nomenclatura,
    "R03": r03_tag_unico,
    "R04": r04_obligatorias,
    "R05": r05_dominio,
    "R06": r06_ubicacion,
    "R07": r07_cruce_produccion,
}


def validar(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Corre todas las reglas. Devuelve (hallazgos, resumen por regla)."""
    hallazgos, resumen = [], []
    for rid, fn in FUNCIONES_REGLA.items():
        nombre, severidad = REGLAS[rid]
        evaluados, fallas = fn(df)
        for f in fallas:
            r = df.loc[f["idx"]]
            hallazgos.append({
                "regla_id": rid, "regla": nombre, "severidad": severidad,
                "tag": r["TagActivo"] if not _vacio(r["TagActivo"]) else "(sin tag)",
                "tipo_activo": r["tipo_activo"], "nombre": r["nombre"],
                "global_id": r["global_id"], "detalle": f["detalle"],
            })
        cumplimiento = round((evaluados - len(fallas)) / evaluados * 100, 1) if evaluados else None
        resumen.append({
            "regla_id": rid, "regla": nombre, "severidad": severidad,
            "evaluados": evaluados, "fallas": len(fallas), "cumplimiento_pct": cumplimiento,
        })
        log.info("  %s %-26s %2d/%-2d fallas", rid, nombre, len(fallas), evaluados)
    return pd.DataFrame(hallazgos), pd.DataFrame(resumen)

# ─── PASO 4: EXPORTAR ────────────────────────────────────────────────────────

def guardar(df: pd.DataFrame, nombre: str) -> None:
    """Mismo formato que el pipeline: CSV UTF-8 con BOM, copiado a docs/data/."""
    CARPETA_OUTPUT.mkdir(exist_ok=True)
    CARPETA_DOCS.mkdir(parents=True, exist_ok=True)
    ruta = CARPETA_OUTPUT / nombre
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    shutil.copy2(ruta, CARPETA_DOCS / nombre)
    log.info("✓ %s  (%s filas)", nombre, len(df))

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 55)
    log.info("  VACA MUERTA — Validación de datos BIM")
    log.info("=" * 55)

    log.info("[1/4] Modelo")
    if len(sys.argv) > 1:
        ruta = Path(sys.argv[1])
        log.info("  Usando modelo propio: %s", ruta)
    else:
        ruta = generar_modelo_demo(CARPETA_MODELO / "locacion_demo.ifc")
    modelo = ifcopenshell.open(str(ruta))

    log.info("[2/4] Extrayendo inventario de activos")
    inventario = extraer_inventario(modelo)
    log.info("  %s activos encontrados", len(inventario))

    log.info("[3/4] Validando")
    hallazgos, resumen = validar(inventario)
    total_ok = inventario.shape[0] - hallazgos["global_id"].nunique() if len(hallazgos) else inventario.shape[0]
    log.info("  Activos sin observaciones: %s de %s", total_ok, len(inventario))

    log.info("[4/4] Exportando")
    guardar(inventario, "10_bim_inventario.csv")
    guardar(hallazgos,  "10_bim_hallazgos.csv")
    guardar(resumen,    "10_bim_resumen.csv")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
