# Validación IFC (modelo de prueba)

## La idea en una frase

Un modelo BIM no es solo un dibujo 3D: cada objeto trae **datos** (tag, fabricante, estado, presión de diseño). Este módulo revisa que esos datos estén **completos, bien escritos, sean únicos y coincidan con otras fuentes** — lo mismo que `09_vm_data_quality.csv` hace con la producción, pero aplicado a activos físicos.

## Cómo encaja en el proyecto

```
PRODUCCIÓN (Capítulo IV)                ACTIVOS (modelo BIM / IFC)
vaca_muerta_pipeline.py                 bim_validacion.py
        │                                        │
07_vm_raw_filtrado.csv  ◄──── R07 ────  PozoId de cada cabeza de pozo
09_vm_data_quality.csv                  10_bim_hallazgos.csv
                                        10_bim_resumen.csv
                                        10_bim_inventario.csv
```

La regla **R07** es el puente: cada cabeza de pozo del modelo tiene que corresponder a un pozo que exista en los datos de producción. Si no, el modelo está desactualizado o el dato está mal cargado.

## Por qué el modelo es sintético

No hay modelos BIM públicos de instalaciones de Vaca Muerta. El módulo genera una locación demo (un pad con 4 pozos y su batería: separador, tanques, bombas y líneas) y le **siembra errores a propósito**, así se ve qué detecta cada regla. Con un `.ifc` real, se valida ese archivo sin tocar el código.

## Las 7 reglas

| Regla | Qué revisa | Severidad | Ejemplo de falla |
|---|---|---|---|
| R01 | Todo activo tiene tag | Crítica | Separador sin tag |
| R02 | Formato del tag según tipo (`CP-001`, `TK-002`) | Menor | `TK-02` |
| R03 | El tag no se repite | Crítica | Dos bombas `BM-001` |
| R04 | Cada tipo tiene sus campos mínimos | Mayor | Tanque sin capacidad |
| R05 | Valores dentro de lo permitido | Mayor | Estado "OK", presión negativa |
| R06 | El activo está ubicado en un sector | Mayor | Bomba "flotando" en el modelo |
| R07 | La cabeza de pozo existe en producción | Crítica | `PozoId` inexistente |

**Severidad:** crítica = el dato no se puede usar; mayor = hay que corregirlo; menor = es de forma.

## Resultado del demo

13 activos, 8 errores sembrados, **8 detectados**. 5 activos quedan sin observaciones.

## Cómo correrlo

```bash
pip install ifcopenshell pandas
python vaca_muerta_pipeline.py      # opcional: genera 07_... para que R07 cruce con pozos reales
python bim_validacion.py            # modelo demo
python bim_validacion.py mi.ifc     # modelo propio
```

## Glosario mínimo

- **IFC**: formato abierto de modelos BIM (como el CSV de los modelos 3D).
- **Pset**: grupo de propiedades de un objeto. Acá se usa `Pset_Activo_VM`.
- **GlobalId**: identificador único e inmutable de cada objeto IFC; es la clave para trazar un hallazgo hasta el modelo.
- **IDS**: estándar de buildingSMART para escribir estas reglas en un archivo neutral. Siguiente paso natural: migrar el catálogo `TIPOS_ACTIVO` a un `.ids` y validarlo con `ifctester`.
- **ISO 19650**: norma de gestión de la información en proyectos BIM; define quién entrega qué dato y cuándo.
