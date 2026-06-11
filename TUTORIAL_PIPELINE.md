# Tutorial del pipeline Vaca Muerta (v2.1)

Guía de estudio del código: qué hace cada etapa, qué analiza cada tabla, qué te devuelve y cómo interpretarlo. Pensada para que puedas explicar cualquier parte del proyecto sin mirar el código.

---

## 1. La idea general en una frase

El pipeline toma archivos CSV crudos y desprolijos de la Secretaría de Energía, los convierte en un dataset limpio y consistente de Vaca Muerta, y a partir de él genera tablas analíticas chicas y específicas, cada una diseñada para responder UNA pregunta de negocio.

El flujo completo:

```
Descarga (con caché) → Lectura → Normalización → Filtro Vaca Muerta
→ Consolidación + deduplicación → Enriquecimiento → Validación de calidad
→ Tablas analíticas → Metadata → Publicación en GitHub Pages
```

---

## 2. Etapa por etapa

### 2.1 Descarga — `descargar()`

**Qué hace:** baja cada CSV de datos.energia.gob.ar y lo guarda en `cache_csv/`. Si ya existe localmente, lo reutiliza sin volver a descargar.

**Por qué importa:** los archivos pesan cientos de MB. Sin caché, cada corrida de desarrollo tardaría minutos y sobrecargaría la API pública. La descarga es `stream=True` (por chunks de 64 KB) para no cargar todo el archivo en memoria.

**Detalle defendible:** si una descarga falla a mitad de camino, el archivo parcial se borra (`ruta.unlink()`) para que la próxima corrida no use un archivo corrupto como caché.

### 2.2 Lectura — `leer_csv()` y `_detectar_separador()`

**Qué hace:** lee el CSV detectando automáticamente si usa coma o punto y coma como separador (las fuentes del gobierno no son consistentes entre años). Normaliza los nombres de columna a minúsculas sin espacios.

**Cómo detecta el separador:** lee solo 2 filas con cada separador candidato; si el resultado tiene más de 3 columnas, ese es el separador correcto. Si un CSV se parsea con el separador equivocado, todo queda en una sola columna gigante.

### 2.3 Normalización — `normalizar()` y `MAPA_COLUMNAS`

**El problema que resuelve:** cada fuente nombra las columnas distinto. La empresa puede venir como `empresa` u `operadora`; el petróleo como `prod_pet`, `produccion_petroleo` o `pet_m3`. Sin unificar esto, no se pueden concatenar las fuentes.

**Cómo lo resuelve:** `MAPA_COLUMNAS` es un diccionario donde la clave es el nombre canónico y los valores son todas las variantes conocidas. La función recorre el mapa y renombra la primera variante que encuentre.

**Además:**
- Parsea la fecha desde tres formatos posibles (`YYYYMM`, `YYYY-MM`, o parsing libre) a una columna `fecha` tipo datetime.
- Convierte producción a numérico con `errors="coerce"` (lo que no se puede convertir queda NaN → 0) para que un valor basura no rompa el pipeline.
- Pasa los strings a MAYÚSCULAS y sin espacios, para que "Loma Campana " y "LOMA CAMPANA" sean el mismo yacimiento.

### 2.4 Filtro Vaca Muerta — `filtrar_vm()`

**Qué hace:** se queda solo con registros de la cuenca Neuquina cuya formación contiene "VACA MUERTA". Si el registro no tiene formación informada, usa como fallback el tipo de recurso "NO CONVENCIONAL".

**Por qué el fallback:** los datos históricos más viejos no siempre informan la formación. Descartar esos registros subestimaría la producción temprana; el tipo de recurso es el mejor proxy disponible.

### 2.5 Consolidación y deduplicación (en `main()`)

**Qué hace:** concatena todas las fuentes en un solo DataFrame y elimina duplicados por la clave compuesta `(pozo_id, fecha)`.

**Por qué importa:** el archivo histórico y los anuales se solapan — un mismo mes de un mismo pozo puede aparecer en dos fuentes. Sin deduplicar, la producción se contaría doble y todos los análisis (market share, rankings) quedarían inflados.

### 2.6 Enriquecimiento — `enriquecer()` y `_clasificar_empresa()`

Acá se agregan las columnas calculadas. Cada una con su lógica:

| Columna | Qué es | Cómo se calcula |
|---|---|---|
| `empresa_grupo` | Holding de la operadora | Busca patrones en el nombre crudo según el dict `GRUPOS`. "YPF S.A.", "YPF SA" e "YSUR" → todos "YPF" |
| `anio`, `mes`, `anio_mes` | Descomposición temporal | Derivadas de `fecha` |
| `boe` | Barriles de petróleo equivalente | `petroleo_m3 × 6.29 + gas_mm3 × 5886`. Permite sumar petróleo y gas en una sola métrica de energía. 1 m³ ≈ 6.29 barriles; 1 millón de m³ de gas ≈ 5886 BOE |
| `water_cut_pct` | Corte de agua (%) | `agua / (agua + petróleo) × 100`. El `where(liquido > 0)` evita la división por cero: si no hay líquido, queda NaN en vez de error |
| `gor` | Gas-oil ratio (m³ gas / m³ petróleo) | Cuánto gas sale por unidad de petróleo. Si sube en el tiempo, el pozo está perdiendo presión |

**Sobre `_clasificar_empresa`:** sin esta agrupación el market share sería incoherente — YPF aparecería partido en 3 o 4 "empresas" distintas según cómo lo escribió cada delegación. Es el típico problema de *entity resolution* en datos reales.

### 2.7 Validación de calidad — `validar()` ★ NUEVO

**Qué hace:** corre 7 chequeos sobre el dataset consolidado y genera un reporte (`09_vm_data_quality.csv`). No modifica los datos — solo informa, y la consola muestra warnings con los chequeos que fallan.

**Los 7 chequeos y su lógica:**

| Chequeo | Regla | Severidad | Por qué importa |
|---|---|---|---|
| `produccion_negativa` | pet/gas/agua < 0 | alta | Físicamente imposible; indica error de carga en la fuente |
| `fecha_futura` | fecha > hoy | alta | Un registro de 2030 distorsionaría toda serie temporal |
| `fecha_invalida` | fecha NaN | media | Registros que quedan fuera de todo análisis temporal |
| `sin_empresa` | empresa vacía/NaN | media | Esos BOE no se asignan a nadie → market share incompleto |
| `sin_pozo_id` | pozo_id NaN | alta | Rompe la deduplicación y los rankings por pozo |
| `water_cut_invalido` | fuera de [0, 100] | baja | Señal de inconsistencia entre agua y petróleo reportados |
| `salto_mensual_brusco` | producción total del mes varía > ±50% vs. mes anterior | media | Casi siempre es un mes con carga incompleta en la fuente, no un cambio real de producción |

**Cómo explicarlo en una entrevista:** "Antes de exportar, el pipeline corre un set de chequeos de calidad con reglas de negocio explícitas — producción negativa, fechas futuras, registros huérfanos, saltos anómalos en la serie. El resultado se publica como tabla, así cualquier consumidor del dashboard puede auditar la confiabilidad del dato." La frase clave es **reglas de negocio explícitas**: la calidad no se asume, se mide.

### 2.8 Metadata del run — `generar_metadata()` ★ NUEVO

**Qué hace:** escribe `_metadata.json` con la trazabilidad de cada ejecución: fecha/hora UTC, período cubierto, totales (registros, pozos, empresas, yacimientos) y cuántas filas tiene cada tabla exportada.

**Para qué sirve:**
1. El dashboard puede leer este JSON y mostrar "Datos actualizados al 2026-05" — credibilidad instantánea.
2. Si una corrida exporta drásticamente menos filas que la anterior, lo ves comparando metadatas sin abrir ningún CSV. Es observabilidad básica del pipeline.

---

## 3. Las tablas analíticas: qué pregunta responde cada una

Principio de diseño: **cada tabla responde una pregunta**. En vez de un CSV gigante que Power BI tenga que masticar, se pre-agregan tablas chicas y específicas. Esto hace el dashboard más rápido y la lógica auditable en Python.

### 01 — Producción mensual (`t_produccion_mensual`)
- **Pregunta:** ¿cuánto produce cada empresa, mes a mes?
- **Cómo:** `groupby` por (anio_mes, empresa_grupo), suma de pet/gas/agua/boe y `nunique` de pozos activos.
- **Devuelve:** una fila por empresa por mes. Es la tabla madre del dashboard: alimenta las series temporales.

### 02 — Por yacimiento (`t_por_yacimiento`)
- **Pregunta:** ¿dónde (geográficamente) se concentra la producción?
- **Devuelve:** producción anual por yacimiento y empresa. Alimenta treemaps — Loma Campana, Fortín de Piedra, etc.

### 03 — Top pozos (`t_top_pozos`)
- **Pregunta:** ¿cuáles son los mejores pozos de la historia de la formación?
- **Cómo:** agrupa por pozo, suma todo su histórico, ordena por BOE acumulado y se queda con el top 200. Incluye `primer_mes`/`ultimo_mes` (vida útil) y `meses_activo`.

### 04 — Eficiencia (`t_eficiencia`)
- **Pregunta:** ¿qué pozos están envejeciendo y son candidatos a intervención?
- **Cómo:** promedio de water cut y GOR por pozo (solo meses con petróleo > 0, para no contaminar el promedio con meses parados). Clasifica cada pozo en etapa: Temprano (<30% agua), Intermedio (30-60%), Maduro (>60%).
- **Interpretación:** un pozo con water cut alto y creciente produce cada vez más agua que petróleo — el costo de tratamiento de agua sube y la rentabilidad cae.

### 05 — Market share (`t_market_share`)
- **Pregunta:** ¿quién gana y quién pierde participación de mercado?
- **Cómo:** BOE anual por empresa dividido por el total del año. El detalle técnico es el `transform("sum")`: calcula el total anual SIN colapsar las filas, equivalente a una window function de SQL (`SUM() OVER (PARTITION BY anio)`).
- **Detalle defendible:** el año en curso se marca con `anio_parcial = True` si tiene menos de 10 meses de datos, para que el dashboard no compare un año incompleto contra años completos.

### 06 — Nuevos pozos (`t_nuevos_pozos`)
- **Pregunta:** ¿cuánta actividad de perforación hay y de quién?
- **Cómo:** el primer mes con producción de cada pozo es un *proxy* de su puesta en marcha (los datos públicos no tienen fecha de perforación). Se cuentan pozos nuevos por mes y empresa.
- **Honestidad metodológica:** es un proxy con lag — un pozo se perfora meses antes de producir. Decir esto espontáneamente suma muchísimo: muestra que entendés las limitaciones de tu propio dato.

### 08 — Declinación por cohortes (`t_declinacion_cohortes`) ★ NUEVO

Este es el análisis más sofisticado del pipeline y el más característico del shale. Vale la pena entenderlo a fondo.

**El concepto:**
- Una **cohorte** (o *vintage*) son todos los pozos que empezaron a producir el mismo año.
- El **mes de vida** de un pozo es su edad productiva: mes 0 = su primer mes con producción, mes 1 el siguiente, y así.
- Para cada (cohorte, mes_vida) se calcula la producción **promedio por pozo**. El resultado es la "curva tipo" (*type curve*) de esa camada.

**Por qué se promedia y no se suma:** si se sumara, una cohorte con más pozos parecería "mejor" solo por cantidad. El promedio responde la pregunta correcta: ¿cómo es el pozo TÍPICO de cada camada?

**Cómo se calcula el mes de vida (la parte ingeniosa del código):**
```python
primer = d.groupby("pozo_id")["fecha"].transform("min")   # primer mes de cada pozo
d["mes_vida"] = (d.fecha.dt.year - primer.dt.year) * 12 \
              + (d.fecha.dt.month - primer.dt.month)
```
Se convierte cada fecha a "meses absolutos" (año×12 + mes) y se resta. El `transform("min")` pega el primer mes de cada pozo en cada una de sus filas — de nuevo, el equivalente pandas de una window function (`MIN(fecha) OVER (PARTITION BY pozo_id)`).

**Filtros de representatividad:**
- Cohortes con menos de 5 pozos se descartan (un solo pozo raro distorsionaría la curva).
- Meses de vida > 120 se cortan (las colas largas tienen muy pocos pozos supervivientes y generan ruido).

**Cómo se lee el resultado:**
1. **Forma de la curva:** los pozos no convencionales tienen un pico inicial altísimo y declinan 60-70% el primer año. Esa forma de "L" es la firma del shale y explica por qué la industria necesita perforar continuamente para sostener producción (el famoso *treadmill* del shale).
2. **Comparación entre cohortes:** si la curva de la cohorte 2024 está POR ENCIMA de la de 2019 en cada mes de vida, los pozos nuevos son genuinamente mejores — ramas laterales más largas, más etapas de fractura, mejor diseño. Si las curvas se aplanan entre cohortes, la mejora tecnológica se agotó o se están perforando zonas de menor calidad.
3. **Lo que NO te dice:** nada sobre costos. Un pozo 2024 puede producir 40% más pero costar 60% más perforarlo.

**Pitch de 20 segundos para entrevista:** "Agrupo los pozos por año de inicio y normalizo el tiempo: en vez de fecha calendario, uso la edad del pozo. Eso me deja comparar manzanas con manzanas — el pozo típico de 2024 contra el típico de 2019, ambos en su mes 6 de vida. Es el análisis estándar para evaluar si la productividad del shale mejora o se estanca."

### 09 — Data quality (`validar`)
Ya descripta en 2.7. Es output del pipeline igual que las otras, así el reporte de calidad es público y versionable.

---

## 4. Conceptos del dominio para tener fluidos

- **BOE (barril de petróleo equivalente):** unidad común para sumar petróleo y gas según su contenido energético. Sin BOE no podrías comparar una empresa gasífera (Tecpetrol) con una petrolera (Vista).
- **Water cut:** % de agua en el líquido total producido. Sube con la edad del pozo. Alto water cut = pozo maduro = más costo de tratamiento por barril.
- **GOR (gas-oil ratio):** m³ de gas por m³ de petróleo. Un GOR creciente en un pozo de petróleo indica caída de presión del reservorio.
- **Declinación:** caída natural de la producción de un pozo con el tiempo. En shale es brutal: 60-70% el primer año.
- **Cohorte / vintage:** camada de pozos del mismo año de inicio. Permite separar "mejora tecnológica" de "más pozos".
- **No convencional:** el hidrocarburo está atrapado en roca de muy baja permeabilidad (shale); requiere perforación horizontal + fractura hidráulica para fluir.

---

## 5. Decisiones de diseño que podés defender

1. **Caché de descargas** → desarrollo rápido, sin abusar de la API pública.
2. **Mapeo canónico de columnas** → tolera que el gobierno cambie nombres entre años; agregar una variante nueva es una línea en un dict.
3. **Deduplicación por clave compuesta** (pozo + fecha) → evita doble conteo por solapamiento de fuentes.
4. **Agrupación de empresas por holding** → market share coherente (entity resolution).
5. **Tablas pre-agregadas en vez de un solo CSV gigante** → dashboard rápido, lógica de negocio en Python (auditable y versionada), no escondida en DAX.
6. **`errors="coerce"` + `try/except` por tabla** → un dato basura o una tabla que falla no tira abajo todo el pipeline (degradación graciosa).
7. **Validación que informa pero no borra** → la decisión sobre datos problemáticos queda explícita y auditable, no escondida en un filtro silencioso.
8. **Metadata por run** → trazabilidad: cualquiera puede saber cuándo y con qué datos se generó cada versión del dashboard.
9. **Promedio (no suma) en las curvas de declinación** → aísla la calidad del pozo típico del efecto "cantidad de pozos".

---

## 6. Equivalencias pandas ↔ SQL (para la entrevista)

| En el pipeline (pandas) | Equivalente SQL |
|---|---|
| `df.groupby(...).agg(...)` | `GROUP BY` con agregaciones |
| `groupby(...).transform("sum")` (market share) | `SUM(boe) OVER (PARTITION BY anio)` |
| `groupby("pozo_id")["fecha"].transform("min")` (cohortes) | `MIN(fecha) OVER (PARTITION BY pozo_id)` |
| `drop_duplicates(subset=["pozo_id","fecha"])` | `ROW_NUMBER() OVER (PARTITION BY pozo_id, fecha)` + filtrar = 1 |
| `df[mask]` con condiciones combinadas | `WHERE ... AND ... OR ...` |
| `pct_change()` (si lo agregás) | `LAG()` + división |
| `pd.concat(frames)` | `UNION ALL` |

---

## 7. Cómo verificar que los cambios nuevos funcionan

Después de correr `python vaca_muerta_pipeline.py`:

1. **`08_vm_declinacion_cohortes.csv`** — abrilo y verificá: (a) para cada cohorte, `boe_prom` en mes_vida=0 debe ser el máximo o casi, y caer hacia adelante; (b) `pozos_en_muestra` debe DECRECER con mes_vida dentro de una cohorte (los pozos viejos van saliendo de la muestra); (c) las cohortes recientes deberían tener picos más altos que las viejas — eso es la historia real de Vaca Muerta.
2. **`09_vm_data_quality.csv`** — con datos reales es esperable algún registro en `salto_mensual_brusco` (el último mes suele venir con carga incompleta) y quizás `sin_empresa`. Ninguno en `produccion_negativa` o `fecha_futura` sería lo sano.
3. **`_metadata.json`** — `periodo_hasta` debe coincidir con el último mes publicado por la Secretaría, y la suma de checks la podés cruzar contra el log de consola.
