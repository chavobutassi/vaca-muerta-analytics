# Vaca Muerta Analytics

**ETL pipeline + interactive dashboard for unconventional well production in Argentina's Vaca Muerta shale play.**

Data source: [Secretaría de Energía — datos.energia.gob.ar](https://datos.energia.gob.ar)  
Live dashboard: [chavobutassi.github.io/vaca-muerta-analytics](https://chavobutassi.github.io/vaca-muerta-analytics)

---

## What this project does

Argentina's well production data is published as raw, inconsistent CSV files split across multiple annual sources — different column names each year, overlapping periods, mixed separators, and no unified company identifiers.

This pipeline downloads those files, normalizes and deduplicates them, filters for Vaca Muerta specifically, and produces 9 analysis-ready tables consumed by an interactive HTML dashboard published via GitHub Pages.

The dashboard tracks production trends, market share, well efficiency, drilling activity, vintage decline curves, and data quality — all without any paid tools or infrastructure.

A companion module, `bim_validacion.py`, applies the same rule-based validation approach to **BIM asset data** (IFC models of well pads and production facilities), and cross-checks the wellheads in the model against the production dataset. See [README_BIM.md](README_BIM.md).

---

## Pipeline architecture

```
Secretaría de Energía (4 CSV sources)
        ↓  download with local cache
        ↓  auto-detect separator + column normalization
        ↓  filter: Cuenca Neuquina + Formación Vaca Muerta
        ↓  deduplicate by (well_id, date)
        ↓  enrich: company grouping, BOE, water cut, GOR
        ↓  data quality validation (7 checks)
        ↓
   9 analytical tables + _metadata.json
        ↓
   docs/data/  →  GitHub Pages  →  Live dashboard
```

---

## Output tables

| File | Description |
|------|-------------|
| `01_vm_produccion_mensual.csv` | Monthly production by company group |
| `02_vm_por_yacimiento.csv` | Annual production by field and company |
| `03_vm_top_pozos.csv` | Top 200 wells by cumulative BOE |
| `04_vm_eficiencia_pozos.csv` | Water cut and GOR per well (efficiency flags) |
| `05_vm_market_share.csv` | Annual market share by company in BOE |
| `06_vm_nuevos_pozos.csv` | New wells per month (drilling activity proxy) |
| `07_vm_raw_filtrado.csv` | Full filtered dataset (all Vaca Muerta records) |
| `08_vm_declinacion_cohortes.csv` | Decline curves by vintage (type curves) |
| `09_vm_data_quality.csv` | Data quality report: 7 rule-based checks |
| `_metadata.json` | Run traceability: date, period covered, row counts |
| `10_bim_inventario.csv` | BIM asset inventory: one row per asset with its properties |
| `10_bim_hallazgos.csv` | BIM validation findings: one row per failed rule |
| `10_bim_resumen.csv` | BIM compliance % per rule |

---

## Key analyses

### Vintage decline curves (type curves)
Wells are grouped by the year they started producing (cohort/vintage). Production is averaged per well per month-of-life — not summed — so curves reflect the *typical* well of each cohort regardless of how many were drilled. Comparing cohorts answers the core shale question: **are new wells genuinely better, or just more of them?**

Vaca Muerta unconventional wells typically decline 60–70% in the first year, characteristic of the shale production profile. Rising peak rates in newer cohorts indicate improvements in lateral length, fracture stage design, or target zone selection.

### Market share & company grouping
Raw operator names vary across sources ("YPF S.A.", "YPF SA", "YSUR Américas"). A canonical grouping dictionary maps all variants to their parent holding. Without this, market share analysis would be meaningless — YPF alone would appear fragmented across multiple "companies."

### Data quality report
Before exporting, the pipeline runs 7 explicit business-rule checks: negative production values, future dates, invalid date parsing, records without operator, records without well ID, water cut outside [0,100], and abrupt monthly jumps (>±50%) that signal incomplete data loads from the source. Results are published as a table — quality is measured, not assumed.

### Efficiency flagging
Each well is classified by water cut stage: Early (<30%), Intermediate (30–60%), Mature (>60%). Rising water cut over time increases treatment costs per barrel and is a standard intervention trigger in field operations.

### BIM asset data validation
A BIM model is not just 3D geometry: every object (wellhead, separator, tank, pump, pipeline) carries data such as tag, manufacturer, status and design pressure. `bim_validacion.py` reads IFC models with IfcOpenShell and runs 7 rules: mandatory tag, tag naming convention, tag uniqueness, required properties per asset type, valid value domains, spatial assignment, and a **cross-check between each wellhead's `PozoId` and the production dataset** — the bridge between the asset model and the production pipeline.

No public BIM models of Vaca Muerta facilities exist, so the module generates a synthetic well pad with deliberately seeded errors (8 seeded, 8 detected). Any real `.ifc` file can be validated instead. Full explanation in [README_BIM.md](README_BIM.md).

---

## Running the pipeline locally

```bash
# Install dependencies
pip install pandas requests tqdm openpyxl

# Run (downloads ~500MB on first run, cached afterwards)
python vaca_muerta_pipeline.py

# Optional: BIM validation module (run after the pipeline so the
# wellhead cross-check uses real well IDs)
pip install ifcopenshell
python bim_validacion.py              # synthetic demo model
python bim_validacion.py model.ifc    # your own IFC model
```

Output files are written to `./output/`. The pipeline copies them to `./docs/data/` automatically for GitHub Pages publishing.

**First run:** downloads all source files to `cache_csv/` (~500 MB total). Subsequent runs reuse the cache — only the current year's file should be refreshed periodically.

---

## Project structure

```
vaca-muerta-analytics/
├── vaca_muerta_pipeline.py   # ETL pipeline (single file, no dependencies beyond pandas)
├── TUTORIAL_PIPELINE.md      # Full code walkthrough: what each function does and why
├── bim_validacion.py         # BIM (IFC) asset data validation module
├── README_BIM.md             # BIM module explanation, rules and glossary
├── modelos_bim/              # IFC models (synthetic demo generated by the module)
├── docs/
│   ├── index.html            # Main interactive dashboard
│   ├── report.html           # Extended analytical report
│   └── data/                 # CSV outputs served by GitHub Pages
├── cache_csv/                # Downloaded source files (gitignored)
└── output/                   # Generated tables (gitignored)
```

---

## Design decisions

- **Local cache** — avoids hammering the public API on every dev run; invalid/partial downloads are deleted automatically so they don't persist as corrupted cache.
- **Canonical column mapping** — the government changes column names between annual files. Adding a new variant is one line in a dictionary; no code changes needed.
- **Deduplication by `(well_id, date)`** — the historical and annual sources overlap. Without deduplication, production would be double-counted and all downstream metrics inflated.
- **Pre-aggregated tables instead of one large CSV** — Power BI and the HTML dashboard consume small, purpose-built tables. Business logic lives in Python (versioned, testable), not hidden in DAX formulas.
- **Validation that reports, not silently filters** — problematic records are flagged and counted in a published report. The decision of what to do with them is explicit and auditable.
- **Average (not sum) in decline curves** — isolates per-well quality from cohort size effects.
- **Run metadata** — every execution writes a JSON with timestamp, period covered, and row counts per table. The dashboard reads it to display "data updated as of..." and it enables basic pipeline observability.

---

## Tech stack

- **Python 3.10+** with pandas, requests, tqdm
- **IfcOpenShell** for reading and generating BIM (IFC) models
- **GitHub Pages** for zero-infrastructure dashboard hosting
- **Vanilla HTML/JS** dashboard (no framework dependencies)

Data covers Vaca Muerta unconventional production from the Neuquén Basin. All source data is public and refreshed periodically by Argentina's Secretaría de Energía.

---

*Built by [Claudio Butassi](https://github.com/chavobutassi)*
