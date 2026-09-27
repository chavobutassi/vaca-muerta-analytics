# Vaca Muerta Analytics

**Production analytics and data-quality validation for unconventional wells in Argentina's Vaca Muerta shale play — built on public data, refreshed every week.**

**Live dashboard:** [chavobutassi.github.io/vaca-muerta-analytics](https://chavobutassi.github.io/vaca-muerta-analytics)  
**Source data:** Secretaría de Energía — Capítulo IV ([datos.energia.gob.ar](https://datos.energia.gob.ar))

---

## What this project does

The project has two parts that share one pipeline:

| | Production analytics | Data validation |
|---|---|---|
| **Question** | How is Vaca Muerta producing, who produces it, and how do wells behave over time? | Can the asset information that operators report to the regulator be trusted? |
| **Output** | Production trends, market share, well efficiency, Arps decline curves, vintage type curves | Rule-based validation of 3,600+ wells, six data-quality dimensions, a verdict on each delivery, and a weekly trend |
| **Dashboard tabs** | Visión General · Por Empresa · Pozos · Eficiencia · Declinación · SITREP · Cohortes | Registro de activos · Calidad del pipeline · Validación IFC |

A GitHub Action downloads the source every Monday, runs the pipeline and the validation, and publishes the results. No servers, no paid tools.

---

## Data validation: the asset register

Every month, operators report each well's attributes to the Secretaría de Energía along with its production: identifier, status, well type, lift system, depth, field, formation and coordinates. Together, those attributes form the sector's **asset register**, an asset information model without 3D geometry.

`validacion_activos.py` validates that register against explicit information requirements, following the **IRAM-ISO 19650** approach: requirements → checks → findings → a decision on the delivery.

**Rules**

| Rule | Checks | Severity |
|---|---|---|
| A01 | One identifier per well and one well per identifier | Critical |
| A02 | Mandatory attributes present (status, type, lift, depth, field, formation…) | Major |
| A03 | Values within valid domains (depth range, non-negative production, producing days ≤ days in month) | Major |
| A04 | Coordinates inside the Neuquén Basin | Major |
| A05 | Abandoned wells reporting production | Major |
| A06 | Depth and field stable over time | Minor |
| A07 | Same well-month reported with different values in different files | Major |
| A08 | Producing wells with missing months (source-wide gaps excluded) | Minor |
| A09 | Well status inconsistent with the month reported (usually a change within the month) | Minor |

**Quality dimensions:** completeness, validity, uniqueness, consistency, timeliness and positional accuracy. There is also a quality index (% of wells with no critical or major findings), a ranking by operator, and a history of every run.

**Findings from the September 2026 run:**
- Quality index **94.1%** across 3,655 wells; verdict: *accepted with observations*.
- Depth is the main missing attribute. One operator group concentrates most of the gaps.
- **September 2024 is incomplete at the source:** 139 wells are missing that month at once. That also means basin production for that month is understated.
- The official data dictionary documents `coordenadax` as latitude, but **latitude is actually in `coordenaday`**. The validator detects the orientation from the data instead of trusting the documentation.

---

## Production analytics

- **Company grouping:** raw operator names ("YPF S.A.", "YSUR…") are mapped to their parent holding. Without it, market share is meaningless.
- **BOE conversion:** oil 6.2898 boe/m³; gas 5.886 boe per thousand m³. The source's `gas` column is in *thousands* of m³, and an earlier factor inflated gas ~1000×.
- **Efficiency:** water cut and GOR are computed as ratios of totals, not averages of monthly ratios, which blow up in months with almost no oil. Wells above 3,000 m³/m³ are flagged as gas wells.
- **Decline curves:** modified hyperbolic Arps fit per well, with 8%/yr terminal decline, EUR and fit quality graded by R².
- **Vintage type curves:** production averaged per well by month of life for each start-year cohort. First-year decline is measured from the peak month, not month 0, because month 0 is usually partial.

---

## Pipeline

```
Secretaría de Energía (4 CSV sources)
  ↓ download with local cache
  ↓ separator detection + column normalization
  ↓ filter: Neuquén Basin + Vaca Muerta formation
  ↓ deduplicate by (well, month)
  ↓ enrich: company group, BOE, water cut, GOR
  ├─→ vaca_muerta_pipeline.py   production tables 01–09 + _metadata.json
  └─→ validacion_activos.py     asset-register validation 11_*
  ↓
docs/data/ → GitHub Pages → dashboard
```

| Output | Content |
|---|---|
| `01`–`06` | Monthly production, fields, top wells, efficiency, market share, new wells |
| `08_vm_declinacion*.csv` | Arps fits per well and vintage type curves |
| `09_vm_data_quality.csv` | Pipeline checks on the consolidated dataset |
| `10_bim_*.csv` | IFC validation on a test model (see below) |
| `11_activos_*.csv` / `.json` | Asset-register validation: register, rules, findings, operators, history, verdict |
| `_metadata.json` | Run traceability: date, period, row counts |

---

## IFC validation (test model)

`bim_validacion.py` reads IFC models with IfcOpenShell, extracts every asset, and applies seven rules: tags, naming, uniqueness, required properties, value domains, spatial assignment, and a cross-check of each wellhead against the production dataset.

No public BIM models of Vaca Muerta facilities exist. The module therefore generates a **synthetic well pad with deliberately seeded errors** (8 seeded, 8 detected). Any real `.ifc` file can be validated without code changes. Details: [guias/validacion_ifc.md](guias/validacion_ifc.md).

---

## Run it locally

```bash
pip install -r requirements.txt

python vaca_muerta_pipeline.py     # downloads ~500 MB on first run (cached afterwards)
python validacion_activos.py       # asset-register validation (uses the cache)
python bim_validacion.py           # IFC validation on the test model
python bim_validacion.py model.ifc # …or on your own model
```

## Repository structure

```
vaca-muerta-analytics/
├── vaca_muerta_pipeline.py     ETL + production analytics
├── validacion_activos.py       asset-register validation (real data)
├── bim_validacion.py           IFC validation (test model)
├── modelos_bim/                synthetic IFC model
├── docs/                       dashboard (GitHub Pages) + published data
├── guias/                      code walkthrough and module guides (Spanish)
├── .github/workflows/          weekly refresh
├── CHANGELOG.md
└── requirements.txt
```

## Design decisions

- **Validation reports, it doesn't silently filter:** every finding is published with its rule and severity, and what to do with it stays explicit.
- **Source problems are separated from operator problems:** months missing for many wells at once are attributed to the source, not to the operator.
- **Specifications are checked against the data:** coordinate orientation is detected from the data, not taken from the documentation.
- **Business logic lives in Python:** it is versioned and testable. The dashboard only reads small, purpose-built tables.
- **Weekly history, no database:** each run appends one row of KPIs to a CSV that the Action commits.

## Tech stack

Python 3.11 (pandas, NumPy, SciPy, IfcOpenShell) · GitHub Actions · GitHub Pages · vanilla HTML/JS with Chart.js and Leaflet.

---

*Built by [Claudio Butassi](https://github.com/chavobutassi) — data analyst. All source data is public.*
