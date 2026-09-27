# Changelog

## 2026-09 — v2.3
- **Asset-register validation** (`validacion_activos.py`): 9 rules on real Capítulo IV data, a verdict on each delivery (IRAM-ISO 19650 approach), 6 quality dimensions, a quality index, a ranking by operator, and a weekly history.
- Detection of source-wide gaps (e.g. September 2024) and of the actual coordinate orientation.
- Map of wells by operator and status, with a hybrid satellite layer.
- Efficiency: water cut and GOR computed as ratios of totals; wells flagged as gas wells.
- Restored cohort curves, the quality report and `_metadata.json`, which were lost in v2.2. First-year decline is now measured from the peak month.
- Field column mapping (`areayacimiento`).
- Dashboard redesign in a technical-report style; production and validation tabs separated.
- IFC validation module (`bim_validacion.py`) with a synthetic test model.

## 2026-07 — v2.2
- Arps decline curves per well with EUR and fit quality.
- **Unit fix:** the gas BOE factor went from 5886 to 5.886 (the source reports thousands of m³).

## 2026-06 — v2.1
- Vintage type curves, pipeline quality checks and run metadata.

## 2026-06 — v2.0
- Initial pipeline, dashboard and weekly GitHub Action.
