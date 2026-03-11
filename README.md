# Calgary Hackathon Data -- Alberta Oil & Gas (Petrinex)

Real Alberta oil and gas production data for the Databricks Calgary Hackathon, March 2026.

**Zero synthetic data.** Everything is sourced from [Petrinex Public Data](https://www.petrinex.ca/PD/Pages/default.aspx) -- Alberta's official Petroleum Registry maintained by the Alberta Energy Regulator (AER).

---

## Quick Start

1. Import this repo into your Databricks workspace
2. Open `notebooks/01_setup_data`
3. Set your catalog and schema name in the widgets (defaults: `hackathon` / `energy_data`)
4. Attach to **serverless** compute or any cluster
5. **Run All** -- takes ~5 minutes (reads from local files, no API downloads)

That's it. All 8 tables + PDFs will be created in your Unity Catalog.

---

## What You Get

| Table | Rows | Source | Description |
|-------|------|--------|-------------|
| **volumetrics** | ~54M | Petrinex Vol API | Facility-level monthly production volumes. Every barrel of oil, MCF of gas, and m3 of water produced, flared, vented, injected, or consumed as fuel at every facility in Alberta. |
| **ngl_volumes** | ~5M | Petrinex NGL API | Well-level monthly production. Gas, oil, condensate, water, plus NGL components (ethane, propane, butane, pentane) per well. |
| **facilities** | ~30K | Derived | Unique facilities with lat/lon coordinates (converted from DLS), type, operator, region. Ready for mapping. |
| **operators** | ~600 | Derived | Real operator profiles aggregated from production data: facility count, total oil/gas/condensate/water, active months. |
| **wells** | ~126K | Derived | Per-well summary: production totals, geological formation (Montney, Mannville, Cardium, etc.), field name (PEMBINA, KAYBOB, etc.), operator, facility linkage. |
| **field_codes** | 80 | AER Reference | Maps numeric AER field codes to names (e.g., 0877 → PEMBINA). |
| **facility_emissions** | ~760K | Derived | Real Petrinex-reported flaring, venting, and fuel gas volumes per facility per month. |
| **market_prices** | 14 | Public benchmarks | Monthly WTI, WCS, AECO, USD/CAD (Jan 2025 -- Feb 2026). |

### PDFs (for Knowledge Assistants)

Downloaded to `/Volumes/{catalog}/{schema}/documentation/`:

| Document | Source | Size |
|----------|--------|------|
| AER Directive 007 | Alberta Energy Regulator | 315 KB |
| AER Manual 011 | Alberta Energy Regulator | 2.6 MB |
| Petrinex Volumetrics Guide | Petrinex | 863 KB |
| Petrinex NGL Guide | Petrinex | 321 KB |
| Petrinex Well Infrastructure Guide | Petrinex | 817 KB |

---

## Data Model

```
volumetrics (54M)          ngl_volumes (5M)
     |                          |
     +---> facilities (30K)     +---> wells (126K)
     |         |                       |
     +---> operators (600)             +---> field_codes (80)
     |
     +---> facility_emissions (760K)

market_prices (14) -- standalone reference
```

### Key Relationships

- `volumetrics.ReportingFacilityID` = `facilities.facility_id`
- `volumetrics.OperatorBAID` = `operators.operator_baid`
- `ngl_volumes.WellID` = `wells.well_id`
- `wells.facility_id` = `facilities.facility_id` (wells belong to facilities)
- `wells.field_name` = `field_codes.field_code` (field name lookup)
- `facility_emissions.facility_id` = `facilities.facility_id`

---

## Key Columns & Codes

### Product IDs (in volumetrics)
| Code Pattern | Product |
|---|---|
| `%OIL%`, `%CRD%` | Crude oil |
| `%GAS%` | Natural gas |
| `%CND%`, `%COND%` | Condensate |
| `%WTR%`, `%WATER%` | Produced water |

### Activity IDs (in volumetrics)
| Code | Activity | Records |
|---|---|---|
| `PROD` | Production | 24M |
| `DISP` | Disposition (sent out) | 4.8M |
| `REC` | Received | 3.4M |
| `FUEL` | Fuel gas consumed | 2.9M |
| `VENT` | Vented gas | 2.9M |
| `INJ` | Injection | 1.1M |
| `FLARE` | Flared gas | 307K |

**Important:** When aggregating total production, filter to `ActivityID = 'PROD'` to avoid double-counting volumes that flow through multiple facilities.

### Facility Types
| Code | Type | Count |
|---|---|---|
| BT | Battery | ~25,000 |
| GS | Gas Gathering System | ~2,800 |
| IF | Injection Facility | ~2,000 |
| GP | Gas Plant | ~500 |

### Formations (in wells table)
Derived from AER pool code prefixes:

| Formation | Wells | Description |
|---|---|---|
| Montney | 48,000 | NW Alberta, major tight gas/condensate play |
| Mannville | 10,700 | Conventional oil & gas across Alberta |
| Spirit River | 5,900 | Deep basin gas |
| Cardium | 4,800 | Light tight oil, central Alberta |
| Duvernay | 3,800 | Shale gas/condensate, west-central |
| Viking | 1,300 | Shallow gas, southern/central Alberta |
| + 19 more | | Leduc, Edmonton, Nisku, Halfway, Wilrich, etc. |

### Unit Conversions (Petrinex reports in metric)
| Petrinex Unit | O&G Standard | Conversion |
|---|---|---|
| m3 (oil/condensate/water) | barrels (bbl) | × 6.2898 |
| e3m3 (gas) | MCF (thousand cubic feet) | × 35.3147 |
| m3/month → bbl/day | bbl/d | × 6.2898 / 30.44 |
| e3m3/month → MCF/day | MCF/d | × 35.3147 / 30.44 |
| GJ | GJ | (no conversion) |

---

## Sample Queries

### Top 10 oil producers by daily rate
```sql
SELECT OperatorName,
       ROUND(SUM(Volume) * 6.2898 / (COUNT(DISTINCT ProductionMonth) * 30.44)) as avg_bbl_per_day,
       COUNT(DISTINCT ReportingFacilityID) as facilities
FROM {catalog}.{schema}.volumetrics
WHERE (ProductID LIKE '%OIL%' OR ProductID LIKE '%CRD%')
AND ActivityID = 'PROD'
GROUP BY OperatorName
ORDER BY avg_bbl_per_day DESC
LIMIT 10
```

### Wells by formation
```sql
SELECT formation, COUNT(*) as wells,
       ROUND(SUM(total_oil_m3) * 6.2898) as total_oil_bbl,
       ROUND(SUM(total_gas_e3m3) * 35.3147) as total_gas_mcf
FROM {catalog}.{schema}.wells
GROUP BY formation
ORDER BY wells DESC
```

### Top flaring facilities
```sql
SELECT f.facility_name, f.operator_name, f.region,
       SUM(e.flare_volume) as total_flare_e3m3,
       SUM(e.vent_volume) as total_vent_e3m3
FROM {catalog}.{schema}.facility_emissions e
JOIN {catalog}.{schema}.facilities f ON e.facility_id = f.facility_id
GROUP BY f.facility_name, f.operator_name, f.region
ORDER BY total_flare_e3m3 DESC
LIMIT 20
```

### Peak producing oil wells with field names
```sql
SELECT w.well_id, w.field_display_name, w.formation, w.operator_name,
       ROUND(MAX(CAST(n.OilProduction AS DECIMAL(12,1))) * 6.2898 / 30.44, 1) as peak_bbl_d
FROM {catalog}.{schema}.ngl_volumes n
JOIN {catalog}.{schema}.wells w ON n.WellID = w.well_id
WHERE n.WellID LIKE 'ABWI%'
GROUP BY w.well_id, w.field_display_name, w.formation, w.operator_name
ORDER BY peak_bbl_d DESC
LIMIT 20
```

---

## Ideas for Hackathon Projects

- **Interactive Map** -- Plot 30K facilities and 126K wells using Leaflet.js, filter by formation/operator
- **Genie Space** -- Text-to-SQL over all 8 tables for natural language production analytics
- **Knowledge Assistant** -- RAG over the AER/Petrinex PDFs for regulatory Q&A
- **Multi-Agent Supervisor** -- Combine Genie + KA for a unified energy intelligence agent
- **Decline Curve Analysis** -- Use well-level monthly data to model production decline curves
- **Emissions Dashboard** -- Rank operators/facilities by flaring and venting intensity
- **Operator Benchmarking** -- Compare operators by production per facility, emissions intensity
- **Formation Analytics** -- Compare Montney vs Duvernay vs Cardium well performance
- **Price Correlation** -- Correlate WTI/WCS prices with Alberta production volumes

---

## Data Sources & Credits

- **Petrinex Public Data** -- [petrinex.ca](https://www.petrinex.ca/PD/Pages/default.aspx) -- Alberta's Petroleum Registry
- **petrinex Python package** -- [github.com/guanjieshen/petrinex-python-api](https://github.com/guanjieshen/petrinex-python-api) -- Thanks to Guanjie Shen
- **AER Directive 007** -- Volumetric and Infrastructure Requirements
- **AER Manual 011** -- How to Submit Volumetric Data
- **Market Prices** -- EIA (WTI), Alberta Government (WCS, AECO), Bank of Canada (USD/CAD)
- **Field Codes** -- Alberta Energy Regulator field code registry

---

## Requirements

- Databricks workspace with **Unity Catalog** enabled
- **Serverless** compute or any cluster (no special requirements)
- **Internet access** only needed for PDF downloads (production data is included in the repo)
- ~5 minutes runtime (reads from local files)
- Compatible with both serverless and classic compute (pure SQL transformations)

## License

Data is sourced from publicly available Petrinex records. For hackathon and educational use only.
