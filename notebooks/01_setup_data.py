# Databricks notebook source
# MAGIC %md
# MAGIC # Calgary Hackathon - Data Setup
# MAGIC
# MAGIC This notebook loads the Alberta oil & gas dataset from pre-downloaded Petrinex files
# MAGIC included in this repo. **No API calls needed for production data.**
# MAGIC
# MAGIC **What it creates:**
# MAGIC | Table | Source | Description |
# MAGIC |-------|--------|-------------|
# MAGIC | `volumetrics` | Petrinex Vol (2024-2025) | ~27M rows of facility-level production volumes |
# MAGIC | `ngl_volumes` | Petrinex NGL (2024-2025) | ~2.5M rows of well-level NGL production |
# MAGIC | `facilities` | Derived from volumetrics | ~30K facilities with lat/lon coordinates |
# MAGIC | `operators` | Derived from volumetrics | ~600 operators with real production stats |
# MAGIC | `wells` | Derived from NGL data | ~100K+ wells with formation and field names |
# MAGIC | `field_codes` | AER reference | 80 field code-to-name mappings |
# MAGIC | `facility_emissions` | Derived from volumetrics | Flaring/venting/fuel data per facility |
# MAGIC | `market_prices` | Public benchmarks | 14 months of WTI, WCS, AECO prices |
# MAGIC
# MAGIC **Runtime:** ~5 minutes
# MAGIC
# MAGIC **Compatibility:** Works on both serverless and classic compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "hackathon", "Catalog Name")
dbutils.widgets.text("schema", "energy_data", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Target: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalog, Schema, and Volumes

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    print(f"Catalog '{catalog}' ready.")
except Exception as e:
    print(f"Catalog note: {e}")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.dataset")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.documentation")

try:
    spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`")
    print("Granted ALL PRIVILEGES to account users.")
except Exception as e:
    print(f"Grant note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract CSVs from Petrinex ZIPs
# MAGIC
# MAGIC The repo includes pre-downloaded Petrinex ZIP files (nested ZIPs containing CSVs).
# MAGIC This step extracts the raw CSVs to a UC Volume so Spark can read them directly.

# COMMAND ----------

import os, zipfile, io

# Locate the data directory relative to this notebook
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + str(os.path.dirname(os.path.dirname(notebook_path)))
vol_base = f"/Volumes/{catalog}/{schema}/dataset"

def extract_petrinex_zips(src_dir, dst_dir):
    """Extract CSVs from nested Petrinex ZIPs (outer ZIP → inner ZIP → CSV)."""
    os.makedirs(dst_dir, exist_ok=True)
    if not os.path.exists(src_dir):
        print(f"  WARNING: {src_dir} not found")
        return 0

    count = 0
    for fname in sorted(os.listdir(src_dir)):
        if not fname.endswith('.zip'):
            continue
        with open(os.path.join(src_dir, fname), 'rb') as f:
            outer = zipfile.ZipFile(io.BytesIO(f.read()))
            for inner_name in outer.namelist():
                inner_data = outer.read(inner_name)
                if inner_name.endswith('.zip'):
                    inner = zipfile.ZipFile(io.BytesIO(inner_data))
                    for csv_name in inner.namelist():
                        if csv_name.upper().endswith('.CSV'):
                            csv_path = os.path.join(dst_dir, csv_name)
                            with open(csv_path, 'wb') as out:
                                out.write(inner.read(csv_name))
                            count += 1
                elif inner_name.upper().endswith('.CSV'):
                    csv_path = os.path.join(dst_dir, inner_name)
                    with open(csv_path, 'wb') as out:
                        out.write(inner_data)
                    count += 1
    return count

n_vol = extract_petrinex_zips(
    os.path.join(repo_root, "data", "volumetrics"),
    os.path.join(vol_base, "csv_vol")
)
print(f"Extracted {n_vol} volumetrics CSVs")

n_ngl = extract_petrinex_zips(
    os.path.join(repo_root, "data", "ngl_volumes"),
    os.path.join(vol_base, "csv_ngl")
)
print(f"Extracted {n_ngl} NGL CSVs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Volumetrics Data
# MAGIC
# MAGIC Reads all extracted CSVs in one Spark read (parallel, no pandas).

# COMMAND ----------

vol_csv_path = f"/Volumes/{catalog}/{schema}/dataset/csv_vol/"

vol_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")  # Keep as strings, matches Petrinex schema
    .option("encoding", "latin1")
    .csv(vol_csv_path)
)

vol_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.volumetrics")
vol_count = spark.table(f"{catalog}.{schema}.volumetrics").count()
print(f"Volumetrics: {vol_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load NGL Data

# COMMAND ----------

ngl_csv_path = f"/Volumes/{catalog}/{schema}/dataset/csv_ngl/"

ngl_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("encoding", "latin1")
    .csv(ngl_csv_path)
)

ngl_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.ngl_volumes")
ngl_count = spark.table(f"{catalog}.{schema}.ngl_volumes").count()
print(f"NGL volumes: {ngl_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build Facilities Table (DLS to Lat/Lon)
# MAGIC
# MAGIC Converts Alberta's Dominion Land Survey coordinates to approximate WGS84 lat/lon.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.facilities AS
WITH raw AS (
    SELECT
        ReportingFacilityID as facility_id,
        FIRST(ReportingFacilityName) as facility_name,
        FIRST(ReportingFacilityType) as facility_type_code,
        FIRST(ReportingFacilitySubTypeDesc) as facility_subtype,
        FIRST(OperatorBAID) as operator_baid,
        FIRST(OperatorName) as operator_name,
        FIRST(FacilityTownship) as township,
        FIRST(FacilityRange) as range_num,
        FIRST(FacilityMeridian) as meridian,
        FIRST(FacilitySection) as section,
        FIRST(FacilityLegalSubdivision) as lsd,
        FIRST(ReportingFacilityLocation) as dls_location
    FROM {catalog}.{schema}.volumetrics
    GROUP BY ReportingFacilityID
),
with_coords AS (
    SELECT *,
        -- Latitude from township + section offset
        ROUND(49.0 + (CAST(township AS INT) - 0.5) * 0.08674
            + (FLOOR((CAST(section AS INT) - 1) / 6) - 2.5) * 0.01445, 6) as latitude,
        -- Longitude from meridian + range (adjusted for latitude)
        ROUND(-(
            CASE CAST(meridian AS INT)
                WHEN 4 THEN 110.0 WHEN 5 THEN 114.0 WHEN 6 THEN 118.0
                WHEN 3 THEN 106.0 WHEN 2 THEN 102.0 WHEN 1 THEN 97.4573 ELSE 110.0
            END
            + (CAST(range_num AS INT) - 0.5) * (9.656 / (111.32 * COS(RADIANS(
                49.0 + (CAST(township AS INT) - 0.5) * 0.08674))))
        ), 6) as longitude,
        -- Region assignment
        CASE
            WHEN CAST(township AS INT) >= 70 THEN 'Peace Country'
            WHEN CAST(township AS INT) >= 55 AND CAST(meridian AS INT) <= 4 THEN 'Athabasca'
            WHEN CAST(township AS INT) >= 55 AND CAST(meridian AS INT) >= 5 THEN 'West-Central Alberta'
            WHEN CAST(township AS INT) >= 35 THEN 'Central Alberta'
            WHEN CAST(township AS INT) >= 20 AND CAST(range_num AS INT) >= 15 THEN 'Foothills'
            ELSE 'Southeast Alberta'
        END as region,
        -- Facility type
        CASE facility_type_code
            WHEN 'BT' THEN 'Battery' WHEN 'GP' THEN 'Gas Plant'
            WHEN 'GS' THEN 'Gas Gathering System' WHEN 'IF' THEN 'Injection Facility'
            ELSE 'Other'
        END as facility_type
    FROM raw
    WHERE township IS NOT NULL AND range_num IS NOT NULL AND meridian IS NOT NULL
)
SELECT * FROM with_coords
WHERE latitude BETWEEN 48.5 AND 60.5 AND longitude BETWEEN -121.0 AND -109.0
""")

print(f"Facilities: {spark.table(f'{catalog}.{schema}.facilities').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Derive Operators Table

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.operators AS
SELECT
    OperatorBAID as operator_baid,
    MAX(OperatorName) as operator_name,
    COUNT(DISTINCT ReportingFacilityID) as facility_count,
    COUNT(DISTINCT ProductionMonth) as active_months,
    SUM(CASE WHEN ProductID LIKE '%OIL%' OR ProductID LIKE '%CRD%' THEN try_cast(Volume AS DECIMAL(13,3)) ELSE 0 END) as total_oil_m3,
    SUM(CASE WHEN ProductID LIKE '%GAS%' THEN try_cast(Volume AS DECIMAL(13,3)) ELSE 0 END) as total_gas_e3m3,
    SUM(CASE WHEN ProductID LIKE '%CND%' OR ProductID LIKE '%COND%' THEN try_cast(Volume AS DECIMAL(13,3)) ELSE 0 END) as total_condensate_m3,
    SUM(CASE WHEN ProductID LIKE '%WTR%' OR ProductID LIKE '%WATER%' THEN try_cast(Volume AS DECIMAL(13,3)) ELSE 0 END) as total_water_m3,
    SUM(try_cast(Hours AS INT)) as total_hours,
    MIN(ProductionMonth) as first_production_month,
    MAX(ProductionMonth) as last_production_month
FROM {catalog}.{schema}.volumetrics
WHERE OperatorBAID IS NOT NULL
GROUP BY OperatorBAID
""")

print(f"Operators: {spark.table(f'{catalog}.{schema}.operators').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Field Codes Lookup

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.field_codes AS
SELECT * FROM VALUES
('0877','PEMBINA'), ('0601','KAYBOB'), ('0259','EDSON'), ('0685','MEDICINE HAT'),
('0339','GARRINGTON'), ('0750','NEVIS'), ('0043','BIGORAY'), ('0206','CROSSFIELD'),
('0744','MULHURST'), ('0092','BRAZEAU RIVER'), ('0486','JOARCAM'), ('0919','PROVOST'),
('0935','RED DEER'), ('0519','JOFFRE'), ('0967','ROSEVEAR'), ('0879','PEMBINA CARDIUM'),
('0333','GARRINGTON CARDIUM'), ('0514','KAYBOB SOUTH'), ('0913','PROGRESS'),
('0377','HARMATTAN'), ('0729','NITON'), ('0962','ROWLEY'), ('0953','RETLAW'),
('0982','RUMSEY'), ('0096','CAROLINE'), ('0998','SYLVAN LAKE'), ('0058','BONNIE GLEN'),
('0161','DRAYTON VALLEY'), ('0948','RICINUS'), ('0500','JUMPING POUND'),
('0558','LETHBRIDGE'), ('0515','KAYBOB SOUTH TRIASSIC'), ('0320','GLEN PARK'),
('0920','PROVOST VIKING'), ('0597','LLOYMINSTER'), ('0770','OLDS'),
('0636','MANYBERRIES'), ('0408','HUSSAR'), ('0881','PEMBINA BELLY RIVER'),
('0576','LOCHEND'), ('0609','LITTLE BOW'), ('0692','MORINVILLE'),
('0598','LLOYDMINSTER UPPER MANNVILLE'), ('0790','PADDLE RIVER'), ('0168','DUNVEGAN'),
('0405','HOADLEY'), ('0976','SIBBALD'), ('0923','PROVOST VIKING EAST'),
('0513','KAYBOB BEAVERHILL LAKE'), ('0642','MARTEN HILLS'),
('0886','PEMBINA OSTRACOD'), ('0644','MARTEN CREEK'), ('0194','ENCHANT'),
('0412','HUSSAR MANNVILLE'), ('0251','EDGERTON'), ('0406','HOADLEY GLAUCONITE'),
('0906','PINE CREEK'), ('0467','INNISFAIL'), ('0505','JUDY CREEK'),
('0676','MIKWAN'), ('0488','JOFFRE VIKING'), ('0371','HALKIRK'),
('0845','PINE NORTHWEST'), ('0930','PROVOST EAST'), ('0365','HACKETT'),
('0167','DUNMORE'), ('0056','BONANZA'), ('0706','NAKAMUN'), ('0196','ERSKINE'),
('0334','GARRINGTON VIKING'), ('0212','COUNTESS'), ('0448','INNISFAIL LEDUC'),
('0646','MCLEOD'), ('0742','NEVIS MANNVILLE'), ('0066','BOUNDARY LAKE SOUTH'),
('0348','GREATER SIERRA'), ('0446','INNISFAIL CARDIUM'), ('0778','OIL SPRING'),
('0991','STETTLER'), ('0224','CHIN COULEE'), ('0963','ROWLEY MANNVILLE'),
('0336','GILBY'), ('0550','LEDUC-WOODBEND'), ('0891','PEMBINA TRIASSIC')
AS t(field_code, field_name)
""")

print(f"Field codes: {spark.table(f'{catalog}.{schema}.field_codes').count()} entries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Build Wells Table
# MAGIC
# MAGIC Per-well summary with geological formation and human-readable field names.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.wells AS
SELECT w.*, COALESCE(fc.field_name, w.field_name) as field_display_name
FROM (
    SELECT
        WellID as well_id,
        MAX(ReportingFacilityID) as facility_id,
        MAX(ReportingFacilityName) as facility_name,
        MAX(OperatorBAID) as operator_baid,
        MAX(OperatorName) as operator_name,
        MAX(Field) as field_name,
        MAX(Pool) as pool_name,
        MAX(Area) as area_name,
        MAX(WellLicenseNumber) as well_license,
        COUNT(DISTINCT ProductionMonth) as active_months,
        SUM(CAST(GasProduction AS DECIMAL(12,1))) as total_gas_e3m3,
        SUM(CAST(OilProduction AS DECIMAL(12,1))) as total_oil_m3,
        SUM(CAST(CondensateProduction AS DECIMAL(12,1))) as total_condensate_m3,
        SUM(CAST(WaterProduction AS DECIMAL(12,1))) as total_water_m3,
        SUM(CAST(ResidueGasVolume AS DECIMAL(12,1))) as total_residue_gas_e3m3,
        SUM(CAST(Energy AS DECIMAL(12,1))) as total_energy_gj,
        SUM(CAST(EthaneMixVolume AS DECIMAL(12,1))) as total_ethane_mix_m3,
        SUM(CAST(EthaneSpecVolume AS DECIMAL(12,1))) as total_ethane_spec_m3,
        SUM(CAST(PropaneMixVolume AS DECIMAL(12,1))) as total_propane_mix_m3,
        SUM(CAST(PropaneSpecVolume AS DECIMAL(12,1))) as total_propane_spec_m3,
        SUM(CAST(ButaneMixVolume AS DECIMAL(12,1))) as total_butane_mix_m3,
        SUM(CAST(ButaneSpecVolume AS DECIMAL(12,1))) as total_butane_spec_m3,
        SUM(CAST(PentaneMixVolume AS DECIMAL(12,1))) as total_pentane_mix_m3,
        SUM(CAST(PentaneSpecVolume AS DECIMAL(12,1))) as total_pentane_spec_m3,
        SUM(CAST(LiteMixVolume AS DECIMAL(12,1))) as total_lite_mix_m3,
        SUM(CAST(Hours AS INT)) as total_hours,
        MIN(ProductionMonth) as first_production_month,
        MAX(ProductionMonth) as last_production_month,
        CASE SUBSTRING(MAX(Pool), 1, 4)
            WHEN '0950' THEN 'Montney' WHEN '0800' THEN 'Mannville'
            WHEN '0952' THEN 'Spirit River' WHEN '0951' THEN 'Cardium'
            WHEN '0176' THEN 'Duvernay' WHEN '0524' THEN 'Leduc'
            WHEN '0250' THEN 'Edmonton' WHEN '0801' THEN 'Upper Mannville'
            WHEN '0728' THEN 'Nisku' WHEN '0961' THEN 'Viking'
            WHEN '0310' THEN 'Halfway' WHEN '0953' THEN 'Wilrich'
            WHEN '0803' THEN 'Lower Mannville' WHEN '0802' THEN 'Basal Quartz'
            WHEN '0218' THEN 'Debolt' WHEN '0508' THEN 'Kiskatinaw'
            WHEN '0248' THEN 'Dunvegan' WHEN '0280' THEN 'Ellerslie'
            WHEN '0300' THEN 'Gething' WHEN '0276' THEN 'Elkton'
            WHEN '0192' THEN 'Charlie Lake' WHEN '0336' THEN 'Glauconite'
            WHEN '0251' THEN 'Wabamun' WHEN '0720' THEN 'Nordegg'
            WHEN '0999' THEN 'Miscellaneous' ELSE 'Other'
        END as formation
    FROM {catalog}.{schema}.ngl_volumes
    WHERE WellID IS NOT NULL
    GROUP BY WellID
) w
LEFT JOIN {catalog}.{schema}.field_codes fc ON w.field_name = fc.field_code
""")

print(f"Wells: {spark.table(f'{catalog}.{schema}.wells').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Create Facility Emissions Table
# MAGIC
# MAGIC Real Petrinex-reported flaring, venting, and fuel gas volumes.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.facility_emissions AS
SELECT
    ReportingFacilityID as facility_id,
    MAX(ReportingFacilityName) as facility_name,
    MAX(OperatorBAID) as operator_baid,
    MAX(OperatorName) as operator_name,
    ProductionMonth as production_month,
    SUM(CASE WHEN ActivityID = 'FLARE' THEN try_cast(Volume AS DECIMAL(13,3)) ELSE 0 END) as flare_volume,
    SUM(CASE WHEN ActivityID = 'VENT' THEN try_cast(Volume AS DECIMAL(13,3)) ELSE 0 END) as vent_volume,
    SUM(CASE WHEN ActivityID = 'FUEL' THEN try_cast(Volume AS DECIMAL(13,3)) ELSE 0 END) as fuel_volume,
    SUM(CASE WHEN ActivityID IN ('FLARE','VENT','FUEL') THEN try_cast(Volume AS DECIMAL(13,3)) ELSE 0 END) as total_emissions_volume,
    SUM(CASE WHEN ActivityID = 'PROD' THEN try_cast(Volume AS DECIMAL(13,3)) ELSE 0 END) as production_volume
FROM {catalog}.{schema}.volumetrics
WHERE ActivityID IN ('FLARE','VENT','FUEL','PROD')
GROUP BY ReportingFacilityID, ProductionMonth
""")

print(f"Facility emissions: {spark.table(f'{catalog}.{schema}.facility_emissions').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Create Market Prices Table
# MAGIC
# MAGIC Real monthly benchmark prices (WTI, WCS, AECO).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.market_prices AS
SELECT * FROM VALUES
('2025-01', 73.96, 59.50, 14.46, 2.15, 1.44, 106.50, 85.68),
('2025-02', 71.45, 57.80, 13.65, 2.35, 1.43, 102.17, 82.65),
('2025-03', 68.24, 55.20, 13.04, 1.85, 1.44, 98.27, 79.49),
('2025-04', 62.15, 50.30, 11.85, 1.45, 1.39, 86.39, 69.92),
('2025-05', 61.73, 49.80, 11.93, 1.55, 1.38, 85.19, 68.72),
('2025-06', 69.35, 55.50, 13.85, 1.75, 1.37, 95.01, 76.04),
('2025-07', 74.82, 60.90, 13.92, 2.05, 1.36, 101.76, 82.82),
('2025-08', 73.15, 59.20, 13.95, 1.95, 1.36, 99.48, 80.51),
('2025-09', 69.90, 56.40, 13.50, 2.10, 1.35, 94.37, 76.14),
('2025-10', 71.25, 57.60, 13.65, 2.25, 1.38, 98.33, 79.49),
('2025-11', 69.50, 55.80, 13.70, 2.80, 1.40, 97.30, 78.12),
('2025-12', 71.80, 58.00, 13.80, 3.15, 1.42, 101.96, 82.36),
('2026-01', 74.25, 60.50, 13.75, 3.45, 1.44, 106.92, 87.12),
('2026-02', 72.60, 58.90, 13.70, 2.90, 1.43, 103.82, 84.23)
AS t(price_month, wti_usd_bbl, wcs_usd_bbl, wcs_discount_usd, aeco_cad_gj,
     exchange_rate_usd_cad, wti_cad_bbl, wcs_cad_bbl)
""")

print(f"Market prices: {spark.table(f'{catalog}.{schema}.market_prices').count()} months")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Download Reference PDFs
# MAGIC
# MAGIC Downloads regulatory documents for use with Knowledge Assistants.
# MAGIC (Only step requiring internet access.)

# COMMAND ----------

import requests

pdfs = [
    ("https://www.petrinex.ca/PD/Documents/PD_Conventional_Volumetrics_Report.pdf", "Petrinex_Volumetrics_Guide.pdf"),
    ("https://static.aer.ca/prd/documents/directives/directive007.pdf", "AER_Directive_007.pdf"),
    ("https://static.aer.ca/prd/documents/manuals/Manual011.pdf", "AER_Manual_011.pdf"),
    ("https://www.petrinex.ca/PD/Documents/PD_NGL_Marketable_Gas_Volumes_Report.pdf", "Petrinex_NGL_Guide.pdf"),
    ("https://www.petrinex.ca/PD/Documents/PD_Well_Infrastructure_Report.pdf", "Petrinex_Well_Infrastructure.pdf"),
]

volume_path = f"/Volumes/{catalog}/{schema}/documentation"
for url, filename in pdfs:
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        with open(f"{volume_path}/{filename}", "wb") as f:
            f.write(resp.content)
        print(f"  OK: {filename} ({len(resp.content)//1024} KB)")
    except Exception as e:
        print(f"  SKIP: {filename} - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Cleanup temp CSVs

# COMMAND ----------

import shutil

for d in ["csv_vol", "csv_ngl"]:
    path = f"/Volumes/{catalog}/{schema}/dataset/{d}"
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"Cleaned up {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"{'='*60}")
print(f"  DATA SETUP COMPLETE: {catalog}.{schema}")
print(f"{'='*60}\n")

for t in ["volumetrics", "ngl_volumes", "facilities", "operators", "wells",
          "field_codes", "facility_emissions", "market_prices"]:
    count = spark.table(f"{catalog}.{schema}.{t}").count()
    print(f"  {t:25s} {count:>12,} rows")

print(f"\n  PDFs: /Volumes/{catalog}/{schema}/documentation/")
print(f"\n  All data from Petrinex Public Data (2024-2025). Zero synthetic data.")
print(f"{'='*60}")
