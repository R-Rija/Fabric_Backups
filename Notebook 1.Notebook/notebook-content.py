# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ca779aa0-014f-4240-8027-2ae2cf30876d",
# META       "default_lakehouse_name": "Loyalty_Customer",
# META       "default_lakehouse_workspace_id": "b4a3c763-a8cb-4108-bf6f-379159cdaf1c",
# META       "known_lakehouses": [
# META         {
# META           "id": "ca779aa0-014f-4240-8027-2ae2cf30876d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==============================================================================
# Microsoft Fabric Analytics Application: Retail Loyalty Analytics
# Phase 2: READ-ONLY Data Discovery & Profiling (FIXED)
# ==============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    NumericType, IntegerType, LongType, DoubleType, FloatType, DecimalType,
    DateType, TimestampType, StringType
)
import datetime

TABLE_NAME = "retail_loyalty_engagement_table"

print("=" * 80)
print(f"STARTING READ-ONLY PROFILING: {TABLE_NAME}")
print(f"Timestamp: {datetime.datetime.utcnow().isoformat()} UTC")
print("=" * 80)

# Load existing Lakehouse table
df = spark.table(TABLE_NAME)

# Helper: Case-insensitive column resolver
col_map = {c.upper(): c for c in df.columns}
def find_col(name):
    return col_map.get(name.upper(), None)

# ------------------------------------------------------------------------------
# 1. TABLE STRUCTURE (Points 1, 2, 3)
# ------------------------------------------------------------------------------
total_rows = df.count()
columns_info = df.dtypes

print("\n" + "=" * 80)
print("1. TABLE STRUCTURE & SCHEMA")
print("=" * 80)
print(f"[OBSERVED FACT] Total Row Count: {total_rows:,}")
print(f"[OBSERVED FACT] Total Column Count: {len(columns_info)}")
print("-" * 80)
print(f"{'#':<4} | {'Column Name':<32} | {'Data Type':<20} | {'Nullable (Catalog)'}")
print("-" * 80)
schema_fields = {f.name: f.nullable for f in df.schema.fields}
for i, (col_name, dtype) in enumerate(columns_info, 1):
    nullable = schema_fields.get(col_name, True)
    print(f"{i:<4} | {col_name:<32} | {dtype:<20} | {str(nullable):<18}")

# ------------------------------------------------------------------------------
# 2. COLUMN-LEVEL DATA QUALITY & STATISTICS (Points 4, 5, 6, 7)
# ------------------------------------------------------------------------------
print("\n" + "=" * 80)
print("2. COLUMN DATA QUALITY & STATISTICAL PROFILE")
print("=" * 80)

numeric_cols = [c for c, t in columns_info if any(nt in t.lower() for nt in ["int", "long", "double", "float", "decimal", "short"])]
date_cols = [c for c, t in columns_info if any(dt in t.lower() for dt in ["date", "timestamp"])]
string_cols = [c for c, t in columns_info if "string" in t.lower()]

agg_exprs = []
for c in df.columns:
    agg_exprs.append(F.count(F.when(F.col(c).isNull(), 1)).alias(f"{c}__null_cnt"))
    agg_exprs.append(F.countDistinct(F.col(c)).alias(f"{c}__distinct_cnt"))

for c in numeric_cols:
    agg_exprs.append(F.min(F.col(c)).alias(f"{c}__min"))
    agg_exprs.append(F.max(F.col(c)).alias(f"{c}__max"))
    agg_exprs.append(F.avg(F.col(c)).alias(f"{c}__avg"))

for c in date_cols:
    agg_exprs.append(F.min(F.col(c)).alias(f"{c}__min_date"))
    agg_exprs.append(F.max(F.col(c)).alias(f"{c}__max_date"))

stats_row = df.agg(*agg_exprs).collect()[0]

print(f"{'Column Name':<32} | {'Null Count':<12} | {'Null %':<8} | {'Distinct':<10} | {'Min':<16} | {'Max':<16} | {'Avg / Details'}")
print("-" * 120)
for c, dtype in columns_info:
    null_cnt = stats_row[f"{c}__null_cnt"]
    null_pct = (null_cnt / total_rows * 100) if total_rows > 0 else 0.0
    dist_cnt = stats_row[f"{c}__distinct_cnt"]
    
    min_val, max_val, avg_val = "N/A", "N/A", "N/A"
    if c in numeric_cols:
        raw_min = stats_row[f"{c}__min"]
        raw_max = stats_row[f"{c}__max"]
        raw_avg = stats_row[f"{c}__avg"]
        min_val = f"{raw_min:,.2f}" if raw_min is not None else "NULL"
        max_val = f"{raw_max:,.2f}" if raw_max is not None else "NULL"
        avg_val = f"{raw_avg:,.2f}" if raw_avg is not None else "NULL"
    elif c in date_cols:
        min_val = str(stats_row[f"{c}__min_date"])
        max_val = str(stats_row[f"{c}__max_date"])
        avg_val = "(Date Range)"
    else:
        avg_val = "(Categorical/String)"
        
    print(f"{c:<32} | {null_cnt:<12,d} | {null_pct:<7.2f}% | {dist_cnt:<10,d} | {min_val:<16} | {max_val:<16} | {avg_val}")

# ------------------------------------------------------------------------------
# 3. SAMPLE DATA (Point 8 - Masking EMAIL_ADDRESS)
# ------------------------------------------------------------------------------
print("\n" + "=" * 80)
print("3. SAMPLE ROWS (EMAIL_ADDRESS MASKED FOR PRIVACY)")
print("=" * 80)
sample_df = df.limit(5)
email_col = find_col("EMAIL_ADDRESS")
if email_col:
    sample_df = sample_df.withColumn(email_col, F.lit("***MASKED_FOR_PRIVACY***"))
sample_df.show(5, truncate=False, vertical=True)

# ------------------------------------------------------------------------------
# 4. GRAIN ANALYSIS (Points 9, 10, 11, 12, 27)
# ------------------------------------------------------------------------------
print("\n" + "=" * 80)
print("4. GRAIN & PRIMARY KEY CANDIDATE TESTING")
print("=" * 80)

cust_col = find_col("CUSTOMER_ID")
period_col = find_col("ACCOUNTING_PERIOD_NUMBER")
year_col = find_col("ACCOUNTING_YEAR")
date_col = find_col("PERIOD_END_DATE")

if cust_col:
    distinct_cust = df.select(cust_col).distinct().count()
    print(f"[OBSERVED FACT] Distinct CUSTOMER_ID Count: {distinct_cust:,}")
    print(f"[OBSERVED FACT] Total Table Rows:          {total_rows:,}")
    print(f"[CALCULATED STAT] Avg Rows per Customer:   {total_rows / distinct_cust:.2f}")

    cust_row_counts = df.groupBy(cust_col).count()
    cust_stats = cust_row_counts.agg(
        F.min("count").alias("min_rows"),
        F.max("count").alias("max_rows"),
        F.avg("count").alias("avg_rows")
    ).collect()[0]
    print(f"[CALCULATED STAT] Rows per Customer - Min: {cust_stats['min_rows']}, Max: {cust_stats['max_rows']}, Avg: {cust_stats['avg_rows']:.2f}")

    if total_rows == distinct_cust:
        print("[INFERENCE] Table appears to be at GRAIN: 1 ROW PER CUSTOMER.")
    else:
        print("[INFERENCE] Table is MULTI-ROW per customer (History / Longitudinal / Period data).")

if cust_col and period_col:
    print("\n--- Testing Business Key: (CUSTOMER_ID + ACCOUNTING_PERIOD_NUMBER) ---")
    combo_count = df.groupBy(cust_col, period_col).count()
    duplicates_df = combo_count.filter(F.col("count") > 1)
    dup_count = duplicates_df.count()
    unique_combos = combo_count.count()
    
    print(f"[OBSERVED FACT] Distinct (CUSTOMER_ID, ACCOUNTING_PERIOD_NUMBER) pairs: {unique_combos:,}")
    print(f"[OBSERVED FACT] Duplicate Combinations with count > 1:                  {dup_count:,}")
    if dup_count == 0 and unique_combos == total_rows:
        print(">>> [CONFIRMED GRAIN] The combination (CUSTOMER_ID + ACCOUNTING_PERIOD_NUMBER) is 100% UNIQUE.")
        print(">>> Grain of the table: ONE ROW PER CUSTOMER PER ACCOUNTING PERIOD.")
    else:
        print(f">>> [OBSERVED] (CUSTOMER_ID + ACCOUNTING_PERIOD_NUMBER) is NOT strictly unique. Violations: {dup_count}")

# ------------------------------------------------------------------------------
# 5. TIME STRUCTURE ANALYSIS (Points 13, 14, 15)
# ------------------------------------------------------------------------------
print("\n" + "=" * 80)
print("5. TIME STRUCTURE & PERIOD DISTRIBUTION")
print("=" * 80)

group_time_cols = [c for c in [year_col, period_col, date_col] if c is not None]
if group_time_cols:
    time_agg = df.groupBy(*group_time_cols).agg(
        F.count("*").alias("record_count"),
        F.countDistinct(cust_col).alias("distinct_customers") if cust_col else F.lit(0).alias("distinct_customers")
    ).orderBy(*group_time_cols)
    
    print(f"{'Year':<8} | {'Period':<10} | {'Period End Date':<18} | {'Record Count':<14} | {'Distinct Customers'}")
    print("-" * 75)
    for r in time_agg.collect():
        yr = str(r[year_col]) if year_col else "N/A"
        prd = str(r[period_col]) if period_col else "N/A"
        dt = str(r[date_col]) if date_col else "N/A"
        rc = r["record_count"]
        dc = r["distinct_customers"]
        print(f"{yr:<8} | {prd:<10} | {dt:<18} | {rc:<14,d} | {dc:,d}")

# ------------------------------------------------------------------------------
# 6. CUSTOMER DEMOGRAPHICS & CONSENT (Points 16, 17)
# ------------------------------------------------------------------------------
print("\n" + "=" * 80)
print("6. CUSTOMER STATUS & CONSENT DISTRIBUTIONS")
print("=" * 80)

for field_name in ["CUSTOMER_STATUS", "CONSENT_TYPE", "STATE"]:
    c = find_col(field_name)
    if c:
        print(f"\n--- Distribution: {c} ---")
        dist = df.groupBy(c).agg(
            F.count("*").alias("count"),
            (F.count("*") / total_rows * 100).alias("percentage")
        ).orderBy(F.desc("count"))
        for row in dist.collect():
            val = str(row[c]) if row[c] is not None else "NULL"
            cnt = row["count"]
            pct = row["percentage"]
            print(f"  * {val:<25} : {cnt:>10,d} ({pct:>6.2f}%)")

# ------------------------------------------------------------------------------
# 7. LOYALTY & MEMBERSHIP PROGRAM ATTRIBUTES (Points 18, 19, 20, 21, 22)
# ------------------------------------------------------------------------------
print("\n" + "=" * 80)
print("7. LOYALTY PROGRAM FLAGS & MEMBERSHIP COMBINATIONS")
print("=" * 80)

loyalty_flags = ["BRAND_CARD_FLAG", "LOYALTY_PROGRAM_FLAG", "PARTNER_PROGRAM_FLAG", "PROGRAM_LINKING_STATUS", "BRAND_LOYALTY_LINKAGE"]
present_flags = [find_col(f) for f in loyalty_flags if find_col(f) is not None]

for c in present_flags:
    print(f"\n--- Distribution: {c} ---")
    dist = df.groupBy(c).agg(
        F.count("*").alias("count"),
        (F.count("*") / total_rows * 100).alias("percentage")
    ).orderBy(F.desc("count"))
    for row in dist.collect():
        val = str(row[c]) if row[c] is not None else "NULL"
        cnt = row["count"]
        pct = row["percentage"]
        print(f"  * {val:<30} : {cnt:>10,d} ({pct:>6.2f}%)")

flag_bcard = find_col("BRAND_CARD_FLAG")
flag_loyalty = find_col("LOYALTY_PROGRAM_FLAG")
flag_partner = find_col("PARTNER_PROGRAM_FLAG")

if flag_bcard and flag_loyalty and flag_partner:
    print("\n--- Loyalty Program Membership Segmentation Combinations ---")
    combo_df = df.groupBy(flag_bcard, flag_loyalty, flag_partner).agg(
        F.count("*").alias("count"),
        (F.count("*") / total_rows * 100).alias("percentage")
    ).orderBy(F.desc("count"))
    
    print(f"{'Brand Card':<12} | {'Loyalty Prog':<14} | {'Partner Prog':<14} | {'Records':<12} | {'Percentage'}")
    print("-" * 65)
    for row in combo_df.collect():
        bc = str(row[flag_bcard])
        lp = str(row[flag_loyalty])
        pp = str(row[flag_partner])
        cnt = row["count"]
        pct = row["percentage"]
        print(f"{bc:<12} | {lp:<14} | {pp:<14} | {cnt:<12,d} | {pct:>6.2f}%")

# ------------------------------------------------------------------------------
# 8. RFM SEGMENTATION DISTRIBUTIONS (Points 23, 24)
# ------------------------------------------------------------------------------
print("\n" + "=" * 80)
print("8. RFM SEGMENT DISTRIBUTIONS")
print("=" * 80)

for rfm_name in ["RFM_HIGH_SEGMENT", "RFM_LOW_SEGMENT"]:
    c = find_col(rfm_name)
    if c:
        print(f"\n--- Distribution: {c} ---")
        dist = df.groupBy(c).agg(
            F.count("*").alias("count"),
            (F.count("*") / total_rows * 100).alias("percentage")
        ).orderBy(F.desc("count"))
        for row in dist.collect():
            val = str(row[c]) if row[c] is not None else "NULL"
            cnt = row["count"]
            pct = row["percentage"]
            print(f"  * {val:<28} : {cnt:>10,d} ({pct:>6.2f}%)")

# ------------------------------------------------------------------------------
# 9. SALES & ENGAGEMENT METRICS INTEGRITY (Point 25)
# ------------------------------------------------------------------------------
print("\n" + "=" * 80)
print("9. SALES & ENGAGEMENT MATHEMATICAL INTEGRITY VALIDATION")
print("=" * 80)

col_on_txn = find_col("ONLINE_TXN_CNT")
col_in_txn = find_col("INSTORE_TXN_CNT")
col_tot_txn = find_col("TOTAL_TXN")

col_on_units = find_col("ONLINE_UNITS")
col_in_units = find_col("INSTORE_UNITS")
col_tot_units = find_col("TOTAL_UNITS")

col_on_sales = find_col("ONLINE_SALES")
col_in_sales = find_col("INSTORE_SALES")
col_tot_sales = find_col("TOTAL_SALES")

if col_on_txn and col_in_txn and col_tot_txn:
    txn_mismatch = df.filter(
        F.coalesce(F.col(col_on_txn), F.lit(0)) + F.coalesce(F.col(col_in_txn), F.lit(0)) != F.coalesce(F.col(col_tot_txn), F.lit(0))
    ).count()
    print(f"Validation: {col_on_txn} + {col_in_txn} == {col_tot_txn}")
    print(f"  -> Mismatch Count: {txn_mismatch:,} (Holds 100%: {txn_mismatch == 0})")

if col_on_units and col_in_units and col_tot_units:
    units_mismatch = df.filter(
        F.coalesce(F.col(col_on_units), F.lit(0)) + F.coalesce(F.col(col_in_units), F.lit(0)) != F.coalesce(F.col(col_tot_units), F.lit(0))
    ).count()
    print(f"Validation: {col_on_units} + {col_in_units} == {col_tot_units}")
    print(f"  -> Mismatch Count: {units_mismatch:,} (Holds 100%: {units_mismatch == 0})")

if col_on_sales and col_in_sales and col_tot_sales:
    sales_mismatch = df.filter(
        F.abs((F.coalesce(F.col(col_on_sales), F.lit(0.0)) + F.coalesce(F.col(col_in_sales), F.lit(0.0))) - F.coalesce(F.col(col_tot_sales), F.lit(0.0))) > 0.01
    ).count()
    print(f"Validation: {col_on_sales} + {col_in_sales} == {col_tot_sales}")
    print(f"  -> Mismatch Count: {sales_mismatch:,} (Holds 100%: {sales_mismatch == 0})")

# ------------------------------------------------------------------------------
# 10. ROLLING 12-MONTH METRICS ANALYSIS (Point 26) - FIXED
# ------------------------------------------------------------------------------
print("\n" + "=" * 80)
print("10. ROLLING 12-MONTH METRICS (SPEND_12M, UNITS_12M, TXN_12M)")
print("=" * 80)

m12_spend = find_col("SPEND_12M")
m12_units = find_col("UNITS_12M")
m12_txn = find_col("TXN_12M")

m12_present = [c for c in [m12_spend, m12_units, m12_txn] if c is not None]
if m12_present:
    m12_exprs = []
    for c in m12_present:
        m12_exprs.extend([
            F.min(F.col(c)).alias(f"{c}__min"),
            F.max(F.col(c)).alias(f"{c}__max"),
            F.avg(F.col(c)).alias(f"{c}__avg"),
            F.count(F.when(F.col(c).isNull(), 1)).alias(f"{c}__nulls")
        ])
    m12_stats = df.select(*m12_exprs).collect()[0]

    for c in m12_present:
        min_v = m12_stats[f"{c}__min"]
        max_v = m12_stats[f"{c}__max"]
        avg_v = m12_stats[f"{c}__avg"]
        nulls_v = m12_stats[f"{c}__nulls"]
        print(f"Metric: {c:<14} | Nulls: {nulls_v:<8,d} | Min: {min_v:<12,.2f} | Max: {max_v:<14,.2f} | Avg: {avg_v:,.2f}")

    if m12_spend and col_tot_sales:
        spend_compare = df.select(
            F.avg(F.col(col_tot_sales)).alias("avg_period_sales"),
            F.avg(F.col(m12_spend)).alias("avg_12m_spend"),
            F.count(F.when(F.col(m12_spend) < F.col(col_tot_sales), 1)).alias("spend_12m_lt_period")
        ).collect()[0]
        
        avg_period = spend_compare["avg_period_sales"]
        avg_12m = spend_compare["avg_12m_spend"]
        anomalies = spend_compare["spend_12m_lt_period"]
        print("-" * 80)
        print(f"[CALCULATED STAT] Avg Period Sales: ${avg_period:,.2f} vs. Avg 12M Spend: ${avg_12m:,.2f}")
        print(f"[CALCULATED STAT] Records where SPEND_12M < TOTAL_SALES: {anomalies:,}")
        if avg_12m > avg_period and anomalies == 0:
            print("[INFERENCE] SPEND_12M exhibits consistent rolling 12-month accumulation behavior.")
        else:
            print("[INFERENCE] SPEND_12M relationship with period sales requires further domain validation.")

print("\n" + "=" * 80)
print("PROFILING COMPLETE. OUTPUT READY FOR PHASE 2 DISCOVERY REPORT.")
print("=" * 80)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
