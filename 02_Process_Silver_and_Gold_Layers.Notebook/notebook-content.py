# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "392e46e1-f99a-40c5-9926-8df185cc4135",
# META       "default_lakehouse_name": "Demand_Gold_LH",
# META       "default_lakehouse_workspace_id": "ca009f72-db1a-4924-bb61-648571da862e",
# META       "known_lakehouses": [
# META         {
# META           "id": "392e46e1-f99a-40c5-9926-8df185cc4135"
# META         },
# META         {
# META           "id": "cb4430c9-9f40-48d2-8b65-6db5dc9ee3cf"
# META         },
# META         {
# META           "id": "5fa5657e-7828-4a63-bc98-8f3b6c509a3d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==========================================
# Notebook 2 (Production Version): Silver & Gold Layer Processing
# ==========================================

from pyspark.sql import functions as F

# Define fully qualified database / lakehouse scopes
BRONZE, SILVER, GOLD = "Demand_Bronze_LH.dbo", "Demand_Silver_LH.dbo", "Demand_Gold_LH.dbo"

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)

print("--- Step 1: Suppliers & Promotions (Bronze Layer Generation) ---")
suppliers = [
    ("SUP-01", "Global Textiles Ltd", "Apparel", "Mumbai", 5),
    ("SUP-02", "Apex Rain Gear Corp", "Rainwear", "Chennai", 3),
    ("SUP-03", "Swift Logistics & Footwear", "Footwear", "Delhi", 7),
    ("SUP-04", "Prime Electronics & Accessories", "Accessories", "Bangalore", 4),
]
save(spark.createDataFrame(suppliers, ["supplier_id", "supplier_name", "category", "city", "lead_time_days"]),
     f"{BRONZE}.bronze_suppliers")

promos = [
    ("PROMO-01", "Monsoon Rainwear Blitz", "Rainwear", "2024-10-01", "2024-12-31", 0.20),
    ("PROMO-02", "Winter Festive Sale", "Winterwear", "2024-11-15", "2025-01-15", 0.15),
    ("PROMO-03", "Summer Clearance", "SummerApparel", "2025-04-01", "2025-05-30", 0.25),
    ("PROMO-04", "Flash Weekend Special", "Accessories", "2025-08-10", "2025-08-12", 0.10),
]
save(spark.createDataFrame(promos, ["promotion_id", "promo_name", "category", "start_date", "end_date", "discount_pct"]),
     f"{BRONZE}.bronze_promotions")

print("--- Step 2: Transforming Bronze to Silver Layer (Cleaning & Standardization) ---")

# Clean Sales Table
save(spark.table(f"{BRONZE}.bronze_sales")
        .dropDuplicates(["transaction_id"])
        .filter(F.col("quantity") > 0)
        .withColumn("transaction_date", F.to_date("transaction_date")),
     f"{SILVER}.silver_sales")

# Clean Inventory Snapshot Table
save(spark.table(f"{BRONZE}.bronze_inventory")
        .dropDuplicates(["snapshot_date", "store_id", "product_id"])
        .withColumn("snapshot_date", F.to_date("snapshot_date")),
     f"{SILVER}.silver_inventory")

# Standardize Master Dimension Tables (Products, Stores, Warehouses, Suppliers, Promotions)
save(spark.table(f"{BRONZE}.bronze_products"),   f"{SILVER}.silver_products")
save(spark.table(f"{BRONZE}.bronze_stores"),     f"{SILVER}.silver_stores")
save(spark.table(f"{BRONZE}.bronze_warehouses"), f"{SILVER}.silver_warehouses")
save(spark.table(f"{BRONZE}.bronze_suppliers"),  f"{SILVER}.silver_suppliers")

# Standardize Promotions with explicit date casting
save(spark.table(f"{BRONZE}.bronze_promotions")
        .withColumn("start_date", F.to_date("start_date"))
        .withColumn("end_date", F.to_date("end_date")),
     f"{SILVER}.silver_promotions")

print("--- Step 3: Building Gold Layer Aggregated Demand (Store x Product x Day) ---")

gold_demand = spark.sql(f"""
    SELECT s.transaction_date AS demand_date,
           s.store_id, st.city, st.region,
           s.product_id, p.category,
           SUM(s.quantity)    AS total_units_sold,
           SUM(s.revenue)     AS total_revenue,
           AVG(s.unit_price)  AS avg_selling_price
    FROM {SILVER}.silver_sales s
    JOIN {SILVER}.silver_stores   st ON s.store_id   = st.store_id
    JOIN {SILVER}.silver_products p  ON s.product_id = p.product_id
    GROUP BY s.transaction_date, s.store_id, st.city, st.region, s.product_id, p.category
""")

save(gold_demand, f"{GOLD}.gold_daily_demand")

print("SUCCESS: All silver tables written to Demand_Silver_LH and gold_daily_demand written to Demand_Gold_LH.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
