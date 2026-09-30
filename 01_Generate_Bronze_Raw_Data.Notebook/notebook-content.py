# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5fa5657e-7828-4a63-bc98-8f3b6c509a3d",
# META       "default_lakehouse_name": "Demand_Bronze_LH",
# META       "default_lakehouse_workspace_id": "ca009f72-db1a-4924-bb61-648571da862e",
# META       "known_lakehouses": [
# META         {
# META           "id": "5fa5657e-7828-4a63-bc98-8f3b6c509a3d"
# META         },
# META         {
# META           "id": "cb4430c9-9f40-48d2-8b65-6db5dc9ee3cf"
# META         },
# META         {
# META           "id": "392e46e1-f99a-40c5-9926-8df185cc4135"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==========================================
# Notebook 1 (Unified & Corrected): Bronze Layer Generation
# Scope: 50 Stores, 5 Warehouses, 2,000 SKUs, 2 Years History
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

print("--- Step 1: Generating Dimension Tables ---")

# 1. Generate 50 Stores Across Regions
regions_cities = [
    ("South", "Chennai"), ("South", "Bangalore"), ("South", "Hyderabad"), 
    ("South", "Kochi"), ("South", "Coimbatore"), ("West", "Mumbai"), 
    ("West", "Pune"), ("North", "Delhi"), ("North", "Gurugram"), 
    ("East", "Kolkata"), ("Central", "Bhopal"), ("West", "Ahmedabad")
]

stores_data = []
for i in range(1, 51):
    region, city = random.choice(regions_cities)
    store_id = f"STORE-{i:03d}"
    store_name = f"{city} Retail Flagship {i}"
    capacity = random.randint(4000, 12000)
    stores_data.append((store_id, store_name, city, region, capacity))

# 2. Generate 5 Regional Warehouses
warehouses_data = [
    ("WH-SOUTH", "Chennai Central Fulfillment Hub", "Chennai", 75000),
    ("WH-WEST", "Mumbai Distribution Center", "Mumbai", 90000),
    ("WH-NORTH", "Delhi NCR Logistics Park", "Delhi", 85000),
    ("WH-EAST", "Kolkata Eastern Hub", "Kolkata", 60000),
    ("WH-CENTRAL", "Nagpur Central Hub", "Nagpur", 70000)
]

# 3. Generate 2,000 SKUs Across Deep Clothing & Retail Categories
clothing_categories = {
    "Rainwear": {"sub": ["Waterproof Jacket", "Poncho", "Windcheater", "Waterproof Pants", "Travel Umbrella"], "cost_range": (400, 1200), "price_range": (1499, 3999)},
    "Winterwear": {"sub": ["Thermal Inner", "Woolen Sweater", "Fleece Jacket", "Heavy Parka", "Beanie & Gloves"], "cost_range": (350, 2000), "price_range": (1199, 5999)},
    "SummerApparel": {"sub": ["Cotton T-Shirt", "Linen Shirt", "Denim Shorts", "Polo Tee", "Casual Chinos"], "cost_range": (200, 800), "price_range": (699, 2499)},
    "Activewear": {"sub": ["Running Tights", "Gym Shorts", "Performance Tee", "Track Pants", "Compression Top"], "cost_range": (300, 900), "price_range": (999, 2999)},
    "Footwear": {"sub": ["Running Shoes", "Waterproof Sandals", "Casual Sneakers", "Formal Shoes", "Hiking Boots"], "cost_range": (800, 2500), "price_range": (2499, 6999)},
    "Accessories": {"sub": ["Waterproof Backpack", "Travel Duffle", "Canvas Belt", "Crossbody Bag", "Sports Cap"], "cost_range": (250, 1000), "price_range": (799, 3499)}
}

products_data = []
sku_counter = 1
while sku_counter <= 2000:
    cat_name = random.choice(list(clothing_categories.keys()))
    cat_info = clothing_categories[cat_name]
    sub_type = random.choice(cat_info["sub"])
    
    sku_id = f"SKU-{sku_counter:04d}"
    product_name = f"{sub_type} Gen-{sku_counter % 5 + 1} ({cat_name[0:3].upper()})"
    unit_cost = float(random.randint(cat_info["cost_range"][0], cat_info["cost_range"][1]))
    selling_price = float(random.randint(cat_info["price_range"][0], cat_info["price_range"][1]))
    
    products_data.append((sku_id, product_name, cat_name, unit_cost, selling_price))
    sku_counter += 1

# Create DataFrames
df_stores = spark.createDataFrame(stores_data, ["store_id", "store_name", "city", "region", "store_capacity"])
df_warehouses = spark.createDataFrame(warehouses_data, ["warehouse_id", "warehouse_name", "city", "warehouse_capacity"])
df_products = spark.createDataFrame(products_data, ["product_id", "product_name", "category", "unit_cost", "selling_price"])

# Write and register as Managed Tables using saveAsTable
df_stores.write.format("delta").mode("overwrite").saveAsTable("bronze_stores")
df_warehouses.write.format("delta").mode("overwrite").saveAsTable("bronze_warehouses")
df_products.write.format("delta").mode("overwrite").saveAsTable("bronze_products")

print("Dimensions saved and registered successfully.")

print("\n--- Step 2: Generating Distributed Fact History (730 Days) ---")

# 1. Create a date sequence for 730 days (2 years)
df_dates = spark.sql("""
    SELECT EXPLODE(SEQUENCE(DATE'2024-09-22', DATE'2026-09-22', INTERVAL 1 DAY)) AS transaction_date
""")

spark.conf.set("spark.sql.shuffle.partitions", "200")

# 2. Cross-join dates, stores, and products
df_sales_raw = df_dates.crossJoin(
    spark.read.table("bronze_stores").select("store_id", "city")
).join(
    spark.read.table("bronze_products").select("product_id", "selling_price", "category"), how="inner"
).select(
    F.col("transaction_date").cast("string").alias("transaction_date"),
    F.col("store_id"),
    F.col("product_id"),
    F.col("selling_price"),
    F.col("category"),
    F.col("city")
)

# 3. Add realistic quantities (with monsoon spikes) and revenue
df_sales_final = df_sales_raw.select(
    F.concat(F.lit("TXN-"), F.date_format("transaction_date", "yyyyMMdd"), F.lit("-"), F.col("store_id"), F.lit("-"), F.col("product_id")).alias("transaction_id"),
    F.col("transaction_date"),
    F.col("store_id"),
    F.col("product_id"),
    F.when(
        (F.col("category") == "Rainwear") & 
        (F.col("city").isin(["Chennai", "Kochi", "Bangalore"])) & 
        (F.month("transaction_date").isin([10, 11, 12])),
        F.abs(F.rand() * 15 + 5).cast("int")
    ).otherwise(F.abs(F.rand() * 8).cast("int")).alias("quantity"),
    F.col("selling_price").alias("unit_price"),
    F.lit(0.0).alias("discount")
).withColumn(
    "revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
).filter(F.col("quantity") > 0)

# 4. Write Fact Sales as Managed Table
df_sales_final.write.format("delta").mode("overwrite").saveAsTable("bronze_sales")
print("bronze_sales generated and registered successfully.")

# 5. Generate Inventory Snapshot Table
df_inventory = df_sales_final.select(
    F.col("transaction_date").alias("snapshot_date"),
    F.col("store_id"),
    F.col("product_id"),
    F.abs(F.rand() * 200 + 50).cast("int").alias("on_hand_qty"),
    F.abs(F.rand() * 20).cast("int").alias("reserved_qty"),
    F.abs(F.rand() * 50).cast("int").alias("in_transit_qty")
).dropDuplicates(["snapshot_date", "store_id", "product_id"])

# 6. Write Fact Inventory as Managed Table
df_inventory.write.format("delta").mode("overwrite").saveAsTable("bronze_inventory")
print("bronze_inventory generated and registered successfully!")
print("\n--- ALL BRONZE TABLES SUCCESSFULLY CREATED ---")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
