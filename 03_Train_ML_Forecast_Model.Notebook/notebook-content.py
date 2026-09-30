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
# META           "id": "5fa5657e-7828-4a63-bc98-8f3b6c509a3d"
# META         },
# META         {
# META           "id": "cb4430c9-9f40-48d2-8b65-6db5dc9ee3cf"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ==========================================
# Notebook 3 (Corrected & Optimized): ML Forecast Generation & Gold Layer Sync
# Scope: Rainwear category, 10 rainy-city stores, 14-day daily forecasting with LightGBM
# ==========================================

from pyspark.sql import functions as F
from datetime import timedelta
import pandas as pd, numpy as np, mlflow
from lightgbm import LGBMRegressor

SILVER, GOLD = "Demand_Silver_LH.dbo", "Demand_Gold_LH.dbo"
CATEGORY, N_STORES, HORIZON, LOOKBACK = "Rainwear", 10, 14, 730
MODEL_VERSION = "lgbm_v2_poisson"

print("--- Step 1: Loading Silver Data & Filling Zero-Sales Days ---")
# Scope to Rainwear and target rainy-city stores (Chennai, Kochi, Bangalore first)
prods  = spark.table(f"{SILVER}.silver_products").filter(F.col("category") == CATEGORY).select("product_id")
stores = (spark.table(f"{SILVER}.silver_stores")
          .orderBy(F.col("city").isin("Chennai", "Kochi", "Bangalore").desc(), "store_id")
          .limit(N_STORES).select("store_id", "city"))
sales  = spark.table(f"{SILVER}.silver_sales")

max_d  = sales.agg(F.max("transaction_date")).first()[0]
min_d  = max_d - timedelta(days=LOOKBACK - 1)
dates  = spark.sql(f"SELECT explode(sequence(to_date('{min_d}'), to_date('{max_d}'), interval 1 day)) AS demand_date")
actual = sales.select(F.col("transaction_date").alias("demand_date"), "store_id", "product_id", "quantity")

# Cross join dates, stores, and products to ensure zero-sales days are filled as 0 instead of dropped
daily = (dates.crossJoin(stores).crossJoin(prods)
         .join(actual, ["demand_date", "store_id", "product_id"], "left")
         .withColumn("units", F.coalesce(F.col("quantity"), F.lit(0)).cast("double"))
         .select("demand_date", "store_id", "city", "product_id", "units"))
pdf = daily.toPandas()

print("--- Step 2: Engineering Daily Features & Future Grid ---")
pdf["demand_date"] = pd.to_datetime(pdf["demand_date"])
pdf = pdf.sort_values(["store_id", "product_id", "demand_date"]).reset_index(drop=True)

# Rolling window feature lagged by horizon days so it's fully known for future prediction
pdf["roll_28"] = pdf.groupby(["store_id", "product_id"])["units"].transform(lambda s: s.shift(HORIZON).rolling(28).mean())
origin = pdf["demand_date"].max()

series = pdf[["store_id", "city", "product_id"]].drop_duplicates()
recent = (pdf[pdf["demand_date"] > origin - pd.Timedelta(days=28)]
          .groupby(["store_id", "product_id"])["units"].mean().rename("roll_28").reset_index())
future = pd.concat([series.assign(demand_date=origin + pd.Timedelta(days=h)) for h in range(1, HORIZON + 1)])
future = future.merge(recent, on=["store_id", "product_id"])

cats = {c: pdf[c].astype("category").cat.categories for c in ["store_id", "product_id", "city"]}
def prep(df):
    df = df.copy()
    df["month"] = df["demand_date"].dt.month
    df["dow"] = df["demand_date"].dt.dayofweek
    for c, k in cats.items():
        df[c] = pd.Categorical(df[c], categories=k)
    return df

pdf, future = prep(pdf).dropna(subset=["roll_28"]), prep(future)
FEATURES = ["store_id", "product_id", "city", "month", "dow", "roll_28"]

print("--- Step 3: Training LightGBM Poisson Model & Measuring WAPE ---")
params = dict(objective="poisson", n_estimators=300, learning_rate=0.05,
              num_leaves=63, random_state=42, verbose=-1)
cutoff = origin - pd.Timedelta(days=HORIZON)
train, test = pdf[pdf["demand_date"] <= cutoff], pdf[pdf["demand_date"] > cutoff].copy()

mlflow.set_experiment("03_Train_ML_Forecast_Model")
with mlflow.start_run(run_name=MODEL_VERSION):
    m = LGBMRegressor(**params).fit(train[FEATURES], train["units"])
    test["pred"] = np.clip(m.predict(test[FEATURES]), 0, None)
    wape_daily = (test["units"] - test["pred"]).abs().sum() / test["units"].sum()
    tot = test.groupby(["store_id", "product_id"], observed=True)[["units", "pred"]].sum()
    wape_14d = float((tot["units"] - tot["pred"]).abs().sum() / tot["units"].sum())
    
    mlflow.log_params(params)
    mlflow.log_metrics({"wape_daily": float(wape_daily), "wape_14d": wape_14d})
    
    # Retrain on full history for production inference
    final = LGBMRegressor(**params).fit(pdf[FEATURES], pdf["units"])

print(f"Model Training Complete | Daily WAPE: {wape_daily:.1%} | 14-day Store-SKU WAPE: {wape_14d:.1%}")

print("--- Step 4: Saving Backtest and Writing Forecasts to Gold Lakehouse ---")
# Save backtest for accuracy notebook evaluation
bt = test[["demand_date", "store_id", "city", "product_id", "units", "pred"]].copy()
bt[["store_id", "city", "product_id"]] = bt[["store_id", "city", "product_id"]].astype(str)
bt["demand_date"] = bt["demand_date"].dt.date
bt["model_version"] = MODEL_VERSION
(spark.createDataFrame(bt).write.format("delta").mode("overwrite")
      .option("overwriteSchema", "true").saveAsTable(f"{GOLD}.gold_forecast_backtest"))

# Generate 14-day future predictions
future["baseline_forecast"] = np.clip(final.predict(future[FEATURES]), 0, None).round(2)
out = future[["demand_date", "store_id", "product_id", "baseline_forecast"]].copy()
out["store_id"], out["product_id"] = out["store_id"].astype(str), out["product_id"].astype(str)
out["forecast_date"] = out["demand_date"].dt.date
out["adjusted_forecast"] = out["baseline_forecast"] 
out["lower_bound"] = (out["baseline_forecast"] * (1 - min(wape_14d, 1))).round(2)
out["upper_bound"] = (out["baseline_forecast"] * (1 + wape_14d)).round(2)
out["confidence"] = round(max(0.0, 1 - wape_14d), 2)
out["model_version"] = MODEL_VERSION
out["run_date"] = pd.Timestamp.today().date()
out = out.drop(columns="demand_date")

sdf = spark.createDataFrame(out)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.gold_forecast")
sdf.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{GOLD}.gold_forecast_history")

print("\n--- NOTEBOOK 3 COMPLETE ---")
print("Successfully generated predictions, backtests, and updated 'gold_forecast' in Demand_Gold_LH!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
