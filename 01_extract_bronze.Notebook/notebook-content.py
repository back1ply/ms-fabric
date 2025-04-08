# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bc5c5ff7-dbfe-46f0-af4c-e5b298762e86",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "8d3001ae-2f0d-4d23-a9ee-f2698798f695",
# META       "known_lakehouses": [
# META         {
# META           "id": "bc5c5ff7-dbfe-46f0-af4c-e5b298762e86"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import requests
from io import StringIO

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enable automatic schema merge globally (optional but useful)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# Disable Arrow (avoids fallback warnings in Fabric)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_table_if_exists(table_name):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"🗑️ Dropped existing table: {table_name}")
    except Exception as e:
        print(f"⚠️ Could not drop table {table_name}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_dataframe(df):
    null_likes = {"", "n/a", "N/A", "NaN", "nan", "None", "null", "NULL"}

    for col in df.columns:
        df[col] = df[col].map(lambda x: None if pd.isna(x) or str(x).strip() in null_likes else str(x).strip())

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_file(system, file_name):
    base_url = "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets"
    url = f"{base_url}/source_{system.lower()}/{file_name}.csv"
    table_name = f"bronze.{system.lower()}_{file_name.lower()}"
    print(f"\n➡️ Loading {table_name}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        pdf = pd.read_csv(StringIO(response.text))
        pdf.columns = [c.lower() for c in pdf.columns]

        pdf = clean_dataframe(pdf)

        if file_name.lower() == "cust_info":
            pdf["cst_id"] = pd.to_numeric(pdf["cst_id"], errors="coerce").fillna(0).astype(int)
            pdf["cst_create_date"] = pd.to_datetime(pdf["cst_create_date"], errors="coerce")

        elif file_name.lower() == "prd_info":
            pdf["prd_id"] = pd.to_numeric(pdf["prd_id"], errors="coerce").fillna(0).astype(int)
            pdf["prd_cost"] = pd.to_numeric(pdf["prd_cost"], errors="coerce")
            pdf["prd_start_dt"] = pd.to_datetime(pdf["prd_start_dt"], errors="coerce")
            pdf["prd_end_dt"] = pd.to_datetime(pdf["prd_end_dt"], errors="coerce")

        elif file_name.lower() == "sales_details":
            pdf["sls_sales"] = pd.to_numeric(pdf["sls_sales"], errors="coerce")
            pdf["sls_price"] = pd.to_numeric(pdf["sls_price"], errors="coerce")
            pdf["sls_quantity"] = pd.to_numeric(pdf["sls_quantity"], errors="coerce").fillna(0).astype(int)
            for col_ in ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]:
                pdf[col_] = pd.to_datetime(pdf[col_].astype(str), format="%Y%m%d", errors="coerce")

        elif file_name.lower() == "cust_az12":
            pdf["bdate"] = pd.to_datetime(pdf["bdate"], errors="coerce")

        sdf = spark.createDataFrame(pdf)

        # Spark casting
        if file_name.lower() == "cust_info":
            sdf = sdf.withColumn("cst_create_date", col("cst_create_date").cast("date"))
            sdf = sdf.withColumn("cst_id", col("cst_id").cast("int"))

        elif file_name.lower() == "prd_info":
            sdf = sdf.withColumn("prd_start_dt", col("prd_start_dt").cast("date"))
            sdf = sdf.withColumn("prd_end_dt", col("prd_end_dt").cast("date"))
            sdf = sdf.withColumn("prd_cost", col("prd_cost").cast("float"))
            sdf = sdf.withColumn("prd_id", col("prd_id").cast("int"))

        elif file_name.lower() == "sales_details":
            sdf = sdf.withColumn("sls_order_dt", col("sls_order_dt").cast("date"))
            sdf = sdf.withColumn("sls_ship_dt", col("sls_ship_dt").cast("date"))
            sdf = sdf.withColumn("sls_due_dt", col("sls_due_dt").cast("date"))
            sdf = sdf.withColumn("sls_quantity", col("sls_quantity").cast("int"))
            sdf = sdf.withColumn("sls_sales", col("sls_sales").cast("float"))
            sdf = sdf.withColumn("sls_price", col("sls_price").cast("float"))

        elif file_name.lower() == "cust_az12":
            sdf = sdf.withColumn("bdate", col("bdate").cast("date"))

        drop_table_if_exists(table_name)
        sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

        print(f"✅ Loaded {sdf.count()} rows into {table_name}")
        return sdf.count()

    except Exception as e:
        print(f"❌ Failed to load {table_name}: {e}")
        return 0


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

FILES = {
    "crm": ["cust_info", "prd_info", "sales_details"],
    "erp": ["CUST_AZ12", "LOC_A101", "PX_CAT_G1V2"]
}

for system, file_list in FILES.items():
    for file_name in file_list:
        process_file(system, file_name)

print("\n🏁 Bronze data load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
