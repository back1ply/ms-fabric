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

import pandas as pd
import requests
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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

def process_file(system, file_name):
    base_url = "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets"
    url = f"{base_url}/source_{system.lower()}/{file_name}.csv"
    table_name = f"bronze.{system.lower()}_{file_name.lower()}"

    print(f"\n➡️ Loading {table_name}")

    try:
        # Download from GitHub
        response = requests.get(url)
        response.raise_for_status()

        # Load into Pandas and clean column names
        pdf = pd.read_csv(StringIO(response.text))
        pdf.columns = [c.lower() for c in pdf.columns]

        # Convert to Spark DataFrame with inferred schema
        sdf = spark.createDataFrame(pdf)

        # Drop and overwrite
        drop_table_if_exists(table_name)
        sdf.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)

        count = sdf.count()
        print(f"✅ Loaded {count} rows into {table_name}")
        return count

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
