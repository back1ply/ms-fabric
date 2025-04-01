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
from pyspark.sql.window import Window

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

def validate_table(table_name, id_column):
    df = spark.table(table_name)
    print(f"\n🧪 Validating {table_name}...")
    print("Nulls:")
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"- {col_name}: {null_count} nulls")
    print("Duplicates:")
    dup_count = df.groupBy(id_column).count().filter("count > 1").count()
    if dup_count > 0:
        print(f"- {id_column}: {dup_count} duplicate IDs")
    else:
        print("- No duplicates found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validate_table("silver.crm_cust_info", "cst_id")
validate_table("silver.crm_prd_info", "prd_id")
validate_table("silver.crm_sales_details", "sls_ord_num")
validate_table("silver.erp_cust_az12", "cid")
validate_table("silver.erp_loc_a101", "cid")
validate_table("silver.erp_px_cat_g1v2", "id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
