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
from pyspark.sql.functions import col, count, when, isnan, isnull, trim, length
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define bronze tables to check
tables = [
    "bronze.crm_cust_info",
    "bronze.crm_prd_info",
    "bronze.crm_sales_details",
    "bronze.erp_cust_az12",
    "bronze.erp_loc_a101",
    "bronze.erp_px_cat_g1v2"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run simple checks for each table
for table_name in tables:
    print(f"\n{'=' * 80}")
    print(f"DATA QUALITY REPORT: {table_name}")
    print(f"{'=' * 80}")
    
    # Load table
    df = spark.table(table_name)
    row_count = df.count()
    print(f"Total rows: {row_count}")
    
    # Check for nulls in each column
    print(f"\n{'-' * 40}")
    print("NULL VALUE CHECKS")
    print(f"{'-' * 40}")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = round((null_count / row_count) * 100, 2) if row_count > 0 else 0
        status = "PASS" if null_count == 0 else "FAIL"
        print(f"Column '{column}': {null_count} nulls ({null_percentage}%) - {status}")
    
    # Check for whitespace issues in string columns
    print(f"\n{'-' * 40}")
    print("WHITESPACE CHECKS (String columns only)")
    print(f"{'-' * 40}")
    for column in df.columns:
        if str(df.schema[column].dataType).startswith("StringType"):
            trim_count = df.filter(col(column).isNotNull() & (trim(col(column)) != col(column))).count()
            status = "PASS" if trim_count == 0 else "FAIL"
            print(f"Column '{column}': {trim_count} whitespace issues - {status}")
    
    # Check for duplicates in ID columns
    print(f"\n{'-' * 40}")
    print("DUPLICATE KEY CHECKS")
    print(f"{'-' * 40}")
    id_columns = [c for c in df.columns if c.endswith('_id') or c in ['prd_key', 'sls_ord_num', 'cid', 'id']]
    for id_column in id_columns:
        if id_column in df.columns:
            dup_count = df.filter(col(id_column).isNotNull()).groupBy(id_column).count().filter("count > 1").count()
            status = "PASS" if dup_count == 0 else "FAIL"
            print(f"Column '{id_column}': {dup_count} duplicates - {status}")
    
    # Check distinct values for categorical columns
    print(f"\n{'-' * 40}")
    print("DISTINCT VALUES (Selected columns)")
    print(f"{'-' * 40}")
    categorical_columns = [c for c in df.columns if c in ["cst_marital_status", "cst_gndr", "prd_line", "gen", "cntry", "cat", "subcat", "maintenance"]]
    for cat_column in categorical_columns:
        if cat_column in df.columns:
            distinct_df = df.select(cat_column).distinct()
            distinct_count = distinct_df.count()
            distinct_values = [row[0] for row in distinct_df.limit(5).collect()]
            print(f"Column '{cat_column}': {distinct_count} distinct values")
            print(f"  Sample values: {', '.join(str(v) for v in distinct_values)}")
    
    # Show sample data
    print(f"\n{'-' * 40}")
    print("SAMPLE DATA")
    print(f"{'-' * 40}")
    df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
