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

# Utility functions for quality checks
def check_nulls(df, column):
    return df.filter(f"{column} IS NULL")

def check_duplicates(df, column):
    return df.groupBy(column).count().filter("count > 1")

def check_trim_issues(df, column):
    return df.filter(f"{column} != TRIM({column})")

def distinct_values(df, column):
    return df.select(column).distinct().orderBy(column)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define bronze tables to check
tables = [
    ("bronze.crm_cust_info", ["cst_id", "cst_key", "cst_firstname", "cst_lastname", "cst_marital_status", "cst_gndr"]),
    ("bronze.crm_prd_info", ["prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt"]),
    ("bronze.crm_sales_details", ["sls_ord_num", "sls_prd_key", "sls_cust_id", "sls_order_dt", "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price"]),
    ("bronze.erp_cust_az12", ["cid", "bdate", "gen"]),
    ("bronze.erp_loc_a101", ["cid", "cntry"]),
    ("bronze.erp_px_cat_g1v2", ["id", "cat", "subcat", "maintenance"])
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run checks
for table_name, columns in tables:
    print(f"\n=== Quality Checks for {table_name} ===")
    df = spark.table(table_name)

    for col in columns:
        print(f"\n-- Nulls in {col} --")
        check_nulls(df, col).show()

        print(f"\n-- Trim Issues in {col} --")
        check_trim_issues(df, col).show()

        if col.endswith("_id") or col in ["cst_key", "prd_key", "sls_ord_num"]:
            print(f"\n-- Duplicates in {col} --")
            check_duplicates(df, col).show()

        if col in ["cst_marital_status", "cst_gndr", "prd_line", "gen", "cntry", "cat", "subcat", "maintenance"]:
            print(f"\n-- Distinct values in {col} --")
            distinct_values(df, col).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
