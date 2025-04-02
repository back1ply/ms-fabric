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

# Helper: Generate transformation suggestion

def suggest_transformations(df, table_name, column):
    suggestions = []

    if df.filter(f"{column} IS NULL").count() > 0:
        suggestions.append(f"- Handle NULLs in `{column}` (e.g., fill, drop, or default value)")

    if df.groupBy(column).count().filter("count > 1").count() > 0:
        suggestions.append(f"- Deduplicate `{column}` records (e.g., keep latest by timestamp)")

    if df.filter(f"{column} != TRIM({column})").count() > 0:
        suggestions.append(f"- Apply `TRIM({column})` to remove unwanted spaces")

    if column in ["cst_marital_status", "cst_gndr", "prd_line", "gen", "cntry"]:
        distinct_vals = [row[0] for row in df.select(column).distinct().collect()]
        if len(distinct_vals) < 10:
            suggestions.append(f"- Normalize `{column}` values: {distinct_vals}")

    if suggestions:
        print(f"\n--- Suggestions for {table_name}.{column} ---")
        for s in suggestions:
            print(s)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List of bronze tables and columns to inspect
targets = [
    ("bronze.crm_cust_info", ["cst_id", "cst_firstname", "cst_lastname", "cst_marital_status", "cst_gndr", "cst_create_date"]),
    ("bronze.crm_prd_info", ["prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line"]),
    ("bronze.crm_sales_details", ["sls_ord_num", "sls_prd_key", "sls_cust_id", "sls_sales", "sls_quantity", "sls_price"]),
    ("bronze.erp_cust_az12", ["cid", "gen"]),
    ("bronze.erp_loc_a101", ["cid", "cntry"]),
    ("bronze.erp_px_cat_g1v2", ["cat", "subcat", "maintenance"])
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run transformation suggestions
for table, cols in targets:
    df = spark.table(table)
    for col in cols:
        suggest_transformations(df, table, col)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
