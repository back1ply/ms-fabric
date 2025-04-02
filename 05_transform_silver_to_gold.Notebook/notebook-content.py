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

# Transform dimension tables using SQL for simplicity
print("Creating gold.dim_customers")
spark.sql("""
SELECT
    cst_id AS customer_id,
    cst_key AS customer_key,
    cst_firstname AS first_name,
    cst_lastname AS last_name,
    cst_marital_status AS marital_status,
    cst_gndr AS gender,
    cst_create_date AS registration_date,
    current_date() AS dwh_create_date
FROM silver.crm_cust_info
""").write.format("delta").mode("overwrite").saveAsTable("gold.dim_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Creating gold.dim_products")
spark.sql("""
SELECT
    prd_id AS product_id,
    prd_key AS product_key,
    cat_id AS category_id,
    prd_nm AS product_name,
    prd_cost AS product_cost,
    prd_line AS product_line,
    prd_start_dt AS valid_from,
    prd_end_dt AS valid_to,
    current_date() AS dwh_create_date
FROM silver.crm_prd_info
""").write.format("delta").mode("overwrite").saveAsTable("gold.dim_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Creating gold.fact_sales")
spark.sql("""
SELECT
    sls_ord_num AS order_number,
    sls_cust_id AS customer_id,
    sls_prd_key AS product_key,
    sls_order_dt AS order_date,
    sls_ship_dt AS ship_date,
    sls_due_dt AS due_date,
    sls_sales AS total_sales,
    sls_quantity AS quantity,
    sls_price AS unit_price,
    current_date() AS dwh_create_date
FROM silver.crm_sales_details
""").write.format("delta").mode("overwrite").saveAsTable("gold.fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("✔ All Silver ➝ Gold transformations completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
