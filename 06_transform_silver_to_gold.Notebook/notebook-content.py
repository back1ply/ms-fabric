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

def transform_dim_customers():
    df = spark.table("silver.crm_cust_info")

    result = df.select(
        col("cst_id").alias("customer_id"),
        col("cst_key").alias("customer_key"),
        col("cst_firstname").alias("first_name"),
        col("cst_lastname").alias("last_name"),
        col("cst_marital_status").alias("marital_status"),
        col("cst_gndr").alias("gender"),
        col("cst_create_date").alias("registration_date"),
        current_date().alias("dwh_create_date")
    )

    result.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("gold.dim_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_dim_products():
    df = spark.table("silver.crm_prd_info")

    result = df.select(
        col("prd_id").alias("product_id"),
        col("prd_key").alias("product_key"),
        col("cat_id").alias("category_id"),
        col("prd_nm").alias("product_name"),
        col("prd_cost").alias("product_cost"),
        col("prd_line").alias("product_line"),
        col("prd_start_dt").alias("valid_from"),
        col("prd_end_dt").alias("valid_to"),
        current_date().alias("dwh_create_date")
    )

    result.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("gold.dim_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_fact_sales():
    df = spark.table("silver.crm_sales_details")

    result = df.select(
        col("sls_ord_num").alias("order_number"),
        col("sls_cust_id").alias("customer_id"),
        col("sls_prd_key").alias("product_key"),
        col("sls_order_dt").alias("order_date"),
        col("sls_ship_dt").alias("ship_date"),
        col("sls_due_dt").alias("due_date"),
        col("sls_sales").alias("total_sales"),
        col("sls_quantity").alias("quantity"),
        col("sls_price").alias("unit_price"),
        current_date().alias("dwh_create_date")
    )

    result.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("gold.fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transform_dim_customers()
transform_dim_products()
transform_fact_sales()

print("✔ All Silver ➝ Gold transformations completed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
