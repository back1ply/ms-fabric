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
from pyspark.sql.functions import col, trim, upper, countDistinct, expr

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

def check_and_report(description, df):
    print(f"\n🔍 {description}")
    df.show(truncate=False)
    print(f"⚠️ {df.count()} rows flagged" if df.count() > 0 else "✅ Passed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SILVER LAYER CHECKS

# Null or duplicate primary keys (cust, product, sales)
check_and_report(
    "Duplicate or NULL cst_id in silver.crm_cust_info",
    spark.sql("""
        SELECT cst_id, COUNT(*) as count
        FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1 OR cst_id IS NULL
    """)
)

check_and_report(
    "Duplicate or NULL prd_id in silver.crm_prd_info",
    spark.sql("""
        SELECT prd_id, COUNT(*) as count
        FROM silver.crm_prd_info
        GROUP BY prd_id
        HAVING COUNT(*) > 1 OR prd_id IS NULL
    """)
)

# Trim check
check_and_report(
    "Unwanted spaces in prd_nm",
    spark.table("silver.crm_prd_info").filter(col("prd_nm") != trim(col("prd_nm")))
)

# Invalid product cost
check_and_report(
    "Negative or NULL product cost",
    spark.table("silver.crm_prd_info").filter((col("prd_cost") < 0) | col("prd_cost").isNull())
)

# Invalid date logic
check_and_report(
    "Product start date after end date",
    spark.table("silver.crm_prd_info").filter(col("prd_end_dt") < col("prd_start_dt"))
)

# Invalid sales logic
check_and_report(
    "Sales amount inconsistent with price × quantity",
    spark.sql("""
        SELECT sls_sales, sls_quantity, sls_price
        FROM silver.crm_sales_details
        WHERE sls_sales != sls_quantity * sls_price
           OR sls_sales IS NULL
           OR sls_quantity IS NULL
           OR sls_price IS NULL
           OR sls_sales <= 0
           OR sls_quantity <= 0
           OR sls_price <= 0
    """)
)

# Future birthdates
check_and_report(
    "Future or invalid birthdates in silver.erp_cust_az12",
    spark.table("silver.erp_cust_az12").filter(col("bdate") > expr("current_date()"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GOLD LAYER CHECKS

# Duplicates in surrogate keys
check_and_report(
    "Duplicate customer_key in gold.dim_customer",
    spark.sql("""
        SELECT customer_key, COUNT(*) as count
        FROM gold.dim_customer
        GROUP BY customer_key
        HAVING COUNT(*) > 1
    """)
)

check_and_report(
    "Duplicate product_key in gold.dim_product",
    spark.sql("""
        SELECT product_key_sk, COUNT(*) as count
        FROM gold.dim_product
        GROUP BY product_key_sk
        HAVING COUNT(*) > 1
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Referential integrity from fact to dims
check_and_report(
    "Missing dimension matches in gold.fct_sales",
    spark.sql("""
        SELECT f.*
        FROM gold.fct_sales f
        LEFT JOIN gold.dim_customer c ON f.customer_key = c.customer_key
        LEFT JOIN gold.dim_product p ON f.product_key = p.product_key_sk
        WHERE c.customer_key IS NULL OR p.product_key_sk IS NULL
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
