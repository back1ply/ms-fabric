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

def check_and_report(description, df, max_rows=10):
    """
    Run a quality check and report the results.
    Shows fewer rows for clarity but reports total count.
    """
    print(f"\n🔍 {description}")
    if df.count() > 0:
        df.show(max_rows, truncate=False)
        count = df.count()
        if count > max_rows:
            print(f"... and {count - max_rows} more rows")
        print(f"⚠️ {count} rows flagged")
    else:
        print("✅ Passed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n==== BRONZE LAYER CHECKS ====")

check_and_report(
    "Missing essential fields in bronze.crm_cust_info",
    spark.sql("""
        SELECT cst_id, cst_key, cst_firstname, cst_lastname, cst_create_date
        FROM bronze.crm_cust_info
        WHERE cst_id IS NULL 
           OR cst_key IS NULL 
           OR (cst_firstname IS NULL AND cst_lastname IS NULL)
    """)
)

# Check for invalid dates
check_and_report(
    "Invalid dates in bronze.crm_sales_details",
    spark.sql("""
        SELECT sls_ord_num, sls_order_dt, sls_ship_dt, sls_due_dt
        FROM bronze.crm_sales_details
        WHERE (sls_order_dt > current_date() OR sls_order_dt < '1900-01-01')
           OR (sls_ship_dt > current_date() OR sls_ship_dt < '1900-01-01')
           OR (sls_due_dt > current_date() OR sls_due_dt < '1900-01-01')
           OR sls_ship_dt < sls_order_dt
           OR sls_due_dt < sls_order_dt
    """)
)

# Check for negative or invalid numeric values
check_and_report(
    "Invalid numeric values in bronze.crm_sales_details",
    spark.sql("""
        SELECT sls_ord_num, sls_sales, sls_quantity, sls_price
        FROM bronze.crm_sales_details
        WHERE sls_sales < 0 
           OR sls_quantity <= 0 
           OR sls_price <= 0
           OR sls_sales != sls_quantity * sls_price
    """)
)

# Check for suspicious product data
check_and_report(
    "Invalid or expired products in bronze.crm_prd_info",
    spark.sql("""
        SELECT prd_id, prd_key, prd_nm, prd_cost, prd_start_dt, prd_end_dt
        FROM bronze.crm_prd_info
        WHERE prd_cost < 0
           OR (prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt)
           OR prd_start_dt > current_date()
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n==== SILVER LAYER CHECKS ====")

# Check for suspicious customer IDs and incomplete records
check_and_report(
    "Suspicious or incomplete customer records in silver.crm_cust_info",
    spark.sql("""
        SELECT cst_id, cst_key, cst_firstname, cst_lastname, cst_gndr, cst_marital_status
        FROM silver.crm_cust_info
        WHERE cst_id = 0 
           OR cst_id < 0
           OR cst_firstname IS NULL
           OR cst_lastname IS NULL
           OR (cst_firstname IS NULL AND cst_lastname IS NULL)
    """)
)

# Check for duplicates in primary keys
check_and_report(
    "Duplicate primary keys in silver.crm_cust_info",
    spark.sql("""
        SELECT cst_id, COUNT(*) as count
        FROM silver.crm_cust_info
        GROUP BY cst_id
        HAVING COUNT(*) > 1
    """)
)

# Check data consistency in silver layer
check_and_report(
    "Data consistency check for silver.crm_sales_details",
    spark.sql("""
        SELECT sls_ord_num, sls_sales, sls_quantity, sls_price,
               ABS(sls_sales - (sls_quantity * sls_price)) as discrepancy
        FROM silver.crm_sales_details
        WHERE ABS(sls_sales - (sls_quantity * sls_price)) > 0.01
    """)
)

# Check for missing relationship keys
check_and_report(
    "Missing foreign keys in silver.crm_sales_details",
    spark.sql("""
        SELECT s.sls_ord_num, s.sls_cust_id, s.sls_prd_key
        FROM silver.crm_sales_details s
        LEFT JOIN silver.crm_cust_info c ON s.sls_cust_id = c.cst_id
        LEFT JOIN silver.crm_prd_info p ON s.sls_prd_key = p.prd_key
        WHERE c.cst_id IS NULL OR p.prd_key IS NULL
    """)
)

# Check for data consistency in product data
check_and_report(
    "Product data consistency check in silver.crm_prd_info",
    spark.sql("""
        SELECT p.prd_id, p.prd_key, p.cat_id, c.id as cat_id_match
        FROM silver.crm_prd_info p
        LEFT JOIN silver.erp_px_cat_g1v2 c ON p.cat_id = c.id
        WHERE c.id IS NULL
    """)
)

# Check normalized values for consistency
check_and_report(
    "Check normalized values in silver.crm_cust_info",
    spark.sql("""
        SELECT DISTINCT cst_gndr, cst_marital_status
        FROM silver.crm_cust_info
        WHERE cst_gndr NOT IN ('Male', 'Female', 'n/a')
           OR cst_marital_status NOT IN ('Single', 'Married', 'n/a')
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n==== GOLD LAYER CHECKS ====")

# Check for suspicious or incomplete customer dimension records
check_and_report(
    "Suspicious or incomplete customer records in gold.dim_customer",
    spark.sql("""
        SELECT customer_id, customer_key, customer_number,
               first_name, last_name, gender, marital_status
        FROM gold.dim_customer
        WHERE customer_id = 0 
           OR customer_id < 0
           OR first_name IS NULL
           OR last_name IS NULL
           OR (first_name IS NULL AND last_name IS NULL)
    """)
)

# Check for duplicate surrogate keys in dimensions
check_and_report(
    "Duplicate surrogate keys in gold.dim_customer",
    spark.sql("""
        SELECT customer_key, COUNT(*) as count
        FROM gold.dim_customer
        GROUP BY customer_key
        HAVING COUNT(*) > 1
    """)
)

check_and_report(
    "Duplicate surrogate keys in gold.dim_product",
    spark.sql("""
        SELECT product_key, COUNT(*) as count
        FROM gold.dim_product
        GROUP BY product_key
        HAVING COUNT(*) > 1
    """)
)

# Check for orphaned fact records
check_and_report(
    "Orphaned fact records in gold.fct_sales",
    spark.sql("""
        SELECT f.order_number, f.customer_key, f.product_key
        FROM gold.fct_sales f
        LEFT JOIN gold.dim_customer c ON f.customer_key = c.customer_key
        LEFT JOIN gold.dim_product p ON f.product_key = p.product_key
        WHERE c.customer_key IS NULL OR p.product_key IS NULL
    """)
)

# Check for negative or invalid measures in fact table
check_and_report(
    "Invalid measures in gold.fct_sales",
    spark.sql("""
        SELECT order_number, quantity, price, sales_amount
        FROM gold.fct_sales
        WHERE quantity <= 0 
           OR price <= 0 
           OR sales_amount <= 0
           OR ABS(sales_amount - (quantity * price)) > 0.01
    """)
)

# Check for date consistency in fact table
check_and_report(
    "Invalid date relationships in gold.fct_sales",
    spark.sql("""
        SELECT order_number, order_date, shipping_date, due_date
        FROM gold.fct_sales
        WHERE shipping_date < order_date
           OR due_date < order_date
           OR (shipping_date IS NULL AND order_date IS NOT NULL)
    """)
)

# Check for data distribution anomalies
check_and_report(
    "Products with no sales",
    spark.sql("""
        SELECT p.product_key, p.product_name
        FROM gold.dim_product p
        LEFT JOIN gold.fct_sales s ON p.product_key = s.product_key
        WHERE s.product_key IS NULL
    """)
)

check_and_report(
    "Customers with no sales",
    spark.sql("""
        SELECT c.customer_key, c.first_name, c.last_name
        FROM gold.dim_customer c
        LEFT JOIN gold.fct_sales s ON c.customer_key = s.customer_key
        WHERE s.customer_key IS NULL
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("✅ All Quality Checks Completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
