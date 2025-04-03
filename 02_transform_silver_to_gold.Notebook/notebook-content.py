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

# Create dimension and fact tables using SQL for better readability
print("Creating gold layer tables...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dimension_customer table
print("\n=== Creating gold.dim_customers ===")

# First, join customer data from CRM and ERP
customer_query = """
SELECT 
    c.cst_id AS customer_id,
    c.cst_firstname AS first_name,
    c.cst_lastname AS last_name,
    c.cst_marital_status AS marital_status,
    c.cst_gndr AS gender,
    c.cst_create_date AS registration_date,
    e.bdate AS birth_date,
    l.cntry AS country,
    current_date() AS dwh_create_date
FROM silver.crm_cust_info c
LEFT JOIN silver.erp_cust_az12 e ON c.cst_id = e.cid
LEFT JOIN silver.erp_loc_a101 l ON c.cst_id = l.cid
"""

spark.sql(customer_query).write.format("delta").mode("overwrite").saveAsTable("gold.dim_customers")

# Show sample data
print("Sample data:")
spark.table("gold.dim_customers").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dimension_product table
print("\n=== Creating gold.dim_products ===")

# Join product data with category information
product_query = """
SELECT
    p.prd_id AS product_id,
    p.prd_key AS product_key,
    p.cat_id AS category_id,
    p.prd_line AS product_line,
    p.prd_cost AS product_cost,
    c.cat AS category,
    c.subcat AS subcategory,
    c.maintenance AS maintenance_category,
    p.prd_start_dt AS valid_from,
    p.prd_end_dt AS valid_to,
    current_date() AS dwh_create_date
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 c ON p.cat_id = c.id
"""

spark.sql(product_query).write.format("delta").mode("overwrite").saveAsTable("gold.dim_products")

# Show sample data
print("Sample data:")
spark.table("gold.dim_products").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create fact_sales table
print("\n=== Creating gold.fact_sales ===")

# Create fact table with foreign keys to dimensions
sales_query = """
SELECT
    s.sls_ord_num AS order_number,
    s.sls_cust_id AS customer_id,
    s.sls_prd_key AS product_key,
    s.sls_order_dt AS order_date,
    s.sls_ship_dt AS ship_date,
    s.sls_due_dt AS due_date,
    s.sls_sales AS total_sales,
    s.sls_quantity AS quantity,
    s.sls_price AS unit_price,
    DATEDIFF(s.sls_ship_dt, s.sls_order_dt) AS days_to_ship,
    CASE 
        WHEN s.sls_ship_dt > s.sls_due_dt THEN 'Late'
        WHEN s.sls_ship_dt = s.sls_due_dt THEN 'On Time'
        WHEN s.sls_ship_dt < s.sls_due_dt THEN 'Early'
        ELSE 'Unknown'
    END AS delivery_status,
    current_date() AS dwh_create_date
FROM silver.crm_sales_details s
"""

spark.sql(sales_query).write.format("delta").mode("overwrite").saveAsTable("gold.fact_sales")

# Show sample data
print("Sample data:")
spark.table("gold.fact_sales").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a simple sales summary view for quick analysis
print("\n=== Creating gold.vw_sales_summary ===")

summary_query = """
CREATE OR REPLACE VIEW gold.vw_sales_summary AS
SELECT 
    p.category,
    p.subcategory,
    c.country,
    YEAR(f.order_date) AS order_year,
    MONTH(f.order_date) AS order_month,
    COUNT(DISTINCT f.order_number) AS order_count,
    SUM(f.total_sales) AS total_sales,
    SUM(f.quantity) AS total_quantity,
    AVG(f.unit_price) AS avg_unit_price,
    AVG(f.days_to_ship) AS avg_days_to_ship
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_id = c.customer_id
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY 
    p.category,
    p.subcategory,
    c.country,
    YEAR(f.order_date),
    MONTH(f.order_date)
"""

spark.sql(summary_query)
print("✅ Created sales summary view")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n✅ All Silver ➝ Gold transformations completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
