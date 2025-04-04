# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

# Create dimension_customer table
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dimension_product table
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create fact_sales table
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
    current_date() AS dwh_create_date
FROM silver.crm_sales_details s
"""

spark.sql(sales_query).write.format("delta").mode("overwrite").saveAsTable("gold.fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Gold transformations completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
