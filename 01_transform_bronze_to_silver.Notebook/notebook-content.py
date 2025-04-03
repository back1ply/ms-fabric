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
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# Define bronze tables to process
bronze_tables = [
    {"name": "bronze.crm_cust_info", "key": "cst_id"},
    {"name": "bronze.crm_prd_info", "key": "prd_id"},
    {"name": "bronze.crm_sales_details", "key": "sls_ord_num"},
    {"name": "bronze.erp_cust_az12", "key": "cid"},
    {"name": "bronze.erp_loc_a101", "key": "cid"},
    {"name": "bronze.erp_px_cat_g1v2", "key": "id"}
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to perform essential quality checks on a table
def check_table_quality(table_info):
    table_name = table_info["name"]
    key_column = table_info["key"]
    
    print(f"\n=== Quality Check: {table_name} ===")
    
    # Load table
    df = spark.table(table_name)
    row_count = df.count()
    print(f"Total rows: {row_count}")
    
    # Check for nulls in key column
    null_count = df.filter(col(key_column).isNull()).count()
    if null_count > 0:
        print(f"⚠️ Warning: {null_count} null values in primary key column '{key_column}'")
    else:
        print(f"✅ No nulls in primary key column '{key_column}'")
    
    # Check for duplicates in key column
    dup_count = df.groupBy(key_column).count().filter("count > 1").count()
    if dup_count > 0:
        print(f"⚠️ Warning: {dup_count} duplicate values in primary key column '{key_column}'")
    else:
        print(f"✅ No duplicates in primary key column '{key_column}'")
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform CRM customer information
print("\n=== Transforming CRM Customer Information ===")
df = check_table_quality({"name": "bronze.crm_cust_info", "key": "cst_id"})

# Apply transformations
silver_crm_cust = df.where("cst_id IS NOT NULL") \
    .withColumn("row_num", row_number().over(Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc()))) \
    .filter("row_num = 1") \
    .drop("row_num") \
    .withColumn("cst_firstname", trim(col("cst_firstname"))) \
    .withColumn("cst_lastname", trim(col("cst_lastname"))) \
    .withColumn("cst_marital_status", 
               when(upper(trim(col("cst_marital_status"))) == "S", "Single")
               .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
               .otherwise("n/a")) \
    .withColumn("cst_gndr", 
               when(upper(trim(col("cst_gndr"))) == "F", "Female")
               .when(upper(trim(col("cst_gndr"))) == "M", "Male")
               .otherwise("n/a")) \
    .withColumn("dwh_create_date", current_date())

# Write to silver table
silver_crm_cust.write.format("delta").mode("overwrite").saveAsTable("silver.crm_cust_info")
print("✅ Transformed silver.crm_cust_info")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform CRM product information
print("\n=== Transforming CRM Product Information ===")
df = check_table_quality({"name": "bronze.crm_prd_info", "key": "prd_id"})

# Apply transformations
silver_crm_prd = df \
    .withColumn("cat_id", regexp_replace(substring(col("prd_key"), 1, 5), "-", "_")) \
    .withColumn("prd_key", expr("substring(prd_key, 7)")) \
    .withColumn("prd_line", 
               when(upper(trim(col("prd_line"))) == "M", "Mountain")
               .when(upper(trim(col("prd_line"))) == "R", "Road")
               .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
               .when(upper(trim(col("prd_line"))) == "T", "Touring")
               .otherwise("n/a")) \
    .withColumn("prd_cost", coalesce(col("prd_cost"), lit(0))) \
    .withColumn("prd_start_dt", to_date(col("prd_start_dt"))) \
    .withColumn("prd_end_dt", to_date(col("prd_end_dt"))) \
    .withColumn("dwh_create_date", current_date())

# Write to silver table
silver_crm_prd.write.format("delta").mode("overwrite").saveAsTable("silver.crm_prd_info")
print("✅ Transformed silver.crm_prd_info")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform CRM sales details
print("\n=== Transforming CRM Sales Details ===")
df = check_table_quality({"name": "bronze.crm_sales_details", "key": "sls_ord_num"})

# Apply transformations
silver_crm_sales = df \
    .withColumn("sls_order_dt", 
               when((col("sls_order_dt") == 0) | (length(col("sls_order_dt")) != 8), None)
               .otherwise(to_date(col("sls_order_dt").cast("string"), "yyyyMMdd"))) \
    .withColumn("sls_ship_dt", 
               when((col("sls_ship_dt") == 0) | (length(col("sls_ship_dt")) != 8), None)
               .otherwise(to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd"))) \
    .withColumn("sls_due_dt", 
               when((col("sls_due_dt") == 0) | (length(col("sls_due_dt")) != 8), None)
               .otherwise(to_date(col("sls_due_dt").cast("string"), "yyyyMMdd"))) \
    .withColumn("sls_sales_calc", col("sls_quantity") * abs(col("sls_price"))) \
    .withColumn("sls_sales", 
               when((col("sls_sales").isNull()) | (col("sls_sales") <= 0) | (col("sls_sales") != col("sls_sales_calc")),
                    col("sls_sales_calc"))
               .otherwise(col("sls_sales"))) \
    .withColumn("sls_price", 
               when((col("sls_price").isNull()) | (col("sls_price") <= 0),
                    when(col("sls_quantity") != 0, col("sls_sales") / col("sls_quantity")))
               .otherwise(col("sls_price"))) \
    .drop("sls_sales_calc") \
    .withColumn("dwh_create_date", current_date())

# Write to silver table
silver_crm_sales.write.format("delta").mode("overwrite").saveAsTable("silver.crm_sales_details")
print("✅ Transformed silver.crm_sales_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform ERP customer information
print("\n=== Transforming ERP Customer Information ===")
df = check_table_quality({"name": "bronze.erp_cust_az12", "key": "cid"})

# Apply transformations
silver_erp_cust = df \
    .withColumn("cid", 
               when(col("cid").startswith("NAS"), substring(col("cid"), 4, 100))
               .otherwise(col("cid"))) \
    .withColumn("bdate", 
               when(col("bdate") > current_date(), None)
               .otherwise(col("bdate"))) \
    .withColumn("gen", 
               when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
               .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
               .otherwise("n/a")) \
    .withColumn("dwh_create_date", current_date())

# Write to silver table
silver_erp_cust.write.format("delta").mode("overwrite").saveAsTable("silver.erp_cust_az12")
print("✅ Transformed silver.erp_cust_az12")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform ERP location information
print("\n=== Transforming ERP Location Information ===")
df = check_table_quality({"name": "bronze.erp_loc_a101", "key": "cid"})

# Apply transformations
silver_erp_loc = df \
    .withColumn("cid", regexp_replace(col("cid"), "-", "")) \
    .withColumn("cntry", 
               when(trim(upper(col("cntry"))) == "DE", "Germany")
               .when(trim(upper(col("cntry"))).isin("US", "USA"), "United States")
               .when((col("cntry").isNull()) | (trim(col("cntry")) == ""), "n/a")
               .otherwise(trim(col("cntry")))) \
    .withColumn("dwh_create_date", current_date())

# Write to silver table
silver_erp_loc.write.format("delta").mode("overwrite").saveAsTable("silver.erp_loc_a101")
print("✅ Transformed silver.erp_loc_a101")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform ERP product category information
print("\n=== Transforming ERP Product Category Information ===")
df = check_table_quality({"name": "bronze.erp_px_cat_g1v2", "key": "id"})

# Apply transformations
silver_erp_px = df \
    .withColumn("cat", trim(col("cat"))) \
    .withColumn("subcat", trim(col("subcat"))) \
    .withColumn("maintenance", trim(col("maintenance"))) \
    .withColumn("dwh_create_date", current_date())

# Write to silver table
silver_erp_px.write.format("delta").mode("overwrite").saveAsTable("silver.erp_px_cat_g1v2")
print("✅ Transformed silver.erp_px_cat_g1v2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n✅ All Bronze ➝ Silver transformations completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
