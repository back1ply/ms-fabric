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
# META       "default_lakehouse_workspace_id": "8d3001ae-2f0d-4d23-a9ee-f2698798f695"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, substring, regexp_replace, current_date, expr
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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

# Transform CRM customer information
df = spark.table("bronze.crm_cust_info")
silver_crm_cust = df.where("cst_id IS NOT NULL") \
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

silver_crm_cust.write.format("delta").mode("overwrite").saveAsTable("silver.crm_cust_info")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform CRM product information
df = spark.table("bronze.crm_prd_info")
silver_crm_prd = df \
    .withColumn("cat_id", regexp_replace(substring(col("prd_key"), 1, 5), "-", "_")) \
    .withColumn("prd_key", expr("substring(prd_key, 7)")) \
    .withColumn("prd_line", 
               when(upper(trim(col("prd_line"))) == "M", "Mountain")
               .when(upper(trim(col("prd_line"))) == "R", "Road")
               .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
               .when(upper(trim(col("prd_line"))) == "T", "Touring")
               .otherwise("n/a")) \
    .withColumn("dwh_create_date", current_date())

silver_crm_prd.write.format("delta").mode("overwrite").saveAsTable("silver.crm_prd_info")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform CRM sales details
df = spark.table("bronze.crm_sales_details")
silver_crm_sales = df \
    .withColumn("sls_sales_calc", col("sls_quantity") * col("sls_price")) \
    .withColumn("sls_sales", 
               when(col("sls_sales").isNull() | (col("sls_sales") <= 0),
                    col("sls_sales_calc"))
               .otherwise(col("sls_sales"))) \
    .drop("sls_sales_calc") \
    .withColumn("dwh_create_date", current_date())

silver_crm_sales.write.format("delta").mode("overwrite").saveAsTable("silver.crm_sales_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform ERP customer information
df = spark.table("bronze.erp_cust_az12")
silver_erp_cust = df \
    .withColumn("cid", 
               when(col("cid").startswith("NAS"), substring(col("cid"), 4, 100))
               .otherwise(col("cid"))) \
    .withColumn("gen", 
               when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
               .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
               .otherwise("n/a")) \
    .withColumn("dwh_create_date", current_date())

silver_erp_cust.write.format("delta").mode("overwrite").saveAsTable("silver.erp_cust_az12")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform ERP location information
df = spark.table("bronze.erp_loc_a101")
silver_erp_loc = df \
    .withColumn("cid", regexp_replace(col("cid"), "-", "")) \
    .withColumn("cntry", 
               when(trim(upper(col("cntry"))) == "DE", "Germany")
               .when(trim(upper(col("cntry"))).isin("US", "USA"), "United States")
               .when((col("cntry").isNull()) | (trim(col("cntry")) == ""), "n/a")
               .otherwise(trim(col("cntry")))) \
    .withColumn("dwh_create_date", current_date())

silver_erp_loc.write.format("delta").mode("overwrite").saveAsTable("silver.erp_loc_a101")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform ERP product category information
df = spark.table("bronze.erp_px_cat_g1v2")
silver_erp_px = df \
    .withColumn("cat", trim(col("cat"))) \
    .withColumn("subcat", trim(col("subcat"))) \
    .withColumn("maintenance", trim(col("maintenance"))) \
    .withColumn("dwh_create_date", current_date())

silver_erp_px.write.format("delta").mode("overwrite").saveAsTable("silver.erp_px_cat_g1v2")

print("Silver transformations completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
