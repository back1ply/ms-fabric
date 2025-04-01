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

# Define mapping between file names and target bronze tables
bronze_files = {
    "bronze_crm_cust_info.csv": "bronze.crm_cust_info",
    "bronze_crm_prd_info.csv": "bronze.crm_prd_info",
    "bronze_crm_sales_details.csv": "bronze.crm_sales_details",
    "bronze_erp_cust_az12.csv": "bronze.erp_cust_az12",
    "bronze_erp_loc_a101.csv": "bronze.erp_loc_a101",
    "bronze_erp_px_cat_g1v2.csv": "bronze.erp_px_cat_g1v2"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loop through files and load into corresponding bronze tables
for file_name, table_name in bronze_files.items():
    print(f"\n=== Loading {file_name} into {table_name} ===")

    # Read CSV from Files/bronze/
    df = spark.read.option("header", True).csv(f"Files/bronze/{file_name}")

    # Optionally preview schema
    df.printSchema()

    # Create table if not exists by writing once in 'append' mode and saving metadata
    if not spark._jsparkSession.catalog().tableExists(table_name):
        print(f"Creating new table: {table_name}")
        df.limit(1).write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

    # Overwrite with full data
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

    print(f"✔ Loaded into {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
