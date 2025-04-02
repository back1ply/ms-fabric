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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# Define explicit schemas for all tables
schemas = {
    "bronze_crm_cust_info.csv": StructType([
        StructField("cst_id", StringType(), False),
        StructField("cst_firstname", StringType(), True),
        StructField("cst_lastname", StringType(), True),
        StructField("cst_marital_status", StringType(), True),
        StructField("cst_gndr", StringType(), True),
        StructField("cst_create_date", DateType(), True)
    ]),
    
    "bronze_crm_prd_info.csv": StructType([
        StructField("prd_id", StringType(), False),
        StructField("prd_key", StringType(), True),
        StructField("prd_line", StringType(), True),
        StructField("prd_cost", DoubleType(), True),
        StructField("prd_start_dt", StringType(), True),
        StructField("prd_end_dt", StringType(), True)
    ]),
    
    "bronze_crm_sales_details.csv": StructType([
        StructField("sls_ord_num", StringType(), False),
        StructField("sls_prd_key", StringType(), True),
        StructField("sls_cust_id", StringType(), True),
        StructField("sls_order_dt", StringType(), True),
        StructField("sls_ship_dt", StringType(), True),
        StructField("sls_due_dt", StringType(), True),
        StructField("sls_sales", DoubleType(), True),
        StructField("sls_quantity", IntegerType(), True),
        StructField("sls_price", DoubleType(), True)
    ]),
    
    "bronze_erp_cust_az12.csv": StructType([
        StructField("cid", StringType(), False),
        StructField("bdate", DateType(), True),
        StructField("gen", StringType(), True)
    ]),
    
    "bronze_erp_loc_a101.csv": StructType([
        StructField("cid", StringType(), False),
        StructField("cntry", StringType(), True)
    ]),
    
    "bronze_erp_px_cat_g1v2.csv": StructType([
        StructField("id", StringType(), False),
        StructField("cat", StringType(), True),
        StructField("subcat", StringType(), True),
        StructField("maintenance", StringType(), True)
    ])
}

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

# Load each file with its explicit schema
for file_name, table_name in bronze_files.items():
    print(f"\n=== Loading {file_name} into {table_name} ===")
    
    # Read CSV with explicit schema
    df = spark.read.option("header", True) \
                  .schema(schemas[file_name]) \
                  .csv(f"Files/bronze/{file_name}")
    
    # Display schema
    print("Schema:")
    df.printSchema()
    
    # Display sample data
    print("Sample data:")
    df.show(5, truncate=False)
    
    # Create or update table
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)
    
    print(f"✅ Successfully loaded into {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
