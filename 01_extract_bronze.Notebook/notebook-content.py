# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import pandas as pd
import requests
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import current_timestamp, lit

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

# Define schemas for all tables
SCHEMAS = {
    # CRM System Tables
    "cust_info": StructType([
        StructField("cst_id", IntegerType(), True),
        StructField("cst_key", StringType(), True),
        StructField("cst_firstname", StringType(), True),
        StructField("cst_lastname", StringType(), True),
        StructField("cst_marital_status", StringType(), True),
        StructField("cst_gndr", StringType(), True),
        StructField("cst_create_date", DateType(), True)
    ]),
    
    "prd_info": StructType([
        StructField("prd_id", IntegerType(), True),
        StructField("prd_key", StringType(), True),
        StructField("prd_nm", StringType(), True),
        StructField("prd_cost", IntegerType(), True),
        StructField("prd_line", StringType(), True),
        StructField("prd_start_dt", DateType(), True),
        StructField("prd_end_dt", DateType(), True)
    ]),
    
    "sales_details": StructType([
        StructField("sls_ord_num", StringType(), True),
        StructField("sls_prd_key", StringType(), True),
        StructField("sls_cust_id", IntegerType(), True),
        StructField("sls_order_dt", DateType(), True),
        StructField("sls_ship_dt", DateType(), True),
        StructField("sls_due_dt", DateType(), True),
        StructField("sls_sales", IntegerType(), True),
        StructField("sls_quantity", IntegerType(), True),
        StructField("sls_price", IntegerType(), True)
    ]),
    
    # ERP System Tables
    "CUST_AZ12": StructType([
        StructField("cid", StringType(), True),
        StructField("bdate", DateType(), True),
        StructField("gen", StringType(), True)
    ]),
    
    "LOC_A101": StructType([
        StructField("cid", StringType(), True),
        StructField("cntry", StringType(), True)
    ]),
    
    "PX_CAT_G1V2": StructType([
        StructField("id", StringType(), True),
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

def fetch_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def process_file(system, file_name):
    # Build URL and table name
    base_url = "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets"
    url = f"{base_url}/source_{system.lower()}/{file_name}.csv"
    table_name = f"bronze.{system.lower()}_{file_name.lower()}"
    
    # Download and process data
    content = fetch_data(url)
    pdf = pd.read_csv(StringIO(content))
    
    # Basic preprocessing
    pdf.columns = [col.lower() for col in pdf.columns]
    
    # Convert to Spark DataFrame
    schema = SCHEMAS.get(file_name)
    sdf = spark.createDataFrame(pdf, schema=schema) if schema else spark.createDataFrame(pdf)
    
    # Add metadata columns
    sdf = sdf.withColumn("ingestion_date", current_timestamp())
    sdf = sdf.withColumn("source_file", lit(file_name))
    
    # Write to bronze table
    sdf.write.format("delta").mode("overwrite").saveAsTable(table_name)
    
    return spark.table(table_name).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Process CRM files
crm_files = ["cust_info", "prd_info", "sales_details"]
for file in crm_files:
    process_file("crm", file)

# Process ERP files
erp_files = ["CUST_AZ12", "LOC_A101", "PX_CAT_G1V2"]
for file in erp_files:
    process_file("erp", file)

print("Bronze data load completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
