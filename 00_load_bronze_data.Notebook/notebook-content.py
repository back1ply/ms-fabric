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

import pandas as pd
import requests
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, to_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Define source files
crm_files = ["cust_info", "prd_info", "sales_details"]
erp_files = ["CUST_AZ12", "LOC_A101", "PX_CAT_G1V2"]
base_url = "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets"

# Define schemas for all tables to match SQL Server definitions
schemas = {
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
        StructField("sls_order_dt", IntegerType(), True),
        StructField("sls_ship_dt", IntegerType(), True),
        StructField("sls_due_dt", IntegerType(), True),
        StructField("sls_sales", IntegerType(), True),
        StructField("sls_quantity", IntegerType(), True),
        StructField("sls_price", IntegerType(), True)
    ]),
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

# Function to load data from GitHub and write directly to bronze tables
def load_to_bronze(file_list, system):
    for file in file_list:
        url = f"{base_url}/source_{system.lower()}/{file}.csv"
        table_name = f"bronze.{system.lower()}_{file.lower()}"
        
        print(f"Processing: {url} -> {table_name}")
        
        try:
            # Download the file
            response = requests.get(url)
            content = StringIO(response.text)
            
            # Read into pandas first to handle small files efficiently
            pdf = pd.read_csv(content)
            
            # Apply type conversions for specific files
            if file == "cust_info":
                # Ensure cst_id is treated as integer
                pdf['cst_id'] = pd.to_numeric(pdf['cst_id'], errors='coerce')
            elif file == "prd_info":
                # Ensure prd_id and prd_cost are integers
                pdf['prd_id'] = pd.to_numeric(pdf['prd_id'], errors='coerce')
                pdf['prd_cost'] = pd.to_numeric(pdf['prd_cost'], errors='coerce')
                # Convert date fields
                pdf['prd_start_dt'] = pd.to_datetime(pdf['prd_start_dt'], errors='coerce')
                pdf['prd_end_dt'] = pd.to_datetime(pdf['prd_end_dt'], errors='coerce')
            elif file == "sales_details":
                # Ensure numeric fields are integers
                pdf['sls_cust_id'] = pd.to_numeric(pdf['sls_cust_id'], errors='coerce')
                pdf['sls_order_dt'] = pd.to_numeric(pdf['sls_order_dt'], errors='coerce')
                pdf['sls_ship_dt'] = pd.to_numeric(pdf['sls_ship_dt'], errors='coerce')
                pdf['sls_due_dt'] = pd.to_numeric(pdf['sls_due_dt'], errors='coerce')
                pdf['sls_sales'] = pd.to_numeric(pdf['sls_sales'], errors='coerce')
                pdf['sls_quantity'] = pd.to_numeric(pdf['sls_quantity'], errors='coerce')
                pdf['sls_price'] = pd.to_numeric(pdf['sls_price'], errors='coerce')
            elif file == "CUST_AZ12":
                # Standardize column names to lowercase
                pdf.columns = [col.lower() for col in pdf.columns]
                # Convert bdate to datetime
                if 'bdate' in pdf.columns:
                    pdf['bdate'] = pd.to_datetime(pdf['bdate'], errors='coerce')
            elif file == "LOC_A101" or file == "PX_CAT_G1V2":
                # Standardize column names to lowercase
                pdf.columns = [col.lower() for col in pdf.columns]
            
            # Convert to Spark DataFrame with schema if defined, otherwise infer
            if file in schemas:
                sdf = spark.createDataFrame(pdf, schema=schemas[file])
            else:
                sdf = spark.createDataFrame(pdf)
            
            # Write directly to bronze table
            sdf.write.format("delta").mode("overwrite").saveAsTable(table_name)
            
            # Show sample and count for verification
            count = spark.table(table_name).count()
            print(f"✅ Loaded {count} rows into {table_name}")
            print("Sample data:")
            spark.table(table_name).show(3, truncate=False)
            
        except Exception as e:
            print(f"❌ Error processing {url}: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Disable Arrow optimization if needed
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# Load CRM data
print("\n== Loading CRM Data ==")
load_to_bronze(crm_files, "crm")

# Load ERP data
print("\n== Loading ERP Data ==")
load_to_bronze(erp_files, "erp")

print("\n✅ All bronze data loaded successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
