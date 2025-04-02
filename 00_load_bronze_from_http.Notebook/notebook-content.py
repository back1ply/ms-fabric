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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define source files
crm_files = ["cust_info", "prd_info", "sales_details"]
erp_files = ["CUST_AZ12", "LOC_A101", "PX_CAT_G1V2"]
base_url = "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Simple function to load and write files
def load_and_write(file_list, system):
    for file in file_list:
        url = f"{base_url}/source_{system.lower()}/{file}.csv"
        print(f"Downloading: {url}")
        
        try:
            # Simple download without retries
            response = requests.get(url)
            content = StringIO(response.text)
            pdf = pd.read_csv(content)
            sdf = spark.createDataFrame(pdf)
            
            # Save to Lakehouse
            output_path = f"Files/bronze/bronze_{system.lower()}_{file.lower()}.csv"
            sdf.write.mode("overwrite").option("header", True).csv(output_path)
            print(f"✅ Saved to: {output_path}")
            
        except Exception as e:
            print(f"❌ Error processing {url}: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

load_and_write(crm_files, "crm")
load_and_write(erp_files, "erp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
