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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

crm_files = ["cust_info", "prd_info", "sales_details"]
erp_files = ["CUST_AZ12", "LOC_A101", "PX_CAT_G1V2"]

base_url = "https://raw.githubusercontent.com/DataWithBaraa/sql-data-warehouse-project/refs/heads/main/datasets"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_and_write(file_list, system):
    for file in file_list:
        url = f"{base_url}/source_{system.lower()}/{file}.csv"
        print(f"Downloading: {url}")

        try:
            # Use pandas to download from HTTP
            pdf = pd.read_csv(url, timeout=30)  # Add timeout to prevent hanging
            
            # Convert to Spark DataFrame
            sdf = spark.createDataFrame(pdf)
            
            # Save to Lakehouse Files as bronze.system_file.csv
            output_path = f"Files/bronze/bronze_{system.lower()}_{file.lower()}.csv"
            sdf.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
            
            print(f"✅ Successfully saved to: {output_path}")
            
        except pd.errors.EmptyDataError:
            print(f"❌ Error: The file at {url} is empty or has no columns.")
            # Log the error and continue with next file
            continue
            
        except pd.errors.ParserError:
            print(f"❌ Error: Could not parse the file at {url}. The file may be corrupted.")
            # Log the error and continue with next file
            continue
            
        except Exception as e:
            print(f"❌ Error downloading or processing {url}: {str(e)}")
            # Implement retry logic if needed
            retry_count = 3
            while retry_count > 0:
                try:
                    print(f"Retrying download ({retry_count} attempts left)...")
                    # Wait before retry (exponential backoff)
                    import time
                    time.sleep(2 * (4 - retry_count))
                    
                    # Retry download
                    pdf = pd.read_csv(url, timeout=30)
                    sdf = spark.createDataFrame(pdf)
                    sdf.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
                    
                    print(f"✅ Successfully saved to: {output_path} after retry")
                    break
                    
                except Exception as retry_e:
                    print(f"Retry failed: {str(retry_e)}")
                    retry_count -= 1
                    
            if retry_count == 0:
                print(f"❌ All retries failed for {url}. Skipping this file.")

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
