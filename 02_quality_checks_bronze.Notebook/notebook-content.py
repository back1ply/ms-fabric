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
from pyspark.sql.functions import col, count, when, isnan, isnull, trim, length
import json
from datetime import datetime

# We'll implement our own data quality checks without relying on Great Expectations
# This avoids the module import error with great_expectations.dataset

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define custom validation functions
def check_not_null(df, column):
    """Check if column values are not null"""
    null_count = df.filter(col(column).isNull()).count()
    total_count = df.count()
    success = null_count == 0
    return {
        "success": success,
        "null_count": null_count,
        "null_percentage": round((null_count / total_count) * 100, 2) if total_count > 0 else 0
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_no_whitespace_issues(df, column):
    """Check if column values don't have leading/trailing whitespace"""
    if str(df.schema[column].dataType).startswith("StringType"):
        trim_df = df.filter(col(column).isNotNull() & (trim(col(column)) != col(column)))
        trim_count = trim_df.count()
        success = trim_count == 0
        return {
            "success": success,
            "trim_issues_count": trim_count,
            "sample": [row.asDict() for row in trim_df.limit(5).collect()] if trim_count > 0 else []
        }
    else:
        return {
            "success": True,
            "status": "SKIP",
            "message": "Not a string column"
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_unique_values(df, column):
    """Check if column values are unique"""
    # Skip if column has nulls (they can't be properly grouped)
    if df.filter(col(column).isNull()).count() > 0:
        dup_df = df.filter(col(column).isNotNull()).groupBy(column).count().filter("count > 1")
    else:
        dup_df = df.groupBy(column).count().filter("count > 1")
    
    dup_count = dup_df.count()
    success = dup_count == 0
    return {
        "success": success,
        "duplicate_keys": dup_count,
        "sample": [row.asDict() for row in dup_df.limit(5).collect()] if dup_count > 0 else []
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_distinct_values(df, column):
    """Get distinct values for a column"""
    distinct_df = df.select(column).distinct()
    distinct_count = distinct_df.count()
    return {
        "distinct_count": distinct_count,
        "sample": [row[0] for row in distinct_df.limit(20).collect()]
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define bronze tables to check
tables = [
    ("bronze.crm_cust_info", ["cst_id", "cst_firstname", "cst_lastname", "cst_marital_status", "cst_gndr", "cst_create_date"]),
    ("bronze.crm_prd_info", ["prd_id", "prd_key", "prd_line", "prd_cost", "prd_start_dt", "prd_end_dt"]),
    ("bronze.crm_sales_details", ["sls_ord_num", "sls_prd_key", "sls_cust_id", "sls_order_dt", "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price"]),
    ("bronze.erp_cust_az12", ["cid", "bdate", "gen"]),
    ("bronze.erp_loc_a101", ["cid", "cntry"]),
    ("bronze.erp_px_cat_g1v2", ["id", "cat", "subcat", "maintenance"])
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize results dictionary
results = {}

# Run checks for each table
for table_name, columns in tables:
    try:
        # Load table as Spark DataFrame
        df = spark.table(table_name)
        
        # No need for Great Expectations validator anymore
        
        # Initialize table results
        table_results = {
            "table_name": table_name,
            "total_rows": df.count(),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "columns": {},
            "summary": {
                "total_checks": 0,
                "passed_checks": 0,
                "failed_checks": 0,
                "missing_columns": []
            }
        }
        
        # Check each column
        for column in columns:
            # Skip columns that don't exist
            if column not in df.columns:
                table_results["summary"]["missing_columns"].append(column)
                continue
            
            # Initialize column results
            table_results["columns"][column] = {
                "checks": {}
            }
            
            # 1. Null check
            null_result = check_not_null(df, column)
            
            table_results["columns"][column]["checks"]["nulls"] = {
                "null_count": null_result["null_count"],
                "null_percentage": null_result["null_percentage"],
                "status": "PASS" if null_result["success"] else "FAIL",
                "sample": []
            }
            
            # Get sample of rows with nulls (up to 5)
            if null_result["null_count"] > 0:
                sample_rows = df.filter(col(column).isNull()).limit(5).collect()
                table_results["columns"][column]["checks"]["nulls"]["sample"] = [row.asDict() for row in sample_rows]
            
            # Update summary
            table_results["summary"]["total_checks"] += 1
            if null_result["success"]:
                table_results["summary"]["passed_checks"] += 1
            else:
                table_results["summary"]["failed_checks"] += 1
            
            # 2. Trim issues check
            trim_result = check_no_whitespace_issues(df, column)
            
            if "status" in trim_result and trim_result["status"] == "SKIP":
                table_results["columns"][column]["checks"]["trim_issues"] = {
                    "status": "SKIP",
                    "message": trim_result["message"]
                }
            else:
                table_results["columns"][column]["checks"]["trim_issues"] = {
                    "trim_issues_count": trim_result["trim_issues_count"],
                    "status": "PASS" if trim_result["success"] else "FAIL",
                    "sample": trim_result["sample"]
                }
                
                # Update summary
                table_results["summary"]["total_checks"] += 1
                if trim_result["success"]:
                    table_results["summary"]["passed_checks"] += 1
                else:
                    table_results["summary"]["failed_checks"] += 1
            
            # 3. Duplicates check
            if column.endswith("_id") or column in ["prd_key", "sls_ord_num"]:
                dup_result = check_unique_values(df, column)
                
                table_results["columns"][column]["checks"]["duplicates"] = {
                    "duplicate_keys": dup_result["duplicate_keys"],
                    "status": "PASS" if dup_result["success"] else "FAIL",
                    "sample": dup_result["sample"]
                }
                
                # Update summary
                table_results["summary"]["total_checks"] += 1
                if dup_result["success"]:
                    table_results["summary"]["passed_checks"] += 1
                else:
                    table_results["summary"]["failed_checks"] += 1
            
            # 4. Distinct values check
            if column in ["cst_marital_status", "cst_gndr", "prd_line", "gen", "cntry", "cat", "subcat", "maintenance"]:
                distinct_result = get_distinct_values(df, column)
                
                table_results["columns"][column]["checks"]["distinct_values"] = {
                    "distinct_count": distinct_result["distinct_count"],
                    "sample": distinct_result["sample"]
                }
        
        # Calculate quality score
        if table_results["summary"]["total_checks"] > 0:
            quality_score = (table_results["summary"]["passed_checks"] / 
                            table_results["summary"]["total_checks"]) * 100
            table_results["summary"]["quality_score"] = round(quality_score, 2)
        else:
            table_results["summary"]["quality_score"] = 0
        
        # Store results
        results[table_name] = table_results
        
    except Exception as e:
        print(f"Error running checks on {table_name}: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Print results
for table_name, table_results in results.items():
    # Print table header
    print("\n" + "=" * 80)
    print(f"DATA QUALITY REPORT: {table_results['table_name']}")
    print("=" * 80)
    
    # Print summary
    summary = table_results["summary"]
    print(f"Total Rows: {table_results['total_rows']}")
    print(f"Quality Score: {summary.get('quality_score', 0)}% ({summary['passed_checks']}/{summary['total_checks']} checks passed)")
    
    if summary["missing_columns"]:
        print(f"Missing Columns: {', '.join(summary['missing_columns'])}")
    
    # Print column results
    for col_name, col_results in table_results["columns"].items():
        print("\n" + "-" * 80)
        print(f"COLUMN: {col_name}")
        print("-" * 80)
        
        for check_name, check_result in col_results["checks"].items():
            if check_name == "nulls":
                print(f"  Null Check: {check_result['status']}")
                print(f"    - Null Count: {check_result['null_count']} ({check_result['null_percentage']}%)")
                if check_result["sample"]:
                    print(f"    - Sample Rows with Nulls: {len(check_result['sample'])}")
            
            elif check_name == "trim_issues":
                if check_result["status"] == "SKIP":
                    print(f"  Trim Check: SKIPPED - {check_result['message']}")
                else:
                    print(f"  Trim Check: {check_result['status']}")
                    print(f"    - Trim Issues Count: {check_result['trim_issues_count']}")
                    if check_result["sample"]:
                        print(f"    - Sample Rows with Trim Issues: {len(check_result['sample'])}")
            
            elif check_name == "duplicates":
                print(f"  Duplicate Check: {check_result['status']}")
                print(f"    - Duplicate Keys: {check_result['duplicate_keys']}")
                if check_result["sample"]:
                    print("    - Sample Duplicates:")
                    for i, sample in enumerate(check_result["sample"][:5], 1):
                        print(f"      {i}. {sample}")
            
            elif check_name == "distinct_values":
                print(f"  Distinct Values: {check_result['distinct_count']}")
                if check_result["sample"]:
                    print("    - Sample Values:")
                    # Format the output based on the number of samples
                    if len(check_result["sample"]) <= 5:
                        print(f"      {', '.join(str(s) for s in check_result['sample'])}")
                    else:
                        print(f"      {', '.join(str(s) for s in check_result['sample'][:5])}... ({len(check_result['sample'])} total)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example of how to export results to JSON (optional)
# Uncomment to use

"""
# Export results to JSON
import json

results_json = json.dumps(results, indent=2, default=str)
print(results_json)

# You can also save to a file or table if needed
# with open('/path/to/quality_results.json', 'w') as f:
#     f.write(results_json)
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
