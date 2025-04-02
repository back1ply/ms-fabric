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

class DataQualityFramework:
    """
    A framework for running data quality checks on Spark DataFrames.
    Provides a structured approach to define, run, and report quality checks.
    """
    
    def __init__(self, spark):
        """Initialize with a SparkSession"""
        self.spark = spark
        self.results = {}
        
    def run_checks(self, table_name, columns, check_types=None):
        """
        Run quality checks on a table
        
        Args:
            table_name (str): Name of the table to check
            columns (list): List of columns to check
            check_types (dict, optional): Dictionary mapping check types to columns
        """
        try:
            df = self.spark.table(table_name)
            actual_columns = df.columns
            total_rows = df.count()
            
            # Initialize results for this table
            table_results = {
                "table_name": table_name,
                "total_rows": total_rows,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "columns": {},
                "summary": {
                    "total_checks": 0,
                    "passed_checks": 0,
                    "failed_checks": 0,
                    "missing_columns": []
                }
            }
            
            # Default check types if not provided
            if check_types is None:
                check_types = {
                    "nulls": columns,
                    "trim_issues": columns,
                    "duplicates": [col for col in columns if col.endswith("_id") or col in ["prd_key", "sls_ord_num"]],
                    "distinct_values": [col for col in columns if col in ["cst_marital_status", "cst_gndr", "prd_line", "gen", "cntry", "cat", "subcat", "maintenance"]]
                }
            
            # Run checks for each column
            for col_name in columns:
                # Skip columns that don't exist
                if col_name not in actual_columns:
                    table_results["summary"]["missing_columns"].append(col_name)
                    continue
                
                # Initialize column results
                table_results["columns"][col_name] = {
                    "checks": {}
                }
                
                # Run null check
                if col_name in check_types.get("nulls", []):
                    null_result = self._check_nulls(df, col_name)
                    table_results["columns"][col_name]["checks"]["nulls"] = null_result
                    self._update_summary(table_results, null_result["status"])
                
                # Run trim issues check
                if col_name in check_types.get("trim_issues", []):
                    trim_result = self._check_trim_issues(df, col_name)
                    table_results["columns"][col_name]["checks"]["trim_issues"] = trim_result
                    self._update_summary(table_results, trim_result["status"])
                
                # Run duplicates check
                if col_name in check_types.get("duplicates", []):
                    dup_result = self._check_duplicates(df, col_name)
                    table_results["columns"][col_name]["checks"]["duplicates"] = dup_result
                    self._update_summary(table_results, dup_result["status"])
                
                # Run distinct values check
                if col_name in check_types.get("distinct_values", []):
                    distinct_result = self._check_distinct_values(df, col_name)
                    table_results["columns"][col_name]["checks"]["distinct_values"] = distinct_result
            
            # Calculate quality score
            if table_results["summary"]["total_checks"] > 0:
                quality_score = (table_results["summary"]["passed_checks"] / 
                                table_results["summary"]["total_checks"]) * 100
                table_results["summary"]["quality_score"] = round(quality_score, 2)
            else:
                table_results["summary"]["quality_score"] = 0
                
            # Store results
            self.results[table_name] = table_results
            
            return table_results
            
        except Exception as e:
            print(f"Error running checks on {table_name}: {str(e)}")
            return None
    
    def _update_summary(self, table_results, status):
        """Update summary statistics based on check result"""
        table_results["summary"]["total_checks"] += 1
        if status == "PASS":
            table_results["summary"]["passed_checks"] += 1
        else:
            table_results["summary"]["failed_checks"] += 1
    
    def _check_nulls(self, df, column):
        """Check for null values in a column"""
        null_df = df.filter(col(column).isNull())
        null_count = null_df.count()
        
        result = {
            "null_count": null_count,
            "null_percentage": round((null_count / df.count()) * 100, 2) if df.count() > 0 else 0,
            "status": "PASS" if null_count == 0 else "FAIL",
            "sample": []
        }
        
        # Get sample of rows with nulls (up to 5)
        if null_count > 0:
            sample_rows = null_df.limit(5).collect()
            result["sample"] = [row.asDict() for row in sample_rows]
            
        return result
    
    def _check_trim_issues(self, df, column):
        """Check for whitespace issues in a column"""
        # Only check string columns
        if column not in df.schema.names or not any(
            isinstance(field.dataType, t) for t in [spark.sql.types.StringType] 
            for field in df.schema.fields if field.name == column
        ):
            return {"status": "SKIP", "message": "Not a string column"}
        
        trim_df = df.filter(col(column).isNotNull() & (trim(col(column)) != col(column)))
        trim_count = trim_df.count()
        
        result = {
            "trim_issues_count": trim_count,
            "status": "PASS" if trim_count == 0 else "FAIL",
            "sample": []
        }
        
        # Get sample of rows with trim issues (up to 5)
        if trim_count > 0:
            sample_rows = trim_df.limit(5).collect()
            result["sample"] = [row.asDict() for row in sample_rows]
            
        return result
    
    def _check_duplicates(self, df, column):
        """Check for duplicate values in a column"""
        # Skip if column has nulls (they can't be properly grouped)
        if df.filter(col(column).isNull()).count() > 0:
            dup_df = df.filter(col(column).isNotNull()).groupBy(column).count().filter("count > 1")
        else:
            dup_df = df.groupBy(column).count().filter("count > 1")
            
        dup_count = dup_df.count()
        
        result = {
            "duplicate_keys": dup_count,
            "status": "PASS" if dup_count == 0 else "FAIL",
            "sample": []
        }
        
        # Get sample of duplicate values (up to 5)
        if dup_count > 0:
            sample_rows = dup_df.limit(5).collect()
            result["sample"] = [row.asDict() for row in sample_rows]
            
        return result
    
    def _check_distinct_values(self, df, column):
        """Get distinct values for a column"""
        distinct_df = df.select(column).distinct()
        distinct_count = distinct_df.count()
        
        result = {
            "distinct_count": distinct_count,
            "sample": []
        }
        
        # Get sample of distinct values (up to 20)
        sample_rows = distinct_df.limit(20).collect()
        result["sample"] = [row[0] for row in sample_rows]
        
        return result
    
    def print_results(self, table_name=None):
        """
        Print formatted results for a table or all tables
        
        Args:
            table_name (str, optional): Name of the table to print results for.
                                       If None, print results for all tables.
        """
        if table_name is not None and table_name in self.results:
            self._print_table_results(self.results[table_name])
        elif table_name is None:
            for table_results in self.results.values():
                self._print_table_results(table_results)
        else:
            print(f"No results found for table: {table_name}")
    
    def _print_table_results(self, table_results):
        """Print formatted results for a table"""
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

# Initialize the data quality framework
dqf = DataQualityFramework(spark)

# Run checks for each table
for table_name, columns in tables:
    dqf.run_checks(table_name, columns)

# Print results
dqf.print_results()

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

results_json = json.dumps(dqf.results, indent=2, default=str)
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
