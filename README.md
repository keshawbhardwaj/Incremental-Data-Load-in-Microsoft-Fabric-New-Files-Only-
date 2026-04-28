# Incremental-Data-Load-in-Microsoft-Fabric-New-Files-Only-

Objective

Load only new files from a folder into a target table without reprocessing previously loaded files.

Architecture Overview
OneLake Files (CSV files)
        ↓
PySpark Notebook (incremental logic)
        ↓
Delta Table (target: sales_data)
        ↓
Tracking Table (processed_files)
        ↓
Fabric Pipeline (orchestration)
Prerequisites
Microsoft Fabric workspace
Lakehouse enabled
Basic familiarity with notebooks
Step 1: Create Lakehouse
Go to Fabric workspace
Click New → Lakehouse
Name it: incremental_lh
Open the Lakehouse
Step 2: Upload Sample Files

Inside Lakehouse:

Go to Files
Create folder: sales
Upload the following 3 CSV files
Sample File 1: sales_202401.csv
order_id,amount
1,100
2,200
3,150
Sample File 2: sales_202402.csv
order_id,amount
4,300
5,250
6,400
Sample File 3: sales_202403.csv
order_id,amount
7,500
8,600
9,550
Step 3: Create Tables (SQL Endpoint)

Open Lakehouse SQL Endpoint and run:

Tracking Table
CREATE TABLE processed_files (
    file_name STRING,
    load_time TIMESTAMP
);
Target Table
CREATE TABLE sales_data (
    order_id INT,
    amount FLOAT,
    file_name STRING
);
Step 4: Create PySpark Notebook
Click New → Notebook
Attach it to your Lakehouse
Step 5: Read All Files
from pyspark.sql.functions import input_file_name

df = spark.read.option("header", True).csv("Files/sales/")
df = df.withColumn("file_name", input_file_name())
Step 6: Extract File Name Only
from pyspark.sql.functions import regexp_extract

df = df.withColumn(
    "file_name",
    regexp_extract("file_name", r'([^/]+$)', 1)
)
Step 7: Get Already Processed Files
processed_df = spark.sql("SELECT file_name FROM processed_files")
Step 8: Filter Only New Files
new_df = df.join(processed_df, on="file_name", how="left_anti")

Explanation:

Removes records from files already processed
Keeps only new file data
Step 9: Load Data into Target Table
new_df.write.mode("append").saveAsTable("sales_data")
Step 10: Update Tracking Table
from pyspark.sql.functions import current_timestamp

new_files = new_df.select("file_name").distinct() \
    .withColumn("load_time", current_timestamp())

new_files.write.mode("append").saveAsTable("processed_files")
Step 11: Validate Output

Run:

spark.sql("SELECT * FROM sales_data").show()
spark.sql("SELECT * FROM processed_files").show()
Step 12: Test Incremental Behavior
First run:
All 3 files will load
Second run:
No new data should load
Add new file:
sales_202404.csv
order_id,amount
10,700
11,800
Run again:
Only new file data gets loaded
Step 13: Create Pipeline (Orchestration)
Go to Fabric workspace
Click New → Data Pipeline
Add activity: Notebook
Select your notebook
Save and Run

Optional:

Add schedule trigger (daily/hourly)
Key Logic Summary
1. Read all files
2. Extract file names
3. Compare with processed_files table
4. Filter new files
5. Load new data
6. Update tracking table
