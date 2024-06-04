# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fb07e568-7a92-469f-b6d3-3b98e17dbca3",
# META       "default_lakehouse_name": "OneLake_Shaun",
# META       "default_lakehouse_workspace_id": "3f5e5494-8a22-4955-abe8-7ecfd0a41bbc",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Reading a CSV file into a Spark DataFrame
# - Basic read
# - with headers, 
# - inferring the schema 


# MARKDOWN ********************

# Using property-sales.csv, which looks like this: 
# 
# Address,Type,City,SalePrice ($),Agent
# 
# 1 Rowley Street ,Detached House,New York,745000,Penelope Pullman 
# 
# 13a lollipop avenue,Apartment,Los Angeles,345000,Jack Smith 
# 
# 34 the drive,House,Atlanta,459000,Sheila Sammi

# CELL ********************

# Declare the path to our file 
csv_path = "Files/pyspark/property-sales.csv"
# csv_path = "abfss://shaun_Fabric01@onelake.dfs.fabric.microsoft.com/OneLake_Shaun.Lakehouse/Files/dir_shaun/property-sales.csv"

# Read a csv file from Files/property-sales.csv
df_csv = spark.read.csv(csv_path, header=True) 

display(df_csv)

# CELL ********************

csv_table_name = "pyspark/csv/property-sales_2.csv"
df_csv.write.mode("overwrite").format("csv").save("Files/" + csv_table_name)

# CELL ********************

df_csv.dtypes

# MARKDOWN ********************

# ## Writing DataFrames to files (JSON) 
# We can write out our DataFrame as a JSON file by calling df.write.json() 

# CELL ********************

# call write.json() on our 
df_csv.write.json("Files/pyspark/json/property-sales.json", mode='overwrite')

# MARKDOWN ********************

# ## Reading from JSON File into DataFrame

# CELL ********************

df_json = spark.read.json('Files/pyspark/json/property-sales.json')
display(df_json)

# MARKDOWN ********************

# ## Writing out to parquet

# CELL ********************

df_json.write.parquet('Files/pyspark/parquet/property-sales.parquet', mode='overwrite')

# MARKDOWN ********************

# Reading in parquet into a DataFrame

# CELL ********************

df_parquet = spark.read.parquet('Files/pyspark/parquet/property-sales.parquet')
display(df_parquet)

# MARKDOWN ********************

# ## Reading multiple files in the same folder
# - creating multiple parquet files in the parquet subfolder first
# - read in all the parquet files into one df 

# CELL ********************

# read all the parquet files in the 'Files/parquet/' folder into a dataframe  
df_all_parquet = spark.read.parquet('Files/pyspark/parquet/*.parquet')

# MARKDOWN ********************

# ## Checking this has worked using _metadata
# Spark provides us with all the file metadata in a 'hidden' column that we can add to our dataframe using _metadata. 

# CELL ********************

# read all the parquet files, then add the _metadata column 
df_all_parquet_plus_metadata = spark.read\
    .parquet('Files/pyspark/parquet/*.parquet')\
    .select("*", "_metadata")

display(df_all_parquet_plus_metadata)

# MARKDOWN ********************

# ## Further learning
# Check out the relevant part of the [Spark documentation](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html) to read about more complex data reading scenarios, like: 
# - Ignoring corrupt/ missing files
# - Custom path filtering (PathGlobFilter) 
# - More recursive file reading patterns within complex folder structures. 

