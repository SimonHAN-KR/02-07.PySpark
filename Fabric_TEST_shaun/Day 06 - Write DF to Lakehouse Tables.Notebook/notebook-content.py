# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fb07e568-7a92-469f-b6d3-3b98e17dbca3",
# META       "default_lakehouse_name": "OneLake_Shaun",
# META       "default_lakehouse_workspace_id": "3f5e5494-8a22-4955-abe8-7ecfd0a41bbc"
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Writing from DataFrame to Lakehouse Table

# CELL ********************

# first, let's get some data 
df = spark.read.json('Files/pyspark/json/property-sales.json')

display(df)

# MARKDOWN ********************

# #### Beware of the limitations of Lakehouse column naming
# - Read more [here](https://learn.microsoft.com/en-us/fabric/data-engineering/load-to-tables) 

# CELL ********************

#inspecting the schema 
df.printSchema()

# CELL ********************

# changing column names to allow write to Lakehouse tables
df = df.withColumnRenamed("SalePrice ($)","SalePrice_USD")\
        .withColumnRenamed("Address ", "Address")\
        .withColumnRenamed("City ", "City")
display(df)

# CELL ********************

df.printSchema()

# MARKDOWN ********************

# #### Writing DF to Table, with different 'modes'
# Using saveAsTable, we save the DataFrame as a 'Managed Table' (Spark terminology) - meaning both the metadata and the data is managed by Spark.
# 
# With a managed table, because Spark manages everything, a SQL command such as DROP TABLE table_name deletes both the metadata and the data. With an unmanaged table, the same command will delete only the metadata, not the actual data.

# CELL ********************

delta_table_name = 'PropertySales'

# use saveAsTable to save as a Managed Table
df.write.mode("overwrite").format("delta").saveAsTable(delta_table_name)
df.toPandas().info()

# MARKDOWN ********************

# **Four different write modes**

# CELL ********************

# these are four different write 'modes' 

# append the new dataframe to the existing Table
df.write.mode("append").format("delta").saveAsTable(delta_table_name)

df_2 = spark.sql("SELECT * FROM OneLake_Shaun.propertysales LIMIT 1000")
df_2.toPandas().info()

# CELL ********************

# overwrite existing Table with new DataFrame
df.write.mode("overwrite").format("delta").saveAsTable(delta_table_name)

df_2 = spark.sql("SELECT * FROM OneLake_Shaun.propertysales LIMIT 1000")
df_2.toPandas().info()

# CELL ********************

# Throw error if data already exists
df.write.mode("error").format("delta").saveAsTable(delta_table_name)

df_2 = spark.sql("SELECT * FROM OneLake_Shaun.propertysales LIMIT 1000")
df_2.toPandas().info()

# CELL ********************

# Fail silently if data already exists 
df.write.mode("ignore").format("delta").saveAsTable(delta_table_name)

df_2 = spark.sql("SELECT * FROM OneLake_Shaun.propertysales LIMIT 1000")
df_2.toPandas().info()

# MARKDOWN ********************

# #### Write to an unmanaged delta table (perhaps for export to external file system/ Databricks/ Snowflake)

# CELL ********************

# unmanaged table
df.write.mode("overwrite").format("delta").save(path="Files/pyspark/delta/unmanaged.delta")


# MARKDOWN ********************

# #### Read from Table into DataFrame

# CELL ********************

df = spark.sql("SELECT * FROM OneLake_Shaun.propertysales LIMIT 1000")
display(df)
