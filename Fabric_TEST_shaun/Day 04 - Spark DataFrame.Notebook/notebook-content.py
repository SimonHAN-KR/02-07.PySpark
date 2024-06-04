# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# 
# 
# In this video: 
# - What is a Spark DataFrame? 
# - Creating our first DataFrame
# - Schemas

# MARKDOWN ********************

# ## What is a Spark DataFrame? 
# 
# A Spark DataFrame is like a distributed, in-memory table with **named columns** and a **schema**. 
# 
# The schema defines the columns and the data types for each column. 
# 
# Inspired by Pandas DataFrames! 

# MARKDOWN ********************

# ## Creating our first data frame (with a schema)


# CELL ********************

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data2 = [("Jack","","Eldridge","36636","M",90000),
    ("Matthew","J", "Munro","28832","M",45400),
    ("Sheila","Oway", "Roberts","12114","F",64000),
    ("Anne","", "Dushane","32192","F",141000),
    ("Jane","Rebecca","Jones","99482","F",56000)
  ]

schema = StructType([ 
    StructField("firstname",StringType(),True), 
    StructField("middlename",StringType(),True), 
    StructField("lastname",StringType(),True), 
    StructField("id", StringType(), True), 
    StructField("gender", StringType(), True), 
    StructField("salary", IntegerType(), True) 
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)


# CELL ********************

type(df)

# MARKDOWN ********************

# ## Why Schemas? 
# - Commonly used, especially with reading from an external data source (including files). 
# - Spark doesn't have to 'infer' the data type (which can be expensive). 
# - You can detect errors early if the data doesn't match the schema.  

# MARKDOWN ********************

# #### Defining a schema using Data Definition Language (DDL) 

# CELL ********************

schema_ddl = "firstname STRING, middlename STRING, lastname STRING, id STRING, gender STRING, salary INT " 
df_with_ddl_schema = spark.createDataFrame(data=data2,schema=schema_ddl)
df.printSchema()

