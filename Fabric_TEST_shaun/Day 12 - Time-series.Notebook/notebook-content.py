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

# #### Let's make some data 

# CELL ********************

data=[["1","2023-02-01"],["2","2023-03-01"],["3","2023-06-01"]]
df=spark.createDataFrame(data,["id","input"])
df.show()


# MARKDOWN ********************

# #### Date formatting

# CELL ********************

from pyspark.sql.functions import col, date_format

df = df.select(col("input"), date_format(col("input"), "MM-dd-yyyy").alias("date_format"))
df.printSchema()

# CELL ********************

df.show()

# MARKDOWN ********************

# #### String to date conversion

# CELL ********************

from pyspark.sql.functions import to_date

df = df.select(col("input"), 
    to_date(col("input"), "yyy-MM-dd").alias("to_date") 
  )
df.show()

# MARKDOWN ********************

# #### Date difference

# CELL ********************

from pyspark.sql.functions import datediff, current_date

df.select(col("input"), 
    datediff(current_date(),col("input")).alias("datediff")  
  ).show()

# MARKDOWN ********************

# #### Getting year,month, dayofmonth etc from a date

# CELL ********************

from pyspark.sql.functions import dayofweek, dayofmonth, dayofyear, month, year

df.select(col("input"),  
     year(col("input")).alias("year"), 
     month(col("input")).alias("month"), 
     dayofmonth(col("input")).alias("day"), 
     dayofweek(col("input")).alias("dayofweek"), 
     dayofmonth(col("input")).alias("dayofmonth"), 
     dayofyear(col("input")).alias("dayofyear"), 
  ).show()

