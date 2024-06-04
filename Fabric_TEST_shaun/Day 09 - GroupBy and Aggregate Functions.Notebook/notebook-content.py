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

# #### First, let's grab some (extended data)
# Added a few more rows to the dataset so we can do some group-by calcs :)

# CELL ********************

df = spark.read.csv('Files/pyspark/property-sales-extended.csv', header=True, inferSchema=True)
df.show()

# MARKDOWN ********************

# #### Simple aggregates

# CELL ********************

#Counting rows in the group 
df.groupBy('City').count().show()


# CELL ********************

# maximum sales price for each agent
df.groupBy('Agent')\
  .max('SalePrice_USD')\
  .show()


# MARKDOWN ********************

# #### Renaming the aggregate column


# CELL ********************

from pyspark.sql.functions import sum, max

# method 1: using withColumnRenamed() 
df.groupBy('Agent')\
  .max('SalePrice_USD')\
  .withColumnRenamed('max(SalePrice_USD)','max_sales_price')\
  .show()

# method 2: using agg() and then alias() 
df.groupBy("Agent") \
  .agg(max('SalePrice_USD').alias('max_sales_price'))\
  .show()


# MARKDOWN ********************

# #### Returning multiple aggregates in same dataframe

# CELL ********************

from pyspark.sql.functions import avg,max, round
df.groupBy("City").agg(
    round(max("SalePrice_USD"),0).alias("max_sale_price"), 
    round(avg("SalePrice_USD"),0).alias("avg_sale_price")
    ).show() 


# MARKDOWN ********************

# #### Filtering on an aggregate (like SQL HAVING statement)

# CELL ********************

from pyspark.sql.functions import avg,max, round, col

df.groupBy("City").agg(
    round(max("SalePrice_USD"),0).alias("max_sale_price"), 
    round(avg("SalePrice_USD"),0).alias("avg_sale_price")
).where(col("avg_sale_price") >= 500000)\
.show() 

# MARKDOWN ********************

# #### Grouping by multiple columns

# CELL ********************

df.groupBy(['City', 'Agent']).avg('SalePrice_USD').show()
