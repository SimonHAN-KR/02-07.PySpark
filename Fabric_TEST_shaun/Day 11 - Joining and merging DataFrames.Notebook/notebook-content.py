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

# #### Let's get one DataFrame to begin with

# CELL ********************

# we'll get the propertysales table - FACT
sales_fact_df = spark.sql("SELECT * FROM OneLake_Shaun.propertysales LIMIT 1000")
sales_fact_df.show()

# MARKDOWN ********************

# #### Let's make a Dimension table

# CELL ********************

cities_data = [(1,"New York","USA"), \
    (2,"Los Angeles","USA"), \
    (3,"London","UK"), \
    (4,"Atlanta","USA"), \
    (5,"Dublin","Ireland")
  ]
cites_columns = ["city_uid","city_name","country"]

city_dimension_df = spark.createDataFrame(data=cities_data, schema = cites_columns)
city_dimension_df.show()

# MARKDOWN ********************

# #### Join the Sales fact table and the city dimension table
# 
# Here's [the official pySpark documentation for the Join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html)
# 
# 
# Generic structure of a join statement: 
# 
# **first_df.join(second_df, first_df.ColumnNameOnWhichToJoin ==  second_df.ColumnNameOnWhichToJoin, join_type)**
# 
# Join types available: 
# default inner. Must be one of: inner, cross, outer, full, fullouter, full_outer, left, leftouter, left_outer, right, rightouter, right_outer, semi, leftsemi, left_semi, anti, leftanti and left_anti.

# CELL ********************

# inner join 
sales_fact_df.join(city_dimension_df,sales_fact_df.City == city_dimension_df.city_name, "inner") \
    .show()

# CELL ********************

# same, but removing some of the columns we don't want  
sales_fact_df.join(city_dimension_df,sales_fact_df.City == city_dimension_df.city_name, "inner") \
    .drop(*('city_uid', 'city_name'))\
    .show()

# MARKDOWN ********************

# #### Joining revision
# What do each of these do? 
# 
# **First one to answer in the comments gets absolutely nothing, but my respect and a well done.** 😂    
# 
# - inner: 
# - cross: 
# - outer: 
# - full: 
# - fullouter: 
# - full_outer:
# - left: 
# - leftouter:
# - left_outer:
# - right:
# - rightouter:
# - right_outer:
# - semi:
# - leftsemi:
# - left_semi:
# - anti:
# - leftanti:
# - left_anti:
