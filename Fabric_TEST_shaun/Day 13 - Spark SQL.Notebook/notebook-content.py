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

# #### Reading table data 

# CELL ********************

df = spark.sql("SELECT * FROM OneLake_Shaun.propertysales LIMIT 1000")
display(df)

# MARKDOWN ********************

# #### Run traditional SQL queries using spark.sql


# CELL ********************

df = spark.sql("SELECT concat(Address,', ', City ) as FullAddress, Type FROM OneLake_Shaun.propertysales WHERE Type like '%House%' ")
display(df)

# MARKDOWN ********************

# #### Dynamic SQL Statements 
# We can make use of f-strings in Python to dynamically inject python variables into the SQL query string - fun! 
# 
# You might use this to loop through a list of table names, read them into a dataframe one-by-one and and perform some actions on them.  

# CELL ********************

# define a list for looping
limits = [1,2,3]

# loop through the list, injecting the dynamic variable into the SQL query string 
for limit in limits: 
    df = spark.sql(f"SELECT * FROM OneLake_Shaun.propertysales LIMIT {limit}")
    # do something more useful 
    df.show() 

# MARKDOWN ********************

# #### Creating temporary views 


# CELL ********************

# first let's rfresh the datafame 
df = spark.sql("SELECT * FROM OneLake_Shaun.propertysales")

# create a temporary view called 'SalesFact'
df.createOrReplaceTempView("SalesFact")

# CELL ********************

# we can now reference that Temporary View within a spark.sql statement 
spark.sql("SELECT * from SalesFact").show()
