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

# #### First, we need a dataframe

# CELL ********************

df = spark.sql("SELECT * FROM OneLake_Shaun.propertysales LIMIT 1000")
display(df)

# MARKDOWN ********************

# #### Simple filtering

# CELL ********************

# simple filter condition (pythonic)
df.filter(df['City'] == "New York").show()

# not equal to
df.filter(df['City'] != "New York").show()

# not equal to (another way) - not very readable
df.filter(~(df['City'] == "New York")).show()



# MARKDOWN ********************

# #### StartsWith, Endswith

# CELL ********************

#startswith 
df.filter(df.City.startswith("L")).show()

#endswith
df.filter(df.City.endswith("ta")).show()


# MARKDOWN ********************

# #### Multiple conditions

# CELL ********************

# Multple conditions, with AND
# where city is not Atlanta and the SalePrice is greater than 400k 
df.filter((df.City != 'Atlanta') & (df.SalePrice_USD > 400000) ).show()

# CELL ********************

# Multple conditions, with OR
# where city is Atlanta OR the city is Los Angeles 
df.filter((df.City == 'Atlanta') | (df.City == 'Los Angeles') ).show()

# MARKDOWN ********************

# #### Is a member of a list 

# CELL ********************

#Filter df if df.City is in the list cities_we_care_about
cities_we_care_about=["Atlanta","Los Angeles"]
df.filter(df.City.isin(cities_we_care_about)).show()

# MARKDOWN ********************

# #### String Contains 

# CELL ********************

# filter df if the Type contains 'House' 

df.filter(df.Type.contains('House')).show() 

# MARKDOWN ********************

# #### SQL LIKE filtering

# CELL ********************

# filer where 'House' appears somewhere in df.Type
df.filter(df.Type.like("%House%")).show()

# filter where df.Type starts with House...
df.filter(df.Type.like("House%")).show()

# filter where df.Address endswith avenue

df.filter(df.Address.like("%avenue")).show()


# MARKDOWN ********************

# #### Other ways to use SQL expressions

# CELL ********************

# filtering using raw WHERE conditions you would use in SQL
df.filter("City != 'Los Angeles'").show()

df.filter("City <> 'Los Angeles'").show()

# MARKDOWN ********************

# #### Using df.where() 
# 
# For any of the above functions, you can also use df.where() instead of df.filter() if you prefer - it gives the same result (when using the Spark SQL API)

# CELL ********************

df.where(df.City == 'Los Angeles').show()
