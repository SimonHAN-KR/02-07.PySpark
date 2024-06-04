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

# #### First, let's read some data into a DataFrame

# CELL ********************

# First we need a DataFrame to look at! 
df = spark.read.csv('Files/pyspark/property-sales.csv', header=True, inferSchema=True)


# MARKDOWN ********************

# #### Ways of viewing our data

# CELL ********************

# show a printed representation of the DataFrame
df.show()

# CELL ********************

# interactive view of the DataFrame
display(df) 

# CELL ********************

# Show me just the first two rows of my DataFrame
display(df.head(2))

# MARKDOWN ********************

# #### Exploring schemas

# CELL ********************

df.printSchema()

# CELL ********************

# get just the data types (not full schema)
df.dtypes

# CELL ********************

# the actual schema can be accessed using df.schema 
df.schema

# CELL ********************

# this is sometimes useful as we might have to do something like this 
source_schema = df.schema

# this saves us having to explicitly write out our the schema for a new df, if we have one that already exists. 
new_df_with_existing_schema = spark.read.csv('Files/pyspark/property-sales.csv', schema = source_schema)

# MARKDOWN ********************

# #### Column operations

# CELL ********************

# to see that columns we have: 
df.columns

# CELL ********************

#selecting just a single column 
# df.select('Type').show()

type(df.select('Type'))

# CELL ********************

#renaming existing columns 
df = df.withColumnRenamed('Address ', 'Address_new')
df.select('Address_new').show()

# CELL ********************

# selecting a few columns 
df.select(['Address_new','Type']).show()

# MARKDOWN ********************

# #### Adding new columns

# CELL ********************

df = df.withColumn('2x_SalePrice', df['SalePrice ($)'] * 2)
df.show()

# MARKDOWN ********************

# #### Removing columns

# CELL ********************

df = df.drop('2x_SalePrice')
df.show()

# MARKDOWN ********************

# #### Renaming multiple columns 
# 
# df.show()

# CELL ********************

df.show()

# CELL ********************

df_new = df.selectExpr("Address_new as ADD","'SalePrice ($)' as SalesPrice_USD","'City ' as MyCity")
df_new.show()
