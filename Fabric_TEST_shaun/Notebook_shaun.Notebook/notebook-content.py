# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "fb07e568-7a92-469f-b6d3-3b98e17dbca3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# CELL ********************

df = spark.read.format("csv").option("header","true").load("abfss://shaun_Fabric01@onelake.dfs.fabric.microsoft.com/OneLake_Shaun.Lakehouse/Files/dir_shaun/Sample_Superstore_one.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://shaun_Fabric01@onelake.dfs.fabric.microsoft.com/OneLake_Shaun.Lakehouse/Files/dir_shaun/Sample_Superstore_one.csv".
display(df.head(5))


# CELL ********************

# Spark SQL을 사용하여 현재 기본 레이크하우스와 동일한 작업 영역에 있는 레이크하우스에 대해 쿼리를 실행하세요.

df = spark.sql("SELECT * FROM OneLake_Shaun.sample_superstore_DM LIMIT 1000")
display(df)
