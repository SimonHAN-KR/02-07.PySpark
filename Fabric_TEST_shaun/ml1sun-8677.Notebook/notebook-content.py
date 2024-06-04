# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fba3d0e3-d739-4864-bef6-2bb670040971",
# META       "default_lakehouse_name": "Onelake_sun",
# META       "default_lakehouse_workspace_id": "2ac6fd98-968e-4275-9d3e-2cd2fd58cf28"
# META     }
# META   }
# META }

# CELL ********************

from mlflow import MlflowClient
client = MlflowClient()
client.create_model_version(
        name="ml1sun",
        source=<"your-model-source-path">,
        run_id=<"your-run-id">
    )

# CELL ********************

# Declare the path to our file 
csv_path = 'Files/property-sales.csv' 

# Read a csv file from Files/property-sales.csv
df_csv = spark.read.csv(csv_path, header=True) 

display(df_csv)

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/property-sales.csv")
# df now is a Spark DataFrame containing CSV data from "Files/property-sales.csv".
display(df)
