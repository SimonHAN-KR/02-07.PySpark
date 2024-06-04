# Fabric notebook source


# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

serverName = "<server_name>.database.windows.net"
database = "<database_name>"
dbPort = 1433
dbUserName = "<username>"
dbPassword = “<db password> or reference based on Keyvault>”

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example") \
    .config("spark.jars.packages", "com.microsoft.azure:azure-sqldb-spark:1.0.2") \
    .config("spark.sql.catalogImplementation", "com.microsoft.azure.synapse.spark") \
    .config("spark.sql.catalog.testDB", "com.microsoft.azure.synapse.spark") \
    .config("spark.sql.catalog.testDB.spark.synapse.linkedServiceName", "AzureSqlDatabase") \ .config("spark.sql.catalog.testDB.spark.synapse.linkedServiceName.connectionString", f"jdbc:sqlserver://{serverName}:{dbPort};database={database};user={dbUserName};password={dbPassword}") \ .getOrCreate()

    
jdbcURL = "jdbc:sqlserver://{0}:{1};database={2}".format(serverName,dbPort,database)
connection = {"user":dbUserName,"password":dbPassword,"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}

df = spark.read.jdbc(url=jdbcURL, table = "dbo.Employee", properties=connection)
df.show()
display(df)

# Write the dataframe as a delta table in your lakehouse
df.write.mode("overwrite").format("delta").saveAsTable("Employee")

# You can also specify a custom path for the table location
# df.write.mode("overwrite").format("delta").option("path", "abfss://yourlakehouse.dfs.core.windows.net/Employee").saveAsTable("Employee")

# CELL ********************


# CELL ********************

ip = "mcsfabricsql01.privatelink.mysql.database.azure.com"
port = "3306" 
user = "mcssql"
passwd = "mavencloud123!@#"
db = "shaun_db"
table_name = "sample_superstore_csv"

url = f"jdbc:mysql://{ip}:{port}/{db}"

properties = {
    "user": user,
    "password": passwd,
    "driver": "com.mysql.jdbc.Driver"
}

df = spark.read.jdbc(url, table_name, properties=properties)
df

# CELL ********************

