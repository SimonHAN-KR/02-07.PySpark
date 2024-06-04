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

# #### Why? 
# 
# Dealing with missing values is a common data processing task in preparation for machine learning. 
# 
# Most machine learning algorithms don't like it when you have missing values. 
# 
# So there are a variety of methods we can use to deal with this. 
# 
# Which one you choose depends greatly on: 
# - the business question you are answering, 
# - what 'makes sense' for your domain. 

# MARKDOWN ********************

# #### Let's get some data 

# CELL ********************

df = spark.read.csv('Files/pyspark/property-sales-missing.csv', header=True, inferSchema=True)
df.show()

# MARKDOWN ********************

# #### Dropping rows with nulls

# CELL ********************

#most basic/drastic drop NAs 
df.na.drop().show()

# MARKDOWN ********************

# #### How='any' vs how='all'

# CELL ********************

#drop the row if ANY values are null  
df.na.drop(how='any').show()


#drop the row if ALL values are null  
df.na.drop(how='all').show()

# MARKDOWN ********************

# #### Dropping with a threshold
# 
# - thresh를 이용하여 정상값의 수를 보장할 수 있습니다.
# - thresh가 4인 경우 정상값이 4개 미만인 경우에 대해서 결측치 보정을 진행합니다.


# CELL ********************

df.na.drop(thresh=4).show()

# MARKDOWN ********************

# #### Only drop if NULL in certain columns (subset)

# CELL ********************

df.na.drop(subset=["SalePrice_USD","Address"]).show()

# MARKDOWN ********************

# #### Filling missing values

# CELL ********************

#two methods, same result
# note: the columns in the subset must be the same data type as the value, otherwise it will be ignored. 
df.na.fill(value='Unknown Address',subset=["Address"]).show()

df.fillna(value='Unknown Address',subset=["Address"]).show()

# CELL ********************

#also works for numbers 

df.na.fill(value=0,subset=["SalePrice_USD", "Address"]).show()

# MARKDOWN ********************

# #### Mean Imputation
# ![image-alt-text](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2Fd2RfVn%2FbtrvKqu0J1H%2Fmytv577YDZv43Z45TZf71K%2Fimg.png)
# https://lsann38.tistory.com/264

# CELL ********************

from pyspark.ml.feature import Imputer

# Initialize the Imputer
imputer = Imputer(
    inputCols= ['SalePrice_USD'], #specifying the input column names
    outputCols=['SalesPriceImputed_USD'], #specifying the output column names
    strategy="mean"                  # or "median" if you want to use the median value
)

# Fit the Imputer
model = imputer.fit(df)

#Transform the dataset
imputed_df = model.transform(df)

imputed_df.show()
