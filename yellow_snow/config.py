# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, when

# COMMAND ----------

snowflake_user = dbutils.secrets.get("poc-analyst", "databricks-snowflake-user")
snowflake_password = dbutils.secrets.get("poc-analyst", "databricks-snowflake-password")

sf_options = {
  "sfUrl": "bp67618.eu-west-1.snowflakecomputing.com"
  , "sfUser": snowflake_user
  , "sfPassword": snowflake_password
  , "sfWarehouse": "dbricks_wh"
}

def readSnowflake(table):
    return spark.read \
  .format("snowflake") \
  .options(**sf_options) \
  .option("dbtable", table) \
  .load()

def writeSnowflake(df, tablename, db="dw_dev", schema="topaz"):
    df.write \
    .format("snowflake") \
    .options(**sf_options) \
    .option("dbtable", tablename) \
    .option("sfDatabase", db) \
    .option("sdSchema", schema)
    .mode("overwrite") \
    .save()

# COMMAND ----------

jdbcHostname = "production-aurora-db-03.c24yqmofl8oi.eu-west-1.rds.amazonaws.com"
jdbcPort = 3306

# TODO: refactor these into metastore to avoid plaintext passwords
jdbcUsername = dbutils.secrets.get("poc-analyst", "budbee-db-user")
jdbcPassword = dbutils.secrets.get("poc-analyst", "budbee-db-password")

def readJDBC(q, db):
    readConfig = {
        "user" : jdbcUsername
        , "password" : jdbcPassword
        , "driver" : "com.mysql.jdbc.Driver"
        , "url": f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{db}"
        , "fetchsize": 2000
        , "dbtable": f"({q}) as foo"
        , "pushDownPredicate": False
    }
    
    return spark.read.format("jdbc").options(**readConfig).load()

def readJDBC_part(q, db, partitionColumn, lowerBound, upperBound):
    readConfig = {
        "user" : jdbcUsername
        , "password" : jdbcPassword
        , "driver" : "com.mysql.jdbc.Driver"
        , "url": f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{db}"
        , "fetchsize": 2000
        , "dbtable": f"({q}) as foo"
        , "pushDownPredicate": False
        , "partitionColumn": partitionColumn
        , "lowerBound": lowerBound
        , "upperBound": upperBound
        , "numPartitions": 8
    }
    
    return spark.read.format("jdbc").options(**readConfig).load()
