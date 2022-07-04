# Databricks notebook source
# Refresh every 10 mins
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import *

# COMMAND ----------

snowflake_user = dbutils.secrets.get("poc-analyst", "databricks-snowflake-user")
snowflake_password = dbutils.secrets.get("poc-analyst", "databricks-snowflake-password")
snowflake_database = "dw_dev" # <---- you probably want to change this
snowflake_schema = "topaz" # <------ and this

sf_options = {
  "sfUrl": "bp67618.eu-west-1.snowflakecomputing.com"
  , "sfUser": snowflake_user
  , "sfPassword": snowflake_password
  , "sfDatabase": snowflake_database
  , "sfSchema": snowflake_schema
  , "sfWarehouse": "dbricks_wh"
}

def readSnowflake(table):
    return spark.read \
  .format("snowflake") \
  .options(**sf_options) \
  .option("dbtable", table) \
  .load()

def writeSnowflake(df, tablename):

    df.write \
    .format('snowflake') \
    .options(**sf_options) \
    .option("dbtable", tablename) \
    .mode('overwrite') \
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
    }
    
    return spark.read.format("jdbc").options(**readConfig).load()


# COMMAND ----------

# DAG: on_demand_pickups
query = """
select date_format(c.date, "%Y-%m-%d"),
       pc.city,
       b.external_name,
       count(c.id),
       sum(odp.estimated_parcels),
       o.token,
       now() as timestamp
from consignments as c
         join orders as o on o.id = c.order_id
         join on_demand_pickups as odp on odp.order_id = o.id
         join postal_code_zones as pc on pc.id = o.delivery_postal_code_zone_id
         join buyers as b on b.id = o.buyer_id
where c.cancellation_id is null
    AND o.cancellation_id is null
    AND c.date >= DATE_ADD(current_date, INTERVAL -12 Month)
group by c.date, pc.city, b.id
"""

on_demand_pickups_df = readJDBC(query, 'budbee')
writeSnowflake(on_demand_pickups_df, 'on_demand_pickups')


# COMMAND ----------

# DAG: on_demand_returns
query = """
SELECT b.id,
       b.external_name,
       o.token,
       p.recall_requested_at,
       p.type,
       now()
FROM buyers AS b
         JOIN buyer_settings AS bs ON bs.id = b.buyer_settings_id
         JOIN orders AS o ON o.buyer_id = b.id
         JOIN parcels AS p ON p.order_id = o.id
WHERE bs.on_demand_pickup = 1
  AND b.id != 11
  AND ((p.type = "RETURN" and p.created_at > date_sub(curdate(), INTERVAL 7 DAY)) or
       (p.recall_requested_at = date_sub(curdate(), INTERVAL 7 DAY)))
"""

on_demand_returns_df = readJDBC(query, 'budbee')
writeSnowflake(on_demand_returns_df, 'on_demand_returns')
