# Databricks notebook source
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

def sfWrite(df, tablename):

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

## 
query = """SELECT
                wh.created_at,
                wh.id,
                wh.name,
                ad.street,
                ad.postal_code,
                wh.code,
                ad.country_code,
                ad.coordinate,
                wh.`sorting_machine_assign_direction`,
                wh.`sorting_machine_books_delivery`,
                wh.`sorting_machine_maximum_routes_per_lane`,
                wh.`sorting_machine_minimum_routes_per_lane`,
                now() as time_stamp
            FROM warehouses as wh
            JOIN addresses as ad on wh.address_id = ad.id
            WHERE wh.deleted_at IS NULL
"""

df_terminals = readJDBC(query, 'budbee')

query2 = """SELECT
                w.code,
                con.id as consignmentId,
                ad.country_code as terminaCountryCode,
                DAYNAME(con.date) AS dayOfWeek,
                pcz.title AS routeoptGroupTitle,
                ca.id AS cancelledOrder
            
            FROM (SELECT * FROM consignments USE INDEX(IDX_date)
                  WHERE date between CURRENT_DATE()
                    AND DATE_ADD(CURRENT_DATE(), INTERVAL 5 DAY)
                    AND type in ("DELIVERY", "RETURN")) AS con
                JOIN orders AS o ON con.order_id = o.id
                JOIN postal_code_zones AS pcz ON o.delivery_postal_code_zone_id = pcz.id 	AND pcz.type ="TO_DOOR"
                JOIN warehouses w ON pcz.terminal_id = w.id
                JOIN addresses ad on w.address_id = ad.id
                LEFT JOIN cancellations AS ca on con.cancellation_id = ca.id
                LEFT JOIN consumer_stop_consignments AS cscon ON cscon.consignment_id = con.id"""

df_orders_booked = readJDBC(query2, 'budbee')

query3 = """SELECT
                lim.terminal,
                lim.id,
                lim.date,
                lim.maximum_number_of_consignments,
                lim.block_direct_checkout_merchants
            FROM chrono_consignment_limits as lim
            WHERE  lim.date >= current_date()
"""

df_consignment_limit = readJDBC(query3, 'budbee')

df_consignment_limit.display()


# COMMAND ----------



