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

# DAG: counting_number_of_budbee_plus_membership (budbee DB - consumer)
query = """

SELECT SUM(ts.active) AS Budbee_Plus_active,
       count(DISTINCT ts.consumer_id) AS Budbee_Plus_signed,
       now() as time_stamp
FROM timeslot_subscription ts
JOIN consumer c ON ts.consumer_id = c.id
where ts.valid_to> now()
"""

counting_number_of_budbee_plus_membership_df = readJDBC(query, 'consumer')
writeSnowflake(counting_number_of_budbee_plus_membership_df, 'counting_number_of_budbee_plus_membership')


# COMMAND ----------

# DAG: app_consumer_orders_stats_by_country (how do we query from more than 1 db in Budbee DB?)
query = """
SELECT app_consumer_stats.country_code                                    AS country_code,
       count(consumer_id)                                                 AS total_app_consumers,
       count(one_order_consumer)                                          AS one_order_consumers,
       count(two_orders_consumer)                                         AS two_orders_consumers,
       count(three_and_more_orders_consumer)                              AS three_and_more_orders_consumers,
       count(one_order_consumer) * 100.0 / count(consumer_id)             AS "one_order_consumers %",
       count(two_orders_consumer) * 100.0 / count(consumer_id)            AS "two_orders_consumers %",
       count(three_and_more_orders_consumer) * 100.0 / count(consumer_id) AS "three_and_more_orders_consumers %",
       now() as time_stamp

FROM (
         SELECT
             pcz.country_code AS country_code,
             c.consumer_id    AS consumer_id,
             count(o.id)      AS consumer_orders,
             (CASE
                  WHEN count(o.id) = 1 THEN TRUE
                 END)         AS one_order_consumer,

             (CASE
                  WHEN count(o.id) = 2 THEN TRUE
                 END)         AS two_orders_consumer,

             (CASE
                  WHEN count(o.id) >= 3 THEN TRUE
                 END)         AS three_and_more_orders_consumer
         FROM consumers.consumer c
                  JOIN consumers.consumer_order co ON c.id = co.consumer_id
                  JOIN budbee.orders o ON co.order_id = o.id
                  JOIN budbee.postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id
         WHERE o.created_at >= ADDDATE(current_date,INTERVAL -12 MONTH)
         GROUP BY c.consumer_id,pcz.country_code
         ORDER BY consumer_orders DESC
     ) app_consumer_stats
GROUP BY app_consumer_stats.country_code

UNION ALL

SELECT app_consumer_stats.country_code                                    AS country_code,
       count(consumer_id)                                                 AS total_app_consumers,
       count(one_order_consumer)                                          AS one_order_consumers,
       count(two_orders_consumer)                                         AS two_orders_consumers,
       count(three_and_more_orders_consumer)                              AS three_and_more_orders_consumers,
       count(one_order_consumer) * 100.0 / count(consumer_id)             AS "one_order_consumers %",
       count(two_orders_consumer) * 100.0 / count(consumer_id)            AS "two_orders_consumers %",
       count(three_and_more_orders_consumer) * 100.0 / count(consumer_id) AS "three_and_more_orders_consumers %",
       now() as time_stamp

FROM (
         SELECT
             'All' AS country_code,
             c.consumer_id    AS consumer_id,
             count(o.id)      AS consumer_orders,
             (CASE
                  WHEN count(o.id) = 1 THEN TRUE
                 END)         AS one_order_consumer,

             (CASE
                  WHEN count(o.id) = 2 THEN TRUE
                 END)         AS two_orders_consumer,

             (CASE
                  WHEN count(o.id) >= 3 THEN TRUE
                 END)         AS three_and_more_orders_consumer
         FROM consumers.consumer c
                  JOIN consumers.consumer_order co ON c.id = co.consumer_id
                  JOIN budbee.orders o ON co.order_id = o.id
         WHERE o.created_at >= ADDDATE(current_date,INTERVAL -12 MONTH)
         GROUP BY c.consumer_id
         ORDER BY consumer_orders DESC
     ) app_consumer_stats
GROUP BY app_consumer_stats.country_code
"""

app_consumer_orders_stats_by_country_df = readJDBC(query, 'budbee')
writeSnowflake(app_consumer_orders_stats_by_country_df, 'app_consumer_orders_stats_by_country')
