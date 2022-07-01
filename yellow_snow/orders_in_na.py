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


query = """
SELECT DISTINCT 
       o.created_at AS order_created_date,
       b.external_name AS merchant,
       pcz.title AS city_pcz,
       w.code AS terminal_code,
       ad.country_code AS terminal_country_code,
       pcz.country_code AS destination_country_code,
       btt.tag_id,
       o.buyer_id,
       o.id AS order_id,
       p.id AS parcel_id,
       IF(pcz.type = "TO_DOOR", 'HOME', 'BOX') AS delivery_type,
       IF(p.visible = 1, last_sl.parcel_id, NULL) as scanned_parcel,
       IF(p.visible = 1,distr_sl.parcel_id, NULL) as scanned_at_destination_terminal_parcel,
       IF(p.visible = 1,destin_sl.parcel_id, NULL) as scanned_at_destination_country_parcel,
       IF(p.visible = 1,nlbe_sl.parcel_id, NULL) as scanned_at_NL_BE_country_parcel
FROM orders o USE INDEX(IDX_order_created_at) -- add index
         JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id  -- information
         JOIN buyers AS b ON b.id = o.buyer_id -- information
         JOIN warehouses w ON pcz.terminal_id = w.id -- information
         JOIN addresses ad ON w.address_id = ad.id -- information
         JOIN parcels p ON o.id = p.order_id -- information
         LEFT JOIN (
                    SELECT
                        tag_id,
                        bt.buyer_id
                    FROM tags t
                             JOIN buyer_tags bt on t.id = bt.tag_id
                    WHERE tag_id = 2) btt
                   ON btt.buyer_id = o.buyer_id -- information
          LEFT JOIN scanning_log last_sl ON last_sl.id = (
                                                SELECT scanning_log.id
                                                FROM scanning_log
                                                WHERE scanning_log.parcel_id = p.id
                                                ORDER BY scanning_log.id DESC
                                                LIMIT 1
                                                )

         LEFT JOIN scanning_log distr_sl ON distr_sl.id = (
                                                SELECT scanning_log.id
                                                FROM scanning_log
                                                JOIN users on scanning_log.user_id = users.id
                                                JOIN user_settings on users.user_settings_id = user_settings.id
                                                JOIN warehouses on user_settings.warehouse_id = warehouses.id
                                                WHERE scanning_log.parcel_id = p.id
                                                      AND warehouses.code = w.code
                                                ORDER BY scanning_log.id DESC
                                                LIMIT 1
                                                )

         LEFT JOIN scanning_log destin_sl ON destin_sl.id = (
                                                SELECT scanning_log.id
                                                FROM scanning_log
                                                         JOIN users on scanning_log.user_id = users.id
                                                         JOIN user_settings on users.user_settings_id = user_settings.id
                                                         JOIN warehouses on user_settings.warehouse_id = warehouses.id
                                                         JOIN addresses on warehouses.address_id = addresses.id
                                                WHERE scanning_log.parcel_id = p.id
                                                  AND addresses.country_code = pcz.country_code
                                                ORDER BY scanning_log.id DESC
                                                LIMIT 1
                                                )

         LEFT JOIN scanning_log nlbe_sl ON nlbe_sl.id = (
                                                SELECT scanning_log.id
                                                FROM scanning_log
                                                         JOIN users on scanning_log.user_id = users.id
                                                         JOIN user_settings on users.user_settings_id = user_settings.id
                                                         JOIN warehouses on user_settings.warehouse_id = warehouses.id
                                                         JOIN addresses on warehouses.address_id = addresses.id
                                                WHERE scanning_log.parcel_id = p.id
                                                  AND addresses.country_code in ('NL', 'BE')
                                                ORDER BY scanning_log.id DESC
                                                LIMIT 1
                                                )
WHERE o.created_at >= DATE_ADD(utc_date(), INTERVAL - 14 DAY)
  AND o.cancellation_id IS NULL
  AND ((pcz.type = "TO_DOOR"
       AND o.id NOT IN (SELECT order_id FROM consignments WHERE consignments.cancellation_id IS NULL)
       AND p.recall_requested_at IS NULL)
  OR (pcz.type = "TO_LOCKER"
      AND p.id NOT IN (SELECT parcel_id FROM parcel_box_assignments)
      AND o.id NOT IN (SELECT order_id FROM locker_consignments)
    AND o.buyer_id NOT IN ('476', '1050','1051','1052','1459')
    AND p.id NOT IN (
        SELECT p.id
        FROM routes AS r USE INDEX(IDX_due_date) -- add index
                 JOIN locker_pallets lp ON r.id = lp.route_id
                 JOIN locker_pallet_parcels lpp
                      ON lp.id = lpp.locker_pallet_id -- AND lp.locker_id = ls.locker_id
                 JOIN parcels p ON lpp.parcel_id = p.id
        WHERE r.type = "LOCKER"
          AND r.due_date = utc_date())
      ))"""

orders_in_na_df = readJDBC(query, 'budbee')

# COMMAND ----------

orders_in_na_df_grouped = orders_in_na_df.groupBy("order_created_date","merchant", "city_pcz", "terminal_code", "terminal_country_code", "destination_country_code", "tag_id", "buyer_id", "delivery_type").agg(countDistinct("order_id").alias("orders"), countDistinct("parcel_id").alias("parcels"),countDistinct("scanned_parcel").alias("scanned_parcels"),
                                   countDistinct("scanned_at_destination_terminal_parcel").alias("scanned_at_destination_terminal_parcels"),
                                   countDistinct("scanned_at_destination_country_parcel").alias("scanned_at_destination_country_parcels"),
                                   countDistinct("scanned_at_NL_BE_country_parcel").alias("scanned_at_NL_BE_country_parcels")        
                                            )
writeSnowflake(orders_in_na_df_grouped, 'orders_in_na_WIP')
