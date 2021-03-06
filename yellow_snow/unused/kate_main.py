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
            o.created_at, 
            external_name, 
            o.buyer_id,
            pcz.title             AS city_pcz,
            w.code                AS terminal_code,
            ad.country_code       AS terminal_country_code,
            pcz.country_code      AS destination_country_code,
            o.id           AS order_id,
            p.id AS parcel_id,
            p.visible,
            pcz.type as delivery_type,
            pba.id as parcel_box_assignment_id,
            lc.id as locker_consignment_id,
            c.id as consignment_id,
            c.cancellation_id as consignment_cancellation_id,
            p.recall_requested_at
            
         FROM orders o USE INDEX(IDX_order_created_at)
         JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id
         JOIN buyers AS b ON b.id = o.buyer_id 
         JOIN warehouses w ON pcz.terminal_id = w.id 
         JOIN addresses ad ON w.address_id = ad.id 
         JOIN parcels AS p ON o.id = p.order_id -- information
         LEFT JOIN parcel_box_assignments AS pba ON p.id = pba.parcel_id -- information
         LEFT JOIN consignments AS c ON c.order_id = o.id -- information
         LEFT JOIN locker_consignments lc ON lc.order_id = o.id -- information
         WHERE o.created_at > Date_add(Current_date, Interval -14 Day)
           AND o.cancellation_id IS NULL
           """

df_main = readJDBC(query, 'budbee')

query2 = """SELECT tag_id, tag_type_id, t.name, bt.buyer_id
                    FROM tags t
                             JOIN buyer_tags bt on t.id = bt.tag_id
                    WHERE tag_id = 2"""

df_buyer_tags = readJDBC(query2, 'budbee')

query3 = """SELECT p.id
            FROM routes AS r USE INDEX(IDX_due_date) -- add index
             JOIN locker_pallets lp ON r.id = lp.route_id
             JOIN locker_pallet_parcels lpp
                  ON lp.id = lpp.locker_pallet_id -- AND lp.locker_id = ls.locker_id
             JOIN parcels p ON lpp.parcel_id = p.id
            WHERE r.type = "LOCKER"
             AND r.due_date = utc_date()"""

df_locker_pallets = readJDBC(query3, 'budbee')

query4 = """SELECT DATE_FORMAT(o.created_at, "%Y-%m-%d %H:%i:%s.%f")          AS order_created_date,
       b.external_name       AS merchant,
       pcz.title             AS city_pcz,
       w.code                AS terminal_code,
       ad.country_code       AS terminal_country_code,
       pcz.country_code      AS destination_country_code,
       btt.tag_id,
       btt.buyer_id,
       btt.name,
       o.id           AS orders,
       p.id AS parcels,
       p.visible,
       'HOME'                AS delivery_type,
       DATE_FORMAT(utc_timestamp() , "%Y-%m-%d %H:%i:%s.%f")                 AS time_stamp
FROM orders o USE INDEX(IDX_order_created_at) -- add index
         JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id   AND binary pcz.type = "TO_DOOR" -- information
         JOIN buyers AS b ON b.id = o.buyer_id -- information
         JOIN warehouses w ON pcz.terminal_id = w.id -- information
         JOIN addresses ad ON w.address_id = ad.id -- information
         LEFT JOIN parcels p on o.id = p.order_id -- information
         LEFT JOIN (SELECT tag_id, tag_type_id, t.name, bt.buyer_id
                    FROM tags t
                             JOIN buyer_tags bt on t.id = bt.tag_id
                    WHERE tag_id = 2) btt
                   ON btt.buyer_id = o.buyer_id -- information
WHERE o.created_at >= DATE_ADD(utc_date(), INTERVAL - 14 DAY)
  AND o.cancellation_id IS NULL
  AND o.id NOT IN (SELECT order_id FROM consignments WHERE consignments.cancellation_id IS NULL)
  AND p.recall_requested_at IS NULL
UNION ALL
SELECT DATE_FORMAT(o.created_at, "%Y-%m-%d %H:%i:%s.%f")     AS order_created_date,
       b.external_name       AS merchant,
       pcz.title             AS city_pcz,
       w.code                AS terminal_code,
       ad.country_code       AS terminal_country_code,
       pcz.country_code      AS destination_country_code,
       btt.tag_id,
       btt.buyer_id,
       btt.name,
       o.id           AS orders,
       p.id AS parcels,
       p.visible,
       'BOX'                AS delivery_type,
       DATE_FORMAT(utc_timestamp(), "%Y-%m-%d %H:%i:%s.%f")                  AS time_stamp

FROM orders AS o USE INDEX(IDX_order_created_at) -- add index
         JOIN postal_code_zones AS pcz ON o.delivery_postal_code_zone_id = pcz.id and binary pcz.type = "TO_LOCKER" -- information
         JOIN buyers AS b ON b.id = o.buyer_id -- information
         JOIN warehouses w ON pcz.terminal_id = w.id -- information
         JOIN addresses ad ON w.address_id = ad.id -- information
         LEFT JOIN parcels AS p ON o.id = p.order_id -- information
         LEFT JOIN parcel_box_assignments AS pba ON p.id = pba.parcel_id -- information
         LEFT JOIN consignments AS c ON c.order_id = o.id -- information
         LEFT JOIN locker_consignments lc ON lc.order_id = o.id -- information
         LEFT JOIN (SELECT tag_id, tag_type_id, t.name, bt.buyer_id
                    FROM tags t
                             JOIN buyer_tags bt on t.id = bt.tag_id
                    WHERE tag_id = 2) btt
                   ON btt.buyer_id = o.buyer_id -- information

WHERE  o.created_at >= DATE_ADD(utc_date(), INTERVAL - 14 DAY)
  AND o.cancellation_id IS NULL
  AND pba.id IS NULL
  AND lc.id IS NULL
  AND o.buyer_id NOT IN ('476', '1050','1051','1052','1459')
  AND p.id NOT IN (
    SELECT p.id
    FROM routes AS r USE INDEX(IDX_due_date) -- add index
             JOIN locker_pallets lp ON r.id = lp.route_id
             JOIN locker_pallet_parcels lpp
                  ON lp.id = lpp.locker_pallet_id -- AND lp.locker_id = ls.locker_id
             JOIN parcels p ON lpp.parcel_id = p.id
    WHERE r.type = "LOCKER"
      AND r.due_date = utc_date())"""

df_origin = readJDBC(query4, 'budbee')



# COMMAND ----------


df_main_with_tags = df_main.join(df_buyer_tags, (df_buyer_tags.buyer_id == df_main.buyer_id), how = 'left')

df_main_locker_pallets_removed = df_main_with_tags.join(df_locker_pallets, (df_main_with_tags.parcel_id == df_locker_pallets.id), how = 'left_anti').cache()

df_main_home_clear = df_main_locker_pallets_removed.where(
                                                            col('recall_requested_at').isNull()
)



sfWrite(df_origin, 'test_table')




# COMMAND ----------


