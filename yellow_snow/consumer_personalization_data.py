# Databricks notebook source
# MAGIC %md
# MAGIC # Consumer Personalization
# MAGIC This notebook pulls set of consumers with orders last 6 months and Merchant info that ML team can create a model/matrix to recommend the best merchants/categories related to consumer's interest.
# MAGIC 
# MAGIC This contains consumers registered through our Budbee app

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# Extract consumer with app
query = """
select c.consumer_id,
       b.external_name as merchant_name,
       o.id as order_id,
       pcz.country_code,
       p.id as parcel_id
FROM consumers.consumer c
    JOIN consumers.consumer_order co ON c.id = co.consumer_id
    JOIN budbee.orders o use index(IDX_order_created_at) ON co.order_id = o.id
    JOIN buyers b on o.buyer_id = b.id
    JOIN budbee.postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id
    JOIN parcels p on o.id = p.order_id
WHERE o.created_at >= ADDDATE(current_date,INTERVAL -6 MONTH)
"""


query_sub = """
select 
    min(id) as min_id,
    max(id) as max_id
    from orders use index(IDX_order_created_at)
    where created_at >= ADDDATE(current_date,INTERVAL -6 MONTH)
"""

order_id_df = readJDBC(query_sub, 'budbee')
min_id = order_id_df.collect()[0][0]
max_id = order_id_df.collect()[0][1]
consumer_df = readJDBC_part(query, 'budbee', "order_id", min_id, max_id, numPartitions=4)

# Group by consumer, merchant, country
consumer_orders_by_merchant_df = consumer_df.groupBy("consumer_id","merchant_name","country_code").agg(F.countDistinct("order_id").alias("orders"),F.count("parcel_id").alias("parcels"))

writeSnowflake(consumer_orders_by_merchant_df, 'consumer_orders_by_merchant', 'machine_learning','main')
