# Databricks notebook source
# MAGIC %md
# MAGIC # 4 Hours
# MAGIC This notebook covers the DAGs/queries that have a 4 hour update frequency
# MAGIC ## DAGs
# MAGIC * on_demand_pickups
# MAGIC * on_demand_returns

# COMMAND ----------

# MAGIC %run ./config

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
    AND c.date >= DATE_ADD(current_date, INTERVAL -7 day) -- reduce amount of data fetch from 12 months to 7 days, since Klipfolio only show data Today and tomorrow
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
