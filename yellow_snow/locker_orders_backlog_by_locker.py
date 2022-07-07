# Databricks notebook source
# MAGIC %md
# MAGIC # Locker orders backlog by order
# MAGIC This notebook runs a single very slow DAG

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# DAG: locker_orders_backlog_by_locker
query24 = """
SELECT  l.identifier,
       o.id as order_id,
       slog.id as scanning_log_id,
       CASE WHEN slog.id IS NOT NULL AND us.warehouse_id = pcz.terminal_id THEN 1 ELSE 0 END AS scanned_in_distribution_terminal,
       now() as time_stamp
FROM (select * from orders use index(IDX_order_created_at)
        where cancellation_id is null
            and created_at >= DATE_ADD(current_date(), INTERVAL -30 DAY) AS o
        JOIN lockers l on o.locker_id = l.id
		JOIN parcels AS p ON o.id = p.order_id
		JOIN postal_code_zones AS pcz ON o.delivery_postal_code_zone_id = pcz.id and pcz.type = "TO_LOCKER"
		LEFT JOIN parcel_box_assignments AS pba ON p.id = pba.parcel_id
		LEFT JOIN consignments AS c ON c.order_id = o.id
		JOIN warehouses AS w ON w.id = pcz.terminal_id
		LEFT JOIN scanning_log AS slog ON slog.id = (SELECT id FROM scanning_log WHERE parcel_id = p.id ORDER BY DATE DESC LIMIT 1)
		LEFT JOIN users AS u ON u.id = slog.user_id
		LEFT JOIN user_settings AS us ON us.id = u.user_settings_id
WHERE  pba.id IS NULL
  		AND c.id IS NULL
"""


query_sub = """
select 
    min(id) as min_id,
    max(id) as max_id
    from orders use index(IDX_order_created_at) 
    where created_at >= DATE_ADD(current_date(), INTERVAL -30 DAY)
"""

order_id_df = readJDBC(query_sub, 'budbee')
min_id = order_id_df.collect()[0][0]
max_id = order_id_df.collect()[0][1]


locker_orders_backlog_by_locker_df_temp = readJDBC_part(query24, 'budbee', "order_id", min_id, max_id, numPartitions=4)

locker_orders_backlog_by_locker_df = locker_orders_backlog_by_locker_df_temp.groupBy("identifier","time_stamp").agg(F.countDistinct("order_id").alias("orders"),
                                                                                                                    F.sum("scanned_in_distribution_terminal").alias("scanned_in_distribution_terminal"),
                                                                                                                   F.count("scanning_log_id").alias("scanned_by_budbee"))

writeSnowflake(locker_orders_backlog_by_locker_df, 'locker_orders_backlog_by_locker')
