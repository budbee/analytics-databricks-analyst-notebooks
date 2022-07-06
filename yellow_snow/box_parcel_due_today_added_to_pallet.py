# Databricks notebook source
# MAGIC %md
# MAGIC # Box Parcel Due Today Added to Pallet
# MAGIC Another slow DAG

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# DAG: box_parcel_due_today_added_to_pallet
# Currently takes 3-4 mins to run



query27 = """
SELECT lc.id as consignment_id,
       p.id as parcel_id,
       lpp.id as locker_pallet_parcel_id,
       w.code as terminal,
       utc_timestamp as time_stamp
FROM (select locker_consignments.id,
            locker_consignments.order_id
     from locker_consignments
        JOIN intervals i on locker_consignments.estimated_interval_id = i.id
        JOIN timestamps t on i.start_timestamp_id = t.id
    WHERE binary locker_consignments.consignment_type in ('DELIVERY', 'RETURN')
        AND locker_consignments.cancellation_id IS NULL) lc
        JOIN orders AS o ON o.id = lc.order_id
        JOIN postal_code_zones AS pcz ON pcz.id = o.delivery_postal_code_zone_id
        JOIN warehouses as w on pcz.terminal_id = w.id
        LEFT JOIN parcels p on (p.order_id = o.id)
        LEFT JOIN locker_pallet_parcels as lpp on p.id = lpp.parcel_id
"""

query_sub = """
select 
    min(lc.id) as min_id,
    max(lc.id) as max_id
    from locker_consignments lc
        JOIN intervals i on lc.estimated_interval_id = i.id
        JOIN timestamps t on i.start_timestamp_id = t.id
    where DATE(t.date) = utc_date()
"""

lc_id_df = readJDBC(query_sub, 'budbee')
min_id = lc_id_df.collect()[0][0]
max_id = lc_id_df.collect()[0][1]

box_parcel_due_today_added_to_pallet_df_temp = readJDBC_part(query27, 'budbee', "consignment_id", min_id, max_id, numPartitions=4)
box_parcel_due_today_added_to_pallet_df = box_parcel_due_today_added_to_pallet_df_temp.groupby("terminal","time_stamp").agg(F.countDistinct("consignment_id").alias("consignments"),
                                                                                                                             F.countDistinct("parcel_id").alias("parcels_due"),
                                                                                                                             F.countDistinct("locker_pallet_parcel_id").alias("parcel_added_to_pallet"))
                                                                                                                            
writeSnowflake(box_parcel_due_today_added_to_pallet_df, 'box_parcel_due_today_added_to_pallet')
