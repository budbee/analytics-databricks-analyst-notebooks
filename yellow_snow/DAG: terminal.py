# Databricks notebook source
# MAGIC %md
# MAGIC # TERMINAL DAG: 1 minute
# MAGIC This notebook covers the DAGs/queries that are used at terminals
# MAGIC ## DAGs
# MAGIC * generalized_orders_booked
# MAGIC * consignment_limit
# MAGIC * routes_saved_today
# MAGIC * routing_deadline_today_all_zones

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

## EXTRACT

query = """
SELECT
    w.code,
    con.id as consignmentId,
    ad.country_code as terminaCountryCode,
    DAYNAME(con.date) AS dayOfWeek,
    pcz.title AS routeoptGroupTitle,
    ca.id AS cancellationId,
    cscon.id AS consumerStopId

FROM (SELECT * FROM consignments USE INDEX(IDX_date)
      WHERE date between CURRENT_DATE()
        AND DATE_ADD(CURRENT_DATE(), INTERVAL 5 DAY)
        AND type in ("DELIVERY", "RETURN")) AS con
JOIN orders o ON con.order_id = o.id
JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id
    AND pcz.type = "TO_DOOR"
JOIN warehouses w ON pcz.terminal_id = w.id
JOIN addresses ad on w.address_id = ad.id
LEFT JOIN cancellations AS ca on con.cancellation_id = ca.id
LEFT JOIN consumer_stop_consignments AS cscon ON cscon.consignment_id = con.id"""

df_orders_booked = readJDBC(query, 'budbee')

query2 = """
SELECT
    lim.terminal,
    lim.id,
    lim.date,
    lim.maximum_number_of_consignments,
    lim.block_direct_checkout_merchants
FROM chrono_consignment_limits lim
WHERE  lim.date >= current_date()
"""

df_consignment_limit = readJDBC(query2, 'budbee')

query3 = """
SELECT
    cscon.consignment_id AS consignmentStopId,
    cs.route_id as routeId,
    r.city as city
FROM consignments con
JOIN consumer_stop_consignments cscon on cscon.consignment_id = con.id
JOIN consumer_stops cs on cscon.consumer_stop_id = cs.id
JOIN routes r on r.id = cs.route_id
WHERE con.date  = current_date()
AND r.type = "DISTRIBUTION"
AND con.cancellation_id is null
"""

df_routes_saved = readJDBC(query3, 'budbee')

query4 = """
SELECT 
   MAX(SUBTIME(CONCAT(DATE_FORMAT(current_date(), '%Y-%m-%d'), '', cpi.collection_start),
               SEC_TO_TIME(cpi.routing_cutoff_seconds))) AS cutoff,
   cpi.collection_start,
   cpi.delivery_day,
   rg.terminal_code,
   now()                                                 AS timestamp
FROM routeopt_groups AS rg
JOIN collection_point_intervals AS cpi
    ON (cpi.routeopt_group_id = rg.id AND cpi.deleted_at IS NULL)
JOIN collection_points AS cp
    ON (cp.id = cpi.collection_point_id AND cp.deleted_at IS NULL)
JOIN buyer_collection_points AS bp
    ON bp.collection_point_id = cp.id
JOIN buyers AS b
    ON (b.id = bp.buyer_id AND b.deleted_at IS NULL)
WHERE cpi.delivery_day = UPPER(DAYNAME(current_date()))
GROUP BY rg.terminal_code
"""

df_routing_deadline_today_all_zones = readJDBC(query4, 'budbee')

# COMMAND ----------

## TRANSFORM

df_orders_booked_grouped = df_orders_booked.groupBy("code", "terminaCountryCode", "dayOfWeek", "routeoptGroupTitle").agg(F.countDistinct("consignmentId").alias("consignments"), F.countDistinct("cancellationId").alias("cancelledOrderCount"), F.countDistinct("consumerStopId").alias("stops"))

df_generalized_orders_booked = df_orders_booked_grouped.withColumn('orderCount', df_orders_booked_grouped.consignments - df_orders_booked_grouped.cancelledOrderCount).withColumn('timestamp', F.current_timestamp()).withColumn('routesSaved', df_orders_booked_grouped.stops >0).drop("consignments", "stops")

df_routes_saved_grouped = df_routes_saved.groupBy("city").agg(F.countDistinct("consignmentStopId").alias("consignments"), F.countDistinct("routeId").alias("routes"))

df_routes_saved_today = df_routes_saved_grouped.withColumn('timestamp', F.current_timestamp()).withColumn('routesSaved', df_routes_saved_grouped.consignments >0).drop("consignments")

# COMMAND ----------

## LOAD

writeSnowflake(df_generalized_orders_booked, 'generalized_orders_booked')
writeSnowflake(df_consignment_limit, 'consignment_limit')
writeSnowflake(df_routes_saved_today, 'routes_saved_today')
writeSnowflake(df_routing_deadline_today_all_zones, 'routing_deadline_today_all_zones')
