# Databricks notebook source
# Set refresh rate at 10 mins
# This covers the DAGs/queries that are refreshed equal/less than 30 mins in Klipfolio

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# DAG: e_commerce_todos 
query = """
SELECT count(con.id)          AS dorapps,
       MAX(DAYNAME(con.date)) AS dayOfWeek,
       "Routeopt group name"  AS routeoptGroupTitle,
       count(canc.id)         AS cancelledDorappsCount,
       CASE
         WHEN SUM(cscon.id) > 0
           THEN TRUE
         ELSE FALSE
         END                  AS routesSaved,
       w.code,
       ad.country_code AS terminal_country_code,
       now() as time_stamp
FROM consignments AS con USE INDEX(IDX_date) -- add index to increase performance
       LEFT JOIN parcel_consignments AS pcon ON pcon.consignment_id = con.id
       JOIN orders AS o ON con.order_id = o.id
       JOIN postal_code_zones AS pcz ON o.delivery_postal_code_zone_id = pcz.id AND pcz.type = "TO_DOOR"
       LEFT JOIN cancellations AS canc ON con.cancellation_id = canc.id
       JOIN buyers AS b ON o.buyer_id = b.id
       LEFT JOIN consumer_stop_consignments AS cscon ON cscon.consignment_id = con.id
       JOIN warehouses w ON pcz.terminal_id = w.id
       JOIN addresses ad ON w.address_id = ad.id
WHERE con.date between current_date() and date_add(current_date(), interval 5 day)
     and (canc.date > con.deadline  or canc.id is null)
  AND con.type IN ("DELIVERY", "RETURN")
GROUP BY w.code, con.date
ORDER BY pcz.country_code, pcz.city DESC, con.date ASC, w.id"""

e_commerce_todos_df = readJDBC(query, 'budbee')
writeSnowflake(e_commerce_todos_df, 'e_commerce_todos')

# COMMAND ----------

# DAG: total_count_of_parcels_in_locker_routes_by_status_route_locker_merchant
query2 = """
SELECT count(con.id)          AS dorapps,
       MAX(DAYNAME(con.date)) AS dayOfWeek,
       "Routeopt group name"  AS routeoptGroupTitle,
       count(canc.id)         AS cancelledDorappsCount,
       CASE
         WHEN SUM(cscon.id) > 0
           THEN TRUE
         ELSE FALSE
         END                  AS routesSaved,
       w.code,
       ad.country_code AS terminal_country_code,
       now() as time_stamp
FROM consignments AS con USE INDEX(IDX_date) -- add index to increase performance
       LEFT JOIN parcel_consignments AS pcon ON pcon.consignment_id = con.id
       JOIN orders AS o ON con.order_id = o.id
       JOIN postal_code_zones AS pcz ON o.delivery_postal_code_zone_id = pcz.id AND pcz.type = "TO_DOOR"
       LEFT JOIN cancellations AS canc ON con.cancellation_id = canc.id
       JOIN buyers AS b ON o.buyer_id = b.id
       LEFT JOIN consumer_stop_consignments AS cscon ON cscon.consignment_id = con.id
       JOIN warehouses w ON pcz.terminal_id = w.id
       JOIN addresses ad ON w.address_id = ad.id
WHERE con.date between current_date() and date_add(current_date(), interval 5 day)
     and (canc.date > con.deadline  or canc.id is null)
  AND con.type IN ("DELIVERY", "RETURN")
GROUP BY w.code, con.date
ORDER BY pcz.country_code, pcz.city DESC, con.date ASC, w.id"""

planned_dorapp_box_df = readJDBC(query2, 'budbee')
writeSnowflake(planned_dorapp_box_df, 'planned_dorapp_box')

# COMMAND ----------

# DAG: orders_created_last_48_hours_per_day_in_ecommerce
query3 = """
select
    count(o.id),
    CONVERT_TZ(o.created_at, "UTC", "Europe/Stockholm") as order_created_at,
    now() as time_stamp
from orders as o
    join buyers as b on o.buyer_id = b.id
    join buyer_tags as bt on b.id = bt.buyer_id
where bt.tag_id = 2
    and o.cancellation_id is null
    and o.created_at > DATE_SUB(current_time(), INTERVAL 48 HOUR)
GROUP BY DAY(o.created_at), HOUR(o.created_at)
order by o.created_at asc"""

orders_created_last_48_hours_per_day_in_ecommerce_df = readJDBC(query3, 'budbee')
writeSnowflake(orders_created_last_48_hours_per_day_in_ecommerce_df, 'orders_created_last_48_hours_per_day_in_ecommerce')

# COMMAND ----------

# DAG: orders_created_per_day_per_city
query4 = """
SELECT
    count(o.id),
    date(o.created_at),
    w.code,
    pcz.country_code,
    now() as time_stamp
FROM orders AS o USE INDEX(IDX_order_created_at) -- add index to increase performance
  	JOIN buyers AS b ON o.buyer_id = b.id
  	JOIN postal_code_zones AS pcz ON pcz.id = o.delivery_postal_code_zone_id
    JOIN warehouses w ON pcz.terminal_id = w.id
WHERE o.cancellation_id IS NULL
  	AND o.created_at > DATE_SUB(CURRENT_DATE(), INTERVAL 14 day)
GROUP BY date(o.created_at), w.code
ORDER BY o.created_at desc
"""

orders_created_per_day_per_city_df = readJDBC(query4, 'budbee')
writeSnowflake(orders_created_per_day_per_city_df, 'orders_created_per_day_per_city')

# COMMAND ----------

# DAG: cancellation_per_day
query_sub = """
select 
    min(id) as min_id,
    max(id) as max_id
    from routes
    where due_date > DATE_SUB(utc_date(),INTERVAL 2 MONTH)
"""


route_id_df = readJDBC(query_sub, 'budbee')
min_id = route_id_df.collect()[0][0]
max_id = route_id_df.collect()[0][1]


query_home = """
SELECT
    c.id as cancellation_id,
    con.id as consignment_id,
    con.date,
    r.due_date,
    r.id as route_id,
    'Home' as delivery_type,
    now() as time_stamp
FROM routes as r USE INDEX(IDX_due_date) -- add index but still run a bit slow
         JOIN consumer_stops cs ON cs.route_id = r.id -- infromation
         JOIN consumer_stop_consignments csc ON csc.consumer_stop_id = cs.id -- restriction, only home delivery stops
         JOIN consignments con ON con.id= csc.consignment_id -- restriction, only orders with consignments
         JOIN orders o ON con.order_id = o.id -- infromation
         JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id AND pcz.type = "TO_DOOR" -- restriction, to door only
         LEFT JOIN cancellations c ON c.id = con.cancellation_id AND c.cancellation_category NOT IN ("REQUESTED_BY_TRANSPORT_BUYER", "DELAYED_FROM_MERCHANT", "DELAYED_FROM_MERCHANT", "CONSUMER_WANTS_TO_CANCEL_CONSIGNMENT", "WRONG_OR_INCOMPLETE_ADDRESS", "DAMAGED_PARCEL_BY_MERCHANT", "CONSUMER_WANTS_TO_CANCEL_BOX_RESERVATION")
 -- infromation
WHERE
  r.due_date > DATE_SUB(utc_date(),INTERVAL 2 MONTH)
  AND r.type="DISTRIBUTION"
"""

Home_cancellation_df = readJDBC_part(query_home, 'budbee', "route_id", min_id, max_id)

query_box = """
SELECT
    c.id as cancellation_id,
    con.id as consignment_id,
    date(t.date) as date,
    r.due_date,
    r.id as route_id,
    'Box' as delivery_type,
    now() as time_stamp
FROM routes as r USE INDEX(IDX_due_date) -- add index but still run a bit slow
         JOIN consumer_stops cs ON r.id = cs.route_id -- infromation
         JOIN locker_stops ls ON cs.id = ls.stop_id -- restriction, only box delivery stops
         JOIN locker_pallets lp ON r.id = lp.route_id -- infromation
         JOIN locker_pallet_parcels lpp ON lp.id = lpp.locker_pallet_id AND lp.locker_id = ls.locker_id -- infromation
         JOIN parcels p ON lpp.parcel_id = p.id -- infromation
         JOIN orders o ON p.order_id = o.id -- infromation
         JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id AND pcz.type = "TO_LOCKER" -- restriction, to box only
         JOIN locker_consignments con ON con.order_id = o.id  -- restriction, only orders with consignments
         JOIN intervals i on con.estimated_interval_id = i.id  -- infromation
         JOIN timestamps t on t.id = i.start_timestamp_id  -- infromation
         LEFT JOIN cancellations c ON c.id = con.cancellation_id AND c.cancellation_category NOT IN ("REQUESTED_BY_TRANSPORT_BUYER", "DELAYED_FROM_MERCHANT", "DELAYED_FROM_MERCHANT", "CONSUMER_WANTS_TO_CANCEL_CONSIGNMENT", "WRONG_OR_INCOMPLETE_ADDRESS", "DAMAGED_PARCEL_BY_MERCHANT", "CONSUMER_WANTS_TO_CANCEL_BOX_RESERVATION")
 -- infromation
WHERE
        r.due_date > DATE_SUB(utc_date(),INTERVAL 2 MONTH)
  AND r.type="LOCKER"
"""


Box_cancellation_df = readJDBC_part(query_box, 'budbee', "route_id", min_id, max_id)

union_df = Home_cancellation_df.union(Box_cancellation_df)


cancellation_per_day_df = union_df.groupby("due_date","delivery_type","time_stamp").agg(F.countDistinct("cancellation_id")/F.countDistinct("consignment_id").alias("cancellation_percent")).orderBy("due_date")

writeSnowflake(cancellation_per_day_df, 'cancellation_per_day')

# COMMAND ----------

# DAG: miss_category_per_city
query5 = """

SELECT w.name                                                                       as terminal_name,
       sum(case when miss.category = "CANNOT_FIND_ADDRESS" then 1 else 0 end)       as CANNOT_FIND_ADDRESS,
       sum(case when miss.category = "CANNOT_FIND_DOOR" then 1 else 0 end)          as CANNOT_FIND_DOOR,
       sum(case when miss.category = "CANNOT_FIND_PARCEL" then 1 else 0 end)        as CANNOT_FIND_PARCEL,
       sum(case when miss.category = "CONSUMER_REJECTS_DELIVERY" then 1 else 0 end) as CONSUMER_REJECTS_DELIVERY,
       sum(case when miss.category = "DOOR_CODE_DOES_NOT_WORK" then 1 else 0 end)   as DOOR_CODE_DOES_NOT_WORK,
       sum(case when miss.category = "NOBODY_HOME" then 1 else 0 end)               as NOBODY_HOME,
       sum(case when miss.category = "OTHER" then 1 else 0 end)                     as OTHER,
       sum(case when miss.category = "WRONG_RECIPIENT" then 1 else 0 end)           as WRONG_RECIPIENT,
       count(con.id)                                                                as consignment_total,
       con.date                                                                     as date,
       count(miss.id)                                                               as miss_total,
       now()                                                                        as time_stamp,
       sum(case when miss.category = "OFFICE_ADDRESS_CLOSED" then 1 else 0 end)     as OFFICE_ADDRESS_CLOSED,
       ad.country_code                                                              as terminal_country_code,
       'Home'                                                                       as delivery_type
FROM routes AS r USE INDEX(IDX_due_date)
    JOIN consumer_stops AS cs ON cs.route_id = r.id -- infromation
    JOIN consumer_stop_consignments AS csc ON cs.id = csc.consumer_stop_id  -- restriction, only home delivery stops
    JOIN consignments AS con ON csc.consignment_id = con.id  -- restriction, only rows with consingments
    LEFT JOIN misses AS miss ON con.miss_id = miss.id -- infromation
    JOIN orders AS o ON con.order_id = o.id -- infromation
    JOIN postal_code_zones AS pcz ON pcz.id = o.delivery_postal_code_zone_id -- infromation
    JOIN warehouses AS w ON pcz.terminal_id = w.id -- infromation
    JOIN addresses AS ad on w.address_id = ad.id -- infromation
WHERE r.due_date = date(date_sub(now(), interval 10 hour))
  AND r.type = "DISTRIBUTION"
  AND pcz.type = "TO_DOOR"
  and con.type in ("DELIVERY", "RETURN")
GROUP BY w.id

UNION

SELECT w.name                                                                       as terminal_name,
       sum(case when miss.category = "CANNOT_FIND_ADDRESS" then 1 else 0 end)       as CANNOT_FIND_ADDRESS,
       sum(case when miss.category = "CANNOT_FIND_DOOR" then 1 else 0 end)          as CANNOT_FIND_DOOR,
       sum(case when miss.category = "CANNOT_FIND_PARCEL" then 1 else 0 end)        as CANNOT_FIND_PARCEL,
       sum(case when miss.category = "CONSUMER_REJECTS_DELIVERY" then 1 else 0 end) as CONSUMER_REJECTS_DELIVERY,
       sum(case when miss.category = "DOOR_CODE_DOES_NOT_WORK" then 1 else 0 end)   as DOOR_CODE_DOES_NOT_WORK,
       sum(case when miss.category = "NOBODY_HOME" then 1 else 0 end)               as NOBODY_HOME,
       sum(case when miss.category = "OTHER" then 1 else 0 end)                     as OTHER,
       sum(case when miss.category = "WRONG_RECIPIENT" then 1 else 0 end)           as WRONG_RECIPIENT,
       count(con.id)                                                                as consignment_total,
    date(t.date) as date,
    count(miss.id)                                                               as miss_total,
    now()                                                                        as time_stamp,
    sum(case when miss.category = "OFFICE_ADDRESS_CLOSED" then 1 else 0 end)     as OFFICE_ADDRESS_CLOSED,
    ad.country_code                                                              as terminal_country_code,
    'Box'                                                                        as delivery_type
FROM routes as r USE INDEX(IDX_due_date)
    JOIN consumer_stops cs ON r.id = cs.route_id -- infromation
    JOIN locker_stops ls ON cs.id = ls.stop_id -- restriction, only box delivery stops
    JOIN locker_pallets lp ON r.id = lp.route_id -- infromation
    JOIN locker_pallet_parcels lpp ON lp.id = lpp.locker_pallet_id AND lp.locker_id = ls.locker_id -- infromation
    JOIN parcels p ON lpp.parcel_id = p.id -- infromation
    JOIN orders o ON p.order_id = o.id -- infromation
    JOIN locker_consignments AS con ON con.order_id = o.id -- restriction, only rows with consingments
    JOIN intervals i on con.estimated_interval_id = i.id -- infromation
    JOIN timestamps t on t.id = i.start_timestamp_id -- infromation
    LEFT JOIN misses AS miss ON con.miss_id = miss.id -- infromation
    JOIN postal_code_zones AS pcz ON pcz.id = o.delivery_postal_code_zone_id -- infromation
    JOIN warehouses AS w ON pcz.terminal_id = w.id -- infromation
    JOIN addresses AS ad on w.address_id = ad.id -- infromation
WHERE r.due_date = date(date_sub(now(), interval 10 hour))
  AND r.type = "LOCKER"
  AND pcz.type = "TO_LOCKER"
GROUP BY w.id"""

miss_category_per_city_df = readJDBC(query5, 'budbee')

writeSnowflake(miss_category_per_city_df, 'miss_category_per_city')

# COMMAND ----------

# DAG: cancellation_category_per_date_and_city
query6 = """

SELECT w.name                                                                                      as terminal_name,
       sum(CASE
               WHEN can.cancellation_category = "CONSUMER_WANTS_TO_CANCEL_CONSIGNMENT"
                   or can.cancellation_category = "CONSUMER_WANTS_TO_CANCEL_BOX_RESERVATION"
                   THEN 1
               ELSE 0 END)                                                                         AS CONSUMER_WANTS_TO_CANCEL_CONSIGNMENT,
       sum(CASE WHEN can.cancellation_category = "DAMAGED_PARCEL_BY_BUDBEE" THEN 1 ELSE 0 END)     AS DAMAGED_BUDBEE,
       sum(CASE WHEN can.cancellation_category = "DAMAGED_PARCEL_BY_COURIER" THEN 1 ELSE 0 END)    AS DAMAGED_COURIER,
       sum(CASE WHEN can.cancellation_category = "DELAYED_DRIVER" THEN 1 ELSE 0 END)               AS DELAYED_DRIVER,
       sum(CASE
               WHEN can.cancellation_category = "DELAYED_FROM_MERCHANT" THEN 1
               ELSE 0 END)                                                                         AS DELAYED_FROM_MERCHANT,
       sum(CASE WHEN can.cancellation_category = "ORDER_MOVED" THEN 1 ELSE 0 END)                  AS ORDER_MOVED,
       sum(CASE WHEN can.cancellation_category = "OTHER" THEN 1 ELSE 0 END)                        AS OTHER,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_IN_WRONG_CITY" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_IN_WRONG_CITY,
       sum(CASE
               WHEN can.cancellation_category = "REQUESTED_BY_MERCHANT" THEN 1
               ELSE 0 END)                                                                         AS REQUESTED_BY_MERCHANT,
       sum(CASE WHEN can.cancellation_category = "REQUESTED_BY_TRANSPORT_BUYER" THEN 1 ELSE 0 END) AS transport_buyer,
       sum(CASE WHEN can.cancellation_category = "WRONG_OR_INCOMPLETE_ADDRESS" THEN 1 ELSE 0 END)  AS address,
       sum(CASE WHEN can.cancellation_category = "WRONG_SORTED_PARCEL" THEN 1 ELSE 0 END)          AS wrong_sorted,
       sum(CASE WHEN can.cancellation_category = "CANCELLED_ROUTE" THEN 1 ELSE 0 END)              AS CANCELLED_ROUTE,
       sum(CASE
               WHEN can.cancellation_category = "PREMATURE_ROUTE_FINISH" THEN 1
               ELSE 0 END)                                                                         AS PREMATURE_ROUTE_FINISH,
       sum(CASE
               WHEN can.cancellation_category = "DOES_NOT_FIT_IN_VEHICLE" THEN 1
               ELSE 0 END)                                                                         AS DOES_NOT_FIT_IN_VEHICLE,
       sum(CASE
               WHEN can.cancellation_category = "CONVERTED_TO_HOME_DELIVERY"
                   or can.cancellation_category = "CONSUMER_WANTS_TO_CHANGE_DELIVERY_TYPE" THEN 1
               ELSE 0 END)                                                                         AS CONSUMER_WANTS_TO_CHANGE_DELIVERY_TYPE,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_NOT_ADDED_TO_ROUTE" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_NOT_ADDED_TO_ROUTE,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_NEVER_ATTEMPTED_TO_ASSIGN" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_NEVER_ATTEMPTED_TO_ASSIGN,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_NOT_PICKED_UP_BY_DRIVER" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_NOT_PICKED_UP_BY_DRIVER,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_TOO_LARGE" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_TOO_LARGE,
       sum(CASE
               WHEN can.cancellation_category = "RESERVATION_EXPIRED" THEN 1
               ELSE 0 END)                                                                         AS RESERVATION_EXPIRED,
       sum(CASE
               WHEN can.cancellation_category = "ROUTE_REACHED_MAX_CAPACITY" THEN 1
               ELSE 0 END)                                                                         AS ROUTE_REACHED_MAX_CAPACITY,
       sum(CASE
               WHEN can.cancellation_category = "LOCKER_IS_FULL" THEN 1
               ELSE 0 END)                                                                         AS LOCKER_IS_FULL,
       sum(CASE
               WHEN can.cancellation_category = "DAMAGED_PARCEL_BY_MERCHANT" THEN 1
               ELSE 0 END)                                                                         AS DAMAGED_PARCEL_BY_MERCHANT,
       sum(CASE
               WHEN can.cancellation_category = "VEHICLE_MALFUNCTION" THEN 1
               ELSE 0 END)                                                                         AS VEHICLE_MALFUNCTION,
       sum(CASE
               WHEN can.cancellation_category = "RECIPIENT_AGE_TOO_LOW" THEN 1
               ELSE 0 END)                                                                         AS RECIPIENT_AGE_TOO_LOW,
       sum(CASE
               WHEN can.cancellation_category = "BOX_WONT_OPEN" THEN 1
               ELSE 0 END)                                                                         AS BOX_WONT_OPEN,
       sum(CASE
               WHEN can.cancellation_category = "BOX_IS_EMPTY" THEN 1
               ELSE 0 END)                                                                         AS BOX_IS_EMPTY,
       count(can.id)                                                                               AS total_cancellations,
       count(con.id)                                                                               AS total_consignments,
       con.date                                                                                    AS date,
       a.country_code,
       'Home' as delivery_type,
       now() as timestamp,
       COUNT(CASE
                 WHEN HOUR(CONVERT_TZ(can_budbee.date, 'UTC', ro.timezone)) <= 15 AND can_budbee.id IS NOT NULL
                     THEN can_budbee.id END)                                                       AS budbee_before_16,
       COUNT(CASE
                 WHEN (HOUR(CONVERT_TZ(can_budbee.date, 'UTC', ro.timezone)) >= 16 AND
                       HOUR(CONVERT_TZ(can_budbee.date, 'UTC', ro.timezone)) <= 17) AND can_budbee.id IS NOT NULL
                     THEN can_budbee.id
                 ELSE NULL END)                                                                    AS budbee_between_16_18,
       COUNT(
               CASE
                   WHEN (HOUR(CONVERT_TZ(can_budbee.date, 'UTC', ro.timezone)) >= 18) AND can_budbee.id IS NOT NULL
                       THEN can_budbee.id END)                                                     AS budbee_after_18

FROM routes AS r USE INDEX(IDX_due_date)
         JOIN consumer_stops AS cs ON cs.route_id = r.id -- infromation
         JOIN consumer_stop_consignments AS csc ON cs.id = csc.consumer_stop_id  -- restriction, only home delivery stops
         JOIN consignments AS con ON csc.consignment_id = con.id  -- restriction, only rows with consingments
         LEFT JOIN cancellations AS can -- infromation
                   ON con.cancellation_id = can.id

         JOIN orders AS o -- infromation
              ON con.order_id = o.id

         JOIN postal_code_zones AS pcz -- infromation
              ON pcz.id = o.delivery_postal_code_zone_id -- infromation
         join warehouses w ON pcz.terminal_id = w.id -- infromation
         join addresses a ON w.address_id = a.id -- infromation
         LEFT JOIN routeopt_routes rr ON r.routeopt_route_id = rr.id
         LEFT JOIN routeopt_optimizations ro ON rr.optimization_id = ro.id
         LEFT JOIN cancellations AS can_budbee
                   ON can_budbee.id = con.cancellation_id AND
                      can_budbee.cancellation_category IN ("DAMAGED_PARCEL_BY_BUDBEE",
                                                           "DAMAGED_PARCEL_BY_COURIER",
                                                           "DELAYED_DRIVER",
                                                           "OTHER",
                                                           "PARCEL_IN_WRONG_CITY",
                                                           "CANCELLED_ROUTE",
                                                           "PREMATURE_ROUTE_FINISH",
                                                           "WRONG_SORTED_PARCEL",
                                                           "VEHICLE_MALFUNCTION",
                                                           "WRONG_OR_INCOMPLETE_ADDRESS")

WHERE r.due_date = date(date_sub(now(), INTERVAL 10 HOUR))
  AND r.type = "DISTRIBUTION"
  and pcz.type = "TO_DOOR"
GROUP BY w.id

UNION

SELECT w.name                                                                                      as terminal_name,
       sum(CASE
               WHEN can.cancellation_category = "CONSUMER_WANTS_TO_CANCEL_CONSIGNMENT"
                   or can.cancellation_category = "CONSUMER_WANTS_TO_CANCEL_BOX_RESERVATION"
                   THEN 1
               ELSE 0 END)                                                                         AS CONSUMER_WANTS_TO_CANCEL_CONSIGNMENT,
       sum(CASE WHEN can.cancellation_category = "DAMAGED_PARCEL_BY_BUDBEE" THEN 1 ELSE 0 END)     AS DAMAGED_BUDBEE,
       sum(CASE WHEN can.cancellation_category = "DAMAGED_PARCEL_BY_COURIER" THEN 1 ELSE 0 END)    AS DAMAGED_COURIER,
       sum(CASE WHEN can.cancellation_category = "DELAYED_DRIVER" THEN 1 ELSE 0 END)               AS DELAYED_DRIVER,
       sum(CASE
               WHEN can.cancellation_category = "DELAYED_FROM_MERCHANT" THEN 1
               ELSE 0 END)                                                                         AS DELAYED_FROM_MERCHANT,
       sum(CASE WHEN can.cancellation_category = "ORDER_MOVED" THEN 1 ELSE 0 END)                  AS ORDER_MOVED,
       sum(CASE WHEN can.cancellation_category = "OTHER" THEN 1 ELSE 0 END)                        AS OTHER,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_IN_WRONG_CITY" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_IN_WRONG_CITY,
       sum(CASE
               WHEN can.cancellation_category = "REQUESTED_BY_MERCHANT" THEN 1
               ELSE 0 END)                                                                         AS REQUESTED_BY_MERCHANT,
       sum(CASE WHEN can.cancellation_category = "REQUESTED_BY_TRANSPORT_BUYER" THEN 1 ELSE 0 END) AS transport_buyer,
       sum(CASE WHEN can.cancellation_category = "WRONG_OR_INCOMPLETE_ADDRESS" THEN 1 ELSE 0 END)  AS address,
       sum(CASE WHEN can.cancellation_category = "WRONG_SORTED_PARCEL" THEN 1 ELSE 0 END)          AS wrong_sorted,
       sum(CASE WHEN can.cancellation_category = "CANCELLED_ROUTE" THEN 1 ELSE 0 END)              AS CANCELLED_ROUTE,
       sum(CASE
               WHEN can.cancellation_category = "PREMATURE_ROUTE_FINISH" THEN 1
               ELSE 0 END)                                                                         AS PREMATURE_ROUTE_FINISH,
       sum(CASE
               WHEN can.cancellation_category = "DOES_NOT_FIT_IN_VEHICLE" THEN 1
               ELSE 0 END)                                                                         AS DOES_NOT_FIT_IN_VEHICLE,
       sum(CASE
               WHEN can.cancellation_category = "CONVERTED_TO_HOME_DELIVERY"
                   or can.cancellation_category = "CONSUMER_WANTS_TO_CHANGE_DELIVERY_TYPE" THEN 1
               ELSE 0 END)                                                                         AS CONSUMER_WANTS_TO_CHANGE_DELIVERY_TYPE,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_NOT_ADDED_TO_ROUTE" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_NOT_ADDED_TO_ROUTE,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_NEVER_ATTEMPTED_TO_ASSIGN" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_NEVER_ATTEMPTED_TO_ASSIGN,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_NOT_PICKED_UP_BY_DRIVER" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_NOT_PICKED_UP_BY_DRIVER,
       sum(CASE
               WHEN can.cancellation_category = "PARCEL_TOO_LARGE" THEN 1
               ELSE 0 END)                                                                         AS PARCEL_TOO_LARGE,
       sum(CASE
               WHEN can.cancellation_category = "RESERVATION_EXPIRED" THEN 1
               ELSE 0 END)                                                                         AS RESERVATION_EXPIRED,
       sum(CASE
               WHEN can.cancellation_category = "ROUTE_REACHED_MAX_CAPACITY" THEN 1
               ELSE 0 END)                                                                         AS ROUTE_REACHED_MAX_CAPACITY,
       sum(CASE
               WHEN can.cancellation_category = "LOCKER_IS_FULL" THEN 1
               ELSE 0 END)                                                                         AS LOCKER_IS_FULL,
       sum(CASE
               WHEN can.cancellation_category = "DAMAGED_PARCEL_BY_MERCHANT" THEN 1
               ELSE 0 END)                                                                         AS DAMAGED_PARCEL_BY_MERCHANT,
       sum(CASE
               WHEN can.cancellation_category = "VEHICLE_MALFUNCTION" THEN 1
               ELSE 0 END)                                                                         AS VEHICLE_MALFUNCTION,
       sum(CASE
               WHEN can.cancellation_category = "RECIPIENT_AGE_TOO_LOW" THEN 1
               ELSE 0 END)                                                                         AS RECIPIENT_AGE_TOO_LOW,
       sum(CASE
               WHEN can.cancellation_category = "BOX_WONT_OPEN" THEN 1
               ELSE 0 END)                                                                         AS BOX_WONT_OPEN,
       sum(CASE
               WHEN can.cancellation_category = "BOX_IS_EMPTY" THEN 1
               ELSE 0 END)                                                                         AS BOX_IS_EMPTY,
       count(can.id)                                                                               AS total_cancellations,
       count(con.id)                                                                               AS total_consignments,
       date(t.date)                                                                                    AS date,
       a.country_code,
       'Box' as delivery_type,
       now() as timestamp,
       COUNT(CASE
                 WHEN HOUR(CONVERT_TZ(can_budbee.date, 'UTC', ro.timezone)) <= 15 AND can_budbee.id IS NOT NULL
                     THEN can_budbee.id END)                                                       AS budbee_before_16,
       COUNT(CASE
                 WHEN (HOUR(CONVERT_TZ(can_budbee.date, 'UTC', ro.timezone)) >= 16 AND
                       HOUR(CONVERT_TZ(can_budbee.date, 'UTC', ro.timezone)) <= 17) AND can_budbee.id IS NOT NULL
                     THEN can_budbee.id
                 ELSE NULL END)                                                                    AS budbee_between_16_18,
       COUNT(
               CASE
                   WHEN (HOUR(CONVERT_TZ(can_budbee.date, 'UTC', ro.timezone)) >= 18) AND can_budbee.id IS NOT NULL
                       THEN can_budbee.id END)                                                     AS budbee_after_18

FROM routes as r USE INDEX(IDX_due_date)
         JOIN consumer_stops cs ON r.id = cs.route_id -- infromation
         JOIN locker_stops ls ON cs.id = ls.stop_id -- restriction, only box delivery stops
         JOIN locker_pallets lp ON r.id = lp.route_id -- infromation
         JOIN locker_pallet_parcels lpp ON lp.id = lpp.locker_pallet_id AND lp.locker_id = ls.locker_id -- infromation
         JOIN parcels p ON lpp.parcel_id = p.id -- infromation
         JOIN orders o ON p.order_id = o.id -- infromation
         JOIN locker_consignments con ON con.order_id = o.id -- restriction, only rows with consingments
         JOIN intervals i on con.estimated_interval_id = i.id -- infromation
         JOIN timestamps t on t.id = i.start_timestamp_id -- infromation
         LEFT JOIN cancellations can ON can.id = con.cancellation_id -- infromation
         JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id -- infromation
         JOIN warehouses w ON pcz.terminal_id = w.id -- infromation
         JOIN addresses a ON w.address_id = a.id -- infromation
         LEFT JOIN routeopt_routes rr ON r.routeopt_route_id = rr.id
         LEFT JOIN routeopt_optimizations ro ON rr.optimization_id = ro.id
         LEFT JOIN cancellations AS can_budbee
                   ON can_budbee.id = con.cancellation_id AND
                      can_budbee.cancellation_category IN ("DAMAGED_PARCEL_BY_BUDBEE",
                                                           "DAMAGED_PARCEL_BY_COURIER",
                                                           "DELAYED_DRIVER",
                                                           "OTHER",
                                                           "PARCEL_IN_WRONG_CITY",
                                                           "CANCELLED_ROUTE",
                                                           "PREMATURE_ROUTE_FINISH",
                                                           "WRONG_SORTED_PARCEL",
                                                           "VEHICLE_MALFUNCTION",
                                                           "WRONG_OR_INCOMPLETE_ADDRESS")
WHERE r.due_date = date(date_sub(now(), INTERVAL 10 HOUR))
  AND r.type = "LOCKER"
  and pcz.type = "TO_LOCKER"
GROUP BY w.id"""

cancellation_category_per_date_and_city_df = readJDBC(query6, 'budbee')

writeSnowflake(cancellation_category_per_date_and_city_df, 'cancellation_category_per_date_and_city')

# COMMAND ----------

# DAG: billable_DORAPPs_cross_country_per_country
query7 = """
SELECT
    count(distinct p.id)  as parcel_count,
    count(distinct con.id) as c_count,
    count(con.id) as parcel_consignment_count,
    pcz.country_code as destination_country,
    a.country_code as terminal_country,
    now() as time_stamp
FROM consignments AS con USE INDEX(IDX_date) -- add index to increase performance
    join orders o ON con.order_id = o.id
    join postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id
    join warehouses w ON pcz.terminal_id = w.id
    join addresses a ON w.address_id = a.id
    LEFT JOIN cancellations as canc on canc.id = con.cancellation_id
    left join parcel_consignments as pcon on pcon.consignment_id = con.id
    left join parcels as p on p.id = pcon.parcel_id
WHERE con.date = CURRENT_DATE()
    and con.type in ("DELIVERY", "RETURN")
    and pcz.city!="OTHER"
    and pcz.type in ("TO_DOOR")
    and (p.visible = true or p.id is null)
    and (canc.date > con.deadline or canc.id is null)
GROUP BY con.date, destination_country, terminal_country
having destination_country != terminal_country
"""

billable_DORAPPs_cross_country_per_country_df = readJDBC(query7, 'budbee')
writeSnowflake(billable_DORAPPs_cross_country_per_country_df, 'billable_DORAPPs_cross_country_per_country')

# COMMAND ----------

# DAG: scheduled_orders_by_group_and_buyer
query8 = """
SELECT  count(DISTINCT con.id) - count(DISTINCT ca.id) as orderCount,
    b.external_name as buyerName,
    MAX(DAYNAME(con.date)) as dayOfWeek,
    MAX(rgroup.title) as routeoptGroupTitle,
    count(DISTINCT ca.id) as cancelledOrderCount,
    CASE
      WHEN SUM(cscon.id)>0
        THEN TRUE Else FALSE
    END as routesSaved,
    w.code,
    now() as time_stamp,
    ad.country_code as terminal_country_code
FROM routeopt_groups as rgroup
    JOIN routeopt_group_settings rgs ON rgroup.routeopt_group_settings_id = rgs.id
    JOIN routeopt_group_settings_consignment_types rgsct ON rgs.id = rgsct.routeopt_group_settings_id
    JOIN consignments AS con USE INDEX(IDX_date) ON upper(dayname(con.date)) = rgs.delivery_day and con.type = rgsct.consignment_type
    JOIN orders AS o ON con.order_id = o.id
    JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id and pcz.city = rgroup.city
    LEFT JOIN cancellations as ca ON con.cancellation_id = ca.id
    JOIN buyers as b ON o.buyer_id = b.id
    LEFT JOIN consumer_stop_consignments AS cscon ON cscon.consignment_id = con.id
    JOIN warehouses AS w ON w.id = pcz.terminal_id and rgroup.terminal_code = w.code
    JOIN addresses as ad on w.address_id = ad.id
WHERE  con.date between CURRENT_DATE() and DATE_ADD(CURRENT_DATE(), INTERVAL 5 DAY) and rgroup.deleted_at is null
    AND upper(b.external_name) NOT LIKE '%BUDBEE BOX%'  #budbee box merchants
    AND pcz.type = "TO_DOOR" -- exclude all Box delivery if any
    GROUP BY w.id, con.date, b.id
    order by con.date asc
"""

scheduled_orders_by_group_and_buyer_df = readJDBC(query8, 'budbee')

writeSnowflake(scheduled_orders_by_group_and_buyer_df, 'scheduled_orders_by_group_and_buyer')

# COMMAND ----------

# DAG: Vehicle ALPR (direct query Klipfolio)
query9 = """
SELECT  rvi.license_plate as actual_plate,  
 		REPLACE(
 			REPLACE(
 				upper(
 					trim(v.license_plate)),"-", "")," ", "") as routed_plate, 
 					
 		STRCMP(
 			REPLACE(
 				REPLACE(
 					REPLACE(
 						UPPER(
 							trim(v.license_plate)), "-", ""), " ",""), "0", "O" ),
 			REPLACE(
 				REPLACE(
 					REPLACE(
 						upper(
 							trim(rvi.license_plate))," ", ""), "-", ""),"0", "O")) as valid,
 		date_format(r.due_date, "%Y-%m-%d") as route_date,
 		concat("https://carriers.budbee.com/admin/users/", u.id) as user_id,
 		concat(u.first_name, ' ', u.last_name),
 		r.city,
 		i.url, 
 		oo.name,
 		a.country_code
FROM route_vehicle_images AS rvi

		JOIN routes AS r
		  ON rvi.route_id = r.id
		  
		JOIN routeopt_routes AS rr
		  ON rr.id = r.routeopt_route_id
		  
		JOIN routeopt_vehicles_and_users as vu
		  ON vu.id = routeopt_vehicle_and_user_id
		  
		JOIN vehicles AS v
		  ON v.id = vu.vehicle_id
		  
		JOIN user_routes AS ur
		  ON ur.route_id = r.id
		 
		JOIN users AS u
		  ON u.id = ur.user_id 
		   
		LEFT JOIN images AS i 
		  ON i.id = rvi.image_id  
		  
		JOIN owner_operators AS oo
		  ON oo.id = r.owner_operator_id
		  
		LEFT JOIN warehouses AS w
		  ON w.city = r.city 
		  
		left JOIN addresses AS a
		  ON a.id = w.address_id 
		  
WHERE r.due_date BETWEEN date_sub(curdate(), interval 2 day) AND curdate()  AND v.license_plate not like "%budbee%"
GROUP BY r.id
"""

license_plate_validation_df = readJDBC(query9, 'budbee')

writeSnowflake(license_plate_validation_df, 'license_plate_validation')

# COMMAND ----------

# DAG: nl_be_terminals_by_country_dorapps_today
query10 = """
SELECT COUNT(pc.parcel_id) AS dorapps,
       w.name              AS source_terminal,
       pcz.country_code    AS destimation_country,
       NOW()               AS extracted_at,
       (SELECT count(DISTINCT z.country_code) from postal_code_zones z where z.terminal_id = w.id and z.deleted_at is null and z.type =pcz.type) as count_country
FROM consignments c USE INDEX(IDX_date)
         JOIN parcel_consignments pc ON c.id = pc.consignment_id
         JOIN orders o ON c.order_id = o.id
         JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id   AND pcz.type = "TO_DOOR"
    AND pcz.country_code IN ("NL", "BE")
         JOIN warehouses w ON pcz.terminal_id = w.id
         JOIN addresses AS ad ON ad.id = w.address_id
         LEFT JOIN cancellations AS canc ON c.cancellation_id = canc.id -- information
WHERE c.date = current_date()
  AND (canc.date > c.deadline  or canc.id is null)

GROUP BY source_terminal, destimation_country, pcz.type
having count_country >1
"""

nl_be_terminals_by_country_dorapps_today_df = readJDBC(query10, 'budbee')

writeSnowflake(nl_be_terminals_by_country_dorapps_today_df, 'nl_be_terminals_by_country_dorapps_today')

# COMMAND ----------

# DAG: This was rewritten to combine 3 DAGs billable_DORAPPs_today_per_country, billable_DORAPPs_today, booked_DORAPPS_per_merchant_today
query11 = """
SELECT
    count(distinct p.id)  as parcel_count,
    count(distinct con.id) as c_count,
    count(con.id) as parcel_consignment_count,
    pcz.country_code,
    b.id as merchant_id,
    b.external_name as merchant_name,
    now() as time_stamp
FROM consignments AS con USE INDEX(IDX_date)
    join orders o ON con.order_id = o.id
    join buyers b on o.buyer_id = b.id
    join postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id
    LEFT JOIN cancellations as canc on canc.id = con.cancellation_id
    left join parcel_consignments as pcon on pcon.consignment_id = con.id
    left join parcels as p on p.id = pcon.parcel_id
WHERE con.date = CURRENT_DATE()
    and con.type in ("DELIVERY", "RETURN")
    and pcz.city!="OTHER"
    and pcz.type in ("TO_DOOR")
    and (p.visible = true or p.id is null)
    and ((canc.cancellation_category in ("REQUESTED_BY_TRANSPORT_BUYER", "DELAYED_FROM_MERCHANT", "DELAYED_FROM_MERCHANT", "CONSUMER_WANTS_TO_CANCEL_CONSIGNMENT", "WRONG_OR_INCOMPLETE_ADDRESS", "DAMAGED_PARCEL_BY_MERCHANT") and canc.date > con.deadline ) or canc.id is null)
GROUP BY con.date, pcz.country_code, b.id
"""

billable_DORAPPs_today_per_country_merchant_df = readJDBC(query11, 'budbee')

writeSnowflake(billable_DORAPPs_today_per_country_merchant_df, 'billable_DORAPPs_today_per_country_merchant')

# COMMAND ----------

# DAG: billable_returns_attempts_today_per_country
query12 = """
select
    count(p.id) as parcels,
    count(DISTINCT con.id) as consignments,
    pcz.country_code,
    now() as time_stamp
from consignments as con
    join orders as o on o.id  =con.order_id
    join buyers as b on b.id = o.buyer_id
    join parcel_consignments as pc on pc.consignment_id = con.id
    join parcels as p on p.id = pc.parcel_id
    join postal_code_zones as pcz on pcz.id = o.delivery_postal_code_zone_id
    left join cancellations c ON con.cancellation_id = c.id
where con.date = current_date()
    and con.type="RETURN"
    and o.delivery_postal_code_zone_id!= 111
    and (c.id is null or (c.date> con.deadline and  c.cancellation_category in ("REQUESTED_BY_MERCHANT","REQUESTED_BY_TRANSPORT_BUYER","CONSUMER_WANTS_TO_CANCEL_CONSIGNMENT")))
group by pcz.country_code
"""

billable_returns_attempts_today_per_country_df = readJDBC(query12, 'budbee')

writeSnowflake(billable_returns_attempts_today_per_country_df, 'billable_returns_attempts_today_per_country')

# COMMAND ----------

# DAG: consumer_returns_today
query13 = """
select
    p.id as parcel_id,
    con.id as consignment_id,
    pcz.title,
    b.external_name,
    pcz.id as postal_code_zone_id,
    pcz.country_code,
    pcz.city,
    pcz.terminal_id,
    o.buyer_id,
    now() as time_stamp
from (select * from consignments
        where date = current_date()
              and type="RETURN"
              and cancellation_id is null) as con
         join orders as o on o.id  =con.order_id and o.delivery_postal_code_zone_id!= 111
         join buyers as b on b.id = o.buyer_id
         join parcel_consignments as pc on pc.consignment_id = con.id
         join parcels as p on p.id = pc.parcel_id
         join postal_code_zones as pcz on pcz.id = o.delivery_postal_code_zone_id and binary pcz.type = "TO_DOOR"

"""


query_sub = """
select 
    min(id) as min_consignment_id,
    max(id) as max_consignment_id
    from consignments
    where date = current_date()
"""

consignment_id_today_df = readJDBC(query_sub, 'budbee')
min_id = consignment_id_today_df.collect()[0][0]
max_id = consignment_id_today_df.collect()[0][1]

consumer_returns_today_df_temp = readJDBC_part(query13, 'budbee', "consignment_id", min_id, max_id)
consumer_returns_today_df = consumer_returns_today_df_temp.groupby("buyer_id","external_name","postal_code_zone_id","title","country_code","city","terminal_id","time_stamp").agg(F.count("parcel_id").alias("parcels"),F.countDistinct("consignment_id").alias("consignments"))
writeSnowflake(consumer_returns_today_df, 'consumer_returns_today')

# COMMAND ----------

# DAG: timeslots_due_today
query14 = """
select
    o.token as token,
    CONVERT_TZ(dstart.date,'GMT','CET') as tstart,
    CONVERT_TZ(dstop.date,'GMT','CET') as tstop,
    pcz.title,
    pcz.country_code,
    now() as time_stamp
from orders as o
    join consignments as con on con.order_id = o.id
    left join cancellations as c on c.id = con.cancellation_id
    join intervals as i on i.id = con.interval_id
    join timestamps as dstart on dstart.id = i.start_timestamp_id
    join timestamps as dstop on dstop.id = i.stop_timestamp_id
    join postal_code_zones as pcz on pcz.id = o.delivery_postal_code_zone_id
    join klarna_payments as kp on kp.consignment_id = con.id and (kp.purchase_type = 'TIMESLOT'
            OR (
                JSON_VALID(kp.reference)
                AND JSON_EXTRACT(kp.reference, "$.timeWindow.timeslotSpec") != CAST('null' AS JSON)))
where con.date = current_date()  
    and con.type in ("DELIVERY", "RETURN") 
    and kp.status = "COMPLETED"  
    AND (con.cancellation_id IS NULL OR (c.cancellation_category IN ("REQUESTED_BY_TRANSPORT_BUYER", "DELAYED_FROM_MERCHANT", "DELAYED_FROM_MERCHANT", "CONSUMER_WANTS_TO_CANCEL_CONSIGNMENT", "WRONG_OR_INCOMPLETE_ADDRESS", "DAMAGED_PARCEL_BY_MERCHANT") AND c.date > con.deadline))
group by con.id
"""

timeslots_due_today_df = readJDBC(query14, 'budbee')

writeSnowflake(timeslots_due_today_df, 'timeslots_due_today')

# COMMAND ----------

# DAG: This was rewritten to combine new_locker_orders_last_24_hours_grouped_by_merchant, new_locker_orders_last_24_hours_grouped_by_locker, new_locker_orders_last_24_hours_grouped_by_terminal
# 2 mins
query15 = """
SELECT
    b.external_name,
    l.id as locker_id,
    w.name as terminal,
    o.id as order_id,
    pcz.country_code,
    now() as time_stamp
FROM orders AS o
    JOIN buyers AS b ON b.id = o.buyer_id
    JOIN postal_code_zones pcz on o.delivery_postal_code_zone_id = pcz.id
    JOIN lockers AS l ON l.id = o.locker_id
    JOIN warehouses AS w ON w.id = pcz.terminal_id
WHERE o.created_at BETWEEN date_sub(NOW(), INTERVAL 1 day) AND NOW()
    AND o.locker_id IS NOT NULL
    AND b.external_name not like '%Budbee Box%'
"""



query_sub = """
select 
    min(id) as min_id,
    max(id) as max_id
    from orders
    where created_at BETWEEN date_sub(NOW(), INTERVAL 1 day) AND NOW()
"""

order_id_df = readJDBC(query_sub, 'budbee')
min_id = order_id_df.collect()[0][0]
max_id = order_id_df.collect()[0][1]


new_locker_orders_last_24_hours_df_temp = readJDBC_part(query15, 'budbee', "order_id", min_id, max_id)
new_locker_orders_last_24_hours_df = new_locker_orders_last_24_hours_df_temp.groupby("external_name","locker_id","terminal","country_code","time_stamp").agg(F.count("order_id").alias("orders"))
writeSnowflake(new_locker_orders_last_24_hours_df, 'new_locker_orders_last_24_hours')

# COMMAND ----------

# DAG: Combine box_returns_picked_up_today and box_returns_present_today
query16 = """
select
    count(distinct case when state = 'PRESENT' THEN order_id END) as returns_present,
    count(distinct case when state = 'PICKED_UP' THEN order_id END) as returns_picked_up,
    now() as time_stamp,
    l.country
from locker_consignments
    JOIN parcel_box_assignments on locker_consignments.parcel_box_assignment_id = parcel_box_assignments.id
    JOIN locker_boxes on parcel_box_assignments.box_id = locker_boxes.id
    JOIN lockers l on l.id = locker_boxes.locker_id
where reservation_expires_at between current_date and date_add(current_date, interval 1 day)
    and state IN ('PRESENT','PICKED_UP')
    and consignment_type = 'RETURN'
group by l.country
"""

box_returns_today_df = readJDBC(query16, 'budbee')

writeSnowflake(box_returns_today_df, 'box_returns_today')

# COMMAND ----------

# DAG: box_returns_present_all_time
query17 = """
select
    count(distinct order_id) as returns_present,
    now() as time_stamp,
    l.country
from locker_consignments
    JOIN parcel_box_assignments on locker_consignments.parcel_box_assignment_id = parcel_box_assignments.id
    JOIN locker_boxes on parcel_box_assignments.box_id = locker_boxes.id
    JOIN lockers l on l.id = locker_boxes.locker_id
where reservation_expires_at > date_sub(current_date, interval 1 month)
    and state = 'PRESENT'
    and consignment_type = 'RETURN'
group by l.country
"""

box_returns_present_all_time_df = readJDBC(query17, 'budbee')

writeSnowflake(box_returns_present_all_time_df, 'box_returns_present_all_time')

# COMMAND ----------

# DAG: box_order_progress_over_time
# Currently it takes 2-3 mins to run
query18 = """
SELECT
        date(o.created_at) as created_at,
        pba.picked_up,
        p.recall_reason,
        o.id as order_id,
        CASE WHEN pba.picked_up IS NULL AND pba.id IS NOT NULL THEN 1 ELSE 0 END AS in_locker,
        CASE WHEN pba.id IS NULL AND slog.id IS NOT NULL AND p.recall_reason IS NULL THEN 1 ELSE 0 END AS at_budbee,
        CASE WHEN slog.id IS NULL THEN 1 ELSE 0 END AS not_at_budbee,
        pcz.country_code,
        now() as time_stamp
FROM (select * from orders
    WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
    and locker_id IS NOT NULL
    AND cancellation_id IS NULL
    and binary substr(cart_id,7,5) <> 'Route') AS o
        JOIN parcels p on o.id = p.order_id
        LEFT JOIN parcel_box_assignments pba on p.id = pba.parcel_id
        LEFT JOIN scanning_log slog on slog.id = (SELECT id FROM scanning_log WHERE parcel_id =  p.id ORDER BY DATE DESC LIMIT 1)
        JOIN postal_code_zones pcz on o.delivery_postal_code_zone_id = pcz.id

#GROUP BY date(o.created_at), pcz.country_code
"""


query_sub = """
select 
    min(id) as min_id,
    max(id) as max_id
    from orders
    where created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
"""

order_id_df = readJDBC(query_sub, 'budbee')
min_id = order_id_df.collect()[0][0]
max_id = order_id_df.collect()[0][1]


box_order_progress_over_time_df_temp = readJDBC_part(query18, 'budbee', "order_id", min_id, max_id)
box_order_progress_over_time_df = box_order_progress_over_time_df_temp.groupby("created_at","country_code","time_stamp").agg(F.count("order_id").alias("orders"),
                                                                                                                             F.count("picked_up").alias("picked_up"),
                                                                                                                             F.sum("in_locker").alias("in_locker"),
                                                                                                                             F.sum("at_budbee").alias("at_budbee"),
                                                                                                                             F.sum("not_at_budbee").alias("not_at_budbee"),
                                                                                                                             F.count("recall_reason").alias("recalled"))


writeSnowflake(box_order_progress_over_time_df, 'box_order_progress_over_time')

# COMMAND ----------

# DAG: Combine lockers_with_info and enabled_and_disabled_lockers
query19 = """

SELECT
    l.name,
    l.identifier,
    street,
    w.code,
    pcz.city,
    pcz.country_code,
    installation_date,
    enabled,
    llp.name as campang_parner,
    l.id, 
    count(boxes.id),
    now() as time_stamp
FROM lockers AS l
		LEFT JOIN locker_location_partners AS llp ON l.location_partner_id = llp.id
		JOIN postal_code_zones AS pcz ON l.postal_code_zone_id = pcz.id
        JOIN warehouses w on pcz.terminal_id = w.id
		JOIN locker_boxes AS boxes ON boxes.locker_id = l.id
WHERE l.deleted_AT IS NULL
GROUP BY l.id
"""

lockers_with_info_df = readJDBC(query19, 'budbee')

writeSnowflake(lockers_with_info_df, 'lockers_with_info')

# COMMAND ----------

# DAG: lockers_with_errors
query20 = """
SELECT
       l.id AS locker_id,
       l.identifier AS locker_identifier,
       l.name AS locker_name,
       l.country,
       sum(CASE WHEN lb.size = "BOX_265_100_600" THEN 1 ELSE 0 END) AS BOX_265_100_600,
       sum(CASE WHEN lb.size = "BOX_265_150_600" THEN 1 ELSE 0 END) AS BOX_265_150_600,
       sum(CASE WHEN lb.size = "BOX_265_225_600" THEN 1 ELSE 0 END) AS BOX_265_225_600,
       sum(CASE WHEN lb.size = "BOX_400_300_600" THEN 1 ELSE 0 END) AS BOX_400_300_600,
       now() as time_stamp

FROM locker_boxes AS lb
    JOIN lockers l on lb.locker_id = l.id
WHERE lb.error_code IS NOT NULL
GROUP BY locker_id
"""

lockers_with_errors_df = readJDBC(query20, 'budbee')

writeSnowflake(lockers_with_errors_df, 'lockers_with_errors')

# COMMAND ----------

# DAG: lastningsinspektion_stockholm_sondag
query21 = """
SELECT
    DATE_FORMAT(CONVERT_TZ(start.date, 'UTC', 'Europe/Stockholm'), '%H:%i') AS Loading_time,
    r.id AS Route,
    r.title as Cage_name,
    NULL AS Bay,
    ur.user_id as User_ID,
    CONCAT(u.first_name, ' ', u.last_name) AS driver,
    u.phone_number,
    oo.name as courier
        ,routeopt_routes.optimization_id,
    r.city,
    r.terminal,
    now()
FROM routes AS r
         LEFT JOIN user_routes AS ur ON ur.route_id = r.id
         LEFT JOIN users AS u ON u.id = ur.user_id
         LEFT JOIN sorting_rule_route AS srr ON srr.route_id = r.id
         LEFT JOIN terminal_sorting_lanes AS tsl ON tsl.id = srr.terminal_sorting_lane_id
         LEFT JOIN owner_operator_users as oou on u.id = oou.user_id
         LEFT JOIN owner_operators as oo on oou.owner_operator_id = oo.id
         JOIN terminal_stops AS ts ON ts.route_id = r.id
         JOIN intervals AS loading ON loading.id = ts.estimated_interval_id
         JOIN timestamps AS start ON start.id = loading.start_timestamp_id
         JOIN timestamps AS stop ON stop.id = loading.stop_timestamp_id
         JOIN terminal_stop_consignments AS tsc ON tsc.terminal_stop_id = ts.id
         JOIN consignments AS c ON c.id = tsc.consignment_id
#LEFT JOIN terminal_docks AS td ON td.route_id = r.id
         join routeopt_routes on r.routeopt_route_id = routeopt_routes.id
WHERE r.due_date = curdate()
  and r.type = 'Distribution'
GROUP BY r.id

"""

lastningsinspektion_stockholm_sondag_df = readJDBC(query21, 'budbee')

writeSnowflake(lastningsinspektion_stockholm_sondag_df, 'lastningsinspektion_stockholm_sondag')

# COMMAND ----------

# DAG: current_locker_status
query22 = """
SELECT
		box.locker_id,
		box.board_id,
		box.slot_id,
		(CASE
			WHEN a.state IS NULL AND box.error_code IS NULL THEN 1
			WHEN a.state = "PRESENT" AND p.recall_requested_at IS NULL AND o.cancellation_id IS NULL AND box.error_code IS NULL AND p.type = "DELIVERY" THEN 2
			WHEN box.error_code IS NOT NULL THEN 3
			WHEN a.state = "PRESENT" AND p.recall_requested_at IS NULL AND o.cancellation_id IS NULL AND box.error_code IS NULL AND p.type = "RETURN" THEN 4
			WHEN a.state = "RESERVED" AND p.recall_requested_at IS NULL AND o.cancellation_id IS NULL AND box.error_code IS NULL THEN 5
			ELSE 1 END
		) AS state,

		(CASE WHEN  p.recall_requested_at IS NULL AND o.cancellation_id IS NULl THEN concat("https://carriers.budbee.com/admin/orders/",o.token) ELSE (IF (box.error_code IS NOT NULL,  box.error_code , NULL)) END) as token,
		now() as time_stamp
FROM locker_boxes AS box

		LEFT JOIN parcel_box_assignments AS a
		  ON box.id = a.box_id AND (a.state ="PRESENT" OR a.state = "RESERVED")

		LEFT JOIN parcels AS p
		  ON p.id = a.parcel_id

		LEFT JOIN orders AS o
		  ON o.id = p.order_id
"""

current_locker_status_df = readJDBC(query22, 'budbee')

writeSnowflake(current_locker_status_df, 'current_locker_status')

# COMMAND ----------

# DAG: boxes_per_locker
query23 = """
SELECT
    l.identifier,
    COUNT( DISTINCT lb.id) AS boxes,
    COUNT(DISTINCT pba.id) AS assigned_parcels,
    now() as time_stamp

FROM lockers AS l
    JOIN locker_boxes lb on l.id = lb.locker_id
    LEFT JOIN parcel_box_assignments pba on lb.id = pba.box_id AND pba.state = "PRESENT"
GROUP BY l.id
"""

boxes_per_locker_df = readJDBC(query23, 'budbee')

writeSnowflake(boxes_per_locker_df, 'boxes_per_locker')

# COMMAND ----------

# DAG: locker_orders_backlog_by_locker
query24 = """
SELECT  l.identifier,
       o.id as order_id,
       slog.id as scanning_log_id,
       CASE WHEN slog.id IS NOT NULL AND us.warehouse_id = pcz.terminal_id THEN 1 ELSE 0 END AS scanned_in_distribution_terminal,
       now() as time_stamp
FROM (select * from orders
    where created_at >= DATE_ADD(current_date(), INTERVAL -30 DAY)
		AND cancellation_id is null) AS o
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
    from orders
    where created_at >= DATE_ADD(current_date(), INTERVAL -30 DAY)
"""

order_id_df = readJDBC(query_sub, 'budbee')
min_id = order_id_df.collect()[0][0]
max_id = order_id_df.collect()[0][1]


locker_orders_backlog_by_locker_df_temp = readJDBC_part(query24, 'budbee', "order_id", min_id, max_id)

locker_orders_backlog_by_locker_df = locker_orders_backlog_by_locker_df_temp.groupBy("identifier","time_stamp").agg(F.countDistinct("order_id").alias("orders"),
                                                                                                                    F.sum("scanned_in_distribution_terminal").alias("scanned_in_distribution_terminal"),
                                                                                                                   F.count("scanning_log_id").alias("scanned_by_budbee"))

writeSnowflake(locker_orders_backlog_by_locker_df, 'locker_orders_backlog_by_locker')

# COMMAND ----------

# DAG: Ratings_per_country_in_routes_today (direct query Klipfolio)
query25 = """
SELECT c.id, r2.score, r2.rating_category, pcz.country_code, pcz.city from routes as r
  join consumer_stops cs ON r.id = cs.route_id
  join consumer_stop_consignments csc ON cs.id = csc.consumer_stop_id
  join consignments c ON csc.consignment_id = c.id
  join orders o ON c.order_id = o.id
  join postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id
  left join ratings r2 ON c.id = r2.consignment_id
  WHERE r.due_date = date(date_sub(now(), interval 10 hour)) and r.type = "DISTRIBUTION"
"""

ratings_per_country_in_routes_today_df = readJDBC(query25, 'budbee')

writeSnowflake(ratings_per_country_in_routes_today_df, 'ratings_per_country_in_routes_today')

# COMMAND ----------

# DAG: parcel_due_today_vs_scanned
query26 = """
SELECT COUNT(c.id)         AS consignments,
       COUNT(DISTINCT p.id)         AS parcels_due,
       COUNT(DISTINCT sl.parcel_id) AS scanned_parcels,
       w.code,
       utc_timestamp() as time_stamp
FROM  consignments c USE INDEX(IDX_date) -- add index but still run a bit slow
          JOIN orders AS o ON o.id = c.order_id
          JOIN postal_code_zones AS pcz ON pcz.id = o.delivery_postal_code_zone_id and pcz.type = 'TO_DOOR'
          JOIN warehouses as w on pcz.terminal_id = w.id
          LEFT JOIN parcel_consignments AS pc ON pc.consignment_id = c.id
          LEFT JOIN parcels AS p ON (p.id = pc.parcel_id AND p.visible IS TRUE)
          LEFT JOIN scanning_log AS sl ON (sl.parcel_id = p.id AND sl.date > c.deadline)
WHERE c.date = utc_date()
  and c.type in ('DELIVERY', 'RETURN')
  AND c.cancellation_id IS NULL
group by w.code
"""

parcel_due_today_vs_scanned_df = readJDBC(query26, 'budbee')

writeSnowflake(parcel_due_today_vs_scanned_df, 'parcel_due_today_vs_scanned')

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
    WHERE DATE(t.date) = utc_date()
        AND binary locker_consignments.consignment_type in ('DELIVERY', 'RETURN')
        AND  locker_consignments.cancellation_id IS NULL) lc
        
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


box_parcel_due_today_added_to_pallet_df_temp = readJDBC_part(query27, 'budbee', "consignment_id", min_id, max_id)
box_parcel_due_today_added_to_pallet_df = box_parcel_due_today_added_to_pallet_df_temp.groupby("terminal","time_stamp").agg(F.countDistinct("consignment_id").alias("consignments"),
                                                                                                                             F.countDistinct("parcel_id").alias("parcels_due"),
                                                                                                                             F.countDistinct("locker_pallet_parcel_id").alias("parcel_added_to_pallet"))

box_parcel_due_today_added_to_pallet_df.display()
# writeSnowflake(box_parcel_due_today_added_to_pallet_df, 'box_parcel_due_today_added_to_pallet')

# COMMAND ----------

# DAG: scanning_progress_status_by_city_with_country_code
query28 = """
SELECT
    w.code,
    pcz.country_code,
    COUNT(DISTINCT c.id) AS consignments,
    COUNT(DISTINCT p.id) AS parcels_due,
    COUNT(DISTINCT sl.parcel_id) AS scanned_parcels,
    now() as time_stamp
FROM consignments AS c USE INDEX(IDX_date)
    JOIN orders AS o ON o.id = c.order_id
    JOIN postal_code_zones AS pcz ON pcz.id = o.delivery_postal_code_zone_id
    JOIN warehouses w ON pcz.terminal_id = w.id
    LEFT JOIN parcel_consignments AS pc ON pc.consignment_id = c.id
    LEFT JOIN parcels AS p ON (p.id = pc.parcel_id AND p.visible IS TRUE)
    LEFT JOIN scanning_log AS sl ON (sl.parcel_id = p.id AND sl.date > c.deadline)
WHERE c.date = CURDATE()
AND c.cancellation_id IS NULL
GROUP BY w.code
"""

scanning_progress_status_by_city_with_country_code_df = readJDBC(query28, 'budbee')

writeSnowflake(scanning_progress_status_by_city_with_country_code_df, 'scanning_progress_status_by_city_with_country_code')
