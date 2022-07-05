# Databricks notebook source
# Set refresh rate at 1 hour
# This covers the DAGs/queries that are refreshed more than 30 mins to less than 4 hours in Klipfolio

# COMMAND ----------

# MAGIC %run ./config

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

counting_number_of_budbee_plus_membership_df = readJDBC(query, 'consumers')
writeSnowflake(counting_number_of_budbee_plus_membership_df, 'counting_number_of_budbee_plus_membership')


# COMMAND ----------

# DAG: app_consumer_orders_stats_by_country
# 6-7 mins
query_sub1 = """
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
"""

query_sub2 = """
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
"""


groupbyconsumer_country_df = readJDBC(query_sub1, 'budbee')

groupbyconsumer_df = readJDBC(query_sub2, 'budbee')

union_df = groupbyconsumer_df.union(groupbyconsumer_country_df)

app_consumer_orders_stats_by_country_df = union_df.groupBy("country_code").agg(F.count("consumer_id").alias("total_app_consumers"),
                                                                              F.count("one_order_consumer").alias("one_order_consumers"),
                                                                              F.count("two_orders_consumer").alias("two_orders_consumers"),
                                                                              F.count("three_and_more_orders_consumer").alias("three_and_more_orders_consumers"),
                                                                              (F.count("one_order_consumer")/F.count("consumer_id")*100).alias("one_order_consumers %"),
                                                                              (F.count("two_orders_consumer")/F.count("consumer_id")*100).alias("one_order_consumers %"),
                                                                              (F.count("three_and_more_orders_consumer")/F.count("consumer_id")*100).alias("one_order_consumers %")).withColumn('timestamp', F.current_timestamp())
app_consumer_orders_stats_by_country_df.display()

# COMMAND ----------

# DAG: orders_created_per_day_in_ecommerce
# 1.5 mins
query3 = """
select
    o.id as order_id,
    date(o.created_at) as created_at,
    now() as time_stamp
from orders as o
    join buyers as b on o.buyer_id = b.id
    join buyer_tags as bt on b.id = bt.buyer_id
where
    bt.tag_id = 2
    and o.cancellation_id is null
    and o.created_at > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) -- reduce amount of data fetch from 12 months to 3 monnths, since Klipfolio only show data last 3 months
#GROUP BY date(o.created_at)
#order by o.created_at desc
"""


query_sub = """
select 
    min(id) as min_id,
    max(id) as max_id
    from orders
    where created_at > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
"""

order_id_df = readJDBC(query_sub, 'budbee')
min_id = order_id_df.collect()[0][0]
max_id = order_id_df.collect()[0][1]


orders_created_per_day_in_ecommerce_df_temp = readJDBC_part(query3, 'budbee', "order_id", min_id, max_id)

orders_created_per_day_in_ecommerce_df = orders_created_per_day_in_ecommerce_df_temp.groupBy("created_at","time_stamp").agg(F.count("order_id").alias("orders"))

writeSnowflake(orders_created_per_day_in_ecommerce_df, 'orders_created_per_day_in_ecommerce')

# COMMAND ----------

# DAG: schedule_of_return_merchant_routes_per_terminal_today
query4 = """
SELECT 	r.terminal,
		(select case
		when ad.country_code = 'SV' then convert_tz(t.date, 'UTC', 'Europe/Stockholm')
		when ad.country_code = 'NL' then convert_tz(t.date, 'UTC', 'Europe/Amsterdam')
		when ad.country_code = 'FI' then convert_tz(t.date, 'UTC', 'Europe/Helsinki')
		when ad.country_code = 'DK' then convert_tz(t.date, 'UTC', 'Europe/Copenhagen')
		when ad.country_code = 'BE' then convert_tz(t.date, 'UTC', 'Europe/Brussels')
		else t.date end) AS loading_time,
		GROUP_CONCAT(DISTINCT b.external_name  SEPARATOR "   |   ") AS merchants,
		GROUP_CONCAT(DISTINCT pall.identifier SEPARATOR "   |   ") AS pallet_identifier,
		concat(u.first_name, " ", u.last_name) AS driver,
		u.phone_number,
		count(pp.parcel_id) > 0,
		oo.name,
        now() as time_stamp

FROM routes AS r
	JOIN terminal_stops AS ts
	  ON ts.route_id = r.id AND ts.sequence = 1

	JOIN intervals AS i
	  ON i.id = ts.estimated_interval_id

	JOIN timestamps AS t
	  ON t.id = i.start_timestamp_id

	JOIN consumer_stops AS cs
	  ON cs.route_id = r.id

	JOIN addresses as ad
	  ON ad.id = cs.address_id

	JOIN consumer_stop_consignments AS csc
	  ON csc.consumer_stop_id = cs.id

	JOIN consignments AS c
	  ON c.id = csc.consignment_id

	JOIN orders AS o
	  ON o.id = c.order_id

	JOIN buyers AS b
	  ON b.id = o.buyer_id

	LEFT JOIN return_pallet_details AS rpd
	  ON rpd.merchant_id = b.id AND rpd.route_id IS NULL

	LEFT JOIN pallets AS pall
	  ON pall.id = rpd.pallet_id

	LEFT JOIN user_routes AS ur
	  ON ur.route_id = r.id

	LEFT JOIN users AS u
	  ON u.id = ur.user_id

	JOIN pallet_parcels AS pp
	  ON pp.pallet_id = pall.id

	JOIN owner_operators AS oo
	  ON oo.id = r.owner_operator_id

WHERE r.type = "MERCHANT"
	AND t.date BETWEEN date_sub(NOW(), INTERVAL 12 HOUR) AND date_add(NOW(), INTERVAL 1 day)
	AND csc.type = "UNLOADING_OF_PARCELS"
	AND pall.created_at > date_sub(now(), INTERVAL 7 DAY)

GROUP BY r.id
order by loading_time
"""

schedule_of_return_merchant_routes_per_terminal_today_df = readJDBC(query4, 'budbee')
writeSnowflake(schedule_of_return_merchant_routes_per_terminal_today_df, 'schedule_of_return_merchant_routes_per_terminal_today')

# COMMAND ----------

# DAG: box_orders_created_per_day
# 1,6 mins
query5 = """
SELECT
    #count(o.id),
    o.id as order_id
    date(o.created_at) as created_at,
    pcz.country_code,
    now() as time_stamp
    
FROM orders AS o
    JOIN postal_code_zones pcz on o.delivery_postal_code_zone_id = pcz.id

WHERE o.locker_id IS NOT NULL and o.cancellation_id is null
  and o.created_at > DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)

#GROUP BY date(o.created_at), pcz.country_code
#ORDER BY o.created_at desc
"""

box_orders_created_per_day_df_temp = readJDBC(query5, 'budbee')

box_orders_created_per_day_df = box_orders_created_per_day_df_temp.groupBy("created_at","country_code","time_stamp").agg(F.count("order_id").alias("orders"))

writeSnowflake(box_orders_created_per_day_df, 'box_orders_created_per_day')

# COMMAND ----------

# DAG: locker_information
query6 = """
SELECT 	l.identifier,
		l.name as locker_name,
		l.enabled as locker_enabled,
		UPPER(l.city),
		pr.name,
		GROUP_CONCAT(left(cps.pickup_day, 3) SEPARATOR "   |   ") AS on_route,
		GROUP_CONCAT(date_format(cps.pickup_time_end, "%H:%i") SEPARATOR "   |   ") AS eta_per_day,
y(l.coordinate),
x(l.coordinate),
(case when enabled = 1 THEN 1 else 0 end) as enabled,
             w.name AS terminal,
          (CASE WHEN pcz.terminal_id != pr.start_terminal_id THEN 1 ELSE 0 END) AS wrong_terminal,
l.id as locker_id,
       l.country,
       now() as time_stamp
FROM lockers AS l

		LEFT JOIN collection_points AS cp
		  ON cp.id = l.collection_point_id

		LEFT JOIN collection_point_schedules AS cps
		  ON cps.pickup_collection_point_id = cp.id

		LEFT JOIN recurring_pickup_schedule_merchant_schedules AS sch
		  ON  sch.id = (SELECT id FROM recurring_pickup_schedule_merchant_schedules WHERE collection_point_schedule_id = cps.id ORDER BY id DESC LIMIT 1)

		LEFT JOIN recurring_pickup_schedule AS pr
	      ON pr.id = sch.recurring_pickup_route_id AND pr.deleted_at IS NULL

        LEFT JOIN postal_code_zones pcz on l.postal_code_zone_id = pcz.id

        LEFT JOIN warehouses w on pcz.terminal_id = w.id


WHERE l.deleted_at IS NULL
GROUP BY l.id
"""

locker_information_df = readJDBC(query6, 'budbee')
writeSnowflake(locker_information_df, 'locker_information')

# COMMAND ----------

# DAG: consumers_with_app_over_time
query7 = """
SELECT
    count(id),
    min(
            CASE WHEN
                         IFNULL(date(created_at),"2020-05-01") < ADDDATE(CURRENT_DATE, INTERVAL  - 12 MONTH)
                     THEN   ADDDATE(CURRENT_DATE, INTERVAL  - 12 MONTH)
                 ELSE   date(created_at) END
        )  AS date,
    now() as time_stamp
FROM consumers.consumer
group by
    MONTH(CASE WHEN
                       IFNULL(date(created_at),"2020-05-01") < ADDDATE(CURRENT_DATE, INTERVAL  - 12 MONTH)
                   THEN   ADDDATE(CURRENT_DATE, INTERVAL  - 12 MONTH)
               ELSE   date(created_at) END),
    YEAR(CASE WHEN
                      IFNULL(date(created_at),"2020-05-01") < ADDDATE(CURRENT_DATE, INTERVAL  - 12 MONTH)
                  THEN   ADDDATE(CURRENT_DATE, INTERVAL  - 12 MONTH)
              ELSE   date(created_at) END)
order by date asc
"""

consumers_with_app_over_time_df = readJDBC(query7, 'consumers')
writeSnowflake(consumers_with_app_over_time_df, 'consumers_with_app_over_time')

# COMMAND ----------

# DAG: counting_new_cancellations
query8 = """
select
    date(cancelled_at) as cancelled_at,
    count(ts.id) as new_cancellations,
    now() as timestamp
from consumers.timeslot_subscription ts
where date(cancelled_at)  >= ADDDATE(current_date,INTERVAL  -12 Month)
group by date(cancelled_at)
order by date(cancelled_at) desc
"""

counting_new_cancellations_df = readJDBC(query8, 'consumers')
writeSnowflake(counting_new_cancellations_df, 'counting_new_cancellations')

# COMMAND ----------

# DAG: counting_new_subscriptions
query9 = """
select
    date(ts.created_at) as created_at,
    count(ts.id) as new_subscriptions,
    now() as timestamp
from consumers.timeslot_subscription ts
where date(ts.created_at) >= ADDDATE(current_date,INTERVAL  -12 Month)
group by date(ts.created_at)
order by date(ts.created_at) desc
"""

counting_new_subscriptions_df = readJDBC(query9, 'consumers')
writeSnowflake(counting_new_subscriptions_df, 'counting_new_subscriptions')

# COMMAND ----------

# DAG: volume_statistics_today_per_country_and_city
query10 = """
SELECT #count(p.id)                                    as parcels,
       #count(d.id)                                    as measured_parcels,
       p.id as parcel_id,
       d.id as dimension_id,
       d.sorting_deviation,
#      SUM(IF(d.sorting_deviation IS NOT NULL, 1, 0)) as heavy_parcels,
       b.external_name,
       pcz.city,
       pcz.country_code,
       o.token,
       'Home' as delivery_type,
       now()                                          as timestamp
FROM consignments AS con USE INDEX(IDX_date)
         JOIN parcel_consignments pc ON con.id = pc.consignment_id -- infromation
         JOIN parcels p ON pc.parcel_id = p.id -- infromation
         LEFT JOIN dimensions AS d ON p.machine_dimensions_id = d.id -- infromation
         JOIN orders o ON con.order_id = o.id -- infromation
         JOIN buyers b ON o.buyer_id = b.id -- infromation
         JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id   AND pcz.type = "TO_DOOR"-- infromation
WHERE con.date = utc_date()
  AND con.type = "DELIVERY"
  AND pcz.id > 111
  AND con.cancellation_id is null
  AND o.cancellation_id is null
#GROUP BY b.id, pcz.id

UNION

SELECT #count(p.id)                                    as parcels,
       #count(d.id)                                    as measured_parcels,
       p.id as parcel_id,
       d.id as dimension_id,
       d.sorting_deviation,
#      SUM(IF(d.sorting_deviation IS NOT NULL, 1, 0)) as heavy_parcels,
       b.external_name,
       pcz.city,
       pcz.country_code,
       o.token,
       'Box' as delivery_type,
       now()                                          as timestamp
FROM locker_consignments AS con
         JOIN intervals i ON con.estimated_interval_id = i.id -- infromation
         JOIN timestamps t ON i.start_timestamp_id = t.id and date(t.date) = utc_date() -- restriction only todays 
         JOIN orders o ON con.order_id = o.id AND o.cancellation_id is null -- restriction only uncanceled 
         JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id AND pcz.type = "TO_LOCKER" -- restiction only box
         JOIN buyers b ON o.buyer_id = b.id -- infromation
         JOIN parcels p ON p.order_id = o.id -- infromation
         LEFT JOIN dimensions AS d ON p.machine_dimensions_id = d.id -- infromation
WHERE
    con.cancellation_id is null
  AND pcz.id > 111

#GROUP BY b.id, pcz.id

"""

volume_temp_df = readJDBC(query10, 'budbee')


volume_statistics_today_per_country_and_city_df = volume_temp_df.groupBy("external_name","city","country_code","delivery_type","timestamp").agg(F.count("parcel_id").alias("parcels"),
                                                                                                                                               F.count("dimension_id").alias("measured_parcels"),
                                                                                                                                               F.count("sorting_deviation").alias("heavy_parcels"))
volume_statistics_today_per_country_and_city_df.display()
# writeSnowflake(volume_statistics_today_per_country_and_city_df, 'volume_statistics_today_per_country_and_city')

# COMMAND ----------

# DAG: consumers_with_app_over_time
query11 = """
SELECT l.identifier,
       l.name,
       l.country,
       count(o.id) failed_parcels,
       r.id as route_id,
       concat(u.first_name, ', ', u.last_name) as driver,
       u.phone_number,
       r.terminal

FROM routes AS r


    JOIN user_routes ur on r.id = ur.route_id
    JOIN users u on ur.user_id = u.id


    JOIN locker_pallets lp on r.id = lp.route_id
    JOIN locker_pallet_parcels lpp on lp.id = lpp.locker_pallet_id
    JOIN parcels p on lpp.parcel_id = p.id
    JOIN orders o on o.id = p.order_id
    JOIN locker_consignments lc on o.id = lc.order_id
    JOIN cancellations c on lc.cancellation_id = c.id
    JOIN lockers l on o.locker_id = l.id
    LEFT JOIN parcel_box_assignments pba on p.id = pba.parcel_id
    LEFT JOIN scanning_log sl on sl.id = (SELECT id FROM scanning_log WHERE scanning_log.parcel_id = p.id ORDER BY date DESC limit 1)


WHERE r.type  = 'LOCKER'
AND r.due_date = date(now())
AND c.cancellation_category != "PARCEL_NOT_ADDED_TO_ROUTE"
AND pba.id IS NULL
GROUP BY l.id
ORDER BY count(id) DESC
"""

consumers_with_app_over_time_df = readJDBC(query11, 'budbee')
writeSnowflake(consumers_with_app_over_time_df, 'consumers_with_app_over_time')

# COMMAND ----------

# DAG: lockers_current_degree_of_filling
query12 = """
SELECT l.id,
       l.name,
       count(lb.id) AS number_of_boxes,
       sum(CASE WHEN pba.state = "PRESENT" THEN 1 ELSE 0 END) AS used_boxes,
       (sum(CASE WHEN pba.state = "PRESENT" THEN 1 ELSE 0 END) / count(lb.id)) * 100  AS ratio,
       pcz.country_code,
       now() as time_stamp

FROM  locker_boxes AS lb


		LEFT JOIN parcel_box_assignments AS pba
		  ON lb.id = pba.box_id AND (pba.state = "PRESENT")

		JOIN lockers AS l
		  ON lb.locker_id = l.id

        JOIN postal_code_zones pcz on l.postal_code_zone_id = pcz.id

GROUP BY l.id
ORDER BY count(pba.id) DESC
"""

lockers_current_degree_of_filling_df = readJDBC(query12, 'budbee')
writeSnowflake(lockers_current_degree_of_filling_df, 'lockers_current_degree_of_filling')
