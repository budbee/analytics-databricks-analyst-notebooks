# Databricks notebook source
## Daily DAGs 
## Run once a day at 7.00

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

## EXTRACT

query = """SELECT
                wh.created_at,
                wh.id,
                wh.name,
                ad.street,
                ad.postal_code,
                wh.code,
                ad.country_code,
                ad.coordinate,
                wh.`sorting_machine_assign_direction`,
                wh.`sorting_machine_books_delivery`,
                wh.`sorting_machine_maximum_routes_per_lane`,
                wh.`sorting_machine_minimum_routes_per_lane`,
                now() as time_stamp
            FROM warehouses as wh
            JOIN addresses as ad on wh.address_id = ad.id
            WHERE wh.deleted_at IS NULL
"""

df_terminals = readJDBC(query, 'budbee')

query2 = """SELECT 
                buyer.id                                                           AS id,
                buyer.external_name                                                AS name,
                zone.title                                                         AS zone,
                rating.score                                                       AS rating
            FROM consignments 
                 JOIN ratings rating ON consignments.id = rating.consignment_id
                 JOIN orders orderr ON consignments.order_id = orderr.id
                 JOIN buyers buyer ON orderr.buyer_id = buyer.id
                 JOIN postal_code_zones zone ON orderr.delivery_postal_code_zone_id = zone.id
            WHERE consignments.date BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND CURDATE()
                  AND consignments.type IN ('DELIVERY', 'RETURN')
                  AND zone.id != 111
          """

df_rating_per_merchant_zone = readJDBC(query2, 'budbee')


query3 = """SELECT 
                id  AS deliveried_to_neighbour,
                date(date) AS date
            FROM parcel_status_updates
            WHERE with_neighbour = 1
                AND date(date) > date_sub(NOW(), INTERVAL 2 MONTH)
            """

df_delivery_done_to_neighbours = readJDBC(query3, 'budbee')


query4 = """SELECT
                sum(CASE WHEN push_notification_token IS NOT NULL then 1 else 0 end) as has_push_activated,
                count(id) as all_consumer_count,
                now() as time_stamp
            FROM consumer"""

df_consumers_push_activated = readJDBC(query4, 'consumers')


query5 = """SELECT 
               b.external_name,
               bs.on_demand_pickup,
               now() as timestamp
             FROM buyers  as b
             JOIN buyer_settings as bs on b.buyer_settings_id = bs.id
             WHERE bs.on_demand_pickup = 1"""
 
df_merchants_with_on_demand_pickups = readJDBC(query5, 'budbee')


query6 ="""SELECT
                concat("https://carriers.budbee.com/admin/orders/", o.token) AS order_token,
                date(o.created_at) AS order_created_at,
                w.name AS terminal,
                pcz.country_code,
                now() as time_stamp

            FROM orders AS o
                JOIN parcels p on o.id = p.order_id
                LEFT JOIN parcel_box_assignments pba on p.id = pba.parcel_id
                LEFT JOIN scanning_log slog on slog.id = (SELECT id FROM scanning_log WHERE parcel_id =  p.id ORDER BY DATE DESC LIMIT 1)
                JOIN lockers AS l ON l.id = o.locker_id
                JOIN postal_code_zones AS pcz ON pcz.id = l.postal_code_zone_id
                JOIN warehouses w on pcz.terminal_id = w.id

            WHERE o.created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
            AND o.locker_id IS NOT NULL
            AND o.cancellation_id IS NULL
            AND o.cart_id NOT LIKE "%route%"
            AND p.recall_reason IS NULL
            AND pba.id IS NULL
            AND slog.id IS NOT NULL
            AND  date(o.created_at) BETWEEN date_sub(NOW(), INTERVAL 90 DAY) AND date_sub(NOW(), INTERVAL 5 DAY)
            """

df_old_box_parcels_that_are_not_delivered = readJDBC(query6, 'budbee')


#query7 = """SELECT 
#                DATE(sent) as date_sent,
#                CONCAT('W', cast(weekofyear(sent) as CHAR),'-',cast(year(sent) AS CHAR)) AS week_sent,
#                COUNT(CASE WHEN delivered IS NOT NULL THEN id END) AS push_delivered,
#                COUNT(ID) AS push_sent,
#                UTC_TIMESTAMP() as timestamp
#            FROM sms_notifications USE INDEX(IDX_message_id)
#            WHERE sent >= date_add(current_date, INTERVAL -1 MONTH)
#              AND message_id LIKE 'projects/%' -- only select push notification - criteria confirmed by Oskar Walker
#            GROUP BY 1,2
#          """

df_push_notification_delivered = readJDBC(query7, 'budbee')

query8 = """SELECT
                    SUM( q.delivered>DATE_ADD(q.tstop, INTERVAL 15 MINUTE) OR q.delivered<DATE_ADD(q.tstart, INTERVAL -15 MINUTE)) as not_ok,
                    count( q.token ) as count,
                    q.due_date as due_date,
                    utc_timestamp() as time_stamp
            FROM (
                SELECT
                        con.cancellation_id as cancellation,
                        o.token as token,
                        r.id as route_id,
                        r.due_date as due_date,
                        ec.sequence as sequence,
                        CONVERT_TZ(dstart.date,'GMT','CET') as tstart,
                        CONVERT_TZ(dstop.date,'GMT','CET') as tstop,
                        CONVERT_TZ(eta.date,'GMT','CET') as eta,
                        (select CONVERT_TZ(psu.date,'GMT','CET') from parcel_status_updates as psu where psu.consignment_id = con.id and psu.parcel_status_id = 3 order by psu.id desc limit 1) as delivered,
                        date(dstart.date) as date
                FROM orders as o
                        JOIN consignments as con on con.order_id = o.id
                        JOIN intervals as i on i.id = con.interval_id
                        JOIN timestamps as dstart on dstart.id = i.start_timestamp_id
                        JOIN timestamps as dstop on dstop.id = i.stop_timestamp_id
                        JOIN consumer_stop_consignments as ccon on ccon.consignment_id =con.id
                        JOIN consumer_stops as ec on ec.id = ccon.consumer_stop_id
                        JOIN intervals as eti on ec.estimated_interval_id = eti.id
                        JOIN timestamps as eta on eta.id = eti.start_timestamp_id
                        JOIN routes as r use index(IDX_due_date) on r.id = ec.route_id
                        JOIN klarna_payments kp ON con.id = kp.consignment_id and kp.status = "COMPLETED" and (
                        kp.purchase_type = 'TIMESLOT'
                            OR (
                            JSON_VALID(kp.reference)
                            AND JSON_EXTRACT(kp.reference, "$.timeWindow.timeslotSpec") != CAST('null' AS JSON)
                            )
                            )
                        WHERE
                        r.type = "DISTRIBUTION" AND
                        con.cancellation_id is null AND
                        r.due_date between date_sub(UTC_DATE(),INTERVAL 14 DAY) and date_sub(UTC_DATE(), interval 1 day)
                        group by o.token) as q
            GROUP BY q.due_date
"""

df_timeslot_success_over_time = readJDBC(query8, 'budbee')

query9 = """SELECT
                r.id AS ratingId,
                con.id AS consignmentId,
                r.score, 
                b.name,
                con.date ,
                b.external_name
            FROM consignments 		as con use index(IDX_date)
                 JOIN orders 			as o 	on con.order_id = o.id
                 LEFT JOIN ratings 		as r 	on r.consignment_id = con.id
                 JOIN buyers 			as b 	on b.id = o.buyer_id
                 JOIN postal_code_zones 	as pcz 	on pcz.id = o.delivery_postal_code_zone_id
            WHERE con.date >= DATE_sub(UTC_DATE(), INTERVAL 1 WEEK) and con.date < UTC_DATE()
              AND pcz.type = "TO_DOOR"
              AND con.cancellation_id is NULL and con.miss_id is null
            """

df_rating_per_day_and_ecommerce_buyer_last_week = readJDBC(query9, 'budbee')


query10 = """SELECT 
                buyer.id                                                           AS id,
                buyer.external_name                                                AS name,
                CASE
                   WHEN (month(consignment.date) = 1 and WEEKOFYEAR(consignment.date) > 5)
                       THEN CONCAT(YEAR(consignment.date - INTERVAL '1' YEAR),
                           '-W',
                           WEEKOFYEAR(consignment.date))
                   ELSE
                       CONCAT(YEAR(consignment.date),
                               '-W',
                               WEEKOFYEAR(consignment.date))
                 END                                                            AS week,
                 rating.score                                                   AS rating
            FROM consignments consignment
                 JOIN ratings rating ON consignment.id = rating.consignment_id
                 JOIN consumer_stop_consignments csc ON consignment.id = csc.consignment_id
                 JOIN consumer_stops cs ON csc.consumer_stop_id = cs.id
                 JOIN orders orderr ON consignment.order_id = orderr.id
                 JOIN buyers buyer ON orderr.buyer_id = buyer.id
            WHERE consignment.date BETWEEN DATE_SUB(CURDATE(), INTERVAL 12 MONTH) AND CURDATE()
              AND consignment.type IN ('DELIVERY', 'RETURN')
          """

df_rating_per_week_per_merchant_last_12_months  = readJDBC(query10, 'budbee')

query11 = """SELECT
                AVG(TIME_TO_SEC(eta.date)-TIME_TO_SEC(rstd.arrival_time))/60,
                STD(TIME_TO_SEC(eta.date)-TIME_TO_SEC(rstd.arrival_time))/60,
                SUM(CASE WHEN (TIME_TO_SEC(eta.date)-TIME_TO_SEC(rstd.arrival_time))/60 < -30 OR (TIME_TO_SEC(eta.date)-TIME_TO_SEC(rstd.arrival_time))/60 > 30 THEN 1 else 0 END) as count_fail,
                SUM(CASE WHEN rstd.arrival_time is NOT NULL  THEN 1 else 0 END) as count_all,
                r.terminal,
                r.due_date,
                utc_timestamp() as time_stamp
            FROM routes as r use index(IDX_due_date)
                 JOIN consumer_stops as cs on cs.route_id = r.id
                 JOIN intervals as i on cs.estimated_interval_id = i.id
                 JOIN timestamps as eta on eta.id = i.start_timestamp_id
                 JOIN routeopt_service_time_data as rstd on rstd.consumer_stop_id = cs.id
            WHERE r.due_date > DATE_SUB(UTC_DATE(), INTERVAL 7 DAY) and r.city!= "OTHER"
            GROUP BY  r.due_date, r.city
"""
df_eta_vs_time_of_arrival = readJDBC(query11, 'budbee')

query12 =  """SELECT
                AVG((TIME_TO_SEC(eta.date)-TIME_TO_SEC(psu.date))/60),
                STD((TIME_TO_SEC(eta.date)-TIME_TO_SEC(psu.date))/60),
                r.city,
                r.due_date,
                pcz.country_code,
                utc_timestamp() as time_stamp
            FROM routes as r use index(IDX_due_date)
                JOIN user_routes ur ON r.id = ur.route_id
                JOIN consumer_stops as cs on cs.route_id = r.id
                JOIN consumer_stop_consignments csc ON cs.id = csc.consumer_stop_id
                JOIN consignments c ON csc.consignment_id = c.id
                JOIN orders o ON c.order_id = o.id
                JOIN postal_code_zones pcz ON o.delivery_postal_code_zone_id = pcz.id
                JOIN parcel_consignments pc ON c.id = pc.consignment_id
                JOIN parcel_status_updates psu ON (c.id = psu.consignment_id and date(psu.date) = c.date and psu.user_id=ur.user_id and ((psu.parcel_status_id in (3,8) and c.type = "DELIVERY") or (psu.parcel_status_id = 2 and c.type = "RETURN")))
                JOIN intervals as i on cs.estimated_interval_id = i.id
                JOIN timestamps as eta on eta.id = i.start_timestamp_id
                LEFT JOIN route_history as rh on rh.route_child_id = r.id
            WHERE r.due_date >date(date_sub(current_date(), interval 1 week)) and r.type = "DISTRIBUTION"
                  AND c.type in("DELIVERY", "RETURN")
                  AND c.cancellation_id is null
                  AND abs(TIME_TO_SEC(eta.date)-TIME_TO_SEC(psu.date))<3*3600
                  AND rh.id is null
            GROUP BY r.city, r.due_date
"""

df_eta_vs_time_of_arrival_per_city_last_week  = readJDBC(query12, 'budbee')


# COMMAND ----------

## TRANSFORM

df_average_rating_per_merchant_zone = df_rating_per_merchant_zone.groupBy("id","name","zone").agg(F.avg("rating").alias("avg_rating")).withColumn('timestamp', F.current_timestamp())

df_deliveries_done_to_neighbours = df_delivery_done_to_neighbours.groupBy("date").agg(F.countDistinct("deliveried_to_neighbour").alias("deliveried_to_neighbour")).withColumn('timestamp', F.current_timestamp())

df_avegare_rating_per_day_and_ecommerce_buyer_last_week = df_rating_per_day_and_ecommerce_buyer_last_week.groupBy("date","name","external_name").agg(F.avg("score").alias("avg_rating"), F.count("ratingId").alias("rated"), F.count("consignmentId").alias("consignments")).withColumn('timestamp', F.current_timestamp())

df_avegare_rating_per_week_per_merchant_last_12_months = df_rating_per_week_per_merchant_last_12_months.groupBy("id","name","week").agg(F.avg("rating").alias("avg_rating")).withColumn('timestamp', F.current_timestamp())


# COMMAND ----------

## LOAD

writeSnowflake(df_terminals, 'terminal_list')
writeSnowflake(df_merchants_with_on_demand_pickups, 'merchants_with_on_demand_pickups')
writeSnowflake(df_consumers_push_activated, 'consumers_push_activated')

writeSnowflake(df_old_box_parcels_that_are_not_delivered, 'old_box_parcels_that_are_not_delivered')
writeSnowflake(df_timeslot_success_over_time, 'timeslot_success_over_time')
writeSnowflake(df_eta_vs_time_of_arrival, 'eta_vs_time_of_arrival')
writeSnowflake(df_eta_vs_time_of_arrival_per_city_last_week, 'eta_vs_time_of_arrival_per_city_last_week')

writeSnowflake(df_average_rating_per_merchant_zone, 'average_rating_per_merchant_zone')
#writeSnowflake(df_push_notification_delivered, 'push_notification_delivered')
