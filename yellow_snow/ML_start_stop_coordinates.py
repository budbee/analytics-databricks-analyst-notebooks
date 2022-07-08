# Databricks notebook source
from pyspark.sql.window import Window as W

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

## EXTRACT

query = """
SELECT
    r.id AS route_id,
    CAST(r.due_date AS datetime) AS route_date,
    r.type AS route_type,
    r.terminal,
    s.sequence AS sequence_stop,
    s.id AS stop_id,
    stop_a.country_code,
    ST_X(stop_a.coordinate) AS lng,
    ST_Y(stop_a.coordinate) AS lat,
    CONVERT_TZ(st_data.departure_time, 'UTC', 'CET') as planned_departure_cet

FROM
    routes r
        JOIN (SELECT * FROM consumer_stops
              WHERE  route_id IN (SELECT DISTINCT id FROM routes WHERE type = "DISTRIBUTION"
                                                                   AND due_date >= DATE_ADD(CURRENT_DATE, INTERVAL -1 MONTH))

                                  UNION
              SELECT * FROM terminal_stops
              WHERE route_id IN (SELECT DISTINCT id FROM routes WHERE type = "DISTRIBUTION"
                                                                    AND due_date >= DATE_ADD(CURRENT_DATE, INTERVAL -1 MONTH))
            ) s ON s.route_id = r.id
        JOIN addresses stop_a ON s.address_id = stop_a.id
        JOIN intervals i ON i.id = s.estimated_interval_id
        JOIN timestamps t ON i.start_timestamp_id = t.id
        JOIN (SELECT
                    COALESCE(consumer_stop_id, terminal_stop_id) as stop_id,
                    departure_time
              FROM routeopt_service_time_data
              WHERE route_id IN (SELECT DISTINCT id FROM routes WHERE type = "DISTRIBUTION"
                                                                  AND due_date >= DATE_ADD(CURRENT_DATE, INTERVAL -1 MONTH))
            ) st_data ON st_data.stop_id = s.id
          
"""

df_raw = readJDBC(query, 'budbee')

query2 = """SELECT
    r.id AS route_id,
    CAST(r.due_date AS datetime) AS route_date,
    r.type AS route_type,
    r.terminal,
    CAST(REPLACE(uos.user_type, "LEVEL_", "") AS UNSIGNED) AS driver_level,
    r.vehicle_type AS vehicle_type,

    -- If either of the consuer stops id are NULL it means we are at a
    -- terminal.
    IF(from_cs.id IS NULL,
       "TERMINAL",
       "CONSUMER_STOP") AS from_stop_type,
    IF(to_cs.id IS NULL,
       "TERMINAL",
       "CONSUMER_STOP") AS to_stop_type,

    -- A vehicle coming from/to the terminal will have
    -- from_cs.sequence/to_cs.sequence = NULL so we need to pick the
    -- sequence from terminal stop consignments table instead
    COALESCE(from_cs.sequence, ts.sequence) AS from_sequence,
    COALESCE(to_cs.sequence, ts.sequence) AS to_sequence,

    -- Coordinates are taken from either consumer_stop_consignments or
    -- terminal_stop_consignments table depending on the type of stop
    -- it was
    IF(from_cs.id IS NOT NULL,
       X(from_cs_ad.coordinate),
       X(ts_ad.coordinate)) AS from_coord_x,
    IF(from_cs.id IS NOT NULL,
       Y(from_cs_ad.coordinate),
       Y(ts_ad.coordinate)) AS from_coord_y,
    IF(to_cs.id IS NOT NULL,
       X(to_cs_ad.coordinate),
       X(ts_ad.coordinate)) AS to_coord_x,
    IF(to_cs.id IS NOT NULL,
       Y(to_cs_ad.coordinate),
       Y(ts_ad.coordinate)) AS to_coord_y,

    -- Address data
    a.country_code,
    a.city,
    a.postal_code,

    -- HERE drive time estimate is defined as
    --
    -- estimated_drive_time = here_drive_time * drive_time_factor
    --
    -- But from the terminal we also have a initial drive time factor,
    -- hence the check before doing the calculation.
    IF(from_cs.id IS NULL,
       rdtd.estimated_drive_time / (uos.drive_time_factor * rgs.drive_time_factor * rgs.initial_drive_time_factor),
       rdtd.estimated_drive_time / (uos.drive_time_factor * rgs.drive_time_factor))
       AS here_drive_time_estimate,
    rdtd.estimated_drive_time as router_drive_time_estimate,
    rdtd.distance,

    COUNT(DISTINCT from_parcels_cs.id) AS from_num_parcels,
    COUNT(DISTINCT to_parcels_cs.id) AS to_num_parcels,

    --  Each stop can have several consignments, pick the latest one
    --  in the from stop and the earliest one in the to stop
    MAX(from_psu_cs.date) AS from_cs_last_processed,
    MIN(to_pcu_cs.date) AS to_cs_first_processed,
    MIN(psu_ts.date) AS ts_first_processed,
    MAX(psu_ts.date) AS ts_last_processed,

    -- If it is a consumer stop we get the departure time from service
    -- time data for consumer_stop, otherwise take it from terminal
    -- stop departure time
    IF(from_cs.id IS NOT NULL,
       st_dt_cs.departure_time,
       st_dt_ts.departure_time) AS departure_actual,
    IF(to_cs.id IS NOT NULL,
       st_dt_cs_next.arrival_time,
       st_dt_ts.arrival_time) AS arrival_actual_next

FROM
    routes AS r
    JOIN user_routes as ur on r.id = ur.route_id
    JOIN warehouses AS w ON w.code = r.terminal
    AND w.deleted_at IS NULL
    JOIN addresses a
        ON w.address_id = a.id

    JOIN routeopt_drive_time_data rdtd
        ON rdtd.route_id = r.id
    LEFT JOIN routeopt_routes AS rr
        ON r.routeopt_route_id = rr.id
    LEFT JOIN routeopt_optimizations AS opt
        ON rr.optimization_id = opt.id
    LEFT JOIN routeopt_settings AS rs
        ON opt.optimization_settings_id = rs.id
    LEFT JOIN routeopt_graph_settings AS rgs
        ON rs.routeopt_graph_settings_id = rgs.id
    LEFT JOIN routeopt_vehicles_and_users AS rvau
        ON rr.routeopt_vehicle_and_user_id = rvau.id
    LEFT JOIN routeopt_vehicles_and_users_settings AS rvaus
        ON rvau.routeopt_vehicle_and_user_settings_id = rvaus.id
    LEFT JOIN user_optimization_settings AS uos
        ON rvaus.user_optimization_settings_id = uos.id

    -- Consumer and terminal stops
    LEFT JOIN consumer_stops AS from_cs
        ON rdtd.from_consumer_stop_id = from_cs.id
    LEFT JOIN consumer_stops AS to_cs
        ON rdtd.to_consumer_stop_id = to_cs.id
    LEFT JOIN terminal_stops AS ts
        ON ts.id = rdtd.terminal_stop_id

   -- Used to get address of terminals using consumer_stop ids
    LEFT JOIN intervals AS from_cs_i
        ON from_cs.estimated_interval_id = from_cs_i.id
    LEFT JOIN timestamps AS from_cs_ts
        ON from_cs_i.stop_timestamp_id = from_cs_ts.id
    LEFT JOIN addresses AS from_cs_ad
        ON from_cs.address_id = from_cs_ad.id
    LEFT JOIN addresses AS to_cs_ad
        ON to_cs_ad.id = to_cs.address_id
    LEFT JOIN addresses ts_ad
        ON ts_ad.id = ts.address_id

    -- Consumer and terminal consignments
    LEFT JOIN consumer_stop_consignments AS from_cs_cons
       ON from_cs.id = from_cs_cons.consumer_stop_id
    LEFT JOIN consumer_stop_consignments AS to_cs_cons
       ON to_cs.id = to_cs_cons.consumer_stop_id
    LEFT JOIN consignments AS from_cons
       ON from_cs_cons.consignment_id = from_cons.id
    LEFT JOIN consignments AS to_cons
       ON to_cs_cons.consignment_id = to_cons.id
    LEFT JOIN terminal_stop_consignments AS ts_cons
       ON ts.id = ts_cons.terminal_stop_id

    -- Service time for consumer and terminal stops
    LEFT JOIN routeopt_service_time_data AS st_dt_cs
       ON from_cs.id = st_dt_cs.consumer_stop_id
    LEFT JOIN routeopt_service_time_data AS st_dt_cs_next
       ON to_cs.id = st_dt_cs_next.consumer_stop_id
    LEFT JOIN routeopt_service_time_data AS st_dt_ts
       ON ts.id = st_dt_ts.terminal_stop_id

    -- Parcel consignments for terminal and consumer stops
    LEFT JOIN parcel_consignments AS from_pc_cs
       ON from_cs_cons.consignment_id = from_pc_cs.consignment_id
    LEFT JOIN parcel_consignments AS to_pc_cs
       ON to_cs_cons.consignment_id = to_pc_cs.consignment_id
    LEFT JOIN parcel_consignments AS ts_pc
       ON ts_cons.consignment_id = ts_pc.consignment_id

    -- Parcel data with order id and type of delivery
    LEFT JOIN parcels AS from_parcels_cs
       ON from_pc_cs.parcel_id = from_parcels_cs.id
    LEFT JOIN parcels AS to_parcels_cs
       ON to_pc_cs.parcel_id = to_parcels_cs.id

    -- Parcel status update data. We look at parcel updates when the
    -- type is DELIVERY and the scan code is either Delivered (3) or
    -- Missed (8). If the parcel type is a RETURN then we look at
    -- Collected (2) instead because at the consumer stop the driver
    -- collects the package there (and missed if he fails).
    LEFT JOIN parcel_status_updates AS from_psu_cs
        ON from_cs_cons.consignment_id = from_psu_cs.consignment_id
        AND DATE_FORMAT(from_psu_cs.date, "%Y-%m-%d") = r.due_date
        AND ((from_psu_cs.parcel_status_id IN (3 , 8) AND from_parcels_cs.type = "DELIVERY")
            OR (from_psu_cs.parcel_status_id IN (2 , 8) AND from_parcels_cs.type = "RETURN"))
    LEFT JOIN parcel_status_updates AS to_pcu_cs
        ON to_cs_cons.consignment_id = to_pcu_cs.consignment_id
        AND DATE_FORMAT(to_pcu_cs.date, "%Y-%m-%d") = r.due_date
        AND ((to_pcu_cs.parcel_status_id IN (3 , 8) AND to_parcels_cs.type = "DELIVERY")
           OR (to_pcu_cs.parcel_status_id IN (2 , 8) AND to_parcels_cs.type = "RETURN"))

    -- At the terminal we only want info on the first and last time
    -- the driver loads or process any package that is to be used in
    -- the route. This happens only at the terminal stops. When the
    -- type of the parcel is DELIVERY we check the code Collected (2)
    -- that means driver loads it in the car. If the parcel type is
    -- RETURN then we check that the driver has Collected the
    -- receiving label (status 10).
    LEFT JOIN parcels AS ts_parcels
        ON ts_pc.parcel_id = ts_parcels.id
    LEFT JOIN parcel_status_updates AS psu_ts
        ON ts_cons.consignment_id = psu_ts.consignment_id
        AND ((psu_ts.parcel_status_id = 2 AND ts_parcels.type = "DELIVERY" )
        OR (psu_ts.parcel_status_id = 10 AND ts_parcels.type = "RETURN"))

WHERE
    r.type = "DISTRIBUTION"
    AND r.due_date BETWEEN :from_date AND :to_date
GROUP BY r.id, ts.id, from_cs.id, to_cs.id
ORDER BY r.id, from_cs_last_processed, from_sequence"""

df_raw2 = readJDBC(query2, 'budbee')

# COMMAND ----------

## TRANSFORM

df_lag = df_raw.withColumn('prev_stop_id',
                        F.lag(df_raw['stop_id'])
                                 .over(W.partitionBy("route_id").orderBy("sequence_stop"))).cache()

df_raw_stop_info = df_raw.drop('route_id','route_date','route_type','terminal','planned_departure_cet', 'country_code')

df_stop_info = df_raw_stop_info.withColumnRenamed("sequence_stop","start_sequence_stop").withColumnRenamed("lng","start_lng").withColumnRenamed("lat","start_lat").withColumnRenamed("stop_id","stop_id_new")

df_join = df_lag.join(df_stop_info, (df_stop_info.stop_id_new == df_lag.prev_stop_id), 'left')

df_main = df_join.drop('route_date','route_type','terminal', 'country_code', 'stop_id_new').withColumnRenamed("sequence_stop","stop_sequence").withColumnRenamed("lng","stop_lng").withColumnRenamed("lat","stop_lat").withColumnRenamed("stop_id","stop_id")


# COMMAND ----------

## LOAD

writeSnowflake(df_main, 'start_stop_coordinates', 'machine_learning','main')
writeSnowflake(df_raw2, 'parcel_consignment_status_update', 'machine_learning','main')

# COMMAND ----------

from datetime import datetime, date, timedelta
import requests

# Perform API call
service_ips = {
    "SE": "http://10.0.21.123:30259",
    "NL": "http://10.0.21.206:31928",
}

def _seconds_since_midnight(date):
    return (date - date.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()

def get_drive_time(start_lon: float, start_lat: float, stop_lon: float, stop_lat: float, date, country_code: str):
    endpoint = f"{service_ips[country_code]}/eta/time-to-destination"
    query_parameters = {
        "stopLatitude": stop_lat,
        "stopLongitude": stop_lon,
        "startLatitude": start_lat,
        "startLongitude": start_lon,
        "fromParkingLatitude": start_lat,
        "fromParkingLongitude": start_lon,
        "driveTimeFactor": 1.0,
        "speedProfile": "HOURLY_PROFILE",
        "dayOfWeek": date.strftime('%A').upper(),
        "timeOfDay": _seconds_since_midnight(date),
    }
    request_str = _form_request_str(endpoint, query_parameters)
    response = requests.get(request_str)
    if response.status_code == 200:
        return float(response.json()["time"])
    raise ValueError(f"Received: {response.text}\nfrom: {request_str}")

    
def _form_request_str(endpoint, query_parameters):
    return endpoint + "?" + "&".join([f"{k}={str(v)}" for k, v in query_parameters.items()])



terminal_lon = 17.733420
terminal_lat = 59.509640
stop_lon = 18.037881
stop_lat = 59.347475

get_drive_time(
    17.733420,
    59.509640,
    18.037881,
    59.347475,
    datetime.now(),
    "SE",
)#df_api = df_main.withColumn()
