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
