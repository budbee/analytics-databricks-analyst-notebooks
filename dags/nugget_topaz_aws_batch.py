from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.operators.dummy import DummyOperator
from lib import slack_notifications


with DAG(
 "topaz_nugget",
  default_args = {
    "depends_on_past": False,
    "owner": "Analytics team",
    "email": ["ji.krochmal@budbee.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=60),
    "max_active_runs": 1,
    "concurrency": 1,
    "on_failure_callback": slack_notifications.dag_failure
  },
  description="Puts AWS Batch jobs on a dedicated job queue",
  schedule_interval='0 6-22 * * *',
  start_date=datetime(2021, 4, 21),
  catchup=False,
  tags=["AWS Batch", "nugget", "topaz"]

) as dag:

  def overrides_gen(job_name, table_name,
    method="LOG_BASED", vcpu="1", memory="2048", batch_size="50000", tap_db="budbee",
    tap_schema="budbee", target_schema="ingestion", target_db="bruce"
  ):
    return {
          "resourceRequirements": [
            {"type": "VCPU", "value": vcpu},
            {"type": "MEMORY", "value": memory}
          ],
          "environment": [
              {"name":"JOB_NAME", "value":job_name},
              {"name":"TAP_SCHEMA", "value":tap_schema},
              {"name":"TAP_DB", "value":tap_db},
              {"name":"TARGET_SCHEMA", "value":target_scheam},
              {"name":"TARGET_DB", "value":target_db},
              {"name":"TAP_TABLE", "value":table_name},
              {"name":"BATCH_SIZE", "value":batch_size},
              {"name":"METHOD", "value":method},
             ]
        }

  task_start = DummyOperator(
    task_id = "dummy"
  )

  addresses = BatchOperator(
    task_id = "addresses",
    job_name = "topaz_addresses",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_addresses", "ADDRESSES")
    )

  cancellations = BatchOperator(
    task_id = "cancellations",
    job_name = "topaz_cancellations",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_cancellations", "CANCELLATIONS")
    )

  consignments = BatchOperator(
    task_id = "consignments",
    job_name = "topaz_consignments",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_consignments", "CONSIGNMENTS")
    )

  consumer_stop_consignments = BatchOperator(
    task_id = "consumer_stop_consignments",
    job_name = "topaz_consumer_stop_consignments",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_consumer_stop_consignments", "CONSUMER_STOP_CONSIGNMENTS")
    )

  consumer_stops = BatchOperator(
    task_id = "consumer_stops",
    job_name = "topaz_consumer_stops",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_consumer_stops", "CONSUMER_STOPS")
    )

  dimensions = BatchOperator(
    task_id = "dimensions",
    job_name = "topaz_dimensions",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_dimensions", "DIMENSIONS")
    )

  intervals = BatchOperator(
    task_id = "intervals",
    job_name = "topaz_intervals",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_intervals", "INTERVALS")
    )

  klarna_payments = BatchOperator(
    task_id = "klarna_payments",
    job_name = "topaz_klarna_payments",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_klarna_payments", "KLARNA_PAYMENTS")
    )

  locker_consignments = BatchOperator(
    task_id = "locker_consignments",
    job_name = "topaz_locker_consignments",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_locker_consignments", "LOCKER_CONSIGNMENTS")
    )

  locker_pallet_parcels = BatchOperator(
    task_id = "locker_pallet_parcels",
    job_name = "topaz_locker_pallet_parcels",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_locker_pallet_parcels", "LOCKER_PALLET_PARCELS")
    )

  locker_stops = BatchOperator(
    task_id = "locker_stops",
    job_name = "topaz_locker_stops",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_locker_stops", "LOCKER_STOPS")
    )

  misses = BatchOperator(
    task_id = "misses",
    job_name = "topaz_misses",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_misses", "MISSES")
    )

  orders = BatchOperator(
    task_id = "orders",
    job_name = "topaz_orders",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_orders", "ORDERS")
    )

  pallet_parcels = BatchOperator(
    task_id = "pallet_parcels",
    job_name = "topaz_pallet_parcels",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_pallet_parcels", "PALLET_PARCELS")
    )

  parcel_box_assignments = BatchOperator(
    task_id = "parcel_box_assignments",
    job_name = "topaz_parcel_box_assignments",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_parcel_box_assignments", "PARCEL_BOX_ASSIGNMENTS")
    )

  parcel_consignments = BatchOperator(
    task_id = "parcel_consignments",
    job_name = "topaz_parcel_consignments",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_parcel_consignments", "PARCEL_CONSIGNMENTS")
    )

  parcel_status_updates = BatchOperator(
    task_id = "parcel_status_updates",
    job_name = "topaz_parcel_status_updates",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_parcel_status_updates", "PARCEL_STATUS_UPDATES")
    )

  parcels = BatchOperator(
    task_id = "parcels",
    job_name = "topaz_parcels",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_parcels", "PARCELS")
    )

  ratings = BatchOperator(
    task_id = "ratings",
    job_name = "topaz_ratings",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_ratings", "RATINGS")
    )

  routeopt_routes = BatchOperator(
    task_id = "routeopt_routes",
    job_name = "topaz_routeopt_routes",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_routeopt_routes", "ROUTEOPT_ROUTES")
    )

  scanning_log = BatchOperator(
    task_id = "scanning_log",
    job_name = "topaz_scanning_log",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_scanning_log", "SCANNING_LOG")
    )

  terminal_stop_consignments = BatchOperator(
    task_id = "terminal_stop_consignments",
    job_name = "topaz_terminal_stop_consignments",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_terminal_stop_consignments", "TERMINAL_STOP_CONSIGNMENTS")
    )

  terminal_stops = BatchOperator(
    task_id = "terminal_stops",
    job_name = "topaz_terminal_stops",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_terminal_stops", "TERMINAL_STOPS")
    )

  timestamps = BatchOperator(
    task_id = "timestamps",
    job_name = "topaz_timestamps",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_timestamps", "TIMESTAMPS")
    )

  # Consumer schema
  consumer_timeslotSubscription = BatchOperator(
    task_id = "consumer_timeslotSubscription",
    job_name = "topaz_consumer_timeslotSubscription",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_consumer_timeslotSubscription", "CONSUMER:TIMESLOT_SUBSCRIPTION", tap_schema="consumers")
    )

  consumer_order = BatchOperator(
    task_id = "consumer_order",
    job_name = "topaz_consumer_order",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_consumer_order", "CONSUMER_ORDER", tap_schema="consumers")
    )

  # Small tables
  small_1 = BatchOperator(
    task_id = "small_1",
    job_name = "topaz_small_1",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_small_1", "BUYER_SETTINGS:BUYERS:CHRONO_CONSIGNMENT_LIMITS:COLLECTION_POINT_INTERVALS", vcpu="5", memory="8096")
    )

  small_2 = BatchOperator(
    task_id = "small_2",
    job_name = "topaz_small_2",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_small_2", "COLLECTION_POINT_SCHEDULES:COLLECTION_POINTS:LOCKER_BOXES:LOCKER_LOCATION_PARTNERS", vcpu="5", memory="8096")
    )

  small_3 = BatchOperator(
    task_id = "small_3",
    job_name = "topaz_small_3",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_small_3", "LOCKER_PALLETS:LOCKERS:ON_DEMAND_PICKUPS:OWNER_OPERATORS", vcpu="5", memory="8096")
    )

  small_4 = BatchOperator(
    task_id = "small_4",
    job_name = "topaz_small_4",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_small_4", "PALLETS:POSTAL_CODE_ZONES:RECURRING_PICKUP_SCHEDULE:RECURRING_PICKUP_SCHEDULE_MERCHANT_SCHEDULES", vcpu="5", memory="8096")
    )

  small_5 = BatchOperator(
    task_id = "small_5",
    job_name = "topaz_small_5",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_small_5", "RETURN_PALLET_DETAILS:ROUTEOPT_GROUP_SETTINGS:ROUTEOPT_GROUPS:ROUTES", vcpu="5", memory="8096")
    )

  small_6 = BatchOperator(
    task_id = "small_6",
    job_name = "topaz_small_6",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_small_6", "SORTING_RULE_ROUTE:TERMINAL_DOCKS:TERMINAL_SORTING_LANES:USER_ROUTES", vcpu="5", memory="8096")
    )

  small_7 = BatchOperator(
    task_id = "small_7",
    job_name = "topaz_small_7",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_small_7", "USER_SETTINGS:USERS:WAREHOUSES", vcpu="4", memory="8096")
    )

  small_8 = BatchOperator(
    task_id = "small_8",
    job_name = "topaz_small_8",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_small_8", "BUYER_COLLECTION_POINTS:BUYER_TAGS:OWNER_OPERATOR_USERS:ROUTEOPT_GROUP_SETTINGS_CONSIGNMENT_TYPES", vcpu="4", memory="8096")
    )

task_start >> [
addresses,
cancellations,
consignments,
consumer_stop_consignments,
consumer_stops,
dimensions,
intervals,
klarna_payments,
locker_consignments,
locker_pallet_parcels,
locker_stops,
misses,
orders,
pallet_parcels,
parcel_box_assignments,
parcel_consignments,
parcel_status_updates,
parcels,
ratings,
routeopt_routes,
scanning_log,
terminal_stop_consignments,
terminal_stops,
timestamps,
consumer_timeslotSubscription,
consumer_order,
topaz_small_1,
topaz_small_2,
topaz_small_3,
topaz_small_4,
topaz_small_5,
topaz_small_6,
topaz_small_7,
topaz_small_8
]
