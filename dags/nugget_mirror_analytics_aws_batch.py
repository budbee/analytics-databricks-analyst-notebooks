from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.operators.dummy import DummyOperator
from lib import slack_notifications


with DAG(
 "mirror_analytics",
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
  tags=["AWS Batch", "nugget", "mirror", "analytics"]

) as dag:

  def overrides_gen(job_name, table_name,
    method="LOG_BASED", vcpu="1", memory="2048", batch_size="50000", tap_db="analytics",
    tap_schema="analytics", tap_key="id",
    tap_host="production-analytics-aurora-57-db-cluster.cluster-c24yqmofl8oi.eu-west-1.rds.amazonaws.com"
  ):
    return {
          "resourceRequirements": [
            {"type": "VCPU", "value": vcpu},
            {"type": "MEMORY", "value": memory}
          ],
          "environment": [
              {"name":"JOB_NAME", "value":job_name},
              {"name":"TAP_SCHEMA", "value":tap_schema},
              {"name":"TAP_PORT", "value":"3306"},
              {"name":"TAP_ID", "value":"tap_mysql"},
              {"name":"TAP_DB", "value":tap_db},
              {"name":"TARGET_ID", "value":"target_snowflake"},
              {"name":"TARGET_SNOWFLAKE_ACCOUNT", "value":"bp67618.eu-west-1"},
              {"name":"TAP_HOST", "value":tap_host},
              {"name":"TARGET_SCHEMA", "value":"analytics"},
              {"name":"TARGET_DB", "value":"mirror"},
              {"name":"TAP_TABLE", "value": table_name},
              {"name":"METHOD", "value":"LOG_BASED"},
              {"name":"S3_BUCKET", "value":"budbee-production-replication"},
              {"name":"BATCH_SIZE", "value":batch_size},
              {"name":"METHOD", "value":method},
              {"name":"TAP_KEY", "value":tap_key}
             ]
        }

  task_start = DummyOperator(
    task_id = "dummy"
  )

  delivery_fact_invoice_rows = BatchOperator(
    task_id = "delivery_fact_invoice_rows",
    job_name = "mirror_analytics_delivery_fact_invoice_rows",
    job_queue = "production-replication-tasks",
    job_definition = "production-mirror-analytics",
    overrides = overrides_gen("delivery_fact_invoice_rows", "DELIVERY_FACT_INVOICE_ROWS")
  )

  f_deliveries = BatchOperator(
    task_id = "f_deliveries",
    job_name = "mirror_analytics_f_deliveries",
    job_queue = "production-replication-tasks",
    job_definition = "production-mirror-analytics",
    overrides = overrides_gen("f_deliveries", "F_DELIVERIES", memory="8192")
  )

  f_consumer_events = BatchOperator(
    task_id = "f_consumer_events",
    job_name = "mirror_analytics_f_consumer_events",
    job_queue = "production-replication-tasks",
    job_definition = "production-mirror-analytics",
    overrides = overrides_gen("f_consumer_events", "F_CONSUMER_EVENTS", memory="8192")
  )

  main_10mplus = BatchOperator(
    task_id = "main_10mplus",
    job_name = "mirror_analytics_main_10mplus",
    job_queue = "production-replication-tasks",
    job_definition = "production-mirror-analytics",
    overrides = overrides_gen("main_10mplus","F_LOCKER_HOURLY_STATISTICS:F_PARCEL_CREATED:F_FIRST_SCAN_PARCELS:F_ORDERS_CREATED:", vcpu="5", memory="5120")
  )

  main_1mplus = BatchOperator(
    task_id = "main_1mplus",
    job_name = "mirror_analytics_main_1mplus",
    job_queue = "production-replication-tasks",
    job_definition = "production-mirror-analytics",
    overrides = overrides_gen("main_1mplus","F_POSTAL_CODE_VALIDATIONS:F_INVOICE_ROWS:F_PARCEL_STATISTICS", vcpu="4", memory="8192")
  )

  # The two following tables need some special treatment due to lack of a primary key in one (full table replication required)
  # and the primary key not being "id" in the other
  route_statistics = BatchOperator(
    task_id = "route_statistics",
    job_name = "mirror_analytics_route_statistics",
    job_queue = "production-replication-tasks",
    job_definition = "production-mirror-analytics",
    overrides = overrides_gen("route_statistics","route_statistics", method="FULL_TABLE")
  )

  zone_statistics = BatchOperator(
    task_id = "zone_statistics",
    job_name = "mirror_analytics_zone_statistics",
    job_queue = "production-replication-tasks",
    job_definition = "production-mirror-analytics",
    overrides = overrides_gen("zone_statistics","zone_statistics", tap_key="zone_id")
  )

  small_tables = BatchOperator(
    task_id = "small_tables",
    job_name = "mirror_analytics_small_tables",
    job_queue = "production-replication-tasks",
    job_definition = "production-mirror-analytics",
    overrides = overrides_gen("small_tables","F_ROUTES:F_TIMESLOTS:F_ORDERS_CANCELLED:F_INVOICES:D_USERS:F_TIMESLOTSUB_SUMMARY:F_CONSIGNMENT_LIMITS:F_CURRENCY_RATES:D_COLLECTION_POINTS:F_MARKET_SUBSIDIES:D_LOCKERS:D_OWNER_OPERATORS:D_BUYERS:D_INVOICE_ARTICLES:D_ZONES:D_CURRENCIES:D_ROUTE_CATEGORIES:D_INVOICE_ARTICLE_UNITS", vcpu="19", memory="2048")
  )

  parcel_packaging_classifications = BatchOperator(
    task_id = "parcel_packaging_classifications",
    job_name = "parcel_packaging_classifications",
    job_queue = "production-replication-tasks",
    job_definition = "production-mirror-analytics",
    overrides = overrides_gen("parcel_packaging_classifications", "parcel_packaging_classifications",
      tap_db="budbee", tap_schema="budbee", tap_host="production-writer.db-endpoint.budbee.com")
  )

task_start >> [
  main_10mplus,
  main_1mplus,
  small_tables,
  f_deliveries,
  f_consumer_events,
  delivery_fact_invoice_rows,
  parcel_packaging_classifications,
  route_statistics,
  zone_statistics
]
