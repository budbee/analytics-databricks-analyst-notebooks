from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.operators.dummy import DummyOperator
from lib import slack_notifications


with DAG(
 "test_nugget",
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
              {"name":"TARGET_SCHEMA", "value":target_schema},
              {"name":"TARGET_DB", "value":target_db},
              {"name":"TAP_TABLE", "value":table_name},
              {"name":"BATCH_SIZE", "value":batch_size},
              {"name":"METHOD", "value":method},
             ]
        }

  task_start = DummyOperator(
    task_id = "dummy"
  )

  buyers = BatchOperator(
    region_name = "eu-west-1",
    task_id = "buyers",
    job_name = "topaz_buyers",
    job_queue = "BatchEfsJQ",
    job_definition = "MySQL2PostgreSQL",
    overrides = overrides_gen("topaz_buyers", "buyers")
    )

task_start >> [
buyers
]
