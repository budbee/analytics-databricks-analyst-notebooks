from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.operators.dummy import DummyOperator


with DAG(
 "batch_test",
  default_args = {
    "depends_on_past": False,
    "owner": "Analytics team",
    "email": ["ji.krochmal@budbee.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=60),
    "max_active_runs": 1,
    "concurrency": 1
  },
  description="Puts AWS Batch jobs on a dedicated job queue",
  schedule_interval='0 6-22 * * *',
  start_date=datetime(2021, 4, 21),
  catchup=False,
  tags=["AWS Batch", "nugget", "topaz"]

) as dag:

  task_start = DummyOperator(
    task_id = "dummy"
  )

  hello_world = BatchOperator(
    task_id = "hello_world",
    job_name = "topaz_hello_world",
    job_queue = "BatchEfsJQ",
    job_definition = "helloworld",
    )

tasl_start >> [
hello_world
]
