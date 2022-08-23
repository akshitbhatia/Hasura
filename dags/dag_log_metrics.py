import os
from datetime import timedelta

import pendulum
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain

from base_dags.base_dag import BASE_DAG
from pipeline_utils.gen_log_metrics import *

USERNAME = 'airflow'
ROOT_DIR = os.path.dirname(os.path.abspath('__file__'))
FILE_PATH = os.path.join(ROOT_DIR, 'dags/test_data.csv')
year, month, day = 2022, 1, 1

DEFAULT_ARGS = {
    "owner": USERNAME,
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": pendulum.datetime(year, month, day, tz="UTC"),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": None,
}

dag_id = 'Hasura-Log-Metrics-Generator'

LogMetrcisGen = BASE_DAG(
    dag_id=dag_id,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False
)

DAG = LogMetrcisGen.Create_Dag(
    dagrun_timeout=timedelta(minutes=5),
    max_active_runs=1
)
globals()[dag_id] = DAG
########################################################################################################################


start = DummyOperator(
    task_id='start',
    dag=globals()[dag_id]
)

validate_logs = PythonOperator(
    task_id='Validate-logs',
    python_callable=validate_columns,
    op_kwargs={'file_path': FILE_PATH},
    provide_context=True,
    dag=globals()[dag_id]
)

raw_data = PythonOperator(
    task_id='Get-Raw-Data',
    python_callable=get_raw_data,
    op_kwargs={'file_path': FILE_PATH},
    provide_context=True,
    dag=globals()[dag_id]
)

clean_data = PythonOperator(
    task_id='Cleanse-Data',
    python_callable=cleanse_data,
    provide_context=True,
    dag=globals()[dag_id]
)

data_transfer = PythonOperator(
    task_id='Data-Transfer-Per-project',
    python_callable=get_data_transfer,
    provide_context=True,
    dag=globals()[dag_id]
)

most_time_consuming_project = PythonOperator(
    task_id='Most-Time-Consuming-Per-Project',
    python_callable=get_most_time_consuming_project,
    provide_context=True,
    dag=globals()[dag_id]
)

end = DummyOperator(
    task_id='End',
    dag=globals()[dag_id]
)

log_metrics = [data_transfer, most_time_consuming_project]

chain(
    start,
    validate_logs,
    raw_data,
    clean_data,
    log_metrics,
    end
)
