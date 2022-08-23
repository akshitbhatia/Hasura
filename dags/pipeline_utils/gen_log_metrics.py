import logging
import traceback
from typing import Dict, Optional, Union, Any

import pandas as pd
from pandasql import sqldf

COL_COUNT, COLUMN_NAMES = 8, ["timestamp", "project_id", "operation_name", "runtime", "request_id", "response_size",
                              "request_size",
                              "http_status"]
if logging.getLogger().hasHandlers():
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def x_com_pull(task_ids, key, context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids=task_ids, key=key)
    logging.info(f"Message Pulled for : {task_ids}, Key: {key}")
    return data


def validate_columns(file_path: str) -> Union[Any]:
    data = pd.read_csv(file_path)
    if data.shape[1] != COL_COUNT:
        logging.error("File not valid\nExpected columns are 8\nPlease verify!")
        raise Exception("File not valid\nExpected columns are 8\nPlease verify!")
    return True


def read_csv(file_path: str) -> pd.core.frame.DataFrame:
    try:
        logs_df = pd.read_csv(file_path)
    except Exception as ex:
        logging.error(f"Error: {read_csv.__name__}\n{traceback.format_exc()}")
        raise Exception(f"Error: {read_csv.__name__}\n{traceback.format_exc()}")
    finally:
        return logs_df


def get_raw_data(file_path: str) -> list:
    logs_df = read_csv(file_path)
    logging.info(f"{get_raw_data.__name__} total Rows: {len(logs_df.index)}")
    return logs_df.values.tolist()


def cleanse_data(raw_data: list = None, **context: Optional[Dict]) -> list:
    if not raw_data:
        raw_data: list = x_com_pull(task_ids='Get-Raw-Data', key='return_value', context=context)
    try:
        final_data = []
        for data in raw_data:
            timestamp = data[1].split('"')[1].strip()
            project_id = data[0].split("{")[-1].split(":")[-1].replace('"', "").strip()
            operation_name = data[2].split(":")[-1].replace('"', "").strip()
            runtime = round(float(data[3].split(":")[-1].replace('"', "").strip()), 4)
            request_id = data[4].split(":")[-1].replace('"', "").strip()
            response_size = int(data[5].split(":")[-1].replace('"', "").strip())
            request_size = int(data[6].split(":")[-1].replace('"', "").strip())
            http_status = int(data[7].split(":")[-1].replace("}", "").replace('"', "").strip())
            final_data.append([timestamp, project_id, operation_name, runtime, request_id, response_size, request_size,
                               http_status])
    except Exception as ex:
        logging.error(f"Error: {cleanse_data.__name__}\n{traceback.format_exc()}")
        raise Exception(f"Error: {cleanse_data.__name__}\n{traceback.format_exc()}")
    finally:
        return final_data


def get_data_transfer(start: str = None, end: str = None, data: list = None,
                      **context: Optional[Dict]) -> pd.core.frame.DataFrame:
    if not data:
        data: list = x_com_pull(task_ids='Cleanse-Data', key='return_value', context=context)
        start = context['dag_run'].conf['start']
        end = context['dag_run'].conf['end']
    try:
        logging.info(f"{get_data_transfer.__name__} Data length: {len(data)}")
        stage_df, df_name = pd.DataFrame(data, columns=COLUMN_NAMES), 'stage_df'
        query = f"""
                SELECT t1.project_id, sum(t1.total_data_transfer) as data_transfer
                FROM (
                SELECT timestamp, project_id, operation_name, runtime, request_id,
                response_size, request_size, http_status, (response_size+request_size) as total_data_transfer FROM {df_name}
                WHERE (timestamp BETWEEN '{start}' AND '{end}')
                ) as t1
                group by t1.project_id;
        """
        result = sqldf(query, {**locals(), **globals()})
        print(result)
        return result.values.tolist()
    except Exception as ex:
        logging.error(f"Error: {get_data_transfer.__name__} | {traceback.format_exc()}")
        raise Exception(f"Error: {get_data_transfer.__name__} | {traceback.format_exc()}")


def get_most_time_consuming_project(data: list = None, **context: Optional[Dict]) -> pd.core.frame.DataFrame:
    if not data:
        data: list = x_com_pull(task_ids='Cleanse-Data', key='return_value', context=context)
    try:
        logging.info(f"{get_most_time_consuming_project.__name__} Data length: {len(data)}")
        stage_df, df_name = pd.DataFrame(data, columns=COLUMN_NAMES), 'stage_df'
        query = f"""
            SELECT t1.project_id, sum(t1.runtime) as total_runtime
            FROM (
            SELECT timestamp, project_id, operation_name, runtime, request_id,
            response_size, request_size, http_status FROM {df_name}
            ) t1
            group by t1.project_id
            order by total_runtime desc
        """
        result = sqldf(query, {**locals(), **globals()})
        print(result)
        return result.values.tolist()
    except Exception as ex:
        logging.error(f"Error: {get_most_time_consuming_project.__name__}\n{traceback.format_exc()}")
        raise Exception(f"Error: {get_most_time_consuming_project.__name__}\n{traceback.format_exc()}")
