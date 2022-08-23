import os
import unittest

from dags.pipeline_utils.gen_log_metrics import *

ROOT_DIR = os.path.dirname(os.path.abspath('__file__'))
FILE_PATH = os.path.join(ROOT_DIR, 'tests/test_data.csv')
START = '2022-07-30 00:01:00'
END = '2022-07-31 23:59:00'


class log_metrics(unittest.TestCase):
    def test_validate_file(self):
        self.assertEqual(validate_columns(FILE_PATH), True)

    def test_raw_data(self):
        raw_data = get_raw_data(FILE_PATH)
        column_len = len(raw_data[0])
        is_data = True if raw_data else False
        self.assertEqual(is_data, True)
        self.assertEqual(column_len, 8)
        self.assertEqual(len(get_raw_data(FILE_PATH)), 3878)

    def test_cleansed_data(self):
        raw_data = get_raw_data(FILE_PATH)
        self.assertEqual(len(cleanse_data(raw_data)), 3878)

    def test_get_data_transfer(self):
        raw_data = get_raw_data(FILE_PATH)
        cleansed_data = cleanse_data(raw_data)
        data_transter_len = len(get_data_transfer(START, END, cleansed_data))
        data_transter_col_len = len(get_data_transfer(START, END, cleansed_data)[0])
        project_id = get_data_transfer(START, END, cleansed_data)[0][0]
        data_transfer_amount = get_data_transfer(START, END, cleansed_data)[0][1]
        self.assertEqual(data_transter_col_len, 2)
        self.assertEqual(data_transter_len, 3)
        self.assertEqual(project_id, '30c4f1f8-d99d-4a86-9393-2c3689365a8b')
        self.assertEqual(data_transfer_amount, 601968)

    def test_get_most_time_consuming_project(self):
        raw_data = get_raw_data(FILE_PATH)
        cleansed_data = cleanse_data(raw_data)
        most_time_consuming_query_len = len(get_most_time_consuming_project(cleansed_data))
        most_time_consuming_query_col_len = len(get_most_time_consuming_project(cleansed_data)[0])
        project_id = get_most_time_consuming_project(cleansed_data)[0][0]
        time_insec = get_most_time_consuming_project(cleansed_data)[0][1]
        self.assertEqual(most_time_consuming_query_len, 3)
        self.assertEqual(most_time_consuming_query_col_len, 2)
        self.assertEqual(project_id, '30c4f1f8-d99d-4a86-9393-2c3689365a8b')
        self.assertEqual(time_insec, 749.4562)


if __name__ == '__main__':
    unittest.main()
