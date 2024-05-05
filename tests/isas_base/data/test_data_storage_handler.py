import shutil
import unittest
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from isas_base.data.data_storage_handler import DataStorageHandler

DATADRIVE = Path('/root/datadrive_tmp')
TABLE_PATH = DATADRIVE / 'static_data/031_sensors/rosette.csv'
DATA_PATH = DATADRIVE / 'dynamic_data/init_tsdb/rosette_strain_x.csv'
TAG_KEYS = ['batch_id', 'service_name', 'batch_datetime']
DATA_SYSTEM = 'file'


class TestDataStorageHandlerInit(unittest.TestCase):
    def test_init(self):
        data_strage_handler = DataStorageHandler(
            datadrive=DATADRIVE,
            )
        self.assertIsInstance(data_strage_handler, DataStorageHandler)
        self.assertEqual(data_strage_handler.datadrive, DATADRIVE)
        self.assertIsInstance(data_strage_handler.data_structure_cfg, dict)


class TestDataStorageHandler(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(DATADRIVE, ignore_errors=True)
        shutil.copytree('/root/datadrive', DATADRIVE)
        self.data_strage_handler = DataStorageHandler(
            datadrive=DATADRIVE,
            )

    def tearDown(self):
        shutil.rmtree(DATADRIVE, ignore_errors=True)

    def test_load_table_data(self):
        df = self.data_strage_handler._load_table_data(TABLE_PATH, data_system=DATA_SYSTEM)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.index.name, 'data_id')
        self.assertEqual(set(df.columns.values.tolist()), {'x', 'y', 'z'})
        self.assertEqual(len(df), 18)

    def test_save_table_data(self):
        df = self.data_strage_handler._load_table_data(TABLE_PATH, data_system=DATA_SYSTEM)
        table_path_tmp = TABLE_PATH.parent / f'{TABLE_PATH.stem}_tmp.csv'
        self.data_strage_handler._save_table_data(table_path_tmp, df, data_system=DATA_SYSTEM)
        _df = self.data_strage_handler._load_table_data(table_path_tmp, data_system=DATA_SYSTEM)
        pd.testing.assert_frame_equal(df, _df)

    def test_load_time_series_data(self):
        df = self.data_strage_handler._load_time_series_data(
            DATA_PATH,
            TAG_KEYS,
            data_system=DATA_SYSTEM
            )
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.index.name, 'time')
        self.assertIsInstance(df.index, pd.DatetimeIndex)
        self.assertEqual(len(df), 12)

    def test_load_time_series_data_with_datetime(self):
        first_datetime = datetime.fromtimestamp(1587772805)
        df = self.data_strage_handler._load_time_series_data(
            DATA_PATH,
            TAG_KEYS,
            data_system=DATA_SYSTEM,
            first_datetime=first_datetime,
            )
        self.assertEqual(len(df), 7)

    def test_save_time_series_data(self):
        df = self.data_strage_handler._load_time_series_data(DATA_PATH, TAG_KEYS, data_system=DATA_SYSTEM)
        data_path_tmp = DATA_PATH.parent / f'{DATA_PATH.stem}_tmp.csv'
        self.data_strage_handler._save_time_series_data(data_path_tmp, df, TAG_KEYS, data_system=DATA_SYSTEM)
        _df = self.data_strage_handler._load_time_series_data(data_path_tmp, TAG_KEYS, data_system=DATA_SYSTEM)
        pd.testing.assert_frame_equal(df, _df)

    def test_import_and_export_dynamic_data_streaming(self):
        df = pd.DataFrame(np.empty((10, 5)))
        tags = {'tag_key': 'tag_value'}
        df = df.assign(**tags)
        data_path = DATADRIVE / f'dynamic_data/streaming_temp/{DATA_PATH.name}'
        self.data_strage_handler._save_time_series_data(data_path, df, tags.keys(), data_system='streaming')
        _df = self.data_strage_handler._load_time_series_data(data_path, tags.keys(), data_system='streaming')
        pd.testing.assert_frame_equal(df, _df)


if __name__ == '__main__':
    unittest.main()
