import queue
import shutil
import unittest
from copy import deepcopy

from isas_base.data import DataManager, DynamicData, ParallelExporter

DATADRIVE = '/root/datadrive_tmp'
DATA_NAME = 'rosette_strain_x'
DATA_SYSTEM = 'file'


class TestUtils(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(DATADRIVE, ignore_errors=True)
        shutil.copytree('/root/datadrive', DATADRIVE)

    def tearDown(self):
        shutil.rmtree(DATADRIVE, ignore_errors=True)

    def test_parallel_exporter(self):
        data_manager = DataManager(datadrive=DATADRIVE)
        dynamic_data_1 = data_manager.import_dynamic_data(
            data_name=DATA_NAME,
            table_data_system=DATA_SYSTEM,
            time_series_data_system=DATA_SYSTEM,
            )
        dynamic_data_tmp = DynamicData(
            time_series_data={f'{DATA_NAME}_tmp': deepcopy(dynamic_data_1.time_series_data[DATA_NAME])}
            )
        export_queue = queue.Queue()
        parallel_exporter = ParallelExporter(
            export_queue,
            data_manager,
            table_data_system=DATA_SYSTEM,
            time_series_data_system=DATA_SYSTEM,
            )
        parallel_exporter.start()
        export_queue.put((f'{DATA_NAME}_tmp', dynamic_data_tmp))
        parallel_exporter.exit = True
        parallel_exporter.join()

        dynamic_data_2 = data_manager.import_dynamic_data(
            data_name=f'{DATA_NAME}_tmp',
            table_data_system=DATA_SYSTEM,
            time_series_data_system=DATA_SYSTEM,
            )
        self.assertEqual(
            dynamic_data_1.time_series_data[DATA_NAME],
            dynamic_data_2.time_series_data[f'{DATA_NAME}_tmp'],
            )


if __name__ == '__main__':
    unittest.main()
