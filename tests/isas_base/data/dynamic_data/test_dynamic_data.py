import shutil
import unittest
from copy import deepcopy

from isas_base.data import DataManager, DynamicData

DATADRIVE = '/root/datadrive'
DATADRIVE_TMP = '/root/datadrive_tmp'
DATA_NAME = 'rosette_strain_x'
DATA_SYSTEM = 'file'


class TestDynamicData(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        shutil.copytree(DATADRIVE, DATADRIVE_TMP)
        data_manager = DataManager(DATADRIVE_TMP)
        self.dynamic_data = data_manager.import_dynamic_data(
            DATA_NAME,
            table_data_system=DATA_SYSTEM,
            time_series_data_system=DATA_SYSTEM,
            )

    def test_update(self):
        dynamic_data = DynamicData()
        dynamic_data.update(self.dynamic_data)
        self.assertEqual(dynamic_data, self.dynamic_data)

        dynamic_data = deepcopy(self.dynamic_data)
        dynamic_data.update(DynamicData())
        self.assertEqual(dynamic_data, self.dynamic_data)


if __name__ == '__main__':
    unittest.main()
