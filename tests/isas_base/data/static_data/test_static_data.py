import shutil
import unittest
from copy import deepcopy

from isas_base.data import DataManager, StaticData

DATADRIVE = '/root/datadrive'
DATADRIVE_TMP = '/root/datadrive_tmp'
DATA_SYSTEM = 'file'


class TestDynamicData(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        shutil.copytree(DATADRIVE, DATADRIVE_TMP)
        data_manager = DataManager(DATADRIVE_TMP)
        self.static_data = data_manager.import_static_data(data_system=DATA_SYSTEM)

    def test_update(self):
        static_data = StaticData()
        static_data.update(self.static_data)
        self.assertEqual(static_data, self.static_data)

        static_data = deepcopy(self.static_data)
        static_data.update(StaticData())
        self.assertEqual(static_data, self.static_data)


if __name__ == '__main__':
    unittest.main()
