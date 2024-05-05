import unittest
from pathlib import Path

from isas_base.subpackage import BaseMeasurement
from isas_base.subpackage.tests import MeasurementTestFactory


class MeasurementTmp(BaseMeasurement):
    MODEL_NAME = 'MeasurementTmp'
    DEFAULT_CFG = {
        'TEST_KEY_0': 0,
        'TEST_KEY_1': {
            'TEST_KEY_1_1': 1.1,
            'TEST_KEY_1_2': None,
            },
        }
    DEFAULT_INPUT_DATA_NAMES = {}
    DEFAULT_OUTPUT_DATA_NAMES = {}

    def _set_model(self):
        pass

    def __call__(self, time_series_dataset):
        return {}

    def exit(self):
        pass


TEST_PARAMS = {
    'MODEL_NAME': 'MeasurementTmp',
    'INSTANCE_NAME': 'tmp',
    'INPUT_DATA_NAMES': [],
    'OUTPUT_DATA_NAMES': [],
    'SENSOR_NAME': None,
    'STRUCTURAL_MODEL_NAME': None,
    'PARAMS': {'TEST_KEY_1': {'TEST_KEY_1_2': 1.2}},
    'DATADRIVE': Path('/root/datadrive'),
    'DATA_SYSTEM': 'file',
    }


class BaseMeasurementTest:
    factory = MeasurementTestFactory(
        model_class=MeasurementTmp,
        test_params=TEST_PARAMS,
        )
    BaseMeasurementTestInit, BaseMeasurementTestSimulation, BaseMeasurementTest = factory()


class TestModelInit(BaseMeasurementTest.BaseMeasurementTestInit):
    pass


class TestModelSimulation(BaseMeasurementTest.BaseMeasurementTestSimulation):
    def _test_set_model(self):
        pass

    def _test_call(self, results):
        pass


class TestModel(BaseMeasurementTest.BaseMeasurementTest):
    def _test_set_model(self):
        pass

    def _test_call(self, results):
        pass


if __name__ == '__main__':
    unittest.main()
