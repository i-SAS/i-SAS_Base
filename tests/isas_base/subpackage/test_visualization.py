import unittest
from pathlib import Path

from isas_base.subpackage import BaseVisualization
from isas_base.subpackage.tests import VisualizationTestFactory


class VisualizationTmp(BaseVisualization):
    MODEL_NAME = 'VisualizationTmp'
    DEFAULT_CFG = {
        'TEST_KEY_0': 0,
        'TEST_KEY_1': {
            'TEST_KEY_1_1': 1.1,
            'TEST_KEY_1_2': None,
            },
        }
    DEFAULT_INPUT_DATA_NAME_DICT = {}
    DEFAULT_OUTPUT_DATA_NAME_DICT = {}

    def _set_model(self):
        pass

    def __call__(self, time_series_dataset, variables):
        return {}, {}

    def exit(self):
        pass


TEST_PARAMS = {
    'MODEL_NAME': 'VisualizationTmp',
    'SIZE_RATIO': (1, 1),
    'INSTANCE_NAME': 'tmp',
    'INPUT_DATA_NAMES': [],
    'INPUT_DATA_NAME_DICT': {},
    'OUTPUT_DATA_NAMES': [],
    'OUTPUT_DATA_NAME_DICT': {},
    'SENSOR_NAME': None,
    'STRUCTURAL_MODEL_NAME': None,
    'VARIABLES': {},
    'PARAMS': {'TEST_KEY_1': {'TEST_KEY_1_2': 1.2}},
    'DATADRIVE': Path('/root/datadrive'),
    'DATA_SYSTEM': 'file',
    }


class BaseVisualizationTest:
    factory = VisualizationTestFactory(
        model_class=VisualizationTmp,
        test_params=TEST_PARAMS,
        )
    BaseVisualizationTestInit, BaseVisualizationTest = factory()


class TestModelInit(BaseVisualizationTest.BaseVisualizationTestInit):
    pass


class TestModel(BaseVisualizationTest.BaseVisualizationTest):
    def _test_set_model(self):
        pass

    def _test_call(self, results, variables):
        pass


if __name__ == '__main__':
    unittest.main()
