import unittest
from abc import ABC, ABCMeta, abstractmethod

from ...data import DataManager, StaticData
from ...data.static_data.instance_metadata import InstanceMetadata
from ...data.static_data.time_series_metadata import TimeSeriesMetadata


class BaseTestFactory(ABC):
    @property
    @abstractmethod
    def TEST_PARAMS_KEYS(self):  # NOQA
        pass

    def __init__(
            self,
            model_class: type,
            test_params: dict,
            ) -> None:
        self.model_class = model_class
        self.check_test_params(test_params)
        self.test_params = test_params

    def check_test_params(
            self,
            test_params: dict,
            ) -> None:
        for key in self.TEST_PARAMS_KEYS:
            if key not in test_params:
                raise ValueError(f'{key} is required in test_params.')


class BaseTestInit(unittest.TestCase, metaclass=ABCMeta):
    @property
    @abstractmethod
    def TASK(self):  # NOQA
        pass

    @property
    @abstractmethod
    def MODEL_CLASS(self):  # NOQA
        pass

    @property
    @abstractmethod
    def TEST_PARAMS(self):  # NOQA
        pass

    @abstractmethod
    def _init_model(self):
        pass

    @abstractmethod
    def _init_model_without_params(self):
        pass

    @abstractmethod
    def _test_attributes(self, model):
        pass

    def test_attributes(self):
        model = self._init_model()
        self.assertEqual(model.TASK, self.TASK)
        self.assertEqual(model.MODEL_NAME, self.TEST_PARAMS['MODEL_NAME'])
        self.assertEqual(model.instance_name, self.TEST_PARAMS['INSTANCE_NAME'])
        for _, sensor_name in model.output_data_name_to_sensor_name.items():
            self.assertEqual(sensor_name, self.TEST_PARAMS['SENSOR_NAME'])
        for _, structural_model_name in model.output_data_name_to_structural_model_name.items():
            self.assertEqual(structural_model_name, self.TEST_PARAMS['STRUCTURAL_MODEL_NAME'])

        def test_cfg(cfg, params):
            for key, value in params.items():
                if isinstance(value, dict):
                    self.assertIsInstance(cfg[key], dict)
                    test_cfg(cfg[key], value)
                else:
                    self.assertEqual(cfg[key], value)

        test_cfg(model.cfg, self.TEST_PARAMS['PARAMS'])
        self._test_attributes(model)

    def test_config_error(self):
        def default_cfg_include_none(cfg):
            for key, value in cfg.items():
                if isinstance(value, dict):
                    including_none = default_cfg_include_none(value)
                else:
                    including_none = value is None
                if including_none:
                    return including_none

        if default_cfg_include_none(self.MODEL_CLASS.DEFAULT_CFG):
            with self.assertRaises(ValueError):
                self._init_model_without_params()


class BaseTest(unittest.TestCase, metaclass=ABCMeta):
    @property
    @abstractmethod
    def MODEL_CLASS(self):  # NOQA
        pass

    @property
    @abstractmethod
    def TEST_PARAMS(self):  # NOQA
        pass

    @abstractmethod
    def _init_model(self):
        pass

    @classmethod
    def setUpClass(cls):
        data_manager = DataManager(datadrive=cls.TEST_PARAMS['DATADRIVE'])
        cls.static_data = data_manager.import_static_data(data_system=cls.TEST_PARAMS['DATA_SYSTEM'])
        cls.dynamic_data = data_manager.import_dynamic_data(
            data_name=cls.TEST_PARAMS['INPUT_DATA_NAMES'],
            table_data_system=cls.TEST_PARAMS['DATA_SYSTEM'],
            time_series_data_system=cls.TEST_PARAMS['DATA_SYSTEM'],
            )

    def setUp(self):
        self.model = self._init_model()

    def tearDown(self):
        self.model.exit()

    def test_set_model(self):
        self.model.set_model(
            static_data=self.static_data,
            )
        self._test_set_model()

    def test_call(self):
        self.model.set_model(
            static_data=self.static_data,
            )
        results = self.model(self.dynamic_data)
        self.assertIsInstance(results, dict)
        self.assertEqual(set(results.keys()), set(self.TEST_PARAMS['OUTPUT_DATA_NAMES']))
        self._test_call(results)

    def test_get_static_data(self):
        self.model.set_model(
            static_data=self.static_data,
            )
        static_data = self.model.get_static_data()
        self.assertIsInstance(static_data, StaticData)
        self.assertEqual(
            set(self.model.output_data_names),
            set(static_data.time_series_metadata.keys())
            )
        for time_series_metadata in static_data.time_series_metadata.values():
            self.assertIsInstance(time_series_metadata, TimeSeriesMetadata)
        self.assertIn(self.model.instance_name, static_data.instance_metadata)
        instance_metadata = static_data.instance_metadata[self.model.instance_name]
        self.assertIsInstance(instance_metadata, InstanceMetadata)
        self.assertEqual(instance_metadata.model_name, self.model.MODEL_NAME)
        self.assertEqual(instance_metadata.service_name, self.model.service_name)
        self.assertEqual(
            set(instance_metadata.input_metadata.keys()),
            set(self.model.input_data_names),
            )
        self.assertEqual(
            set(instance_metadata.output_metadata.keys()),
            set(self.model.output_data_names),
            )
