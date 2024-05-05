from .base_test_factory import BaseTest, BaseTestFactory, BaseTestInit


class VisualizationTestFactory(BaseTestFactory):
    TEST_PARAMS_KEYS = [
        'MODEL_NAME',
        'SIZE_RATIO',
        'INSTANCE_NAME',
        'INPUT_DATA_NAMES',
        'INPUT_DATA_NAME_DICT',
        'OUTPUT_DATA_NAMES',
        'OUTPUT_DATA_NAME_DICT',
        'SENSOR_NAME',
        'STRUCTURAL_MODEL_NAME',
        'PARAMS',
        'VARIABLES',
        'DATADRIVE',
        'DATA_SYSTEM',
        ]

    def __call__(self):
        class BaseVisualizationTestInit(BaseTestInit):
            TASK = 'visualization'
            MODEL_CLASS = self.model_class
            TEST_PARAMS = self.test_params

            def _init_model(self):
                return self.MODEL_CLASS(
                    instance_name=self.TEST_PARAMS['INSTANCE_NAME'],
                    size_ratio=self.TEST_PARAMS['SIZE_RATIO'],
                    sensor_name=self.TEST_PARAMS['SENSOR_NAME'],
                    structural_model_name=self.TEST_PARAMS['STRUCTURAL_MODEL_NAME'],
                    **self.TEST_PARAMS['PARAMS'],
                    )

            def _init_model_without_params(self):
                return self.MODEL_CLASS(
                    instance_name=self.TEST_PARAMS['INSTANCE_NAME'],
                    size_ratio=self.TEST_PARAMS['SIZE_RATIO'],
                    sensor_name=self.TEST_PARAMS['SENSOR_NAME'],
                    structural_model_name=self.TEST_PARAMS['STRUCTURAL_MODEL_NAME'],
                    )

            def _test_attributes(self, model):
                self.assertEqual(model.input_data_name_dict, model.DEFAULT_INPUT_DATA_NAME_DICT)
                self.assertEqual(model.output_data_name_dict, model.DEFAULT_OUTPUT_DATA_NAME_DICT)

        class BaseVisualizationTest(BaseTest):
            MODEL_CLASS = self.model_class
            TEST_PARAMS = self.test_params

            def _init_model(self):
                return self.MODEL_CLASS(
                    instance_name=self.TEST_PARAMS['INSTANCE_NAME'],
                    size_ratio=self.TEST_PARAMS['SIZE_RATIO'],
                    input_data_name_dict=self.TEST_PARAMS['INPUT_DATA_NAME_DICT'],
                    output_data_name_dict=self.TEST_PARAMS['OUTPUT_DATA_NAME_DICT'],
                    sensor_name=self.TEST_PARAMS['SENSOR_NAME'],
                    structural_model_name=self.TEST_PARAMS['STRUCTURAL_MODEL_NAME'],
                    **self.TEST_PARAMS['PARAMS'],
                    )

            @classmethod
            def setUpClass(cls):
                super().setUpClass()

            @classmethod
            def tearDownClass(cls):
                cls.app.quit()
                super().tearDownClass()

            def test_set_model(self):
                self.model.create_widget()
                super().test_set_model()

            def test_call(self):
                self.model.create_widget()
                self.model.set_model(
                    static_data=self.static_data,
                    )
                results, variables = self.model(self.dynamic_data, self.TEST_PARAMS['VARIABLES'])
                self.assertIsInstance(results, dict)
                self.assertEqual(set(results.keys()), set(self.TEST_PARAMS['OUTPUT_DATA_NAMES']))
                self.assertIsInstance(variables, dict)
                self._test_call(results, variables)

            def test_get_static_data(self):
                self.model.create_widget()
                super().test_get_static_data()

        return BaseVisualizationTestInit, BaseVisualizationTest
