from .base_test_factory import BaseTest, BaseTestFactory, BaseTestInit


class AnalysisTestFactory(BaseTestFactory):
    TEST_PARAMS_KEYS = [
        'MODEL_NAME',
        'INSTANCE_NAME',
        'INPUT_DATA_NAMES',
        'INPUT_DATA_NAME_DICT',
        'OUTPUT_DATA_NAMES',
        'OUTPUT_DATA_NAME_DICT',
        'SENSOR_NAME',
        'STRUCTURAL_MODEL_NAME',
        'PARAMS',
        'DATADRIVE',
        'DATA_SYSTEM',
        ]

    def __call__(self):
        class BaseAnalysisTestInit(BaseTestInit):
            TASK = 'analysis'
            MODEL_CLASS = self.model_class
            TEST_PARAMS = self.test_params

            def _init_model(self):
                return self.MODEL_CLASS(
                    instance_name=self.TEST_PARAMS['INSTANCE_NAME'],
                    sensor_name=self.TEST_PARAMS['SENSOR_NAME'],
                    structural_model_name=self.TEST_PARAMS['STRUCTURAL_MODEL_NAME'],
                    **self.TEST_PARAMS['PARAMS'],
                    )

            def _init_model_without_params(self):
                return self.MODEL_CLASS(
                    instance_name=self.TEST_PARAMS['INSTANCE_NAME'],
                    sensor_name=self.TEST_PARAMS['SENSOR_NAME'],
                    structural_model_name=self.TEST_PARAMS['STRUCTURAL_MODEL_NAME'],
                    )

            def _test_attributes(self, model):
                self.assertEqual(model.input_data_name_dict, model.DEFAULT_INPUT_DATA_NAME_DICT)
                self.assertEqual(model.output_data_name_dict, model.DEFAULT_OUTPUT_DATA_NAME_DICT)
                self.assertEqual(model.structural_model_connections, {})

        class BaseAnalysisTest(BaseTest):
            MODEL_CLASS = self.model_class
            TEST_PARAMS = self.test_params

            def _init_model(self):
                return self.MODEL_CLASS(
                    instance_name=self.TEST_PARAMS['INSTANCE_NAME'],
                    input_data_name_dict=self.TEST_PARAMS['INPUT_DATA_NAME_DICT'],
                    output_data_name_dict=self.TEST_PARAMS['OUTPUT_DATA_NAME_DICT'],
                    sensor_name=self.TEST_PARAMS['SENSOR_NAME'],
                    structural_model_name=self.TEST_PARAMS['STRUCTURAL_MODEL_NAME'],
                    **self.TEST_PARAMS['PARAMS'],
                    )

        return BaseAnalysisTestInit, BaseAnalysisTest
