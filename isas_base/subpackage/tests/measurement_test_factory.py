from .base_test_factory import BaseTest, BaseTestFactory, BaseTestInit


class MeasurementTestFactory(BaseTestFactory):
    TEST_PARAMS_KEYS = [
        'MODEL_NAME',
        'INSTANCE_NAME',
        'INPUT_DATA_NAMES',
        'OUTPUT_DATA_NAMES',
        'SENSOR_NAME',
        'STRUCTURAL_MODEL_NAME',
        'PARAMS',
        'DATADRIVE',
        'DATA_SYSTEM',
        ]

    def __call__(self):
        class BaseMeasurementTestInit(BaseTestInit):
            TASK = 'measurement'
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
                self.assertEqual(model.input_data_names, model.DEFAULT_INPUT_DATA_NAMES)
                self.assertEqual(model.output_data_names, model.DEFAULT_OUTPUT_DATA_NAMES)
                self.assertIsInstance(model.simulation, bool)
                self.assertEqual(model.structural_model_connections, {})

        class BaseMeasurementTestSimulation(BaseTest):
            MODEL_CLASS = self.model_class
            TEST_PARAMS = self.test_params

            def _init_model(self):
                return self.MODEL_CLASS(
                    instance_name=self.TEST_PARAMS['INSTANCE_NAME'],
                    input_data_names=self.TEST_PARAMS['INPUT_DATA_NAMES'],
                    output_data_names=self.TEST_PARAMS['OUTPUT_DATA_NAMES'],
                    sensor_name=self.TEST_PARAMS['SENSOR_NAME'],
                    structural_model_name=self.TEST_PARAMS['STRUCTURAL_MODEL_NAME'],
                    simulation=True,
                    **self.TEST_PARAMS['PARAMS'],
                    )

        class BaseMeasurementTest(BaseTest):
            MODEL_CLASS = self.model_class
            TEST_PARAMS = self.test_params

            def _init_model(self):
                return self.MODEL_CLASS(
                    instance_name=self.TEST_PARAMS['INSTANCE_NAME'],
                    input_data_names=self.TEST_PARAMS['INPUT_DATA_NAMES'],
                    output_data_names=self.TEST_PARAMS['OUTPUT_DATA_NAMES'],
                    sensor_name=self.TEST_PARAMS['SENSOR_NAME'],
                    structural_model_name=self.TEST_PARAMS['STRUCTURAL_MODEL_NAME'],
                    simulation=False,
                    **self.TEST_PARAMS['PARAMS'],
                    )

        return BaseMeasurementTestInit, BaseMeasurementTestSimulation, BaseMeasurementTest
