import shutil
import unittest
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import pandas as pd

from isas_base.data import DataManager
from isas_base.data.dynamic_data.time_series_batch_metadata import (
    TimeSeriesBatchDependencies, TimeSeriesBatchMetadata
)
from isas_base.data.dynamic_data.time_series_data import TimeSeriesData
from isas_base.data.static_data.instance_metadata import (
    InstanceInputMetadata, InstanceMetadata, InstanceOutputMetadata
)
from isas_base.data.static_data.sensor import Sensor, SensorInfo
from isas_base.data.static_data.service_metadata import ServiceMetadata
from isas_base.data.static_data.structural_model import (
    StructuralModel, StructuralModelInfo
)
from isas_base.data.static_data.time_series_metadata import TimeSeriesMetadata

DATADRIVE = Path('/root/datadrive')
DATADRIVE_TMP = Path('/root/datadrive_tmp')
DATA_SYSTEM = 'file'


class TestDataManagerInit(unittest.TestCase):
    def test_init(self):
        data_manager = DataManager(
            datadrive=DATADRIVE_TMP,
            )
        self.assertIsInstance(data_manager, DataManager)
        self.assertIsInstance(data_manager.postgres_cfg, dict)


class TestDataManagerStaticData(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        shutil.copytree(DATADRIVE, DATADRIVE_TMP)
        self.data_manager = DataManager(
            datadrive=DATADRIVE_TMP,
            )

    def tearDown(self):
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)

    def test_import_static_data(self):
        static_data = self.data_manager.import_static_data(data_system=DATA_SYSTEM)
        self.assertIsInstance(static_data.service_metadata, dict)
        for v in static_data.service_metadata.values():
            self.assertIsInstance(v, ServiceMetadata)
        self.assertIsInstance(static_data.time_series_metadata, dict)
        for v in static_data.time_series_metadata.values():
            self.assertIsInstance(v, TimeSeriesMetadata)
        self.assertIsInstance(static_data.instance_metadata, dict)
        for v in static_data.instance_metadata.values():
            self.assertIsInstance(v, InstanceMetadata)
        self.assertIsInstance(static_data.sensors, dict)
        for v in static_data.sensors.values():
            self.assertIsInstance(v, Sensor)
        self.assertIsInstance(static_data.structural_models, dict)
        for v in static_data.structural_models.values():
            self.assertIsInstance(v, StructuralModel)

    def test_export_static_data(self):
        static_data = self.data_manager.import_static_data(data_system=DATA_SYSTEM)
        static_data_1 = deepcopy(static_data)
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        self.data_manager.export_static_data(static_data, data_system=DATA_SYSTEM)
        static_data_2 = self.data_manager.import_static_data(data_system=DATA_SYSTEM)
        self.assertEqual(static_data_1, static_data_2)

    def test_import_service_metadata(self):
        service_name = 'test_service'
        service_metadata = self.data_manager._import_service_metadata(service_name, data_system=DATA_SYSTEM)
        self.assertIsInstance(service_metadata, dict)
        self.assertEqual(set(service_metadata.keys()), {service_name})
        for metadata in service_metadata.values():
            self.assertIsInstance(metadata, ServiceMetadata)

    def test_export_service_metadata(self):
        service_metadata = self.data_manager._import_service_metadata(data_system=DATA_SYSTEM)
        service_metadata_1 = deepcopy(service_metadata)
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        self.data_manager._export_service_metadata(service_metadata, data_system=DATA_SYSTEM)
        service_metadata_2 = self.data_manager._import_service_metadata(data_system=DATA_SYSTEM)
        self.assertEqual(service_metadata_1, service_metadata_2)

    def test_import_time_series_metadata(self):
        data_name = 'rosette_strain_x'
        sensor_name = 'rosette'
        structural_model_name = 'beam'
        time_series_metadata = self.data_manager._import_time_series_metadata(data_name, data_system=DATA_SYSTEM)
        self.assertIsInstance(time_series_metadata, dict)
        self.assertEqual(set(time_series_metadata.keys()), {data_name})
        for metadata in time_series_metadata.values():
            self.assertIsInstance(metadata, TimeSeriesMetadata)
            self.assertIsInstance(metadata.coord_sys, str)
            self.assertIsInstance(metadata.sensor_info, dict)
            self.assertEqual(set(metadata.sensor_info.keys()), {sensor_name})
            for info in metadata.sensor_info.values():
                self.assertIsInstance(info, SensorInfo)
                self.assertIsInstance(info.id, int)
            self.assertIsInstance(metadata.structural_model_info, dict)
            self.assertEqual(set(metadata.structural_model_info.keys()), {structural_model_name})
            for info in metadata.structural_model_info.values():
                self.assertIsInstance(info, StructuralModelInfo)
                self.assertIsInstance(info.id, int)
                self.assertIsInstance(info.component_name, str)
                if info.connection is not None:
                    self.assertIsInstance(info.connection, pd.DataFrame)

    def test_export_time_series_metadata(self):
        time_series_metadata = self.data_manager._import_time_series_metadata(data_system=DATA_SYSTEM)
        time_series_metadata_1 = deepcopy(time_series_metadata)
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        self.data_manager._export_time_series_metadata(time_series_metadata, data_system=DATA_SYSTEM)
        time_series_metadata_2 = self.data_manager._import_time_series_metadata(data_system=DATA_SYSTEM)
        self.assertEqual(time_series_metadata_1, time_series_metadata_2)

    def test_import_instance_metadata(self):
        instance_name = 'sensor_simulator'
        instance_metadata = self.data_manager._import_instance_metadata(instance_name, data_system=DATA_SYSTEM)
        self.assertIsInstance(instance_metadata, dict)
        self.assertEqual(set(instance_metadata.keys()), {instance_name})
        for metadata in instance_metadata.values():
            self.assertIsInstance(metadata, InstanceMetadata)
            self.assertIsInstance(metadata.model_name, str)
            for key, class_ in zip(
                    ('input_metadata', 'output_metadata'),
                    (InstanceInputMetadata, InstanceOutputMetadata)
                    ):
                _metadata = getattr(metadata, key)
                self.assertIsInstance(_metadata, dict)
                for data_name, __metadata in _metadata.items():
                    self.assertIsInstance(data_name, str)
                    self.assertIsInstance(__metadata, class_)
                    self.assertIsInstance(__metadata.id, int)

    def test_export_instance_metadata(self):
        instance_metadata = self.data_manager._import_instance_metadata(data_system=DATA_SYSTEM)
        instance_metadata_1 = deepcopy(instance_metadata)
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        self.data_manager._export_instance_metadata(instance_metadata, data_system=DATA_SYSTEM)
        instance_metadata_2 = self.data_manager._import_instance_metadata(data_system=DATA_SYSTEM)
        self.assertEqual(instance_metadata_1, instance_metadata_2)

    def test_import_sensors(self):
        sensor_name = 'rosette'
        structural_model_name = 'beam'
        sensors = self.data_manager._import_sensors(sensor_name, data_system=DATA_SYSTEM)
        self.assertIsInstance(sensors, dict)
        self.assertEqual(set(sensors.keys()), {sensor_name})
        for sensor in sensors.values():
            self.assertIsInstance(sensor, Sensor)
            self.assertIsInstance(sensor.locational, bool)
            self.assertIsInstance(sensor.directional, bool)
            self.assertIsInstance(sensor.structural_model_info, dict)
            self.assertEqual(set(sensor.structural_model_info.keys()), {structural_model_name})
            for info in sensor.structural_model_info.values():
                self.assertIsInstance(info, StructuralModelInfo)
                self.assertIsInstance(info.id, int)
                self.assertIsInstance(info.component_name, str)
                if info.connection is not None:
                    self.assertIsInstance(info.connection, pd.DataFrame)
            self.assertIsInstance(sensor.data, pd.DataFrame)
            self.assertEqual(sensor.data.index.name, 'data_id')
            self.assertEqual(set(sensor.data.columns.values.tolist()), {'x', 'y', 'z'})
            self.assertEqual(len(sensor.data), 18)

    def test_export_sensors(self):
        sensors = self.data_manager._import_sensors(data_system=DATA_SYSTEM)
        sensors_1 = deepcopy(sensors)
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        self.data_manager._export_sensors(sensors, data_system=DATA_SYSTEM)
        sensors_2 = self.data_manager._import_sensors(data_system=DATA_SYSTEM)
        self.assertEqual(sensors_1, sensors_2)

    def test_import_structural_models(self):
        structural_model_name = 'beam'
        structural_models = self.data_manager._import_structural_models(structural_model_name, data_system=DATA_SYSTEM)
        self.assertIsInstance(structural_models, dict)
        self.assertEqual(set(structural_models.keys()), {structural_model_name})
        for structural_model in structural_models.values():
            self.assertIsInstance(structural_model, StructuralModel)
            self.assertIsInstance(structural_model.MODEL_TYPE, str)
            for component_name in structural_model.COMPONENT_NAMES:
                component = getattr(structural_model, component_name)
                self.assertIsInstance(component, pd.DataFrame)

    def test_export_structural_models(self):
        structural_models = self.data_manager._import_structural_models(data_system=DATA_SYSTEM)
        structural_models_1 = deepcopy(structural_models)
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        self.data_manager._export_structural_models(structural_models, data_system=DATA_SYSTEM)
        structural_models_2 = self.data_manager._import_structural_models(data_system=DATA_SYSTEM)
        self.assertEqual(structural_models_1, structural_models_2)


class TestDataManagerDynamicData(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        shutil.copytree(DATADRIVE, DATADRIVE_TMP)
        self.data_manager = DataManager(
            datadrive=DATADRIVE_TMP,
            )

    def tearDown(self):
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)

    def test_import_dynamic_data(self):
        data_name = 'rosette_strain_x'
        dynamic_data = self.data_manager.import_dynamic_data(
            data_name,
            table_data_system=DATA_SYSTEM,
            time_series_data_system=DATA_SYSTEM,
            )
        self.assertIsInstance(dynamic_data.time_series_data, dict)
        for v in dynamic_data.time_series_data.values():
            self.assertIsInstance(v, TimeSeriesData)
        self.assertIsInstance(dynamic_data.time_series_batch_metadata, dict)
        for v in dynamic_data.time_series_batch_metadata.values():
            self.assertIsInstance(v, TimeSeriesBatchMetadata)

    def test_export_dynamic_data(self):
        data_name = 'rosette_strain_x'
        dynamic_data = self.data_manager.import_dynamic_data(
            data_name,
            table_data_system=DATA_SYSTEM,
            time_series_data_system=DATA_SYSTEM,
            )
        dynamic_data_1 = deepcopy(dynamic_data)
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        self.data_manager.export_dynamic_data(
            dynamic_data,
            data_name,
            table_data_system=DATA_SYSTEM,
            time_series_data_system=DATA_SYSTEM,
            )
        dynamic_data_2 = self.data_manager.import_dynamic_data(
            data_name,
            table_data_system=DATA_SYSTEM,
            time_series_data_system=DATA_SYSTEM,
            )
        self.assertEqual(dynamic_data_1, dynamic_data_2)

    def test_import_time_series_data(self):
        data_name = 'rosette_strain_x'
        time_series_data = self.data_manager._import_time_series_data(data_name, data_system=DATA_SYSTEM)
        self.assertIsInstance(time_series_data, dict)
        self.assertEqual(set(time_series_data.keys()), {data_name})
        for data in time_series_data.values():
            self.assertIsInstance(data, TimeSeriesData)
            # fields
            self.assertIsInstance(data.fields, pd.DataFrame)
            self.assertEqual(data.fields.index.name, 'time')
            self.assertEqual(len(data.fields), 12)
            # tags
            self.assertIsInstance(data.tags, pd.DataFrame)
            pd.testing.assert_index_equal(data.tags.index, data.fields.index)
            self.assertEqual(data.tags.index.name, 'time')
            self.assertEqual(len(data.tags), 12)

    def test_import_time_series_data_with_datetime(self):
        data_name = 'rosette_strain_x'
        first_datetime = datetime.fromtimestamp(1587772805)
        time_series_data = self.data_manager._import_time_series_data(
            data_name,
            data_system=DATA_SYSTEM,
            first_datetime=first_datetime,
            )
        self.assertIsInstance(time_series_data, dict)
        self.assertEqual(set(time_series_data.keys()), {data_name})
        for data in time_series_data.values():
            self.assertEqual(len(data.fields), 7)
            self.assertEqual(len(data.tags), 7)

    def test_export_time_series(self):
        data_name = 'rosette_strain_x'
        time_series_data = self.data_manager._import_time_series_data(data_name, data_system=DATA_SYSTEM)
        time_series_data_1 = deepcopy(time_series_data)
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        self.data_manager._export_time_series_data(time_series_data, data_name, data_system=DATA_SYSTEM)
        time_series_data_2 = self.data_manager._import_time_series_data(data_name, data_system=DATA_SYSTEM)
        self.assertEqual(time_series_data_1, time_series_data_2)

    def test_import_time_series_batch_metadata(self):
        time_series_batch_metadata = self.data_manager._import_time_series_batch_metadata(data_system=DATA_SYSTEM)
        self.assertIsInstance(time_series_batch_metadata, dict)
        for batch_id, metadata in time_series_batch_metadata.items():
            self.assertIsInstance(batch_id, str)
            self.assertIsInstance(metadata, TimeSeriesBatchMetadata)
            self.assertIsInstance(metadata.service_name, str)
            self.assertIsInstance(metadata.batch_datetime, datetime)
            self.assertIsInstance(metadata.dependencies, dict)
            for dependent_batch_id, dependencies in metadata.dependencies.items():
                self.assertIsInstance(dependent_batch_id, str)
                self.assertIsInstance(dependencies, TimeSeriesBatchDependencies)
                self.assertIsInstance(dependencies.id, int)

    def test_export_time_series_batch_metadata(self):
        time_series_batch_metadata = self.data_manager._import_time_series_batch_metadata(data_system=DATA_SYSTEM)
        time_series_batch_metadata_1 = deepcopy(time_series_batch_metadata)
        shutil.rmtree(DATADRIVE_TMP, ignore_errors=True)
        self.data_manager._export_time_series_batch_metadata(time_series_batch_metadata, data_system=DATA_SYSTEM)
        time_series_batch_metadata_2 = self.data_manager._import_time_series_batch_metadata(data_system=DATA_SYSTEM)
        self.assertEqual(time_series_batch_metadata_1, time_series_batch_metadata_2)


if __name__ == '__main__':
    unittest.main()
