import logging
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import sqlalchemy
import yaml

from ..utils import set_list_dim
from .data_storage_handler import DataStorageHandler
from .dynamic_data.dynamic_data import DynamicData
from .dynamic_data.time_series_batch_metadata import (
    TimeSeriesBatchDependencies, TimeSeriesBatchMetadata
)
from .dynamic_data.time_series_data import TimeSeriesData
from .static_data.instance_metadata import (
    InstanceInputMetadata, InstanceMetadata, InstanceOutputMetadata
)
from .static_data.sensor import Sensor, SensorInfo
from .static_data.service_metadata import ServiceMetadata
from .static_data.static_data import StaticData
from .static_data.structural_model import (
    STRUCTURAL_MODEL_CLASS, StructuralModel, StructuralModelInfo
)
from .static_data.time_series_metadata import TimeSeriesMetadata

logger = logging.getLogger(__name__)


class DataManager(DataStorageHandler):
    POSTGRES_CFG_PATH: Path = Path(__file__).parent / 'cfg/postgres.yml'
    STATIC_DATA_TABLES = (
        '000_service_metadata',
        '010_time_series_metadata',
        '011_time_series_structural_models',
        '013_time_series_sensors',
        '020_instance_metadata',
        '021_instance_inputs',
        '022_instance_outputs',
        '030_sensor_metadata',
        '032_sensors_structural_models',
        '040_structural_model_metadata',
        )
    DYNAMIC_DATA_TABLES = (
        '001_time_series_batch_metadata',
        '002_time_series_batch_dependencies',
        )

    def __init__(
            self,
            datadrive: str | Path,
            ) -> None:
        """Initialize DataManager.

        Args:
            datadrive: Path to datadrive, used in 'file' mode.
        """
        super().__init__(datadrive)
        with self.POSTGRES_CFG_PATH.open('r') as f:
            self.postgres_cfg = yaml.safe_load(f)

    def init_rdb(
            self,
            data_system: str = 'postgres',
            ) -> None:
        """Initialize relational database.

        Args:
            data_system: A data sytem.
        """
        self._check_data_system(data_system, ('postgres', ))
        _create_rdb_tables = {
            'postgres': self._create_postgres_tables,
            }
        _create_rdb_tables[data_system]()
        self._insert_rdb_init_data(data_system)

    def _create_postgres_tables(self) -> None:
        """Create postgres tables at static database."""
        for table_name, columns in self.postgres_cfg.items():
            database = f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}' \
                    f'@{self.POSTGRES_HOST}/{os.environ["POSTGRES_DATABASE"]}'
            sql = f'CREATE TABLE IF NOT EXISTS \"{table_name}\" ({", ".join(columns)});'
            logger.debug(f'[DataManager] Create table on DB: {database=}, {sql=}')
            engine = sqlalchemy.create_engine(database, connect_args={'client_encoding': 'utf8'}, echo=False)
            with engine.begin() as connection:
                connection.execute(sqlalchemy.text(sql))

    def _insert_rdb_init_data(
            self,
            data_system: str = 'postgres',
            ) -> None:
        """Insert init data to relational database.

        Args:
            data_system: A data sytem.
        """
        data_dirs = (
            self.datadrive / 'static_data/init_rdb',
            self.datadrive / 'dynamic_data/init_rdb',
            )
        for data_dir in data_dirs:
            if not data_dir.exists():
                continue
            for path in data_dir.iterdir():
                table_name = path.stem
                if table_name not in self.postgres_cfg:
                    continue
                df = self._load_table_data(path, data_system='file')
                if df is None:
                    continue
                self._save_table_data(table_name, df, data_system=data_system, mode='append')

    def init_tsdb(
            self,
            data_system: str = 'influx',
            ) -> None:
        """Initialize time-series database.

        Args:
            data_system: A data sytem.
        """
        self._check_data_system(data_system, ('influx', ))
        self._insert_tsdb_init_data(data_system)

    def _insert_tsdb_init_data(
            self,
            data_system: str = 'influx',
            ) -> None:
        """Insert init data to time-series database.

        Args:
            data_system: A data sytem.
        """
        dynamic_data_dir = self.datadrive / 'dynamic_data/init_tsdb'
        if not dynamic_data_dir.exists():
            return
        for path in dynamic_data_dir.iterdir():
            df = self._load_time_series_data(
                path,
                TimeSeriesData.TAG_KEYS,
                data_system='file',
                )
            if df is None:
                continue
            self._save_time_series_data(
                path.stem,
                df,
                TimeSeriesData.TAG_KEYS,
                data_system=data_system,
                )

    def import_static_data(
            self,
            data_system: str,
            ) -> StaticData:
        """Import static data.

        Args:
            data_system: A data sytem.

        Returns:
            An instance of StaticData class containing imported data.
        """
        return StaticData(
            service_metadata=self._import_service_metadata(data_system=data_system),
            time_series_metadata=self._import_time_series_metadata(data_system=data_system),
            instance_metadata=self._import_instance_metadata(data_system=data_system),
            sensors=self._import_sensors(data_system=data_system),
            structural_models=self._import_structural_models(data_system=data_system),
            )

    def _import_service_metadata(
            self,
            service_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> dict[str, ServiceMetadata]:
        """Import service metadata.

        Args:
            service_name: A name of service.
            data_system: A data sytem.

        Returns:
            A dict of imported ServiceMetadata.
        """
        # load service_metadata table
        df_service_metadata = self._load_table_data(
            self._get_name('000_service_metadata', data_system),
            data_system=data_system,
            )
        if df_service_metadata is None:
            return {}
        service_metadata = df_service_metadata.to_dict(orient='index')

        # set service_name
        if service_name is None:
            service_name = list(service_metadata.keys())
        service_names = set_list_dim(service_name, 1)

        return {
            service_name: ServiceMetadata()
            for service_name in service_names
            }

    def _import_time_series_metadata(
            self,
            data_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> dict[str, TimeSeriesMetadata]:
        """Import time-series metadata.

        Args:
            data_name: A name of data.
            data_system: A data sytem.

        Returns:
            A dict of imported TimeSeriesMetadata.
        """
        # load time_series_metadata table
        df_time_series_metadata = self._load_table_data(
            self._get_name('010_time_series_metadata', data_system),
            data_system=data_system,
            )
        if df_time_series_metadata is None:
            return {}
        time_series_metadata = df_time_series_metadata.to_dict(orient='index')

        # set data_name
        if data_name is None:
            data_name = list(time_series_metadata.keys())
        data_names = set_list_dim(data_name, 1)

        # load time_series_structural_models table
        df_time_series_structural_models = self._load_table_data(
            self._get_name('011_time_series_structural_models', data_system),
            data_system=data_system,
            )
        time_series_structural_models = {}
        if df_time_series_structural_models is not None:
            df_time_series_structural_models = df_time_series_structural_models.reset_index()
            for data_name, group_df in df_time_series_structural_models.groupby('data_name'):
                if data_name not in data_names:
                    continue
                time_series_structural_models[data_name] = group_df.to_dict(orient='records')

        # load time_series_structural_model_connection table
        for data_name in data_names:
            if data_name not in time_series_structural_models:
                continue
            for record in time_series_structural_models[data_name]:
                path = self.datadrive / 'static_data/012_time_series_structural_model_connections/' \
                    f'{data_name}_{record["structural_model_name"]}.csv'
                record['connection'] = self._load_table_data(
                    path,
                    data_system='file',
                    )

        # load time_series_sensors table
        df_time_series_sensors = self._load_table_data(
            self._get_name('013_time_series_sensors', data_system),
            data_system=data_system,
            )
        time_series_sensors = {}
        if df_time_series_sensors is not None:
            df_time_series_sensors = df_time_series_sensors.reset_index()
            for data_name, group_df in df_time_series_sensors.groupby('data_name'):
                if data_name not in data_names:
                    continue
                time_series_sensors[data_name] = group_df.to_dict(orient='records')

        return {
            data_name: TimeSeriesMetadata(
                coord_sys=time_series_metadata[data_name]['coord_sys'],
                sensor_info={
                    record['sensor_name']: SensorInfo(
                        id=record['id'],
                        )
                    for record in time_series_sensors.get(data_name, [])
                    },
                structural_model_info={
                    record['structural_model_name']: StructuralModelInfo(
                        id=record['id'],
                        component_name=record['component_name'],
                        connection=record['connection'],
                        )
                    for record in time_series_structural_models.get(data_name, [])
                    },
                )
            for data_name in data_names
            }

    def _import_instance_metadata(
            self,
            instance_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> dict[str, InstanceMetadata]:
        """Import instance metadata.

        Args:
            instance_name: A name of instance.
            data_system: A data sytem.

        Retunrs:
            A dict of imported InstanceMetadata.
        """
        # load instance_metadata table
        df_instance_metadata = self._load_table_data(
            self._get_name('020_instance_metadata', data_system),
            data_system=data_system,
            )
        if df_instance_metadata is None:
            return {}
        instance_metadata = df_instance_metadata.to_dict(orient='index')

        # set instance_name
        if instance_name is None:
            instance_name = list(instance_metadata.keys())
        instance_names = set_list_dim(instance_name, 1)

        # load instance_inputs and instance_outputs tables
        io_dict = {}
        for table_name in ('021_instance_inputs', '022_instance_outputs'):
            df = self._load_table_data(
                self._get_name(table_name, data_system),
                data_system=data_system,
                )
            io_dict[table_name] = {}
            if df is not None:
                df = df.reset_index()
                for instance_name, group_df in df.groupby('instance_name'):
                    if instance_name not in instance_names:
                        continue
                    io_dict[table_name][instance_name] = group_df.to_dict(orient='records')

        return {
            instance_name: InstanceMetadata(
                model_name=instance_metadata[instance_name]['model_name'],
                service_name=instance_metadata[instance_name]['service_name'],
                input_metadata={
                    record['data_name']: InstanceInputMetadata(
                        id=record['id']
                        )
                    for record in io_dict['021_instance_inputs'].get(instance_name, [])
                    },
                output_metadata={
                    record['data_name']: InstanceOutputMetadata(
                        id=record['id']
                        )
                    for record in io_dict['022_instance_outputs'].get(instance_name, [])
                    },
                )
            for instance_name in instance_names
            }

    def _import_sensors(
            self,
            sensor_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> dict[str, Sensor]:
        """Import sensors.

        Args:
            sensor_name: A name of sensor.
            data_system: A data sytem.

        Retunrs:
            A dict of imported Sensor.
        """
        # load sensor_metadata table
        df_sensor_metadata = self._load_table_data(
            self._get_name('030_sensor_metadata', data_system),
            data_system=data_system,
            )
        if df_sensor_metadata is None:
            return {}
        sensor_metadata = df_sensor_metadata.to_dict(orient='index')

        # set_sensor_name
        if sensor_name is None:
            sensor_name = list(sensor_metadata.keys())
        sensor_names = set_list_dim(sensor_name, 1)

        # load sensor table
        sensors = {}
        for sensor_name in sensor_names:
            path = self.datadrive / f'static_data/031_sensors/{sensor_name}.csv'
            sensors[sensor_name] = self._load_table_data(
                path,
                data_system='file',
                )

        # load sensor_structural_model table
        df_sensors_structural_models = self._load_table_data(
            self._get_name('032_sensors_structural_models', data_system),
            data_system=data_system,
            )
        sensors_structural_models = {}
        if df_sensors_structural_models is not None:
            df_sensors_structural_models = df_sensors_structural_models.reset_index()
            for sensor_name, group_df in df_sensors_structural_models.groupby('sensor_name'):
                if sensor_name not in sensor_names:
                    continue
                sensors_structural_models[sensor_name] = group_df.to_dict(orient='records')

        # load sensor_structural_model_connection table
        for sensor_name in sensor_names:
            if sensor_name not in sensors_structural_models:
                continue
            for record in sensors_structural_models[sensor_name]:
                path = self.datadrive / 'static_data/033_sensor_structural_model_connections/' \
                    f'{sensor_name}_{record["structural_model_name"]}.csv'
                record['connection'] = self._load_table_data(
                    path,
                    data_system='file',
                    )

        return {
            sensor_name: Sensor(
                data=sensors[sensor_name],
                structural_model_info={
                    record['structural_model_name']: StructuralModelInfo(
                        id=record['id'],
                        component_name=record['component_name'],
                        connection=record['connection'],
                        )
                    for record in sensors_structural_models[sensor_name]
                    },
                **sensor_metadata[sensor_name],
                )
            for sensor_name in sensor_names
            }

    def _import_structural_models(
            self,
            structural_model_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> dict[str, Sensor]:
        """Import structural models.

        Args:
            structural_model_name: A name of structural model.
            data_system: A data sytem.

        Retunrs:
            A dict of imported StructuralModels.
        """
        # load structural_model_metadata table
        df_structural_model_metadata = self._load_table_data(
            self._get_name('040_structural_model_metadata', data_system),
            data_system=data_system,
            )
        if df_structural_model_metadata is None:
            return {}
        structural_model_metadata = df_structural_model_metadata.to_dict(orient='index')

        # set structural_model_names
        if structural_model_name is None:
            structural_model_name = list(structural_model_metadata.keys())
        structural_model_names = set_list_dim(structural_model_name, 1)

        # load structural_model table
        model_classes = {}
        structural_models = {}
        for structural_model_name in structural_model_names:
            model_type = structural_model_metadata[structural_model_name]['model_type']
            model_class = getattr(STRUCTURAL_MODEL_CLASS, model_type)
            model_classes[structural_model_name] = model_class
            structural_models[structural_model_name] = {
                component_name: self._load_table_data(
                    self.datadrive / f'static_data/041_structural_models/{structural_model_name}_{component_name}.csv',
                    data_system=data_system,
                    )
                for component_name in model_class.COMPONENT_NAMES
                }

        return {
            structural_model_name: model_classes[structural_model_name](
                **structural_models[structural_model_name],
                )
            for structural_model_name in structural_model_names
            }

    def export_static_data(
            self,
            static_data: StaticData,
            data_system: str = 'postgres',
            ) -> None:
        """Export static data.

        Args:
            static_data: An instance of StaticData class containing data to be exported.
            data_system: A data sytem.
        """
        # The order is fixed
        self._export_service_metadata(static_data.service_metadata, data_system=data_system)
        self._export_structural_models(static_data.structural_models, data_system=data_system)
        self._export_sensors(static_data.sensors, data_system=data_system)
        self._export_time_series_metadata(static_data.time_series_metadata, data_system=data_system)
        self._export_instance_metadata(static_data.instance_metadata, data_system=data_system)

    def _export_service_metadata(
            self,
            service_metadata: dict[str, ServiceMetadata],
            service_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> None:
        """Import service metadata.

        Args:
            service_metadata: A dict of ServiceMetadata to be exported.
            service_name: A name of service.
            data_system: A data sytem.
        """
        if service_name is None:
            service_name = list(service_metadata.keys())
        service_names = set_list_dim(service_name, 1)
        if len(service_names) == 0:
            return

        # save service_metadata table
        df_service_metadata = pd.DataFrame(
            [{
                key: getattr(service_metadata[service_name], key)
                for key in ()
                } for service_name in service_names],
            index=service_names,
            )
        df_service_metadata.index.name = 'service_name'
        self._save_table_data(
            self._get_name('000_service_metadata', data_system),
            df_service_metadata,
            data_system=data_system,
            mode='sync',
            )

    def _export_time_series_metadata(
            self,
            time_series_metadata: dict[str, TimeSeriesMetadata],
            data_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> None:
        """Export time-series metadata.

        Args:
            time_series_metadata: A dict of TimeSeriesMetadata to be exported.
            data_name: A name of data. If None, run about all data. The default, None
            data_system: A data sytem.
        """
        if data_name is None:
            data_name = list(time_series_metadata.keys())
        data_names = set_list_dim(data_name, 1)
        if len(data_names) == 0:
            return

        # save time_series_metadata table
        df_time_series_metadata = pd.DataFrame(
            [{
                key: getattr(time_series_metadata[data_name], key)
                for key in time_series_metadata[data_name].METADATA_KEYS
                } for data_name in data_names],
            index=data_names,
            )
        df_time_series_metadata.index.name = 'data_name'
        self._save_table_data(
            self._get_name('010_time_series_metadata', data_system),
            df_time_series_metadata,
            data_system=data_system,
            mode='sync',
            )

        # save time_series_structural_models table
        time_series_structural_models = []
        for data_name in data_names:
            structural_model_info = time_series_metadata[data_name].structural_model_info
            for structural_model_name, info in structural_model_info.items():
                info_dict = asdict(info)
                # save time_series_structural_model_connection table
                path = self.datadrive / 'static_data/012_time_series_structural_model_connections/' \
                    f'{data_name}_{structural_model_name}.csv'
                self._save_table_data(
                    path,
                    info_dict.pop('connection'),  # remove connection
                    data_system='file',
                    mode='replace',
                    )
                time_series_structural_models.append({
                    **{
                        'data_name': data_name,
                        'structural_model_name': structural_model_name,
                        },
                    **info_dict,
                    })
        if len(time_series_structural_models):
            df_time_series_structural_models = pd.DataFrame.from_dict(time_series_structural_models)
            idx_id_exists = ~df_time_series_structural_models['id'].isna()
            df = df_time_series_structural_models[idx_id_exists]
            df = df.set_index('id')
            self._save_table_data(
                self._get_name('011_time_series_structural_models', data_system),
                df,
                data_system=data_system,
                mode='sync',
                )
            df = df_time_series_structural_models[~idx_id_exists]
            df = df.drop('id', axis=1)
            self._save_table_data(
                self._get_name('011_time_series_structural_models', data_system),
                df,
                data_system=data_system,
                mode='append',
                )

        # save time_series_sensors table
        time_series_sensors = []
        for data_name in data_names:
            sensor_info = time_series_metadata[data_name].sensor_info
            for sensor_name, info in sensor_info.items():
                info_dict = asdict(info)
                time_series_sensors.append({
                    **{
                        'data_name': data_name,
                        'sensor_name': sensor_name,
                        },
                    **info_dict,
                    })
        if len(time_series_sensors):
            df_time_series_sensors = pd.DataFrame.from_dict(time_series_sensors)
            idx_id_exists = ~df_time_series_sensors['id'].isna()
            df = df_time_series_sensors[idx_id_exists]
            df = df.set_index('id')
            self._save_table_data(
                self._get_name('013_time_series_sensors', data_system),
                df,
                data_system=data_system,
                mode='sync',
                )
            df = df_time_series_sensors[~idx_id_exists]
            df = df.drop('id', axis=1)
            self._save_table_data(
                self._get_name('013_time_series_sensors', data_system),
                df,
                data_system=data_system,
                mode='append',
                )

    def _export_instance_metadata(
            self,
            instance_metadata: dict[str, InstanceMetadata],
            instance_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> None:
        """Export time-series metadata.

        Args:
            instance_metadata: A dict of InstanceMetadata to be exported.
            instance_name: A name of instance. If None, run about all instances. The default, None
            data_system: A data sytem.
        """
        if instance_name is None:
            instance_name = list(instance_metadata.keys())
        instance_names = set_list_dim(instance_name, 1)
        if len(instance_names) == 0:
            return

        # save instance_metadata table
        df_instance_metadata = pd.DataFrame(
            [{
                key: getattr(instance_metadata[instance_name], key)
                for key in instance_metadata[instance_name].METADATA_KEYS
                } for instance_name in instance_names],
            index=instance_names,
            )
        df_instance_metadata.index.name = 'instance_name'
        self._save_table_data(
            self._get_name('020_instance_metadata', data_system),
            df_instance_metadata,
            data_system=data_system,
            mode='sync',
            )

        # save instance_inputs and instance_outputs tables
        for table_name, key in zip(
                ('021_instance_inputs', '022_instance_outputs'),
                ('input_metadata', 'output_metadata')
                ):
            io = []
            for instance_name in instance_names:
                io_info = getattr(instance_metadata[instance_name], key)
                for data_name, info in io_info.items():
                    info_dict = asdict(info)
                    io.append({
                        **{
                            'instance_name': instance_name,
                            'data_name': data_name,
                            },
                        **info_dict,
                        })
            if len(io):
                df_io = pd.DataFrame.from_dict(io)
                idx_id_exists = ~df_io['id'].isna()
                df = df_io[idx_id_exists]
                df = df.set_index('id')
                self._save_table_data(
                    self._get_name(table_name, data_system),
                    df,
                    data_system=data_system,
                    mode='sync',
                    )
                df = df_io[~idx_id_exists]
                df = df.drop('id', axis=1)
                self._save_table_data(
                    self._get_name(table_name, data_system),
                    df,
                    data_system=data_system,
                    mode='append',
                    )

    def _export_sensors(
            self,
            sensors: dict[str, Sensor],
            sensor_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> None:
        """Export time-series metadata.

        Args:
            sensors: A dict of Sensor to be exported.
            sensor_name: A name of sensor. If None, run about all sensors. The default, None
            data_system: A data sytem.
        """
        if sensor_name is None:
            sensor_name = list(sensors.keys())
        sensor_names = set_list_dim(sensor_name, 1)
        if len(sensor_names) == 0:
            return

        # save sensor_metadata table
        df_sensor_metadata = pd.DataFrame(
            [{
                key: getattr(sensors[sensor_name], key)
                for key in sensors[sensor_name].METADATA_KEYS
                } for sensor_name in sensor_names],
            index=sensor_names,
            )
        df_sensor_metadata.index.name = 'sensor_name'
        self._save_table_data(
            self._get_name('030_sensor_metadata', data_system),
            df_sensor_metadata,
            data_system=data_system,
            mode='sync',
            )

        # save sensor tables
        for sensor_name in sensor_names:
            path = self.datadrive / f'static_data/031_sensors/{sensor_name}.csv'
            self._save_table_data(
                path,
                sensors[sensor_name].data,
                data_system='file',
                mode='replace',
                )

        # save sensors_structural_models table
        sensors_structural_models = []
        for sensor_name in sensor_names:
            structural_model_info = sensors[sensor_name].structural_model_info
            for structural_model_name, info in structural_model_info.items():
                info_dict = asdict(info)
                # save sensor_structural_model_connection table
                path = self.datadrive / 'static_data/033_sensor_structural_model_connections/' \
                    f'{sensor_name}_{structural_model_name}.csv'
                self._save_table_data(
                    path,
                    info_dict.pop('connection'),  # remove connection
                    data_system='file',
                    mode='replace',
                    )
                sensors_structural_models.append({
                    **{
                        'sensor_name': sensor_name,
                        'structural_model_name': structural_model_name,
                        },
                    **info_dict,
                    })

        if len(sensors_structural_models):
            df_sensors_structural_models = pd.DataFrame.from_dict(sensors_structural_models)
            idx_id_exists = ~df_sensors_structural_models['id'].isna()
            df = df_sensors_structural_models[idx_id_exists]
            df = df.set_index('id')
            self._save_table_data(
                self._get_name('032_sensors_structural_models', data_system),
                df,
                data_system=data_system,
                mode='sync',
                )
            df = df_sensors_structural_models[~idx_id_exists]
            df = df.drop('id', axis=1)
            self._save_table_data(
                self._get_name('032_sensors_structural_models', data_system),
                df,
                data_system=data_system,
                mode='append',
                )

    def _export_structural_models(
            self,
            structural_models: dict[str, StructuralModel],
            structural_model_name: str | list[str] | None = None,
            data_system: str = 'postgres',
            ) -> None:
        """Export time-series metadata.

        Args:
            structural_models: A dict of StructuralModel to be exported.
            structural_model_name: A name of structural model.
                If None, run about all structural models. The default, None
            data_system: A data sytem.
        """
        if structural_model_name is None:
            structural_model_name = list(structural_models.keys())
        structural_model_names = set_list_dim(structural_model_name, 1)
        if len(structural_model_names) == 0:
            return

        # save structural_model_metadata table
        df_structural_models = pd.DataFrame(
            [{
                key.lower(): getattr(structural_models[structural_model_name], key)
                for key in structural_models[structural_model_name].METADATA_KEYS
                } for structural_model_name in structural_model_names],
            index=structural_model_names
            )
        df_structural_models.index.name = 'structural_model_name'
        self._save_table_data(
            self._get_name('040_structural_model_metadata', data_system),
            df_structural_models,
            data_system=data_system,
            mode='sync',
            )

        # save structural_model tables
        for structural_model_name in structural_model_names:
            for component_name in structural_models[structural_model_name].COMPONENT_NAMES:
                component = getattr(structural_models[structural_model_name], component_name)
                if component is None:
                    continue
                path = self.datadrive / 'static_data/041_structural_models/'\
                    f'{structural_model_name}_{component_name}.csv'
                self._save_table_data(
                    path,
                    component,
                    data_system='file',
                    mode='replace',
                    )

    def import_dynamic_data(
            self,
            data_name: str | list[str],
            table_data_system: str = 'postgres',
            time_series_data_system: str = 'influx',
            first_datetime: datetime | None = None,
            last_datetime: datetime | None = None,
            import_metadata=True,
            ) -> DynamicData:
        """Import dynamic data.

        Args:
            data_name: A name of data.
            table_data_system: A data sytem for table data.
            time_series_data_system: A data sytem for time-series data.
            first_datetime: The data is imported after this datetime. The default, None
            last_datetime: The data is imported before this datetime. The default, None

        Returns:
            An instance of DynamicData class containing imported data.
        """
        time_series_batch_metadata = {}
        if import_metadata:
            time_series_batch_metadata = self._import_time_series_batch_metadata(data_system=table_data_system)

        return DynamicData(
            time_series_data=self._import_time_series_data(
                data_name,
                data_system=time_series_data_system,
                first_datetime=first_datetime,
                last_datetime=last_datetime,
                ),
            time_series_batch_metadata=time_series_batch_metadata,
            )

    def _import_time_series_data(
            self,
            data_name: str | list[str],
            data_system: str = 'influx',
            first_datetime: datetime | None = None,
            last_datetime: datetime | None = None,
            ) -> dict[str, TimeSeriesData]:
        """Import time-series data.

        Args:
            data_name: A name of data.
            data_system: A data sytem.
            first_datetime: The data is imported after this datetime. The default, None
            last_datetime: The data is imported before this datetime. The default, None

        Returns:
            A dict of imported TimeSeriesData.
        """
        data_names = set_list_dim(data_name, 1)
        if len(data_names) == 0:
            return {}

        fields = {}
        tags = {}
        for data_name in data_names:
            df = self._load_time_series_data(
                self._get_name(data_name, data_system),
                TimeSeriesData.TAG_KEYS,
                data_system,
                first_datetime=first_datetime,
                last_datetime=last_datetime,
                )
            if df is None:
                continue
            tag_idx = df.columns.str.match('|'.join(TimeSeriesData.TAG_KEYS), na=False)
            fields[data_name] = df.loc[:, ~tag_idx]
            tags[data_name] = df.loc[:, tag_idx]
        return {
            data_name: TimeSeriesData(
                fields=fields[data_name],
                tags=tags[data_name],
                )
            for data_name in fields
            }

    def _import_time_series_batch_metadata(
            self,
            data_system: str = 'postgres',
            ) -> dict[int, TimeSeriesBatchMetadata]:
        """Import time-series batch metadata.

        Args:
            data_system: A data sytem.

        Returns:
            A dict of imported TimeSeriesBatchMetadata.
        """
        # load time_series_batch_metadata table
        df_time_series_batch_metadata = self._load_table_data(
            self._get_name('001_time_series_batch_metadata', data_system),
            data_system=data_system,
            )
        if df_time_series_batch_metadata is None:
            return {}
        time_series_batch_metadata = df_time_series_batch_metadata.to_dict(orient='index')

        # set ids
        ids = list(time_series_batch_metadata.keys())

        # load time_series_batch_metadata table
        df_time_series_batch_dependencies = self._load_table_data(
            self._get_name('002_time_series_batch_dependencies', data_system),
            data_system=data_system,
            )
        time_series_batch_dependencies = {}
        if df_time_series_batch_dependencies is not None:
            df_time_series_batch_dependencies = df_time_series_batch_dependencies.reset_index()
            for batch_id, group_df in df_time_series_batch_dependencies.groupby('batch_id'):
                time_series_batch_dependencies[batch_id] = group_df.to_dict(orient='records')

        return {
            _id: TimeSeriesBatchMetadata(
                service_name=time_series_batch_metadata[_id]['service_name'],
                batch_datetime=time_series_batch_metadata[_id]['batch_datetime'],
                dependencies={
                    record['dependent_batch_id']: TimeSeriesBatchDependencies(
                        id=record['id'],
                        )
                    for record in time_series_batch_dependencies.get(_id, [])
                    },
                )
            for _id in ids
            }

    def export_dynamic_data(
            self,
            dynamic_data: DynamicData,
            data_name: str | list[str],
            table_data_system: str = 'postgres',
            time_series_data_system: str = 'influx',
            ) -> None:
        """Export dynamic data.

        Args:
            dynamic_data: An instance of DynamicData class containing data to be exported.
            data_name: A name of data.
            table_data_system: A data sytem for table data.
            time_series_data_system: A data sytem for time-series data.
        """
        self._export_time_series_data(
            dynamic_data.time_series_data,
            data_name=data_name,
            data_system=time_series_data_system,
            )
        self._export_time_series_batch_metadata(
            dynamic_data.time_series_batch_metadata,
            data_system=table_data_system,
            )

    def _export_time_series_data(
            self,
            time_series_data: dict[str, TimeSeriesData],
            data_name: str | list[str],
            data_system: str = 'influx',
            ) -> None:
        """Export time-series data.

        Args:
            time_series_data: A dict of TimeSeriesData to be exported.
            data_name: A name of data. If None, run about all data. The default, None
            data_system: A data sytem.
        """
        data_names = set_list_dim(data_name, 1)
        if len(data_names) == 0:
            return

        for data_name in data_names:
            if data_name not in time_series_data:
                continue
            df = time_series_data[data_name].fields.join(
                time_series_data[data_name].tags,
                how='left',
                )
            self._save_time_series_data(
                self._get_name(data_name, data_system),
                df,
                TimeSeriesData.TAG_KEYS,
                data_system=data_system,
                )

    def _export_time_series_batch_metadata(
            self,
            time_series_batch_metadata: dict[int, TimeSeriesBatchMetadata],
            data_system: str = 'postgres',
            ) -> None:
        """Export time-series batch metadata.

        Args:
            data_system: A data sytem.
        """
        ids = list(time_series_batch_metadata.keys())
        if len(ids) == 0:
            return

        # save time_series_batch_metadata table
        df_time_series_batch_metadata = pd.DataFrame(
            [{
                key: getattr(time_series_batch_metadata[_id], key)
                for key in time_series_batch_metadata[_id].METADATA_KEYS
                } for _id in ids],
            index=ids,
            )
        df_time_series_batch_metadata.index.name = 'id'
        self._save_table_data(
            self._get_name('001_time_series_batch_metadata', data_system),
            df_time_series_batch_metadata,
            data_system=data_system,
            mode='sync',
            )

        # save time_series_batch_dependencies table
        time_series_batch_dependencies = []
        for batch_id in ids:
            dependencies = time_series_batch_metadata[batch_id].dependencies
            for dependent_batch_id, info in dependencies.items():
                info_dict = asdict(info)
                time_series_batch_dependencies.append({
                    **{
                        'batch_id': batch_id,
                        'dependent_batch_id': dependent_batch_id,
                        },
                    **info_dict,
                    })

        if len(time_series_batch_dependencies):
            df_time_series_batch_dependencies = pd.DataFrame.from_dict(time_series_batch_dependencies)
            idx_id_exists = ~df_time_series_batch_dependencies['id'].isna()
            df = df_time_series_batch_dependencies[idx_id_exists]
            df = df.set_index('id')
            self._save_table_data(
                self._get_name('002_time_series_batch_dependencies', data_system),
                df,
                data_system=data_system,
                mode='sync',
                )
            df = df_time_series_batch_dependencies[~idx_id_exists]
            df = df.drop('id', axis=1)
            self._save_table_data(
                self._get_name('002_time_series_batch_dependencies', data_system),
                df,
                data_system=data_system,
                mode='append',
                )

    def _get_name(
            self,
            table_name: str,
            data_system: str,
            ) -> str | Path:
        """Get table name or path of the data.

        Args:
            table_name: A name of the table.
            data_system: A data sytem.

        Retruns:
            table name or path of the data.
        """
        if data_system not in ('file', 'streaming'):
            return table_name

        if data_system == 'file':
            if table_name in self.STATIC_DATA_TABLES:
                path = self.datadrive / f'static_data/init_rdb/{table_name}.csv'
            elif table_name in self.DYNAMIC_DATA_TABLES:
                path = self.datadrive / f'dynamic_data/init_rdb/{table_name}.csv'
            else:
                path = self.datadrive / f'dynamic_data/init_tsdb/{table_name}.csv'
        else:
            path = self.datadrive / f'dynamic_data/streaming_temp/{table_name}.csv'
        return path
