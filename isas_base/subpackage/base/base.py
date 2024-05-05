from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any

import pandas as pd

from ...data import DynamicData, StaticData
from ...data.static_data.instance_metadata import (
    InstanceInputMetadata, InstanceMetadata, InstanceOutputMetadata
)
from ...data.static_data.sensor import Sensor, SensorInfo
from ...data.static_data.structural_model import StructuralModelInfo
from ...data.static_data.time_series_metadata import TimeSeriesMetadata
from ...utils import get_cfg

logger = getLogger(__name__)


class Base(ABC):
    TIME_SERIES_METADATA_INPUT_KEYS = {
        'output_data_names',
        'output_data_name_to_sensor_name',
        'output_data_name_to_structural_model_name'
        'structural_model_connection',
        'coord_sys',
        'component_name',
        'r1',
        'r2',
        'r3',
        }

    INSTANCE_METADATA_INPUT_KEYS = (
        'instance_name',
        'MODEL_NAME',
        'input_data_names',
        'output_data_names',
        )

    @property
    @staticmethod
    @abstractmethod
    def TASK() -> str:  # NOQA
        """A name of task. One of followings.
            sensor_simulator: A module to sumulate sensor.
            sensor_connector: A module to connect sensor.
            analysis_solver: A module to analyse data.
            gui_widgit_creator: A module to create widget for GUI.
        """
        raise NotImplementedError()

    @property
    @staticmethod
    @abstractmethod
    def MODEL_NAME() -> str:  # NOQA
        """A name of model name."""
        raise NotImplementedError()

    @property
    @staticmethod
    @abstractmethod
    def DEFAULT_CFG() -> dict[str, Any]:  # NOQA
        """A dict of default config of the model."""
        raise NotImplementedError()

    @abstractmethod
    def __init__(
            self,
            input_data_names: list[str],
            output_data_names: list[str],
            service_name: str,
            instance_name: str,
            sensor_name: str | None,
            output_data_name_to_sensor_name: dict[str, str] | None,
            structural_model_name: str | None,
            output_data_name_to_structural_model_name: dict[str, str] | None,
            params: dict[str, Any] | None = None
            ) -> None:
        self.service_name = service_name
        self.input_data_names = input_data_names
        self.output_data_names = output_data_names
        self.instance_name = instance_name
        if output_data_name_to_sensor_name is None:
            output_data_name_to_sensor_name = {
                data_name: sensor_name for data_name in output_data_names
                }
        self.output_data_name_to_sensor_name = output_data_name_to_sensor_name
        if output_data_name_to_structural_model_name is None:
            output_data_name_to_structural_model_name = {
                data_name: structural_model_name for data_name in output_data_names
                }
        self.output_data_name_to_structural_model_name = output_data_name_to_structural_model_name
        self.cfg = self._get_cfg(params=params)

        self.coord_sys = {}
        self.component_name = {}
        self.r1 = {}
        self.r2 = {}
        self.r3 = {}

    @staticmethod
    @abstractmethod
    def _check_data_names() -> None:
        """Check data names."""
        raise NotImplementedError()

    @classmethod
    def _get_cfg(
            cls,
            params: dict[str, Any] | None = None
            ) -> dict[str, Any]:
        """Set config.

        Args:
            params: Parameters to overwrite config.

        Returns:
            A config updated.
        """
        return get_cfg(params, cls.DEFAULT_CFG)

    def set_model(
            self,
            static_data: StaticData | None = None,
            streaming: bool = False,
            ) -> None:
        """Set model and calculate intermediate data.
        For details of args, see https://github.com/i-SAS/i-SAS_documentation/wiki/Data-Structure-on-Python

        Args:
            static_data: static data.
            streaming: A flag for streaming.
        """
        logger.info(f'[{self.TASK}/{self.MODEL_NAME}/{self.instance_name}] Set model')
        self.static_data = static_data

        for data_name, sensor_name in self.output_data_name_to_sensor_name.items():
            if sensor_name is None:
                continue
            if self.static_data.sensors is None or sensor_name not in self.static_data.sensors:
                raise ValueError(f'Sensor {sensor_name} for {data_name} does not exist.')

        for data_name, structural_model_name in self.output_data_name_to_structural_model_name.items():
            if structural_model_name is None:
                continue
            if self.static_data.structural_models is None \
                    or structural_model_name not in self.static_data.structural_models:
                raise ValueError(f'Structural model {structural_model_name} for {data_name} does not exist.')

        self.streaming = streaming
        self._set_model()

    @abstractmethod
    def _set_model(self) -> None:
        """Arbitrary process executed in model setting."""
        raise NotImplementedError()

    @abstractmethod
    def __call__(
            self,
            dynamic_data: DynamicData,
            ) -> dict[str, pd.DataFrame]:
        """Arbitrary process to time-series data.

        Args:
            dynamic_data: dynamic_data.

        Returns:
            Results of analysis. The keys are names of output data, and the values are results.
        """
        raise NotImplementedError()

    @abstractmethod
    def exit(self) -> None:
        """Exit model."""
        raise NotImplementedError()

    def get_static_data(self):
        time_series_metadata = {}
        for data_name in self.output_data_names:
            sensor_info = {}
            sensor_name = self.output_data_name_to_sensor_name.get(data_name, None)
            if sensor_name is not None:
                sensor_info = {sensor_name: SensorInfo()}
            structural_model_info = {}
            structural_model_name = self.output_data_name_to_structural_model_name.get(data_name, None)
            if structural_model_name is not None:
                connection = None
                if hasattr(self, 'structural_model_connections'):
                    connection = self.structural_model_connections.get(data_name, None)
                if connection is None and sensor_name is not None:
                    connection = self._get_connection_from_sensors(
                        self.static_data.sensors[sensor_name],
                        structural_model_name,
                        )
                structural_model_info = {
                    structural_model_name: StructuralModelInfo(
                        component_name=self.component_name.get(data_name, None),
                        connection=connection,
                        )}
            time_series_metadata[data_name] = TimeSeriesMetadata(
                coord_sys=self.coord_sys.get(data_name, None),
                sensor_info=sensor_info,
                structural_model_info=structural_model_info,
                )
        instance_metadata = {
            self.instance_name: InstanceMetadata(
                model_name=self.MODEL_NAME,
                service_name=self.service_name,
                input_metadata={
                    data_name: InstanceInputMetadata()
                    for data_name in self.input_data_names
                    },
                output_metadata={
                    data_name: InstanceOutputMetadata()
                    for data_name in self.output_data_names
                    },
                )}
        return StaticData(
            time_series_metadata=time_series_metadata,
            instance_metadata=instance_metadata,
            )

    @staticmethod
    def _get_connection_from_sensors(
            sensor: Sensor,
            structural_model_name: str,
            ) -> pd.DataFrame | None:
        """Get connection from sensors.

        Args:
            sensor: An instance of Sensor dataclass.
            structural_model_name: A name of the structural model.
        """
        if structural_model_name not in sensor.structural_model_info:
            return None
        connection = sensor.structural_model_info[structural_model_name].connection
        connection.index.name = 'data_id'
        return connection
