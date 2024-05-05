from abc import ABCMeta, abstractmethod
from logging import getLogger

from .base import Base

logger = getLogger(__name__)


class BaseMeasurement(Base, metaclass=ABCMeta):
    TASK = 'measurement'
    INPUT_DATA_NUM = None
    OUTPUT_DATA_NUM = None

    @property
    @staticmethod
    @abstractmethod
    def DEFAULT_INPUT_DATA_NAMES() -> list[str]:  # NOQA
        pass

    @property
    @staticmethod
    @abstractmethod
    def DEFAULT_OUTPUT_DATA_NAMES() -> list[str]:  # NOQA
        pass

    def __init__(
            self,
            instance_name: str,
            input_data_names: list[str] | None = None,
            output_data_names: list[str] | None = None,
            service_name: str | None = None,
            sensor_name: str | None = None,
            output_data_name_to_sensor_name: dict[str, str] | None = None,
            structural_model_name: str | None = None,
            output_data_name_to_structural_model_name: dict[str, str] | None = None,
            simulation: bool = False,
            **params,
            ) -> None:
        """Initialize Measurement model.

        Args:
            instance_name: A name of the instance.
            input_data_names: A list of names of input data.
            output_data_name_dict: A list of names of output data.
            service_name: A name of the service.
            sensor_name: A name of sensor.
            output_data_name_to_sensor_name: A dict mapping output data name to sensor name.
            structural_model_name: A name of structural model.
            output_data_name_to_structural_model_name: A dict mapping output data name to strucutral model name.
            **params: Parameters of this model.

        Examples:
            >>> Model(
                    ['input_data_1', 'input_data_2'],
                    ['output_data_1', 'output_data_2'],
                    instance_name='instance_1',
                    sensor_name='sensor_1',
                )
        """
        input_data_names, output_data_names = self._check_data_names(input_data_names, output_data_names)
        logger.info(
            f'Initialize subpackage: {self.TASK}/{self.MODEL_NAME}/{instance_name}'
            f'{input_data_names=}, {output_data_names=}'
            )
        self.simulation = simulation

        # Following instance variables are required for i-SAS Inteface.
        self.structural_model_connections = {}
        super().__init__(
            input_data_names,
            output_data_names,
            service_name,
            instance_name,
            sensor_name,
            output_data_name_to_sensor_name,
            structural_model_name,
            output_data_name_to_structural_model_name,
            params,
            )

    @classmethod
    def _check_data_names(
            cls,
            input_data_names: list[str] | None,
            output_data_names: list[str] | None,
            ) -> tuple[list[str]]:
        """Check input and output data names.

        Args:
            input_data_names: A list of names of input data.
            output_data_name_dict: A list of names of output data.

        Returns:
            Input and output data names.
        """
        if input_data_names is None:
            input_data_names = cls.DEFAULT_INPUT_DATA_NAMES
        elif cls.INPUT_DATA_NUM is not None and len(input_data_names) != cls.INPUT_DATA_NUM:
            raise ValueError(f'The number of input data must be {cls.INPUT_DATA_NUM}')
        if output_data_names is None:
            output_data_names = cls.DEFAULT_OUTPUT_DATA_NAMES
        elif cls.OUTPUT_DATA_NUM is not None and len(output_data_names) != cls.OUTPUT_DATA_NUM:
            raise ValueError(f'The number of output data must be {cls.OUTPUT_DATA_NUM}')
        return input_data_names, output_data_names
