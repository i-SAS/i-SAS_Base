import itertools
from abc import ABCMeta, abstractmethod
from logging import getLogger

from .base import Base

logger = getLogger(__name__)


class BaseAnalysis(Base, metaclass=ABCMeta):
    TASK = 'analysis'

    @property
    @staticmethod
    @abstractmethod
    def DEFAULT_INPUT_DATA_NAME_DICT(self) -> dict[str, list[str]]:  # NOQA
        pass

    @property
    @staticmethod
    @abstractmethod
    def DEFAULT_OUTPUT_DATA_NAME_DICT(delf) -> dict[str, list[str]]:  # NOQA    
        pass

    def __init__(
            self,
            instance_name: str,
            input_data_name_dict: dict[str, list[str]] | None = None,
            output_data_name_dict: dict[str, list[str]] | None = None,
            service_name: str | None = None,
            sensor_name: str | None = None,
            output_data_name_to_sensor_name: dict[str, str] | None = None,
            structural_model_name: str | None = None,
            output_data_name_to_structural_model_name: dict[str, str] | None = None,
            **params,
            ) -> None:
        """Initialize Analysis model.

        Args:
            instance_name: A name of instance.
            input_data_name_dict: A dict whose keys are keys of input data,
                and values are names of input data.
            output_data_name_dict: A dict whose keys are keys of output data,
                and values are names of output data.
            service_name: A name of the service.
            sensor_name: A name of sensor.
            output_data_name_to_sensor_name: A dict mapping output data name to sensor name.
            structural_model_name: A name of structural model.
            output_data_name_to_structural_model_name: A dict mapping output data name to strucutral model name.
            **params: Parameters of this model.

        Examples:
            >>> Model(
                    {
                        'input_data_key': ['input_data_name_1', 'input_data_name_2'],
                    },
                    {
                        'output_data_key': ['output_data_name_1', 'output_data_name_2'],
                    },
                    instance_name='instance_1',
                    structural_model_name='structural_model_1',
                )
        """
        input_data_name_dict, output_data_name_dict = self._check_data_names(
            input_data_name_dict, output_data_name_dict)
        logger.info(
            f'[{self.TASK}/{self.MODEL_NAME}/{instance_name}] Initialize subpackage:'
            f'{input_data_name_dict=}, {output_data_name_dict=}'
            )
        self.input_data_name_dict = input_data_name_dict
        self.output_data_name_dict = output_data_name_dict

        # Following instance variables are required for i-SAS Inteface.
        self.structural_model_connections = {}
        super().__init__(
            list(itertools.chain.from_iterable(input_data_name_dict.values())),
            list(itertools.chain.from_iterable(output_data_name_dict.values())),
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
            input_data_name_dict: dict[str, list[str]] | None,
            output_data_name_dict: dict[str, list[str]] | None,
            ) -> tuple[dict[str, list[str]]]:
        """Check input and output data names.

        Args:
            input_data_name_dict: A dict whose keys are keys of input data (str),
                and values are names of input data (str).
            output_data_name_dict: A dict whose keys are keys of output data (str),
                and values are names of output data (str).

        Returns:
            Input and output data names.
        """
        if input_data_name_dict is None:
            input_data_name_dict = cls.DEFAULT_INPUT_DATA_NAME_DICT
        elif not set(input_data_name_dict.keys()) <= set(cls.DEFAULT_INPUT_DATA_NAME_DICT.keys()):
            raise ValueError(
                f'Invalid names of input quantity. Required: {set(cls.DEFAULT_INPUT_DATA_NAME_DICT.keys())}.')
        if output_data_name_dict is None:
            output_data_name_dict = cls.DEFAULT_OUTPUT_DATA_NAME_DICT
        elif not set(output_data_name_dict.keys()) <= set(cls.DEFAULT_OUTPUT_DATA_NAME_DICT.keys()):
            raise ValueError(
                f'Invalid names of output quantity. Required: {set(cls.DEFAULT_OUTPUT_DATA_NAME_DICT.keys())}.')
        return input_data_name_dict, output_data_name_dict
