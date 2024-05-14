import itertools
from abc import ABCMeta, abstractmethod
from logging import getLogger

from PySide6 import QtWidgets

from .base import Base

logger = getLogger(__name__)


class BaseVisualization(Base, metaclass=ABCMeta):
    TASK = 'visualization'
    COLOR_THEME = {
        'default': '#000000',  # automatically defined by selected CSS theme
        'white': '#000000',
        'dark': '#ffffff',
        }

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

    def get_update_priority(self) -> int:
        return self.cfg.get('UPDATE_PRIORITY', 0)

    def set_color_theme(self, color_theme: str) -> None:
        if self.cfg['COLOR_THEME'] == 'default':
            self.cfg['COLOR_THEME'] = color_theme

    def __init__(
            self,
            instance_name: str,
            size_ratio: tuple,
            input_data_name_dict: dict[str, list[str]] | None = None,
            output_data_name_dict: dict[str, list[str]] | None = None,
            service_name: str | None = None,
            sensor_name: str | None = None,
            output_data_name_to_sensor_name: dict[str, str] | None = None,
            structural_model_name: str | None = None,
            output_data_name_to_structural_model_name: dict[str, str] | None = None,
            **params,
            ) -> None:
        """Initialize Visualization model.

        Args:
            instance_name: A name of instance.
            size_ratio: Figure size ratio, (width, height). The number must be int within [1, 12].
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
                    (1, 1)
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
            input_data_name_dict, output_data_name_dict, size_ratio)
        logger.info(
            f'Initialize subpackage: {self.TASK}/{self.MODEL_NAME}/{instance_name}'
            f'{input_data_name_dict=}, {output_data_name_dict=}'
            )
        self.input_data_name_dict = input_data_name_dict
        self.output_data_name_dict = output_data_name_dict

        # Following instance variables are required for i-SAS Inteface.
        self.size_ratio = size_ratio
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
            size_ratio: tuple[int],
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
        if not all([isinstance(n, int) and 1 <= n <= 12 for n in size_ratio]):
            raise ValueError('The number of size ratio must be int with in [1, 12].')
        return input_data_name_dict, output_data_name_dict

    def create_widget(self) -> QtWidgets.QWidget:
        """Create widget.

        Returns:
            A widget of Qt.
        """
        self.widget = self._create_widget()
        size_policy = self.widget.sizePolicy()
        size_policy.setVerticalStretch(self.size_ratio[0])
        size_policy.setHorizontalStretch(self.size_ratio[1])
        self.widget.setSizePolicy(size_policy)
        return self.widget

    @abstractmethod
    def _create_widget(self) -> QtWidgets.QWidget:
        """Arbitrary process executed in widget creating.

        Returns:
            A widget of Qt.
        """
        raise NotImplementedError()
