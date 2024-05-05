from dataclasses import dataclass
from typing import Any, Final

import pandas as pd

from ..base.base import Base
from .structural_model import StructuralModelInfo


@dataclass
class Sensor(Base):
    """Dataclass for a sensor."""
    locational: bool
    directional: bool
    structural_model_info: dict[str, StructuralModelInfo]
    data: pd.DataFrame | None
    METADATA_KEYS: Final[tuple[str]] = ('locational', 'directional')

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)


@dataclass
class SensorInfo(Base):
    """Dataclass for the information on the sensor."""
    id: int | None = None

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)
