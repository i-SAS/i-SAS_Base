from dataclasses import dataclass
from typing import Any, Final

from ..base.base import Base
from .sensor import SensorInfo
from .structural_model import StructuralModelInfo


@dataclass
class TimeSeriesMetadata(Base):
    """Dataclass for metadata on time-series data."""
    coord_sys: str | None
    sensor_info: dict[str, SensorInfo]
    structural_model_info: dict[str, StructuralModelInfo]
    METADATA_KEYS: Final[tuple[str]] = ('coord_sys', )

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)
