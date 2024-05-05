from dataclasses import dataclass, field
from typing import Any, Self

from ..base.base import Base
from .instance_metadata import InstanceMetadata
from .sensor import Sensor
from .service_metadata import ServiceMetadata
from .structural_model import StructuralModel
from .time_series_metadata import TimeSeriesMetadata


@dataclass
class StaticData(Base):
    service_metadata: dict[str, ServiceMetadata] = field(default_factory=dict)
    time_series_metadata: dict[str, TimeSeriesMetadata] = field(default_factory=dict)
    instance_metadata: dict[str, InstanceMetadata] = field(default_factory=dict)
    sensors: dict[str, Sensor] = field(default_factory=dict)
    structural_models: dict[str, StructuralModel] = field(default_factory=dict)

    def update(
            self,
            other: Self,
            ) -> None:
        """Update attributes.

        Args:
            other: Another instance of StaticData.
        """
        self.service_metadata.update(other.service_metadata)
        self.time_series_metadata.update(other.time_series_metadata)
        self.instance_metadata.update(other.instance_metadata)
        self.sensors.update(other.sensors)
        self.structural_models.update(other.structural_models)

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)
