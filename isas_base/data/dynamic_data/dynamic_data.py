from dataclasses import dataclass, field
from typing import Any, Self

from ..base.base import Base
from .time_series_batch_metadata import TimeSeriesBatchMetadata
from .time_series_data import TimeSeriesData


@dataclass
class DynamicData(Base):
    time_series_data: dict[str, TimeSeriesData] = field(default_factory=dict)
    time_series_batch_metadata: dict[str, TimeSeriesBatchMetadata] = field(default_factory=dict)

    def update(
            self,
            other: Self,
            ) -> None:
        """Update attributes.

        Args:
            other: Another instance of DynamicData.
        """
        self.time_series_data.update(other.time_series_data)
        self.time_series_batch_metadata.update(other.time_series_batch_metadata)

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)
