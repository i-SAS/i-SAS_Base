from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from ..base.base import Base


@dataclass
class TimeSeriesBatchDependencies(Base):
    id: int | None = None

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)


@dataclass
class TimeSeriesBatchMetadata(Base):
    """Dataclass for time-series batch metadata."""
    service_name: str
    batch_datetime: datetime
    dependencies: dict[int, TimeSeriesBatchDependencies]
    METADATA_KEYS: Final[tuple[str]] = ('service_name', 'batch_datetime')

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)
