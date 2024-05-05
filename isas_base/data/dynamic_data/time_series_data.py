from dataclasses import dataclass
from typing import Any, Final

import pandas as pd

from ..base.base import Base


@dataclass
class TimeSeriesData(Base):
    """Dataclass for time-series data."""
    fields: pd.DataFrame
    tags: pd.DataFrame | None = None
    TAG_KEYS: Final[tuple[str]] = ('batch_id', 'service_name', 'batch_datetime')

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)
