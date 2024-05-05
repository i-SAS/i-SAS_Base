from dataclasses import dataclass
from typing import Any

from ..base.base import Base


@dataclass
class ServiceMetadata(Base):
    """Dataclass for metadata on the service."""

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)
