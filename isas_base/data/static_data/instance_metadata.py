from dataclasses import dataclass
from typing import Any, Final

from ..base.base import Base


@dataclass
class InstanceInputMetadata(Base):
    """Dataclass for metadata on the input of the instance."""
    id: int | None = None

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)


@dataclass
class InstanceOutputMetadata(Base):
    """Dataclass for metadata on the output of the instance."""
    id: int | None = None

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)


@dataclass
class InstanceMetadata(Base):
    """Dataclass for metadata on the instance."""
    model_name: str
    service_name: str
    input_metadata: dict[str, InstanceInputMetadata]
    output_metadata: dict[str, InstanceOutputMetadata]
    METADATA_KEYS: Final[tuple[str]] = ('model_name', 'service_name')

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)
