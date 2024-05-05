from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Final

import pandas as pd

from ..base.base import Base


@dataclass
class StructuralModelInfo(Base):
    """Dataclass for the information on the strcutural model."""
    id: int | None = None
    component_name: str | None = None
    connection: pd.DataFrame | None = None

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)


@dataclass
class StructuralModel(Base):
    def __post_init__(self) -> None:
        self.METADATA_KEYS: Final[tuple] = ('MODEL_TYPE', )


@dataclass
class PointCloud(StructuralModel):
    point_cloud: pd.DataFrame | None
    MODEL_TYPE: Final[str] = 'point_cloud'
    COMPONENT_NAMES: Final[tuple] = ('point_cloud', )

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)


@dataclass
class FiniteElementModel(StructuralModel):
    fe_node: pd.DataFrame | None
    fe_elem: pd.DataFrame | None
    fe_connection: pd.DataFrame | None
    fe_constraint: pd.DataFrame | None = None
    MODEL_TYPE: Final[str] = 'fe'
    COMPONENT_NAMES: Final[tuple] = (
        'fe_node',
        'fe_elem',
        'fe_connection',
        'fe_constraint',
        )

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)


@dataclass
class GraphModel(StructuralModel):
    graph_node: pd.DataFrame | None
    graph_edge: pd.DataFrame | None
    MODEL_TYPE: Final[str] = 'graph'
    COMPONENT_NAMES: Final[tuple] = (
        'graph_node',
        'graph_edge',
        )

    def __eq__(
            self,
            other: Any,
            ) -> bool:
        return super().__eq__(other)


STRUCTURAL_MODEL_CLASS: Final[namedtuple] = namedtuple(
    'STRUCTURAL_MODEL_CLASS',
    ('point_cloud', 'fe', 'graph'),
    )(PointCloud, FiniteElementModel, GraphModel)
