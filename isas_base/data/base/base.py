import math
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class Base:
    def __eq__(
            self,
            other: Any,
            ) -> bool:
        if other is None or type(other) is not type(self):
            return False
        for k in vars(self):
            v1 = getattr(self, k)
            if not hasattr(other, k):
                return False
            v2 = getattr(other, k)

            # pd.DataFrame
            if isinstance(v1, pd.DataFrame):
                if v1.equals(v2):
                    continue
            # np.nan | math.nan
            elif isinstance(v1, float) and math.isnan(v1):
                if math.isnan(v2):
                    continue
            # the others
            else:
                if v1 == v2:
                    continue
            return False
        return True
