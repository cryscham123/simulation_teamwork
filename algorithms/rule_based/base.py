from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseScheduler(ABC):
    """공통 스케줄러 인터페이스."""

    @abstractmethod
    def solve(self) -> pd.DataFrame:
        """최적 스케줄을 반환한다."""
        raise NotImplementedError
