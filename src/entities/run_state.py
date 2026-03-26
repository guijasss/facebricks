from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RunState:
    life_cycle_state: Optional[str] = None
    result_state: Optional[str] = None
    state_message: Optional[str] = None
