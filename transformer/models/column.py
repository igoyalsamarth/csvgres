from dataclasses import dataclass
from typing import Any, Optional

@dataclass
class ColumnDefinition:
    name: str
    type: str
    is_serial: bool = False
    not_null: bool = False
    primary_key: bool = False
    unique: bool = False
    default: Optional[Any] = None
    initial_counter_value: Optional[int] = 1
    auto_increment_counter: Optional[int] = 1  # For SERIAL columns
