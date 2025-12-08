from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional


class DbOperationType(str, Enum):
    INSERT = "insert"
    UPDATE = "update"


@dataclass
class DbOperation:
    """
    A single DB mutation operation.
    """
    table: str
    op_type: DbOperationType
    id_value: Any  # primary key or unique identifier
    payload: Mapping[str, Any]  # column -> value
    # metadata is optional contextual info, not used by core logic
    metadata: Optional[Mapping[str, Any]] = None

