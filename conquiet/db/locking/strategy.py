from enum import Enum


class LockStrategy(str, Enum):
    ADVISORY = "advisory"
    ROW = "row"
    ADVISORY_AND_ROW = "advisory_and_row"
    TABLE = "table"
    NONE = "none"

