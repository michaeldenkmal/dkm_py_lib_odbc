from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Callable, Literal


DKM_DB_TYPE = Literal["mssql", "mysql"]

RowDict = Dict[str, Any]
OnRowFound = Callable[[RowDict], bool]


@dataclass
class DbConnInfo:
    host:str
    db_name:str
    user:str
    pwd:str
    db_type:DKM_DB_TYPE



