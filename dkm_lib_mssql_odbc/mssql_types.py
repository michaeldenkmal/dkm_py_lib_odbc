import dataclasses
from dataclasses import dataclass
from typing import List, Any, TypeVar, Dict, Protocol


@dataclass
class SqlAndParams:
    sql:str
    dbparams:List[Any]


class DataclassProto(Protocol):
    __dataclass_fields__: dict


TDC = TypeVar("TDC", bound=dataclasses.dataclass)

PrimaryKeyInfo = Dict[str, Any]




