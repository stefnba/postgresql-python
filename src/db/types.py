from typing import Any, Dict, TypedDict, TypeVar, Union

from pydantic import BaseModel

# create_model_from_typeddict


class ConnectionObject(TypedDict):
    host: str
    port: int
    user: str
    password: str
    db_name: str


class ConnectionModel(BaseModel):
    host: str
    port: int
    user: str
    password: str
    db_name: str


ConnectionInfo = Union[str, ConnectionObject, ConnectionModel]

DbModelRecord = TypeVar("DbModelRecord", bound=BaseModel)

QueryParams = Dict[str, Any]
QueryData = QueryParams
DbDictRecord = QueryParams
