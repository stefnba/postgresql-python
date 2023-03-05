from typing import TypedDict, Union

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
