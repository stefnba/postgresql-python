from datetime import datetime

from pydantic import BaseModel

# from db.client import PgClient
from db.client import PgClient


class SecuritiesTypes(BaseModel):
    id: int
    type: str
    is_active: bool
    updated_at: datetime | None
    created_at: datetime


class Currencies(BaseModel):
    id: str
    currency_name: str
    updated_at: datetime | None
    created_at: datetime | None


class CurrenciesAdd(BaseModel):
    id: str
    currency_name: str


class CurrenciesUpdate(BaseModel):
    id: str
    updated_at: datetime | None
    currency_name: str | None


def main():
    db = PgClient(
        connection={
            "db_name": "uniquestocks",
            "host": "localhost",
            "password": "password",
            "user": "admin",
            "port": 5871,
        }
    )

    db.find(
        query="SELECT * from currencies",
        # filters='WHERE "id" IS NULL'
        filters=[
            {"column": "id", "operator": "IN", "value": ["1"]},
            {"column": "id", "operator": "EQUAL", "value": "1"},
        ],
    )

    db.run("SELECT * FROM securities_types")
    db.update(
        data={"id": 123},
        table="currencies",
        filters=[
            {"column": "id", "operator": "IN", "value": ["1"]},
            {"column": "id", "operator": "EQUAL", "value": "1"},
        ],
        returning="*",
    )
    add = db.add(
        data=[
            {"id": "EEE", "currency_name": "name"},
            {"currency_name": "name", "id": "DDD"},
            {"id": "PPP", "currency_name": "name"},
        ],
        table="currencies",
        conflict="d",
        returning="*",
    ).get_all()
    print(add)

    return


if __name__ == "__main__":
    main()
