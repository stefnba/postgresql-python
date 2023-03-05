from datetime import datetime

from pydantic import BaseModel

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

    user = db.find("SELECT * FROM securities_types").get_all(SecuritiesTypes)
    print(user)

    return

    # def find_user() -> User:
    #     return db.find_one(query="SELECT * FROM securities_types")

    user = db.find_one(
        query="SELECT * FROM securities_types", return_model=SecuritiesTypes
    )
    if user:
        # print(user.type)
        pass

    # find many
    db.find_all(query="SELECT * FROM securities_types", return_model=SecuritiesTypes)
    # print(types_aaa)

    # run
    run_manul = db.run("SELECT * FROM securities_types")
    # print(run_manul.fetchone())
    run_manul.close()

    data = CurrenciesAdd(**{"id": "CHF", "currency_name": "US Dollar"})

    # add
    add = db.add_one(
        data=data,
        table="currencies",
        returning=Currencies,
    )
    print(add)


if __name__ == "__main__":
    main()
