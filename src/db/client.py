from typing import Any, Dict, Optional, Type, TypeVar, overload

import psycopg
from psycopg import sql
from psycopg.abc import Query
from psycopg.cursor import Cursor
from psycopg.rows import Row, RowFactory, class_row, dict_row
from pydantic import BaseModel

from db.types import ConnectionInfo, ConnectionModel

ReturnModel = TypeVar("ReturnModel", bound=BaseModel)

QueryParams = Dict[str, Any]
QueryData = QueryParams
DbRecord = QueryParams


class User(BaseModel):
    id: int
    type: str


class PgClient:
    _conninfo: str

    def __init__(self, connection: ConnectionInfo) -> None:
        """_summary_

        Args:
            connection (ConnectionInfo): The connection string (a postgresql:// url)
            to specify where and how to connect.
        """
        self._create_connection(connection)

    def _create_connection(self, connection: ConnectionInfo):
        self._build_connection_str(connection)

        # test
        if True:
            self._test_connection()

    def _build_connection_str(self, connection: ConnectionInfo) -> str:
        """
        Creates a connection string from connection object or connection model

        Args:
            connection (ConnectionInfo): either connection string, object or model

        Returns:
            str: The connection string (a postgresql:// url)
                to specify where and how to connect.
        """
        if isinstance(connection, str):
            self._conninfo = connection
        else:
            if isinstance(connection, ConnectionModel):
                conn = connection
            else:
                conn = ConnectionModel.parse_obj(connection)

            self._conninfo = f"postgresql://{conn.user}:{conn.password}@{conn.host}:\
                {conn.port}/{conn.db_name}"

        return self._conninfo

    def _test_connection(self):
        """
        Tests whether the provided connection details are valid.
        """
        with psycopg.connect(self._conninfo) as conn:
            conn.execute("SELECT 1")
            print("✅ Connection successful")

    @overload
    def add_one(
        self,
        data: QueryData | BaseModel,
        table: str,
        *,
        conflict: Optional[str] = None,
    ) -> None:
        ...

    @overload
    def add_one(
        self,
        data: QueryData | BaseModel,
        table: str,
        returning: str | list[str],
        conflict: Optional[str] = None,
    ) -> DbRecord:
        ...

    @overload
    def add_one(
        self,
        data: QueryData | BaseModel,
        table: str,
        returning: Type[ReturnModel],
        conflict: Optional[str] = None,
    ) -> ReturnModel:
        ...

    def add_one(
        self,
        data: QueryData | BaseModel,
        table: str,
        returning: Optional[str | list[str] | Type[ReturnModel]] = None,
        conflict: Optional[str] = None,
    ) -> DbRecord | None | ReturnModel:
        """
        Adds one record to the specified table.

        Args:
            data (QueryData): _description_
            table (str): _description_
            returning (Optional[str  |  list[str]  |  Type[ReturnModel]], optional):
                _description_. Defaults to None.
            conflict (Optional[str], optional): _description_. Defaults to None.

        Returns:
            DbRecord | None | ReturnModel: _description_
        """

        if isinstance(data, dict):
            fields = data.keys()
        elif isinstance(data, BaseModel):
            fields = data.__fields__.keys()
            data = data.dict()

        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(map(sql.Identifier, fields)),
            values=sql.SQL(", ").join(map(sql.Placeholder, fields)),
        )

        row_factory: RowFactory = dict_row

        if returning:
            returning_part = self.__construct_return_part(returning)
            if (
                not isinstance(returning, str)
                and not isinstance(returning, list)
                and issubclass(returning, BaseModel)
            ):
                row_factory = class_row(returning)
            # concetenate query and returning_part
            query = sql.SQL(" ").join([query, returning_part])

        if conflict:
            pass

        cursor = self.run(query=query, params=data, row_factory=row_factory)
        result = None
        if returning:
            result = cursor.fetchone()
        cursor.close()
        return result

    def __construct_return_part(
        self, returning: str | list[str] | Type[ReturnModel]
    ) -> sql.Composable:
        """
        Constructs RETURNING part of query

        Args:
            returning (str | list[str] | Type[ReturnModel]): _description_

        Returns:
            sql.Composable: RETURNING part of query
        """
        returning_part = sql.SQL("RETURNING").format()
        if isinstance(returning, str):
            if returning == "*":
                returning_part = sql.SQL(" ").join(
                    [
                        returning_part,
                        sql.SQL("*"),
                    ]
                )
                return returning_part

            returning_part = sql.SQL(" ").join(
                [
                    returning_part,
                    sql.SQL("{}").format(returning),
                ]
            )
            return returning_part

        if isinstance(returning, list):
            returning_part = sql.SQL(" ").join(
                [
                    returning_part,
                    sql.SQL(", ").join(map(sql.Identifier, returning)),
                ]
            )
            return returning_part

        if issubclass(returning, BaseModel):
            returning_part = sql.SQL(" ").join(
                [
                    returning_part,
                    sql.SQL(", ").join(
                        map(sql.Identifier, returning.__fields__.keys())
                    ),
                ]
            )
            return returning_part
        return sql.SQL("").format()

    def update_one(
        self, data: QueryData, table: str, returning: Optional[str] = None
    ) -> None:
        query = "INSERT INTO table VALUES"

        if returning:
            query += " RETURNING *"

        print(query)

        # cursor = self.run(query=query)
        # cursor.close()

    @overload
    def find_one(
        self, query: Query, params: Optional[QueryParams] = None
    ) -> DbRecord | None:
        ...

    @overload
    def find_one(
        self,
        query: Query,
        params: Optional[QueryParams],
        return_model: Type[ReturnModel],
    ) -> ReturnModel | None:
        ...

    @overload
    def find_one(
        self,
        query: Query,
        params: Optional[QueryParams] = ...,
        *,
        return_model: Type[ReturnModel],
    ) -> ReturnModel | None:
        ...

    def find_one(
        self,
        query: Query,
        params: Optional[QueryParams] = None,
        return_model: Type[ReturnModel] | None = None,
    ) -> ReturnModel | DbRecord | None:
        """
        Retrieves and returns one record from the database.
        Connection and cursor is closed automatically.

        Args:
            query (Query): _description_
            params (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """

        result = None

        if return_model:
            with self.run(
                query=query, params=params, row_factory=class_row(return_model)
            ) as cursor:
                result = cursor.fetchone()
            return result
        with self.run(query=query, params=params) as cursor:
            result = cursor.fetchone()
        return result

    @overload
    def find_all(
        self, query: Query, params: Optional[QueryParams] = None
    ) -> list[dict]:
        ...

    @overload
    def find_all(
        self,
        query: Query,
        params: Optional[QueryParams],
        return_model: Type[ReturnModel],
    ) -> list[ReturnModel]:
        ...

    @overload
    def find_all(
        self,
        query: Query,
        params: Optional[QueryParams] = ...,
        *,
        return_model: Type[ReturnModel],
    ) -> list[ReturnModel]:
        ...

    def find_all(
        self,
        query: Query,
        params: Optional[QueryParams] = None,
        return_model: Type[ReturnModel] | None = None,
    ) -> list[ReturnModel] | list[QueryParams]:
        """
        Retrieves and returns one record from the database.
        Connection and cursor is closed automatically.

        Args:
            query (Query): _description_
            params (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """

        results = []

        if return_model:
            with self.run(
                query=query, params=params, row_factory=class_row(return_model)
            ) as cursor:
                results = cursor.fetchall()
            return results
        with self.run(query=query, params=params) as cursor:
            results = cursor.fetchall()
        return results

    @overload
    def run(self, query: Query, params=None) -> Cursor:
        ...

    @overload
    def run(
        self, query: Query, params=None, *, row_factory: RowFactory[Row]
    ) -> Cursor[Row]:
        ...

    def run(
        self, query: Query, params=None, row_factory: RowFactory[Row] | None = None
    ) -> Cursor[Row] | Cursor:
        """
        Executes a query to the database.
        Attention: Connection must be closed manually with .close()

        Args:
            query (Query): The query to execute.
            params (_type_, optional): _description_. Defaults to None.

        Returns:
            Cursor: _description_
        """

        conn: psycopg.Connection | None = None
        _query: str | None = None

        try:
            with psycopg.connect(
                self._conninfo, row_factory=row_factory or dict_row
            ) as conn:
                if isinstance(query, sql.Composable):
                    _query = query.as_string(conn)
                elif isinstance(query, sql.SQL):
                    _query = query.as_string(conn)
                elif isinstance(query, str):
                    _query = query

                cur = conn.cursor()
                print(_query)
                return cur.execute(query=query, params=params)

        except psycopg.errors.UniqueViolation as error:
            print("vioatl", error.sqlstate, error.pgresult, _query)
            raise

        finally:
            if conn:
                conn.close()
