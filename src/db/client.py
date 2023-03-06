from typing import Iterable, Optional, Type, overload

import psycopg
from psycopg import sql
from psycopg.abc import Params, Query
from psycopg.cursor import Cursor
from psycopg.rows import Row, RowFactory, dict_row
from pydantic import BaseModel

from db.filter import construct_filter
from db.record import PgRecord
from db.types import (
    ConnectionInfo,
    ConnectionModel,
    DbModelRecord,
    FilterParams,
    QueryData,
    QueryDataMultiple,
    QueryParams,
)


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

    def run(self, query: Query, params: Optional[QueryParams] = None):
        """
        Ececute any query to the database.

        Args:
            query (Query): _description_
            params (Optional[QueryParams], optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        cursor = self._execute(query=query, params=params)
        return PgRecord(cursor=cursor)

    def find(self, query: Query, params: Optional[QueryParams] = None, filter=None):
        """

        Args:
            query (Query): _description_
            params (QueryParams): _description_
            filter (): _description_

        Returns:
            _type_: _description_
        """

        if filter:
            pass

        cursor = self._execute(query=query, params=params)
        return PgRecord(cursor=cursor)

    def update(
        self,
        data: QueryData,
        table: str,
        filter: FilterParams,
        returning: Optional[str | list[str]] = None,
    ):
        """

        Args:
            query (Query): _description_
            params (QueryParams): _description_
            filter (): _description_

        UPDATE
            "currencies" AS t
        SET
            "val" = v. "val",
            "msg" = v. "msg"
        FROM (
            VALUES(1, 123, 'hello'),
                (2,
                    456,
                    'world!')) AS v ("id",
                "val",
                "msg")
        WHERE
            v.id = t.id

        Returns:
            _type_: _description_
        """

        if isinstance(data, dict):
            fields = data.keys()
        elif isinstance(data, BaseModel):
            fields = data.dict(exclude_unset=True).keys()
            data = data.dict(exclude_unset=True)

        # fields and values
        set_part = sql.SQL(", ").join(
            [
                sql.SQL("{column}={value}").format(
                    column=sql.Identifier(field), value=sql.Placeholder(field)
                )
                for field in fields
            ]
        )

        # filter
        filter_part = self.__construct_filter_part(filter)

        query = sql.SQL("UPDATE {table} SET {set_part}").format(
            table=sql.Identifier(table), set_part=set_part
        )
        query = sql.SQL(" ").join([query, filter_part])

        if returning:
            returning_part = self.__construct_returning_part(returning)
            # concetenate query and returning_part
            query = sql.SQL(" ").join([query, returning_part])

        cursor = self._execute(query=query, params=data)
        return PgRecord(cursor=cursor)

    def add(
        self,
        data: QueryData | QueryDataMultiple,
        table: str,
        returning: str | list[str],
        conflict: Optional[str] = None,
    ):
        """

        Args:
            query (Query): _description_
            params (QueryParams): _description_
            filter (): _description_

        Returns:
            _type_: _description_
        """
        fields: list[str] = []
        data_record = None
        data_records = None
        is_multi = False

        # single data record
        if isinstance(data, dict):
            fields = list(data.keys())
            data_record = data
        elif isinstance(data, BaseModel):
            fields = list(data.dict().keys())
            data_record = data.dict()
        # multiple data records
        elif isinstance(data, list):
            is_multi = True
            fields = list(data[0].dict().keys())
            data_records = [a.dict() for a in data]

        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(map(sql.Identifier, fields)),
            values=sql.SQL(", ").join(map(sql.Placeholder, fields)),
        )

        # returning
        if returning:
            returning_part = self.__construct_returning_part(returning)
            # concetenate query and returning_part
            query = sql.SQL(" ").join([query, returning_part])

        # conflict
        if conflict:
            conflict_part = self.__construct_conflict_part(conflict)
            query = sql.SQL(" ").join([query, conflict_part])

        if is_multi:
            cursor = self._execute(query=query, params_seq=data_records)
        else:
            cursor = self._execute(query=query, params=data_record)
        return PgRecord(cursor=cursor)

    def __construct_conflict_part(self, conflict: str) -> sql.Composable:
        """
        Constructs ON CONFLICT part of query

        Args:
            conflict (str): _description_

        Returns:
            sql.Composable: ON CONFLICT part of query
        """
        conflict_part = sql.SQL("ON CONFLICT").format()
        if isinstance(conflict, str):
            conflict_part = sql.SQL(" ").join(
                [
                    conflict_part,
                    sql.SQL("*"),
                ]
            )
            return conflict_part

        return conflict_part

    def __construct_returning_part(
        self, returning: str | list[str] | Type[DbModelRecord]
    ) -> sql.Composable:
        """
        Constructs RETURNING part of query

        Args:
            returning (str | list[str] | Type[DbModelRecord]): _description_

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

    def __construct_filter_part(self, filter: FilterParams) -> sql.Composable:
        filter_part = sql.SQL("WHERE").format()

        if isinstance(filter, str):
            filter_part = sql.SQL(" ").join([filter_part, sql.Literal(filter)])
            return filter_part

        if isinstance(filter, list):
            filter_part = sql.SQL(" ").join(
                [
                    filter_part,
                    sql.SQL(" AND ").join(
                        [
                            construct_filter(f)
                            # sql.SQL("{column}{operator}{value}").format(
                            #     column=sql.Identifier(f["column"]),
                            #     operator=sql.SQL("="),
                            #     value=sql.Literal(f["value"]),
                            # )
                            for f in filter
                        ]
                    ),
                ]
            )
            return filter_part

        return filter_part

    @overload
    def _execute(
        self,
        query: Query,
        params=None,
        *,
        params_seq: Optional[Iterable[Params]] = None,
    ) -> Cursor:
        ...

    @overload
    def _execute(
        self,
        query: Query,
        params=None,
        *,
        row_factory: RowFactory[Row],
        params_seq: Optional[Iterable[Params]] = None,
    ) -> Cursor[Row]:
        ...

    def _execute(
        self,
        query: Query,
        params=None,
        row_factory: RowFactory[Row] | None = None,
        params_seq: Optional[Iterable[Params]] = None,
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

                print(_query)
                cur = conn.cursor()

                cur._query

                # execute many for multiple add query
                if params_seq:
                    cur.executemany(query=query, params_seq=params_seq, returning=False)
                    return cur

                return cur.execute(query=query, params=params)

        except psycopg.errors.UniqueViolation as error:
            print("vioatl", error.sqlstate, error.pgresult, _query)
            raise

        finally:
            if conn:
                conn.close()
