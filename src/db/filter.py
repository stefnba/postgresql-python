from psycopg import sql

from db.types import FilterObject


def construct_filter(filter: FilterObject) -> sql.Composable:
    operator = filter["operator"]
    column = filter["column"]
    value = filter["value"]

    if operator == "EQUAL":
        if not value:
            raise ValueError("Filter value not defined.")
        return sql.SQL("{column} = {value}").format(
            column=sql.Identifier(column), value=sql.Literal(value)
        )
    if operator == "IS_NULL":
        return sql.SQL("{column} IS NULL").format(
            column=sql.Identifier(column), value=sql.Literal(value)
        )
    if operator == "HIGHER":
        if not value:
            raise ValueError("Filter value not defined.")
        return sql.SQL("{column} > {value}").format(
            column=sql.Identifier(column), value=sql.Literal(value)
        )
    if operator == "HIGHER_EQUAL":
        if not value:
            raise ValueError("Filter value not defined.")
        return sql.SQL("{column} >= {value}").format(
            column=sql.Identifier(column), value=sql.Literal(value)
        )
    if operator == "LOWER":
        if not value:
            raise ValueError("Filter value not defined.")
        return sql.SQL("{column} < {value}").format(
            column=sql.Identifier(column), value=sql.Literal(value)
        )
    if operator == "LOWER_EQUAL":
        if not value:
            raise ValueError("Filter value not defined.")
        return sql.SQL("{column} <= {value}").format(
            column=sql.Identifier(column), value=sql.Literal(value)
        )
    if operator == "IN":
        if not value:
            raise ValueError("Filter value not defined.")
        if not isinstance(value, list):
            raise ValueError("Filter value must be of type list for IN operator.")

        value_list = sql.SQL(", ").join([sql.Literal(v) for v in value])
        return sql.SQL("{column} = [{value}]").format(
            column=sql.Identifier(column), value=value_list
        )

    raise ValueError("Filter Operator not provided.")


# def filter_operator(operator: FilterOperator) -> sql.SQL:
#     if operator == "EQUAL":
#         return sql.SQL("=")
#     if operator == "EQUAL":
#         return sql.SQL("=")
#     raise Exception("Filter Operator not provided.")
