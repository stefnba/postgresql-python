from db.query.add import AddQuery
from db.query.find import FindQuery
from db.query.run import RunQuery
from db.query.update import UpdateQuery


class PgQuery(AddQuery, FindQuery, RunQuery, UpdateQuery):
    pass
