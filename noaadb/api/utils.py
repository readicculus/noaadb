from sqlalchemy.dialects import postgresql

def get_raw_sql(q):
    return q.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
