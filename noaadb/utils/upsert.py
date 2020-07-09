import warnings

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.inspection import inspect

def upsert(session, schema, table_name, records=[]):

    metadata = MetaData(schema=schema)
    # metadata.bind = engine

    table = Table(table_name, metadata, schema=schema, autoload=True)

    # get list of fields making up primary key
    primary_keys = [key.name for key in inspect(table).primary_key]

    # assemble base statement
    stmt = postgresql.insert(table).values(records)

    # define dict of non-primary keys for updating
    update_dict = {
        c.name: c
        for c in stmt.excluded
        if not c.primary_key
    }

    # cover case when all columns in table comprise a primary key
    # in which case, upsert is identical to 'on conflict do nothing.
    if update_dict == {}:
        warnings.warn('no updateable columns found for table')
        # we still wanna insert without errors
        # insert_ignore(table_name, records)
        return None


    # assemble new statement with 'on conflict do update' clause
    update_stmt = stmt.on_conflict_do_update(
        index_elements=primary_keys,
        set_=update_dict,
    )

    res = session.execute(update_stmt)
    return res
    # # execute
    # with engine.connect() as conn:
    #     result = conn.execute(update_stmt)
    #     return result