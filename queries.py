import psycopg2
from sqlalchemy.orm import sessionmaker



# Get queries
from noaadb.models import Images


def get_image(con, im_name):
    qs = "SELECT * FROM images WHERE file_name='%s'" % im_name
    cur = con.cursor()
    cur.execute(qs)
    return cur.fetchall()





# Schema related queries
def get_tables(con, schema):
    cur = con.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'" % schema)
    return cur.fetchall()

def table_exists(con, table_str):

    exists = False
    try:
        cur = con.cursor()
        cur.execute("select exists(select relname from pg_class where relname='" + table_str + "')")
        exists = cur.fetchone()[0]
        cur.close()
    except psycopg2.Error as e:
        print(e)
    return exists

def get_table_col_names(con, table_str):

    col_names = []
    try:
        cur = con.cursor()
        cur.execute("select * from " + table_str + " LIMIT 0")
        for desc in cur.description:
            col_names.append(desc[0])
        cur.close()
    except psycopg2.Error as e:
        print(e)

    return col_names


def image_exists(session, name):
    return session.query(session.query(Images).filter_by(file_name=name).exists()).scalar()