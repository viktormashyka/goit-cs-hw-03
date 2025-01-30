import psycopg2
from contextlib import contextmanager

database = "dbname=postgres user=postgres password=12345 host=localhost port=5434"

@contextmanager
def create_connection(db_file):
    """ create a database connection to a PostgreSQL database """
    conn = None
    try:
        conn = psycopg2.connect(db_file)
        yield conn
    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
