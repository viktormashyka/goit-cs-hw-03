from psycopg2 import Error

from connect import create_connection, database

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        conn.commit()
    except Error as e:
        print(e)

if __name__ == '__main__':        
    sql_create_users_table = """
    DROP TABLE IF EXISTS users CASCADE;
    CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    fullname VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
    );
    """

    sql_create_status_table = """
    DROP TABLE IF EXISTS status CASCADE;
    CREATE TABLE IF NOT EXISTS status (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
    );
    """

    sql_create_tasks_table = """
    DROP TABLE IF EXISTS tasks CASCADE;
    CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    description TEXT,
    status_id INT,
    user_id INT,
    FOREIGN KEY (status_id) REFERENCES status (id)
        ON DELETE SET NULL 
        ON UPDATE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users (id)
        ON DELETE CASCADE 
        ON UPDATE CASCADE
    );
    """

    with create_connection(database) as conn:
        if conn is not None:
						# create users table
            create_table(conn, sql_create_users_table)
						# create status table
            create_table(conn, sql_create_status_table)
						# create tasks table
            create_table(conn, sql_create_tasks_table)
            print("Success! Database connection created!")
        else:
            print("Error! cannot create the database connection.")