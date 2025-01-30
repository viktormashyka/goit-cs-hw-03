from psycopg2 import Error
from faker import Faker
fake = Faker()

from connect import create_connection, database

def create_user(conn, user):
    """
    Create a new user into the users table
    :param conn:
    :param user:
    :return: user id
    """
    sql = '''
    INSERT INTO users(fullname, email) VALUES(%s, %s);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, user)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()

    return cur.lastrowid

def create_status(conn, status):
    """
    Create a new status
    :param conn:
    :param status:
    :return:
    """

    sql = '''
    INSERT INTO status(name) VALUES(%s);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, status)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()

    return cur.lastrowid

def create_task(conn, task):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """

    sql = '''
    INSERT INTO tasks(title, description, status_id, user_id) VALUES(%s, %s, %s, %s);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, task)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()

    return cur.lastrowid

if __name__ == '__main__':
    with create_connection(database) as conn:
        # create 10 new users
        user_ids = []
        for _ in range(10):
            user = (fake.name(), fake.email())
            user_id = create_user(conn, user)
            user_ids.append(user_id)
            print(user_id)

        # create status
        status = (fake.random_element(elements=('new', 'in progress', 'completed')),)
        status_id = create_status(conn, status)
        print(status_id)

        # create 10 tasks
        for _ in range(10):
            task = (fake.sentence(), fake.text(), status_id, fake.random_element(elements=user_ids))
            task_id = create_task(conn, task)
            print(task_id)