from psycopg2 import Error

from connect import create_connection, database

def select_task_by_user_id(conn, user_id):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param user_id:
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM tasks WHERE user_id=%s", (user_id,))
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def select_tasks_by_status(conn, status):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param status:
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM tasks WHERE status_id=(SELECT id FROM status WHERE name=%s)", (status,))
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def update_task_by_task_id(conn, title, description, status_id, task_id):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param title, description, status_id, task_id:
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("UPDATE tasks SET title=%s, description=%s, status_id=%s WHERE id=%s", (title, description, status_id, task_id,))
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def select_users_without_tasks(conn,):
    """
    Query tasks by priority
    :param conn: the Connection object
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM tasks) ORDER BY fullname")
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def add_task_by_user_id(conn, title, description, status_id, user_id):
    """
    Add a task by user ID
    :param conn: the Connection object
    :param title: the title of the task
    :param description: the description of the task
    :param status_id: the status ID of the task
    :param user_id: the user ID associated with the task
    :return: the ID of the newly inserted task
    """
    task_id = None
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO tasks (title, description, status_id, user_id) VALUES (%s, %s, %s, %s) RETURNING id;", (title, description, status_id, user_id))
        conn.commit()
        task_id = cur.fetchone()[0]
    except Error as e:
        print(e)
        conn.rollback()
    finally:
        cur.close()
    return task_id

def select_not_completed_tasks(conn,):
    """
    Query tasks by priority
    :param conn: the Connection object
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM tasks WHERE status_id!=(SELECT id FROM status WHERE name='completed')",)
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def delete_task_by_task_id(conn, task_id):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param task_id:
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM tasks WHERE id=%s", (task_id,))
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def select_users_with_email(conn, partial_email):
    """
    Query users by priority
    :param conn: the Connection object
    :param email:
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM users WHERE email LIKE %s ORDER BY fullname", ('%' + partial_email + '%',))
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def update_user_name_by_task_id(conn, fullname, user_id):
    """
    Query users by priority
    :param conn: the Connection object
    :param fullname, user_id:
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET fullname=%s WHERE id=%s", (fullname, user_id,))
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def select_count_of_tasks_by_status(conn, status):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param status:
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(id) as users FROM tasks WHERE status_id=(SELECT id FROM status WHERE name=%s) GROUP BY status_id", (status,))
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def select_tasks_for_users_with_partial_email(conn, partial_email):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param partial_email:
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM tasks WHERE user_id IN (SELECT id FROM users WHERE email LIKE %s)", ('%' + partial_email + '%',))
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def select_tasks_without_description(conn,):
    """
    Query tasks by priority
    :param conn: the Connection object
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM tasks WHERE description IS NULL",)
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def select_pending_tasks(conn,):
    """
    Query tasks by priority
    :param conn: the Connection object
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM tasks WHERE status_id=(SELECT id FROM status WHERE name='pending')",)
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def select_count_of_users_with_count_of_tasks(conn,):
    """
    Query users by priority
    :param conn: the Connection object
    :return: rows tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(users.id) as users FROM users LEFT JOIN tasks ON users.id = tasks.user_id GROUP BY users.id",)
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

if __name__ == '__main__':
    with create_connection(database) as conn:
        print("\nQuery task by user ID:")
        tasks_by_user_id = select_task_by_user_id(conn, 1)
        print(tasks_by_user_id)

        print("\nQuery tasks by status:")
        tasks_by_status = select_tasks_by_status(conn, 'pending')
        print(tasks_by_status)

        print("\nUpdate task by task ID:")
        update_task_by_task_id(conn, 'New Title', 'New Description', 2, 1)

        print("\nQuery users without tasks:")
        users_without_tasks = select_users_without_tasks(conn)
        print(users_without_tasks)

        print("\nAdd task by user ID:")
        new_task_id = add_task_by_user_id(conn, 'New Task', 'Task Description', 1, 1)
        print(f"New task ID: {new_task_id}")

        print("\nQuery not completed tasks:")
        not_completed_tasks = select_not_completed_tasks(conn)
        print(not_completed_tasks)

        print("\nDelete task by task ID:")
        delete_task_by_task_id(conn, 1)

        print("\nQuery users with partial email:")
        users_with_email = select_users_with_email(conn, 'example')
        print(users_with_email)

        print("\nUpdate user name by task ID:")
        update_user_name_by_task_id(conn, 'New Fullname', 1)

        print("\nQuery count of tasks by status:")
        count_of_tasks_by_status = select_count_of_tasks_by_status(conn, 'pending')
        print(count_of_tasks_by_status)

        print("\nQuery tasks for users with partial email:")
        tasks_for_users_with_partial_email = select_tasks_for_users_with_partial_email(conn, 'example')
        print(tasks_for_users_with_partial_email)

        print("\nQuery tasks without description:")
        tasks_without_description = select_tasks_without_description(conn)
        print(tasks_without_description)

        print("\nQuery pending tasks:")
        pending_tasks = select_pending_tasks(conn)
        print(pending_tasks)

        print("\nQuery count of users with count of tasks:")
        count_of_users_with_count_of_tasks = select_count_of_users_with_count_of_tasks(conn)
        print(count_of_users_with_count_of_tasks)
