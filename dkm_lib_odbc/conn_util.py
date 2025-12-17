import pyodbc # type: ignore

def conn_set_autocommit(conn: pyodbc.Connection, value: bool):
    conn.autocommit = value


def conn_close(conn: pyodbc.Connection):
    conn.close()


def conn_commit(conn: pyodbc.Connection):
    conn.commit()


def conn_rollback(conn: pyodbc.Connection):
    conn.rollback()



