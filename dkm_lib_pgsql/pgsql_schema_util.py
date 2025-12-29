from operator import truediv
from typing import Any

from psycopg import Connection
from psycopg.sql import SQL

def cre_table_if_not_exists_fp(conn:Connection[Any], table_name:str, full_file_path:str, schema:str="public") -> bool:
    """
    Rückgabe true, wenn neu angelegt
    """
    if not table_exists(conn, table_name, schema):
        with open(full_file_path,"r", encoding="utf8") as f:
            sql = f.read()
        conn.execute(SQL(sql))
        return True
    return False


def cre_table_if_not_exists(conn:Connection[Any], table_name:str, sql:str, schema:str="public") -> bool:
    """
    Rückgabe true, wenn neu angelegt
    """
    if not table_exists(conn, table_name, schema):
        conn.execute(SQL(sql))
        return True
    return False


def table_exists(conn:Connection[Any], table_name: str, schema: str = "public") -> bool:
    sql = SQL("SELECT to_regclass(%s)")
    full_name = f"{schema}.{table_name}"

    with conn.cursor() as cur:
        cur.execute(sql, (full_name,))
        return cur.fetchone()[0] is not None
