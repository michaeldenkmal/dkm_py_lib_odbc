from typing import Optional, Sequence, Any, Iterable

import pyodbc

from dkm_lib_db.db_driver import DbCursor, DbConn, DbDriver
from dkm_lib_db.db_types import DbConnInfo


class MssqlOdbcDbCursor(DbCursor):

    def __init__(self, real_cursor:pyodbc.Cursor):
        self.real_cursor = real_cursor


    def get_description(self) -> Optional[Sequence[Sequence[Any]]]:
        return self.real_cursor.description

    def get_rowcount(self) -> int:
        return self.real_cursor.rowcount

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        if params:
            return self.real_cursor.execute(sql, params)
        else:
            return self.real_cursor.execute(sql)

    def __iter__(self) -> Iterable[Sequence[Any]]:
        return self.real_cursor.__iter__()

    def close(self) -> None:
        self.real_cursor.close()



class MssqlOdbcDbConn(DbConn):

    def __init__(self, real_conn:pyodbc.Connection):
        self.real_conn = real_conn

    def cursor(self) -> DbCursor:
        return MssqlOdbcDbCursor(self.real_conn.cursor())

    def commit(self) -> None:
        self.real_conn.commit()

    def rollback(self) -> None:
        self.real_conn.rollback()

    def close(self) -> None:
        self.real_conn.close()

    def normalize_sql(self, sql:str) -> str:
        return sql.replace("%s", "?")


def build_odbc_mssql_conn_str(server:str, user:str, password:str, database:str)->str:
    """
    pyodbc-Verbindung zu SQL Server herstellen.
    Treibername ggf. anpassen (z.B. "ODBC Driver 17 for SQL Server").
    """
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
    )
    if user:
        conn_str += f"UID={user};PWD={password};"
    else:
        # Falls Windows-Auth verwendet wird, user/password leer lassen
        conn_str += "Trusted_Connection=yes;"

    # TrustServerCertificate kannst du ggf. hart setzen oder aus ini holen
    conn_str += "TrustServerCertificate=yes;"
    return conn_str



class MssqlOdbcDriver(DbDriver):
    def connect(self, conn_info:DbConnInfo) -> DbConn:
        try:
            conn_str = build_odbc_mssql_conn_str(
                server = conn_info.host
                ,user = conn_info.user
                ,password = conn_info.pwd
                ,database = conn_info.db_name)
            real_conn= pyodbc.connect(conn_str)
            return MssqlOdbcDbConn(real_conn)# type: ignore[return-value]
        except pyodbc.InterfaceError as e:
            raise Exception(f"Fehler beim Verbinden zu DB: {e}")

