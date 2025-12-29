from __future__ import annotations

from typing import Optional, Sequence, Any, Iterable

import psycopg
from psycopg import Connection, Cursor

from dkm_lib_db.db_driver import DbCursor, DbConn, DbDriver
from dkm_lib_db.db_types import DbConnInfo


class PgsqlDbCursor(DbCursor):
    def __init__(self, real_cursor: Cursor[Any]):
        self.real_cursor = real_cursor

    def get_description(self) -> Optional[Sequence[Sequence[Any]]]:
        # psycopg cursor.description ist vorhanden, Typen sind anders als bei pyodbc,
        # aber für dein Interface passt "Sequence[...]"
        return self.real_cursor.description  # type: ignore[return-value]

    def get_rowcount(self) -> int:
        # rowcount ist bei SELECT teils -1 bis fetch, bei DML sinnvoll
        return int(self.real_cursor.rowcount)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        if params:
            return self.real_cursor.execute(sql, params)
        else:
            return self.real_cursor.execute(sql)

    def __iter__(self) -> Iterable[Sequence[Any]]:
        # psycopg Cursor ist iterierbar (liefert tuples)
        return self.real_cursor.__iter__()  # type: ignore[return-value]

    def close(self) -> None:
        self.real_cursor.close()


class PgsqlDbConn(DbConn):
    def __init__(self, real_conn: Connection[Any]):
        self.real_conn = real_conn

    def cursor(self) -> DbCursor:
        return PgsqlDbCursor(self.real_conn.cursor())

    def commit(self) -> None:
        self.real_conn.commit()

    def rollback(self) -> None:
        self.real_conn.rollback()

    def close(self) -> None:
        self.real_conn.close()

    def normalize_sql(self, sql: str) -> str:
        # Wenn dein Framework intern "%s" verwendet: bei PostgreSQL passt das direkt.
        # Falls du irgendwann intern "?" verwendest, würdest du hier "? -> %s" machen.
        return sql

    def get_real_conn(self) -> Any:
        return self.real_conn


def build_postgres_conn_str(
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 5432,
) -> str:
    # DSN-String; psycopg akzeptiert auch keyword-args, aber DSN passt gut zu deinem Stil.
    # Achtung: bei speziellen Zeichen im Password wäre URL-encoding notwendig,
    # daher lieber DSN-Style statt "postgresql://..."
    dsn = f"host={host} port={port} dbname={database}"
    if user:
        dsn += f" user={user}"
    if password:
        dsn += f" password={password}"
    return dsn


class PgsqlDriver(DbDriver):
    def connect(self, conn_info: DbConnInfo) -> DbConn:
        try:
            # Falls DbConnInfo kein port hat: default 5432
            port = getattr(conn_info, "port", 5432)

            dsn = build_postgres_conn_str(
                host=conn_info.host,
                user=conn_info.user,
                password=conn_info.pwd,
                database=conn_info.db_name,
                port=int(port),
            )

            self.real_conn = psycopg.connect(dsn)
            return PgsqlDbConn(self.real_conn)  # type: ignore[return-value]

        except psycopg.OperationalError as e:
            raise Exception(f"Fehler beim Verbinden zu PostgreSQL: {e}") from e
