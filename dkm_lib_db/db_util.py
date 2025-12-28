from typing import Callable, Dict, List, Any

from dkm_lib_db.db_driver import DbConn, DbCursor


def create_cursor(conn: DbConn) -> DbCursor:
    # pyodbc kennt as_dict nicht, wir liefern dict-Rows in iterQuery
    return conn.cursor()


def iter_query(conn:DbConn, sql:str, fn_on_row_found:Callable[[Dict],bool], *params):
    """
    :param conn:
    :param sql:
    :param fn_on_row_found: function(row: dict) -> bool
    :param params:
    :return:
    """
    cursor = create_cursor(conn)
    sql_exec = _convert_paramstyle(sql)
    try:
        if params:
            cursor.execute(sql_exec, params)
        else:
            cursor.execute(sql_exec)

        # Spaltennamen holen
        col_names = [col[0] for col in cursor.get_description()] if cursor.get_description() else None

        for row in cursor:
            if col_names is not None:
                row_map = {name: row[i] for i, name in enumerate(col_names)}
            else:
                # Fallback, sollte praktisch nie passieren
                row_map = {str(i): v for i, v in enumerate(row)}

            if not fn_on_row_found(row_map):
                break
    except Exception as err:
        raise Exception("%s bei sql=%s params=%s" % (err, sql_exec, params))


def get_dict_rows(conn:DbConn, sql:str, *params) -> List[Dict[str, Any]]:
    assert isinstance(sql, str)

    rows: list[Dict[str, Any]] = []

    def on_row(row):
        rows.append(row)
        return True

    iter_query(conn, sql, on_row, *params)
    return rows


def _convert_paramstyle(sql: str) -> str:
    # Annahme: %s wird nur als Param-Platzhalter verwendet
    return sql.replace("%s", "?")


def exec_query(conn:DbConn, sql:str, *params):
    cursor = create_cursor(conn)
    sql_exec = conn.normalize_sql(sql)
    try:
        if params:
            return cursor.execute(sql_exec, params)
        else:
            return cursor.execute(sql_exec)
    except Exception as err:
        raise Exception("%s bei sql=%s params=%s" % (err, sql, params))


def get_cursor_row_count(cursor:DbCursor):
    return cursor.get_rowcount()


def rows_exists(conn, sql, *params):
    found: list[int] = []

    # noinspection PyUnusedLocal
    def on_row(row):
        found.append(1)
        return True

    iter_query(conn, sql, on_row, *params)
    return len(found) > 0


def query_to_raw_list(conn, sql, fn_on_row, *params):
    ret = []

    def on_row(row):
        entry = fn_on_row(row)
        if entry is None:
            return True
        if entry is False:
            return False
        ret.append(entry)
        return True

    # hier war vorher ein Bug: params statt *params
    iter_query(conn, sql, on_row, *params)
    return ret

# def build_odbc_conn_str(server:str, user:str, password:str, database:str, db_type:ODBC_DB_TYPE):
#     """
#     hier müssen neue ODBC Treiber hinzugefügt werden
#     """
#     if db_type=="mssql":
#         return build_odbc_mssql_conn_str(
#             server,user, password,database
#         )
#     else:
#         raise Exception(f"build odbc conn str ist nicht implementiert für {db_type}")
#
#
#
# def build_odbc_mssql_conn_str(server:str, user:str, password:str, database:str)->str:
#     """
#     pyodbc-Verbindung zu SQL Server herstellen.
#     Treibername ggf. anpassen (z.B. "ODBC Driver 17 for SQL Server").
#     """
#     conn_str = (
#         "DRIVER={ODBC Driver 18 for SQL Server};"
#         f"SERVER={server};"
#         f"DATABASE={database};"
#     )
#     if user:
#         conn_str += f"UID={user};PWD={password};"
#     else:
#         # Falls Windows-Auth verwendet wird, user/password leer lassen
#         conn_str += "Trusted_Connection=yes;"
#
#     # TrustServerCertificate kannst du ggf. hart setzen oder aus ini holen
#     conn_str += "TrustServerCertificate=yes;"
#     return conn_str


def get_column_names_4_table(conn:DbConn, table_name):
    names: list[str] = []

    def iter_rs(row):
        names.append(row['column_name'])
        return True

    def exec_conn(conn1):
        sql = """
        select cols.name column_name from sys.all_columns cols
        inner join sys.all_objects tab on cols.object_id = tab.object_id
        where tab.name= %s
        """
        iter_query(conn1, sql, iter_rs, table_name)

    exec_conn(conn)
    return names


def get_long_val_from_row(row, field_name):
    """
    wenn wert None ist dann None, sonst long value
    :param row:
    :param field_name:
    :return:
    """
    if field_name in row:
        ret = row[field_name]
        if ret is None:
            return None
        return int(ret)
    else:
        raise Exception("Konnte fieldName %s nicht in row %s finden" % (field_name, row))
