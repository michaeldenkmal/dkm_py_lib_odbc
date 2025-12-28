# import traceback
# from dataclasses import dataclass
# from typing import List, Dict, Any, Literal, Callable, Optional
#
# #import pyodbc
#
# from dkm_lib_odbc.odbc_types import ODBC_DB_TYPE
# from dkm_lib_odbc.oh_ini_db_file import TOhIniDBFile, get_test_offh_ini_file_path
#
#
# @dataclass
# class TOdbcConnInfo:
#     conn_str:str
#     odbc_db_type:ODBC_DB_TYPE
#
#
# @dataclass
# class TDbConf(object):
#     #def __init__(self, conn:pyodbc.Connection, i_bin:int, binfakt:int, login_name:str):
#     conn:Optional[pyodbc.Connection]
#     i_bin:int
#     binfakt:int
#     login_name:str
#
#
# class TDbConfGuard(object):
#     def __init__(self, conn_info: TOdbcConnInfo, login_name: str, bin_fakt: int, i_bin: int):
#         self.conn_info = conn_info
#         self.dbconf = TDbConf(
#             conn=None,
#             login_name=login_name,
#             binfakt=bin_fakt,
#             i_bin=i_bin
#         )
#
#     def __enter__(self) -> TDbConf:
#         # connection erzeugen
#         self.dbconf.conn = odbc_connect(self.conn_info.conn_str)
#         return self.dbconf
#
#     def __exit__(self, exc_type, exc_value, tb):
#         if exc_type is not None:
#             traceback.print_exception(exc_type, exc_value, tb)
#         if self.dbconf.conn:
#             self.dbconf.conn.close()
#         return True
#
#
# class TOdbcConn(object):
#     def __init__(self, conn_info: TOdbcConnInfo):
#         self.conn_info = conn_info
#         self.conn: pyodbc.Connection | None = None
#
#     def __enter__(self) -> pyodbc.Connection:
#         # connection erzeugen
#         self.conn = odbc_connect(self.conn_info.conn_str)
#         return self.conn
#
#     def __exit__(self, exc_type, exc_value, tb):
#         if exc_type is not None:
#             traceback.print_exception(exc_type, exc_value, tb)
#         if self.conn:
#             self.conn.close()
#         return True
#
#
# def odbc_connect(conn_str:str):
#     try:
#         return pyodbc.connect(conn_str)
#     except pyodbc.InterfaceError as e:
#         raise Exception(f"Fehler beim verbindern zu Datenbank {conn_str}:{e}")
#
#
# def create_cursor(conn: pyodbc.Connection):
#     # pyodbc kennt as_dict nicht, wir liefern dict-Rows in iterQuery
#     return conn.cursor()
#
#
# def iter_query(conn:pyodbc.Connection, sql, fn_on_row_found:Callable[[Dict],bool], *params):
#     """
#     :param conn:
#     :param sql:
#     :param fn_on_row_found: function(row: dict) -> bool
#     :param params:
#     :return:
#     """
#     cursor = create_cursor(conn)
#     sql_exec = _convert_paramstyle(sql)
#     try:
#         if params:
#             cursor.execute(sql_exec, params)
#         else:
#             cursor.execute(sql_exec)
#
#         # Spaltennamen holen
#         col_names = [col[0] for col in cursor.description] if cursor.description else None
#
#         for row in cursor:
#             if col_names is not None:
#                 row_map = {name: row[i] for i, name in enumerate(col_names)}
#             else:
#                 # Fallback, sollte praktisch nie passieren
#                 row_map = {str(i): v for i, v in enumerate(row)}
#
#             if not fn_on_row_found(row_map):
#                 break
#     except pyodbc.ProgrammingError as err:
#         raise Exception("%s bei sql=%s params=%s" % (err, sql, params))
#     except Exception as err:
#         raise Exception("%s bei sql=%s params=%s" % (err, sql, params))
#
#
# def get_dict_rows(conn:pyodbc.Connection, sql:str, *params) -> List[Dict[str, Any]]:
#     assert isinstance(sql, str)
#
#     rows: list[Dict[str, Any]] = []
#
#     def on_row(row):
#         rows.append(row)
#         return True
#
#     iter_query(conn, sql, on_row, *params)
#     return rows
#
#
# def _convert_paramstyle(sql: str) -> str:
#     # Annahme: %s wird nur als Param-Platzhalter verwendet
#     return sql.replace("%s", "?")
#
#
# def exec_query(conn:pyodbc.Connection, sql:str, *params):
#     cursor = create_cursor(conn)
#     sql_exec = _convert_paramstyle(sql)
#     try:
#         if params:
#             return cursor.execute(sql_exec, params)
#         else:
#             return cursor.execute(sql_exec)
#     except pyodbc.ProgrammingError as err:
#         raise Exception("%s bei sql=%s params=%s" % (err, sql, params))
#     except AttributeError as err:
#         raise Exception("%s bei sql=%s params=%s" % (err, sql, params))
#     except ValueError as err:
#         raise Exception("%s bei sql=%s params=%s" % (err, sql, params))
#     except pyodbc.OperationalError as err:
#         raise Exception("%s bei sql=%s params=%s" % (err, sql, params))
#
#
# def get_cursor_row_count(cursor):
#     return cursor.rowcount
#
#
# def rows_exists(conn, sql, *params):
#     found: list[int] = []
#
#     # noinspection PyUnusedLocal
#     def on_row(row):
#         found.append(1)
#         return True
#
#     iter_query(conn, sql, on_row, *params)
#     return len(found) > 0
#
#
# def query_to_raw_list(conn, sql, fn_on_row, *params):
#     ret = []
#
#     def on_row(row):
#         entry = fn_on_row(row)
#         if entry is None:
#             return True
#         if entry is False:
#             return False
#         ret.append(entry)
#         return True
#
#     # hier war vorher ein Bug: params statt *params
#     iter_query(conn, sql, on_row, *params)
#     return ret
#
# def create_conn_info_from_ini_file_path(offh_ini_path: str) -> TOdbcConnInfo:
#     offh_ini = TOhIniDBFile(offh_ini_path)
#     return create_conn_info_from_ini_file(offh_ini)
#
#
# def create_conn_info_from_ini_file(oh_ini_db_file: TOhIniDBFile) -> TOdbcConnInfo:
#     #return TConnInfo(ohIniDBFile.host, ohIniDBFile.userName, ohIniDBFile.password, ohIniDBFile.database)
#     conn_str = build_odbc_conn_str(server=oh_ini_db_file.host,
#                                    database=oh_ini_db_file.database,
#                                    password=oh_ini_db_file.password,
#                                    user=oh_ini_db_file.userName,
#                                    db_type=oh_ini_db_file.odbc_db_type
#                                    )
#     return TOdbcConnInfo(
#         conn_str=conn_str,
#         odbc_db_type=oh_ini_db_file.odbc_db_type
#     )
#
#
# def using_odbc_conn_test() -> TOdbcConn:
#     return using_odbc_conn(get_test_offh_ini_file_path())
#
# def using_odbc_conf_test() -> TDbConfGuard:
#     return using_odbc_conf(get_test_offh_ini_file_path(), login_name="michael")
#
#
# def using_odbc_conn(offh_ini_path: str) -> TOdbcConn:
#     db_ini_file_obj = TOhIniDBFile(offh_ini_path)
#     #conn_info = createConnInfoFromIniFile(db_ini_file_obj)
#     conn_info = create_conn_info_from_ini_file(db_ini_file_obj)
#     return TOdbcConn(conn_info)
#
#
# def using_odbc_conf(offh_ini_path: str, login_name: str) -> TDbConfGuard:
#     db_ini_file_obj = TOhIniDBFile(offh_ini_path)
#     conn_info = create_conn_info_from_ini_file(db_ini_file_obj)
#     return TDbConfGuard(
#         conn_info=conn_info,
#         login_name=login_name,
#         bin_fakt=db_ini_file_obj.bin_fakt,
#         i_bin=db_ini_file_obj.bin
#     )
#
#
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
#
#
# def get_column_names_4_table(conn, table_name):
#     names: list[str] = []
#
#     def iter_rs(row):
#         names.append(row['column_name'])
#         return True
#
#     def exec_conn(conn1):
#         sql = """
#         select cols.name column_name from sys.all_columns cols
#         inner join sys.all_objects tab on cols.object_id = tab.object_id
#         where tab.name= %s
#         """
#         iter_query(conn1, sql, iter_rs, table_name)
#
#     exec_conn(conn)
#     return names
#
#
# def get_long_val_from_row(row, field_name):
#     """
#     wenn wert None ist dann None, sonst long value
#     :param row:
#     :param field_name:
#     :return:
#     """
#     if field_name in row:
#         ret = row[field_name]
#         if ret is None:
#             return None
#         return int(ret)
#     else:
#         raise Exception("Konnte fieldName %s nicht in row %s finden" % (field_name, row))
