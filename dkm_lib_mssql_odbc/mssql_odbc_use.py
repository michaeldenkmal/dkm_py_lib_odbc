from dkm_lib_db import db_use
from dkm_lib_db.db_conf import TDbConnGuard, TDbConfGuard
from dkm_lib_mssql_odbc.mssql_odbc_db_driver import MssqlOdbcDriver


def using_db_conn_test() -> TDbConnGuard:
    return db_use.using_db_conn_test(MssqlOdbcDriver())

def using_db_conf_test() -> TDbConfGuard:
    return db_use.using_db_conf_test(MssqlOdbcDriver())

def using_db_conn(offh_ini_path: str) -> TDbConnGuard:
    return db_use.using_db_conn(MssqlOdbcDriver(), offh_ini_path)

def using_db_conf(offh_ini_path: str, login_name: str) -> TDbConfGuard:
    return db_use.using_db_conf(MssqlOdbcDriver(), offh_ini_path, login_name)



