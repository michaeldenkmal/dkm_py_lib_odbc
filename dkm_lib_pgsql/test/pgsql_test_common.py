import os.path

from dkm_lib_pgsql.pgsql_use import using_db_conn


def get_test_ini_file() -> str:
    mypath = os.path.dirname(__file__)
    return os.path.join(mypath, "pgsql_test.ini")


def use_test_conn():
    return using_db_conn(get_test_ini_file())
