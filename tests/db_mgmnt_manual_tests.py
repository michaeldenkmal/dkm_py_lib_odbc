from dkm_lib_mssql_odbc import mssql_dbmgmnt_util
from dkm_lib_odbc import odbc_util


def test_set_db_offline_online():
    with odbc_util.using_odbc_conn_test() as conn:
        db_name="ohemm_foegeb"
        mssql_dbmgmnt_util.set_db_offline(conn, db_name)


test_set_db_offline_online()

def test_get_specific_database_infos():
    db_name ="ohemm_foegeb"
    with odbc_util.using_odbc_conn_test() as conn:
        db_info = mssql_dbmgmnt_util.get_specific_database_infos(conn, db_name)
        print(db_info)

#test_get_specific_database_infos()




