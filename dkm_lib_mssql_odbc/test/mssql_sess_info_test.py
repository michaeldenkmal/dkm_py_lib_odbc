import unittest

from dkm_lib_mssql_odbc import mssql_sess_info, mssql_odbc_use
from dkm_lib_mssql_odbc.mssql_odbc_use import using_db_conn_test


class MssqlSessInfoTest(unittest.TestCase):
    def test_get_server_session_info2(self):
        with using_db_conn_test() as conn:
            sess_infos = mssql_sess_info.get_server_session_info2(conn)
            for sess_info in sess_infos:
                if sess_info.DBName and sess_info.DBName.lower()=="ohant":
                    print(sess_info)

    def test_kill_all_sessions_by_dbname(self):
        with mssql_odbc_use.using_db_conn_test () as conn:
            mssql_sess_info.kill_all_sessions_by_dbname(conn, "ohant")

if __name__ == '__main__':
    unittest.main()
