import unittest

from dkm_lib_mssql_odbc import mssql_sess_info
from dkm_lib_odbc import odbc_util


class MssqlSessInfoTest(unittest.TestCase):
    def test_get_server_session_info2(self):
        with odbc_util.using_odbc_conn_test() as conn:
            sess_infos = mssql_sess_info.get_server_session_info2(conn)
            for sess_info in sess_infos:
                if sess_info.DBName and sess_info.DBName.lower()=="ohant":
                    print(sess_info)

    def test_kill_all_sessions_by_dbname(self):
        with odbc_util.using_odbc_conn_test() as conn:
            mssql_sess_info.kill_all_sessions_by_dbname(conn, "ohant")

if __name__ == '__main__':
    unittest.main()
