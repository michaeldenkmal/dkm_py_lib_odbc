import unittest

from dkm_lib_mssql_odbc import kk_variable, mssql_odbc_use


class KkVariableTest(unittest.TestCase):
    def test_get_var(self):
        with mssql_odbc_use.using_db_conn_test() as conn:
            w = kk_variable.get_var(conn, "KURZNAME")
            self.assertIsNotNone(w)
            print(w)


if __name__ == '__main__':
    unittest.main()
