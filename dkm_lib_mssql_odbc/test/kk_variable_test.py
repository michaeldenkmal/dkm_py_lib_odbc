import unittest

from dkm_lib_mssql_odbc import kk_variable
from dkm_lib_odbc import odbc_util


class KkVariableTest(unittest.TestCase):
    def test_get_var(self):
        with odbc_util.using_odbc_conn_test() as conn:
            w = kk_variable.get_var(conn, "KURZNAME")
            self.assertIsNotNone(w)
            print(w)


if __name__ == '__main__':
    unittest.main()
