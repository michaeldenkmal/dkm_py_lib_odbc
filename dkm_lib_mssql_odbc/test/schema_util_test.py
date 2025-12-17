import unittest

from dkm_lib_mssql_odbc import schema_util
from dkm_lib_odbc import odbc_util


class SchemaUtilTest(unittest.TestCase):
    def test_table_exists(self):
        with odbc_util.using_odbc_conn_test() as conn:
            exists = schema_util.table_exists(conn,"kk_variable")
            self.assertTrue(exists)
            exists = schema_util.table_exists(conn,"kk_variable1")
            self.assertFalse(exists)


if __name__ == '__main__':
    unittest.main()
