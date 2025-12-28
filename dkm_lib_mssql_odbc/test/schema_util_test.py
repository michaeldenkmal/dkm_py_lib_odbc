import unittest

from dkm_lib_mssql_odbc import mssql_schema_util, mssql_odbc_use


class SchemaUtilTest(unittest.TestCase):
    def test_table_exists(self):
        with mssql_odbc_use.using_db_conn_test() as conn:
            exists = mssql_schema_util.table_exists(conn,"kk_variable")
            self.assertTrue(exists)
            exists = mssql_schema_util.table_exists(conn,"kk_variable1")
            self.assertFalse(exists)


if __name__ == '__main__':
    unittest.main()
