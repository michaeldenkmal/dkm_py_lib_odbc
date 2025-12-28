import unittest

from dkm_lib_mssql_odbc import dbshared_conf
from dkm_lib_mssql_odbc.mssql_odbc_use import using_db_conf_test


class DbSharedConfTest(unittest.TestCase):

    def test_get_next_sq_with_bin(self):
        with using_db_conf_test()  as conf:
            new_id = dbshared_conf.get_next_sq_with_bin(conf, "eab_gpartner")
            print(new_id)
            self.assertTrue(new_id is not None)


if __name__ == '__main__':
    unittest.main()
