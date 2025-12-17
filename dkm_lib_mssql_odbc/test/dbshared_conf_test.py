import unittest

from dkm_lib_mssql_odbc import dbshared_conf



class DbSharedConfTest(unittest.TestCase):

    def test_get_next_sq_with_bin(self):
        with dbshared_conf.using_dkm_conf_test() as conf:
            new_id = dbshared_conf.get_next_sq_with_bin(conf, "eab_gpartner")
            print(new_id)
            self.assertTrue(new_id is not None)