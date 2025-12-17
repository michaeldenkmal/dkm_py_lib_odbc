import unittest

from dkm_lib_mssql_odbc.mssql_dbmgmnt_util import get_all_database_infos
from dkm_lib_odbc import odbc_util


class DbMgmntTest(unittest.TestCase):

    def test_get_all_database_infos(self):
        with odbc_util.using_odbc_conn_test() as conn:
            db_infos =  get_all_database_infos(conn)
        db_check_dict =dict()
        some_dups_found=False
        for db_info in db_infos:
            found = db_check_dict.get(db_info.name)
            if found:
                print ("2 x mal die db %s gefunden" % db_info.name)
                some_dups_found = True
            if not db_info.status:
                raise RuntimeError("status fehlt bei datenbank %s" % (db_info.name))
            db_check_dict[db_info.name] = db_info
            print(db_info)
        self.assertFalse(some_dups_found)




if __name__ == "__main__":
    unittest.main()
