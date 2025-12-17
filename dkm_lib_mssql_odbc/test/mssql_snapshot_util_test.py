import unittest

from dkm_lib_mssql_odbc import mssql_snapshot_util
from dkm_lib_odbc import odbc_util


class MssqlSnapshotUtilTest(unittest.TestCase):
    def test_get_db_phys_file_info(self):
        with odbc_util.using_odbc_conn_test() as conn:
            db_files = mssql_snapshot_util.get_db_phys_file_info(conn,"OHANT")
            for db_file in db_files:
                print(db_file)


    def test_create_snapshot(self):
        # create_snapshot
        with odbc_util.using_odbc_conn_test() as conn:
            mssql_snapshot_util.create_snapshot(conn,db_name="OHANT",snapshot_name="ohant_test")

    def test_revert_to_snap_shot(self):
        with odbc_util.using_odbc_conn_test() as conn:
            mssql_snapshot_util.revert_to_snapshot(conn, db_name="OHANT", snap_shot_name="ohant_test")


if __name__ == '__main__':
    unittest.main()
