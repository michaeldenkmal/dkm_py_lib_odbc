# -*- coding: utf-8 -*-
__author__ = 'michael'

import unittest
import os

from dkm_lib_pure import dkmFsUtil

from dkm_lib_mssql_odbc import mssql_dbmgmnt_util
from dkm_lib_mssql_odbc.mssql_dbmgmnt_util import backup_database_to_disk, drop_database_if_exists, read_backup_info, \
    restore_database_from_disk_by_backup_file_info, db_exists, TBackupFileInfo
from dkm_lib_odbc import odbc_util
from dkm_lib_odbc.odbc_util import get_dict_rows


# noinspection PyPep8Naming
class MsqlUtilTest(unittest.TestCase):
    def _get_test_file_path(self):
        my_path = os.path.abspath(__file__)
        my_path, file1 = os.path.split(my_path)
        return my_path

    def checkExportOhTstDataBase(self, conn):
        dmpFileName = os.path.join(os.environ["temp"], "ohtst.dmp")
        if not os.path.exists(dmpFileName):
            backup_database_to_disk(conn, "OHTST", dmpFileName)
        return dmpFileName

    def test_kk_variable(self):
        FN_V="V"
        FN_W="W"
        sql = "SELECT v V, w W from KK_VARIABLE"

        def onRow(row):
            print("%s=%s" %(row[FN_V], row[FN_W]))
            return True

        #mssqlUtil.execInConnection(dbConf, connExec)
        with odbc_util.using_odbc_conn_test() as conn:
            odbc_util.iter_query(conn,sql,onRow)

    def test_kk_var_with_params(self):
        FN_V = "V"
        FN_W = "W"
        sql = "SELECT v V, w W from KK_VARIABLE WHERE V = %s"
        #dbConf = ohIniDBFile.createTestDbConf(mssqlUtil.createConnInfoFromIniFile)


        def onRow(row):
            print("%s=%s" % (row[FN_V], row[FN_W]))
            return True

        #mssqlUtil.execInConnection(dbConf, connExec)
        with odbc_util.using_odbc_conn_test() as conn:
            odbc_util.iter_query(conn, sql, onRow, 'KURZNAME')


    def test_dbExists(self):
        #def connExec(conn):
        #    self.assertTrue(mssqlUtil.dbExists(conn, "OHTST"))
        #dbConf = ohIniDBFile.createTestDbConf(mssqlUtil.createConnInfoFromIniFile)
        #mssqlUtil.execInConnection(dbConf, connExec)
        with odbc_util.using_odbc_conn_test() as conn:
            self.assertTrue(mssql_dbmgmnt_util.db_exists(conn, "OHTST"))

    def test_createAndDropDataBase(self):
        with odbc_util.using_odbc_conn_test() as conn:
            if mssql_dbmgmnt_util.db_exists(conn, "xxx"):
                mssql_dbmgmnt_util.drop_database(conn, "xxx")
            mssql_dbmgmnt_util.create_database(conn, "xxx")
            self.assertTrue(mssql_dbmgmnt_util.db_exists(conn, "xxx"))
            mssql_dbmgmnt_util.drop_database(conn, "xxx")



    def test_backUpDataBaseToDisk(self):
        dmpFileName = os.path.join(os.environ['TEMP'], "xxx.dmp")
        dkmFsUtil.killFileIfExists(dmpFileName)

        with odbc_util.using_odbc_conn_test() as conn:
            mssql_dbmgmnt_util.create_database_if_not_exists(conn, "xxx")
            mssql_dbmgmnt_util.backup_database_to_disk(conn, "xxx", dmpFileName)
            self.assertTrue(os.path.exists(dmpFileName))
            mssql_dbmgmnt_util.drop_database(conn, "xxx")
            dkmFsUtil.killFileIfExists(dmpFileName)



    def test_restoreDataBaseFromDisk(self):
        with odbc_util.using_odbc_conn_test() as conn:
            dstDbName = "ohjunit"
            dmpFileName = self.checkExportOhTstDataBase(conn)
            drop_database_if_exists(conn, dstDbName)
            backupInfo = read_backup_info(conn, dmpFileName)
            restore_database_from_disk_by_backup_file_info(conn, dmpFileName, backupInfo, dstDbName)
            self.assertTrue(db_exists(conn, dstDbName))

    def test_readBackupInfo(self):

        def assertCheck(backupFileInfo, logicalName, endOfFileName):
            assert isinstance(backupFileInfo, TBackupFileInfo)
            self.assertEqual(backupFileInfo.logical_file_name.lower(), logicalName.lower())
            self.assertTrue(backupFileInfo.physical_file_path.lower().endswith(endOfFileName))

        with odbc_util.using_odbc_conn_test() as conn:
            dmpFileName = self.checkExportOhTstDataBase(conn)
            self.assertTrue(os.path.exists(dmpFileName))
            backupInfo = read_backup_info(conn, dmpFileName)
            self.assertIsNotNone(backupInfo.dataInfo)
            assertCheck(backupInfo.dataInfo, "OHTST_DATA","ohtst.mdf")
            self.assertIsNotNone(backupInfo.logInfo)
            assertCheck(backupInfo.logInfo, "OHTST_LOG", "ohtst_1.ldf")
            self.assertIsNotNone(backupInfo.ftInfo)
            assertCheck(backupInfo.ftInfo, "ftrow_ftOfficeh", ".ndf")

    def test_using_dkm_conn(self):
        with odbc_util.using_odbc_conn_test() as conn:
            sql="select v,w from KK_VARIABLE where v like %s order by 1"
            rows = get_dict_rows(conn,sql, "A%")
            for row in rows:
                print("%s = %s" % (row['v'], row['w']))



if __name__ == "__main__":
    unittest.main()

