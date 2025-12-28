# -*- coding: utf-8 -*-
__author__ = 'michael'

import unittest
import os

from dkm_lib_mssql_odbc import mssql_merge_sql


# noinspection PyMethodMayBeStatic
class MssqlMergeSqlTest(unittest.TestCase):
    def _get_test_file_path(self):
        my_path = os.path.abspath(__file__)
        my_path, file1 = os.path.split(my_path)
        return my_path

    # @Test
    # public void test_buildMergeStmtWOAutInc(){
    def test_buildMergeStmtWOAutInc(self):
        print("test_buildMergeStmtWOAutInc")
        # Achtung nicht formatieren
        expected = """SET NOCOUNT ON;
MERGE ohlkz AS target

USING (SELECT %s,%s) as Source (lkz,szLand)
ON ((target.lkz = source.lkz ))
WHEN MATCHED THEN
UPDATE SET szLand = source.szLand
WHEN NOT MATCHED THEN
INSERT (lkz, szLand) 
 VALUES (source.lkz, source.szLand)
;"""
        sql = mssql_merge_sql._build_merge_stmt(table="ohlkz",
                                                using_source_select_cols=["lkz", "szLand"],
                                                primary_key_fields= ["lkz"],
                                                update_columns=["szLand"],
                                                insert_columns=["lkz", "szLand"]
                                                , unique_key_fields=[]
                                                ,auto_inc_col=None
                                                ,change_result_id_data_type=None
                                                )
        self.assertEqual(expected, sql)


if __name__ == "__main__":
    unittest.main()

