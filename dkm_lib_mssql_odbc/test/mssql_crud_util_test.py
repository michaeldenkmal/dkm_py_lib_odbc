import unittest
from dataclasses import dataclass
from typing import Optional

from dkm_lib_db import db_util
from dkm_lib_mssql_odbc import mssql_crud_util, norm_util, mssql_schema_util, mssql_odbc_use
from dkm_lib_mssql_odbc.mssql_merge_sql import BuildMergeStmtParams
from dkm_lib_mssql_odbc.mssql_odbc_use import using_db_conn_test, using_db_conf_test
from dkm_lib_mssql_odbc.test import test_firmen

PERSON_CREATE_SQL="""
create table Person(
	id float,
	nname varchar(50),
	Primary Key(id)
)"""


@dataclass
class Person:
    id:float
    nname:str


PERSON_MERGE_INFO = mssql_crud_util.build_std_merge_info(
    clazz=Person, primary_key="id", table="person"
)

@dataclass
class KkVariable:
    v:str
    w:str


KK_VARIABLE_MERGE_INFO = BuildMergeStmtParams(
    table= "kk_variable",
    usingSourceSelectCols = ["v","w"] ,# : List[str]
    primaryKeyFields = ["v"], # : List[str]
    updateColumns =["w"],
    insertColumns = ["v","w"]
)


def get_test_kk_var() -> KkVariable:
    return  KkVariable(
            v="mssql_crud_util_test",
            w="test"
        )




class MssqlCrudUtilTest(unittest.TestCase):
    def test_build_dbparams(self):
        pers = Person(id=1, nname="NName")
        dbparams = mssql_crud_util.build_dbparams(pers)
        self.assertEqual(1,dbparams[0])
        self.assertEqual("NName", dbparams[1])

    def test_build_sql_and_params_data_row(self):
        kk_var_rec = get_test_kk_var()
        sql_and_params = mssql_crud_util.build_sql_and_params_data_row(kk_var_rec,KK_VARIABLE_MERGE_INFO)
        print(sql_and_params)

    def ensure_person_table(self):
        with mssql_odbc_use.using_db_conn_test() as conn:
            if not mssql_schema_util.table_exists(conn, "Person"):
                db_util.exec_query(conn, PERSON_CREATE_SQL)
                conn.commit()

    def test_merge_data_row(self):
        self.ensure_person_table()
        pers = Person(id=2, nname="NName2")

        def get_saved_row()->Optional[Person]:
            with using_db_conn_test() as conn_s1:
                #sql = f"select  {norm_util.get_select_fields_list(Person)} from pers where id = %s"
                return norm_util.find_entity_by_id(conn_s1, table_name="person", primary_key_info= {"id": 2},
                                                   fn_create_rec= lambda: pers
                                                   )

        with using_db_conn_test() as conn:
            mssql_crud_util.merge_data_row(conn,pers, PERSON_MERGE_INFO)
            conn.commit()

        saved_row = get_saved_row()
        self.assertEqual(pers, saved_row)

        with using_db_conn_test() as conn:
            mssql_crud_util.del_data_row(conn, pers,"person", {"id": 2.0})
            conn.commit()

        saved_row = get_saved_row()
        self.assertTrue(saved_row is None)

    def test_merge_data_row_oh_calced_bin_pk(self):
        row = test_firmen.create_new(
            suchbegriff = "sb",
            bez1 = "bez1",
            bez2 = "bez2"
        )
        print(row)
        with using_db_conf_test() as conf:
            test_firmen.test_firmen_ensure_table(conf.conn)
            saved_row =test_firmen.save(conf, row)
            self.assertTrue(saved_row.id is not None)
            self.assertTrue(saved_row.local_id is not None)
            self.assertTrue(saved_row.login_name is not None)
            self.assertEqual(saved_row.local_bin, conf.i_bin )
            self.assertEqual(saved_row.local_binfakt, conf.binfakt)
            conf.commit()

    def test_primary_key_Info_to_where_sql(self):
        # gr_id und mi_id
        pk_fields = ["gr_id","mi_id"]
        where_sql_exprs = mssql_crud_util.primary_key_info_to_where_sql(pk_fields)
        self.assertEqual("gr_id = %s AND mi_id = %s", where_sql_exprs)






if __name__ == '__main__':
    unittest.main()
