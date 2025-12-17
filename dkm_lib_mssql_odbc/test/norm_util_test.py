import unittest
from dataclasses import dataclass
from typing import List

from dkm_lib_mssql_odbc import norm_util
from dkm_lib_mssql_odbc.norm_util import TFillRecDataOpts
from dkm_lib_odbc import odbc_util


@dataclass
class TKKVariable:
    v:str=None
    w:str=None

@dataclass
class TKKVariableExt:
    v:str=None
    w:str=None
    id:float=None

@dataclass
class TKKVariableCase:
    v:str=None
    W:str=None


class NormUtilTest(unittest.TestCase):

    def test_fill_recs_data(self):
        with odbc_util.using_odbc_conn_test() as conn:
            sql ="select v,w from kk_variable where v = %s"
            rows:List[TKKVariable] = norm_util.fill_recs_data(conn,sql, lambda r: TKKVariable(), "KURZNAME")
        for row in rows:
            print("%s=%s" % (row.v, row.w))
        self.assertEqual(len(rows), 1)  # add assertion here

    def test_fill_recs_data_with_opts_ignore(self):
        with odbc_util.using_odbc_conn_test() as conn:
            sql ="select v,w from kk_variable where v = %s"
            rows:List[TKKVariable] = norm_util.fill_recs_data_with_opts(conn,sql, lambda r: TKKVariableExt(),
                TFillRecDataOpts(
                    ignored_fields=["id"]
                )
            ,"KURZNAME")
        for row in rows:
            print("%s=%s" % (row.v, row.w))
        self.assertEqual(len(rows), 1)  # add assertion here

    def test_fill_recs_data_with_opts_getter(self):
        def handle_w(_row_s1, v):
            return "__@@" +v

        with odbc_util.using_odbc_conn_test() as conn:
            sql ="select v,w from kk_variable where v = %s"
            opts = TFillRecDataOpts(
                prop_getter_map={
                    "w":handle_w
                }
            )
            rows:List[TKKVariable] = norm_util.fill_recs_data_with_opts(conn,sql, lambda r: TKKVariable(),opts,"KURZNAME")
        for row in rows:
            self.assertTrue(row.w.startswith("__@@"))
            print("%s=%s" % (row.v, row.w))
        self.assertEqual(len(rows), 1)  # add assertion here

    def test_fill_recs_data_case(self):
        with odbc_util.using_odbc_conn_test() as conn:
            sql ="select v,w from kk_variable where v = %s"
            rows:List[TKKVariableCase] = norm_util.fill_recs_data(conn,sql, lambda r: TKKVariableCase(), "KURZNAME")
        for row in rows:
            print("%s=%s" % (row.v, row.W))
        self.assertEqual(len(rows), 1)  # add assertion here

    def test_get_next_max_id(self):
        with odbc_util.using_odbc_conf_test() as conf:
            next_id = norm_util.get_next_max_id(conf.conn, "mitarbeiter","nr")
            print(f"without_bin:{next_id}")
            next_id = norm_util.get_next_max_id(conf.conn, "mitarbeiter","nr",with_bin=True,
                                                i_bin=conf.i_bin, bin_fakt=conf.binfakt)
            print(f"with_bin:{next_id} binfakt={conf.binfakt} bin={conf.i_bin}")



if __name__ == '__main__':
    unittest.main()
