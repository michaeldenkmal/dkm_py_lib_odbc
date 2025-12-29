import os.path
import unittest

from psycopg.sql import SQL

from dkm_lib_pgsql import pgsql_dc_util
from dkm_lib_pgsql.pgsql_schema_util import cre_table_if_not_exists_fp
from dkm_lib_pgsql.test.pgsql_test_common import use_test_conn


class PgSqlQToDcTest(unittest.TestCase):

    def _get_test_fp(self, file_name: str) -> str:
        mypath = os.path.dirname(__file__)
        return os.path.join(mypath, "files", file_name)

    def ensure_billung_customer(self):
        with use_test_conn() as conn:
            if cre_table_if_not_exists_fp(conn.get_real_conn(), "billing_customer",
                                       full_file_path=self._get_test_fp("cretab_billing_customer.sql")
                                       ):
                conn.commit()

    def test_qto_billing_customer_query(self):
        self.ensure_billung_customer()
        sql = """
              select bc."name", bc.id, bc.updated_at
              from billing_customer bc
              limit 0"""
        with use_test_conn() as conn:
            print(pgsql_dc_util.generate_dataclass_from_query(conn=conn.get_real_conn(),sql=sql,all_fields_optional=False))

    def test_qto_billing_customer_table(self):
        self.ensure_billung_customer()
        sql = """
              select bc."name", bc.id, bc.updated_at
              from billing_customer bc
              limit 0"""
        with use_test_conn() as conn:
            print(pgsql_dc_util.generate_dataclass_from_table(conn=conn.get_real_conn(),table="billing_customer"))

if __name__ == '__main__':
    unittest.main()
