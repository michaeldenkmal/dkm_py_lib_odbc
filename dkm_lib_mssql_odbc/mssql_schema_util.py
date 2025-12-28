from dkm_lib_db import db_util
from dkm_lib_db.db_driver import DbConn
from dkm_lib_db.db_util import rows_exists


def create_table_if_not_exists(conn:DbConn, table_name:str, ddl:str,schema:str="dbo"):
    if not table_exists(conn, table_name,schema):
        db_util.exec_query(conn, ddl)
        conn.commit()


def table_exists(conn:DbConn, table_name:str, schema:str="dbo")->bool:
    sql="""
    SELECT top 1 1 as c1  FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
            AND  TABLE_NAME = %s 
    """
    return rows_exists(conn, sql, schema, table_name)