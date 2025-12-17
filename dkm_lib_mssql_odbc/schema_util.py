from dkm_lib_odbc import odbc_util


def table_exists(conn, table_name:str, schema:str="dbo")->bool:
    sql="""
    SELECT top 1 1 as c1  FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
            AND  TABLE_NAME = %s 
    """
    return odbc_util.rows_exists(conn, sql, schema, table_name)