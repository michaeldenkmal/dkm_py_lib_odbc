from typing import Optional

from dkm_lib_odbc import odbc_util


def get_var(conn,varname:str)-> Optional[str]:
    sql ="select w from kk_variable where v= %s"
    rows = odbc_util.get_dict_rows(conn,sql,varname)
    if not rows:
        return None
    if len(rows) ==0:
        return None
    return rows[0]["w"]


