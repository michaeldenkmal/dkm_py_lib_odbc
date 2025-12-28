from typing import Optional

from dkm_lib_db import db_util
from dkm_lib_db.db_driver import DbConn


def get_var(conn:DbConn,varname:str)-> Optional[str]:
    sql ="select w from kk_variable where v= %s"
    rows = db_util.get_dict_rows(conn,sql,varname)
    if not rows:
        return None
    if len(rows) ==0:
        return None
    return rows[0]["w"]


