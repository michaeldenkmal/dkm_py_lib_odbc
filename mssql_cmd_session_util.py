# offh.ini 0: office Help Ini
import os.path

from dkm_lib_db import db_util
from dkm_lib_db.oh_ini_db_file import TOhIniDBFile, get_test_offh_ini_file_path
from dkm_lib_mssql_odbc import mssql_snapshot_util, mssql_sess_info, mssql_odbc_use
from dkm_lib_mssql_odbc.mssql_odbc_use import using_db_conn_test, using_db_conn

ARG_POS_OFFH_INI=1
# Parameter 1: action=KILL_ALL_SESSIONS
ARG_POS_ACTION=2
# Parameter 2: Db Name
ARG_POS_DBNAME=3


import sys
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional




class EnAction(str,Enum):
    KILL_ALL_SESSIONS="KILL_ALL_SESSIONS"
    DROP_DATABASE="DROP_DATABASE"

@dataclass
class CmdParams:
    offh_ini_path:str
    action:EnAction
    dbname:str




def analyze_cmd_args(args:List[str])->Optional[CmdParams]:
    if len(args)!=4:
        return None
    else:
        arg_action:str =args[ARG_POS_ACTION]
        try:
            action = EnAction(arg_action)
        except ValueError:
            raise ValueError(f"Unknown action: {arg_action}")
        return CmdParams(
            action=action,
            dbname=args[ARG_POS_DBNAME],
            offh_ini_path=args[ARG_POS_OFFH_INI]
        )


def exec_action_create(cmd_params):
    dbinfo_file = TOhIniDBFile(get_test_offh_ini_file_path())
    with using_db_conn_test() as conn:
        mssql_snapshot_util.create_snapshot(conn, dbinfo_file.database, cmd_params.snapshot_name)


def exec_action_revert(cmd_params):
    dbinfo_file = TOhIniDBFile(get_test_offh_ini_file_path())
    with mssql_odbc_use.using_db_conn_test() as conn:
        mssql_snapshot_util.revert_to_snapshot(conn, dbinfo_file.database, cmd_params.snapshot_name)



def args_info()->str:
    arg_len = len(sys.argv)
    params=[]
    pos=0
    for arg in sys.argv:
        output = f"#{pos}: {arg}"
        params.append(output)
        pos +=1

    params_str="\n".join(params)
    return f"arg_len={arg_len}, params={params_str}"


# noinspection PyShadowingNames
def exec_action_kill_all_sessions(cmd_params:CmdParams):
    if not os.path.exists(cmd_params.offh_ini_path):
        raise Exception(f"offhinipath @@{cmd_params.offh_ini_path}@@ existiert nicht")
    with using_db_conn(cmd_params.offh_ini_path) as conn:
        mssql_sess_info.kill_all_sessions_by_dbname(conn, cmd_params.dbname)


def exec_action_drop_database(cmd_params):
    if not os.path.exists(cmd_params.offh_ini_path):
        raise Exception(f"offhinipath @@{cmd_params.offh_ini_path}@@ existiert nicht")
    with using_db_conn(cmd_params.offh_ini_path) as conn:
        mssql_sess_info.kill_all_sessions_by_dbname(conn, cmd_params.dbname)
        db_util.exec_query(conn,"use master")
        db_util.exec_query(conn,f"drop database {cmd_params.dbname}")


if __name__ == "__main__":
    print(args_info())
    g_cmd_params:CmdParams = analyze_cmd_args(sys.argv)
    if g_cmd_params:
        if g_cmd_params.action.value==EnAction.KILL_ALL_SESSIONS:
            print(EnAction.KILL_ALL_SESSIONS)
            exec_action_kill_all_sessions(g_cmd_params)
        elif g_cmd_params.action.value==EnAction.DROP_DATABASE:
            print(EnAction.DROP_DATABASE)
            exec_action_drop_database(g_cmd_params)
    else:
        raise Exception("action dbName SnapShotName")