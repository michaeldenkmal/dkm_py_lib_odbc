# Parameter 1: action=CREATE|REVERT
# Parameter 2: Db Name
# Parameter 3: Snapshot_name
import sys
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from dkm_lib_db.oh_ini_db_file import TOhIniDBFile, get_test_offh_ini_file_path
from dkm_lib_mssql_odbc import mssql_snapshot_util
from dkm_lib_mssql_odbc.mssql_odbc_use import using_db_conn_test


class EnAction(str,Enum):
    CREATE="CREATE"
    REVERT="REVERT"

@dataclass
class CmdParams:
    action:EnAction
    snapshot_name:str


def analyze_cmd_args(args:List[str])->Optional[CmdParams]:
    if len(args)!=3:
        return None
    else:
        try:
            action = EnAction(args[1])
        except ValueError:
            raise ValueError(f"Unknown action: {args[1]}")
        return CmdParams(
            action=action,
            snapshot_name=args[2]
        )


def exec_action_create(cmd_params):
    dbinfo_file = TOhIniDBFile(get_test_offh_ini_file_path())
    with using_db_conn_test() as conn:
        mssql_snapshot_util.create_snapshot(conn, dbinfo_file.database, cmd_params.snapshot_name)


def exec_action_revert(cmd_params):
    dbinfo_file = TOhIniDBFile(get_test_offh_ini_file_path())
    with using_db_conn_test() as conn:
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


if __name__ == "__main__":
    print(args_info())
    g_cmd_params:CmdParams = analyze_cmd_args(sys.argv)
    if g_cmd_params:
        if g_cmd_params.action.value==EnAction.CREATE:
            print("CREATE")
            exec_action_create(g_cmd_params)
        elif g_cmd_params.action.value==EnAction.REVERT:
            print("REVERT")
            exec_action_revert(g_cmd_params)
    else:
        raise Exception("action dbName SnapShotName")