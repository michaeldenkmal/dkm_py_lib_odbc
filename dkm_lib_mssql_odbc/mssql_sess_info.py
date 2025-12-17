import datetime
from dataclasses import dataclass
from typing import List

import pyodbc

from dkm_lib_mssql_odbc import norm_util
from dkm_lib_odbc import conn_util, odbc_util


@dataclass
class MssqlSessionInfo2:
    SPID:str=0
    Status:str=""
    Login:str=""
    HostName:str=""
    BlkBy:str=""
    DBName:str=""
    Command:str=""
    CPUTime:int=0
    DiskIO:int=0
    LastBatch:datetime.date=0
    ProgramName:str=""
    REQUESTID:int=0


def get_server_session_info2(conn:pyodbc.Connection)->List[MssqlSessionInfo2]:
    sql="exec sp_who2"
    return norm_util.fill_recs_data(conn,sql,lambda: MssqlSessionInfo2())


def kill_session(conn:pyodbc.Connection, pid:str):
    sql=f"KILL {pid}"
    conn_util.conn_set_autocommit(conn, True)
    print(f"kill_session: pid={pid}: {sql}")
    odbc_util.exec_query(conn, sql)
    conn_util.conn_set_autocommit(conn, False)


def kill_all_sessions_by_dbname(conn:pyodbc.Connection, dbname:str):
    sessions = get_server_session_info2(conn)
    my_spid = str(get_own_session_id(conn))
    print(f"my spid:{my_spid}, session.len={len(sessions)}")
    pids=[]
    for sess in sessions:
        print(f"sess.DBName={sess.DBName}, SPID={sess.SPID}")
        if sess.DBName and sess.SPID and int(sess.SPID)>0 and sess.DBName.lower()==dbname.lower():
            print(f"pids.append(sess.SPID): {sess.SPID}")
            pids.append(sess.SPID)
    for pid in pids:
        nowhitespace_pid = pid.strip()
        print(f"@@{my_spid}@@=@@{nowhitespace_pid}@@")
        try:
            if my_spid != nowhitespace_pid:
                kill_session(conn, nowhitespace_pid)
        except Exception as err:
            print(f"{err} bei spid={pid}")


def get_own_session_id(conn:pyodbc.Connection)->str:
    sql="SELECT @@SPID as spid"
    rows=odbc_util.get_dict_rows(conn,sql)
    return rows[0]["spid"]

