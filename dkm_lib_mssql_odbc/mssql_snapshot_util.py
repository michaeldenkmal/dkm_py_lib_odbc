#https://www.sqlshack.com/understanding-database-snapshots-vs-database-backups-in-sql-server/
import os.path
from dataclasses import dataclass
from typing import List, Optional

from pyodbc import Connection

from dkm_lib_mssql_odbc import norm_util, mssql_sess_info
from dkm_lib_odbc import conn_util, odbc_util


@dataclass
class DbPhysFileInfo:
    name:str=""
    physical_name:str=""
    state_desc:str=""
    type_desc:str=""


def get_db_phys_file_info(conn:Connection, db_name:str)->List[DbPhysFileInfo]:
    sql=f"""
        -- To fetch the logical name of a data file
        SELECT name, physical_name, state_desc,type_desc
        FROM sys.master_files
        WHERE database_id = DB_ID(N'{db_name}')
    """
    return norm_util.fill_recs_data(conn,sql,lambda :DbPhysFileInfo())


def get_db_phys_mdf_fileinfo(conn:Connection, db_name:str)->Optional[DbPhysFileInfo]:
    db_files = get_db_phys_file_info(conn, db_name)
    for db_file in db_files:
        if db_file.name.endswith("_DATA"):
            return db_file
    return None


@dataclass
class GetGbPhysStorePathRes:
    phys_store_folder:str
    data_file_info:DbPhysFileInfo


def get_db_phys_store_path(conn:Connection, db_name:str)->Optional[GetGbPhysStorePathRes]:
    db_file_info = get_db_phys_mdf_fileinfo(conn, db_name)
    if db_file_info:
        return GetGbPhysStorePathRes(
            phys_store_folder=        os.path.dirname(db_file_info.physical_name)
            , data_file_info=db_file_info
        )
    return None


def create_snapshot(conn:Connection, db_name:str, snapshot_name:str):
    create_db_snapshot(conn,db_name=db_name,snapshot_name=snapshot_name,
                       db_data_files=get_db_phys_file_info(conn,db_name))



def create_db_snapshot(conn:Connection, db_name:str, snapshot_name:str, db_data_files:List[DbPhysFileInfo]):
    def build_name_file_name(_db_file_info:DbPhysFileInfo)->str:
        # (NAME = AdventureWorks_Data, FILENAME = 'C:\AdventureWorks_data_042007.ss'),
        # (NAME = fg0103SALES , FILENAME = < Specify filename > )
        # (NAME = fg0406SALES , FILENAME = < Specify filename > ),
        db_path,ext = os.path.splitext(_db_file_info.physical_name)
        folder = os.path.dirname(_db_file_info.physical_name)
        snap_phys_file_name=os.path.join(folder,f"ss_{snapshot_name}_{_db_file_info.name}.ss{ext}")
        return f"(NAME = {_db_file_info.name}, FILENAME = '{snap_phys_file_name}')"

    name_files=[]
    db_data_files_wo_log = filter(lambda x:x.type_desc!="LOG",db_data_files)
    for db_file_info in db_data_files_wo_log:
        name_files.append(build_name_file_name(db_file_info))

    name_files_stmt = "\n,".join(name_files)

    sql =f"""
    CREATE DATABASE ss_{snapshot_name}
    ON {name_files_stmt}
        AS SNAPSHOT OF {db_name}
    """
    print(sql)
    # siehe https://stackoverflow.com/questions/9918129/how-can-i-create-a-database-using-pymssql
    #conn.autocommit(True)#
    conn_util.conn_set_autocommit(conn, True)
    odbc_util.exec_query(conn, sql)
    conn_util.conn_set_autocommit(conn, False)



def revert_to_snapshot(conn:Connection,db_name:str, snap_shot_name:str):
    """Revert the entire database using restore operation
A database revert operation requires the RESTORE DATABASE permissions on the source database. To revert the database, use the following Transact-SQL statement:

RESTORE DATABASE <database_name> FROM DATABASE_SNAPSHOT =<database_snapshot_name>

USE master;
GO
-- Reverting source database from the snapshot
RESTORE DATABASE SQLShackDSDemo from
DATABASE_SNAPSHOT = 'SQLShackDSDemo_Snapshot';
GO"""
    mssql_sess_info.kill_all_sessions_by_dbname(conn, db_name)
    sql=f"""
    RESTORE DATABASE {db_name} from   
        DATABASE_SNAPSHOT = 'ss_{snap_shot_name}';  
    """
    conn_util.conn_set_autocommit(conn,True)
    odbc_util.exec_query(conn,"use master")
    odbc_util.exec_query(conn,sql)
    conn_util.conn_set_autocommit(conn,False)


