import os

from dkm_lib_db import db_util
from dkm_lib_db.db_driver import DbConn
from dkm_lib_db.db_util import exec_query, iter_query
from dkm_lib_mssql_odbc import mssql_sess_info
from dkm_lib_mssql_odbc.mssql_odbc_use import using_db_conn


def drop_db_force(conn:DbConn, dbname:str):
    mssql_sess_info.kill_all_sessions_by_dbname(conn, dbname)
    conn.set_autocommit(True)
    sql=f"DROP DATABASE {dbname}"
    db_util.exec_query(conn, sql)


def create_database(conn, db_name):
    sql = "create database %s" % db_name
    exec_query(conn, sql)


def create_database_if_not_exists(conn, db_name):
    if not db_exists(conn, db_name):
        create_database(conn, db_name)


def drop_database(conn:DbConn, db_name:str):
    sql = "drop database %s" % db_name
    exec_query(conn, sql)


def exec_script(conn, script_sql: str):
    sql_statements = script_sql.split("\nGO\n")
    for sql_stmt in sql_statements:
        if sql_stmt.strip():
            exec_query(conn, sql_stmt)


def drop_database_if_exists(conn, db_name):
    if db_exists(conn, db_name):
        drop_database(conn, db_name)


def backup_database_to_disk(conn, db_name, file_path):
    exec_query(conn, get_backup_database_to_disc_sql(db_name, file_path))


def get_backup_database_to_disc_sql(db_name, file_path):
    return " BACKUP DATABASE %s TO DISK = '%s'" % (db_name, file_path)


def restore_database_from_disk_by_backup_file_info(conn, backup_file_name, backup_info, dst_db_name):
    assert isinstance(backup_info, TBackupInfo)
    assert isinstance(backup_info.dataInfo, TBackupFileInfo)
    assert isinstance(backup_info.logInfo, TBackupFileInfo)
    b_ft_file_exists = False
    if backup_info.ftInfo is not None:
        assert isinstance(backup_info.ftInfo, TBackupFileInfo)
        b_ft_file_exists = True
    mssql_data_file_path = get_mssql_data_dir_from_phys_path(backup_info.dataInfo.physical_file_path)
    # Data
    data_file_logical_name = backup_info.dataInfo.logical_file_name
    data_file_path = os.path.join(mssql_data_file_path, backup_info.dataInfo.build_physical_file_name_from_db_name(dst_db_name))
    # Logs
    log_file_logical_name = backup_info.logInfo.logical_file_name
    log_file_path = os.path.join(mssql_data_file_path, backup_info.logInfo.build_physical_file_name_from_db_name(dst_db_name))
    # FullText
    if b_ft_file_exists:
        full_text_logical_name = backup_info.ftInfo.logical_file_name
        full_text_path = os.path.join(mssql_data_file_path, backup_info.ftInfo.build_physical_file_name_from_db_name(dst_db_name))
    else:
        full_text_logical_name = None
        full_text_path = None
    # Restore it

    restore_database_from_disk(conn, backup_file_name, dst_db_name,
                                data_file_logical_name, str(data_file_path), log_file_logical_name, str(log_file_path),
                                full_text_logical_name, str(full_text_path))


def restore_database_from_disk(conn:DbConn, file_path:str, db_name:str, data_file_logical_name:str,
                                data_file_path:str, log_file_logical_name:str, log_file_path:str,
                                full_text_logical_name:str, full_text_path:str):
    move_str = "MOVE N'%s' TO N'%s'"
    move_parts = [
        move_str % (data_file_logical_name, data_file_path),
        move_str % (log_file_logical_name, log_file_path)
    ]
    if full_text_logical_name is not None:
        move_parts.append(move_str % (full_text_logical_name, full_text_path))
    sql = " RESTORE DATABASE %s FROM DISK = '%s' WITH %s " % (db_name, file_path, ", ".join(move_parts))
    print(sql)
    exec_query(conn, sql)


# noinspection PyClassHasNoInit
class TBackupFileType:
    DATA = "D"
    LOG = "L"
    FULLTEXT = "F"


class TBackupFileInfo(object):
    def __init__(self, logical_file_name=None, physical_name=None, sztype=None):
        self.logical_file_name = logical_file_name
        self.physical_file_path = physical_name
        self.sztype = sztype
        self.fileType = None

    def get_physical_file_name_only(self):
        p, f = os.path.split(self.physical_file_path)
        return f

    def build_logical_name_from_db_name(self, db_name):
        if self.fileType == TBackupFileType.DATA:
            return "%s_DATA" % db_name
        elif self.fileType == TBackupFileType.LOG:
            return "%s_LOG" % db_name
        else:
            return "ftrow_ftOfficeh"

    def build_physical_file_name_from_db_name(self, db_name):
        if self.fileType == TBackupFileType.DATA:
            return "%s.mdf" % db_name
        elif self.fileType == TBackupFileType.LOG:
            return "%s_1.ldf" % db_name
        else:
            return "%s_ft.ndf" % db_name


class TBackupInfo(object):
    def __init__(self, data_info=None, log_info=None, ft_info=None):
        self.dataInfo = data_info
        self.logInfo = log_info
        self.ftInfo = ft_info


def is_full_text_file(file_name:str):
    pwfn, ext = os.path.splitext(file_name)
    return ext.upper() == ".NDF"


# noinspection PyPep8Naming
def read_backup_info(conn:DbConn, backup_file_path:str):
    sql = "RESTORE FILELISTONLY FROM DISK ='%s'" % backup_file_path
    FN_LogicalName = "LogicalName"
    FN_PhysicalName = "PhysicalName"
    FN_Type = "Type"
    backupInfos: list[TBackupFileInfo] = []

    def on_row(row):
        backupInfos.append(TBackupFileInfo(row[FN_LogicalName], row[FN_PhysicalName], row[FN_Type]))
        return True

    iter_query(conn, sql, on_row)
    ret = TBackupInfo()
    for backupInfo in backupInfos:
        assert isinstance(backupInfo, TBackupFileInfo)
        sztype = backupInfo.sztype
        if sztype == "D":
            if is_full_text_file(backupInfo.physical_file_path):
                backupInfo.fileType = TBackupFileType.FULLTEXT
                ret.ftInfo = backupInfo
            else:
                backupInfo.fileType = TBackupFileType.DATA
                ret.dataInfo = backupInfo
        elif sztype == "L":
            backupInfo.fileType = TBackupFileType.LOG
            ret.logInfo = backupInfo
    return ret


def get_mssql_data_dir_from_phys_path(phys_path:str):
    r, fn = os.path.split(phys_path)
    return r


# noinspection PyPep8Naming
def copy_mssql_db(offh_ini_path:str, db_src_name:str, db_dst_name:str):
    with using_db_conn(offh_ini_path) as conn:
        # src Tabelle exportieren
        dmpFileName = os.path.join(os.environ["temp"], "%s.dmp" % db_dst_name)
        if os.path.exists(dmpFileName):
            os.remove(dmpFileName)
        backup_database_to_disk(conn, db_src_name, dmpFileName)
        # dst Tabelle löschen falls vorhanden
        drop_database_if_exists(conn, db_dst_name)
        # dst Tabelle importieren
        backupInfo = read_backup_info(conn, dmpFileName)
        restore_database_from_disk_by_backup_file_info(conn, dmpFileName, backupInfo, db_dst_name)


def db_exists(conn, db_name):
    sql = """
    SELECT name
    FROM master.dbo.sysdatabases
    where name= %s
    """
    found: list[bool] = []

    # noinspection PyUnusedLocal
    def on_row(row):
        found.append(True)
        return False

    iter_query(conn, sql, on_row, db_name)
    return len(found) > 0


# https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/databases-and-files-catalog-views-transact-sql?view=sql-server-ver15
from typing import List, Optional

from dkm_lib_pure import dkmFsUtil

_DATA_INFO_SQL_SELECT_PART = """
select dbs.name as db_name,
	type_desc, physical_name, dbs.state_desc 
	, mf.name as db_file_name
from sys.master_files mf
inner join sys.databases dbs on dbs.database_id = mf.database_id
"""

_DATA_INFO_SQL_WHERE_NO_SYSDBS = "dbs.database_id > 4"


def _build_get_all_database_info_sql():
    return _DATA_INFO_SQL_SELECT_PART + "\nWHERE " + _DATA_INFO_SQL_WHERE_NO_SYSDBS + " \norder by 1,2"

def _build_get_specific_database_info_sql()->str:
    return _DATA_INFO_SQL_SELECT_PART +"\nWHERE dbs.name= %s order by 1,2"


class TDatabaseFile(object):
    def __init__(self, db_name, db_file_path, type_desc):
        self.db_name = db_name
        self.db_file_path = db_file_path
        self.type_desc = type_desc

    def __repr__(self):
        return "db_name=%s, db_file_path=%s, type_desc=%s" %(
            self.db_name, self.db_file_path, self.type_desc
        )

class TDatabaseInfo(object):
    def __init__(self, name: str, status: str):
        self.name = name
        self.status = status
        self.db_files:List[TDatabaseFile] =[]

    def is_status_online(self):
        return self.status == "ONLINE"

    def __repr__(self):
        db_files = map(lambda x:x.__repr__(), self.db_files)
        db_files_str = "\n".join(db_files)
        return "name=%s, status=%s, db_files=%s" % (
            self.name, self.status, db_files_str
        )


def is_data_file(file_path: str) -> bool:
    ext = dkmFsUtil.get_ext(file_path)
    return ext.casefold() == ".MDF".casefold()


# noinspection PyPep8Naming
def _database_infos_raw_rows_to_list(rows:List[dict])-> List[TDatabaseInfo]:
    COL_NAME = "db_name"
    COL_TYPE = "type_desc"
    COL_FILE_PATH = "physical_name"
    COL_STATE_DESC = "state_desc"
    COL_DB_FILE_NAME="db_file_name"
    db_dict = dict()
    ret = []
    for row in rows:
        db_name = row[COL_NAME]
        file_type = row[COL_TYPE]
        file_path = row[COL_FILE_PATH]
        db_status = row[COL_STATE_DESC]
        db_file_name = row[COL_DB_FILE_NAME]
        db_info_obj = db_dict.get(db_name)
        if not db_info_obj:
            db_info_obj = TDatabaseInfo(db_name, db_status)
            ret.append(db_info_obj)
            db_dict[db_name] = db_info_obj

        db_file_info = TDatabaseFile(db_name=db_file_name, db_file_path=file_path,
                                     type_desc=file_type)
        db_info_obj.db_files.append(db_file_info)

    return ret


def get_all_database_infos(conn) -> List[TDatabaseInfo]:
    rows = db_util.get_dict_rows(conn, _build_get_all_database_info_sql())
    return _database_infos_raw_rows_to_list(rows)


def get_specific_database_infos(conn, db_name:str) -> Optional[TDatabaseInfo]:
    rows = db_util.get_dict_rows(conn, _build_get_specific_database_info_sql(), db_name)
    lst= _database_infos_raw_rows_to_list(rows)
    if lst.__len__()==0:
        return None
    return lst[0]


"""ALTER DATABASE AdventureWorks2014 SET OFFLINE;  
GO"""


def _build_set_db_status(db_name: str, status: str) -> str:
    sql = "ALTER DATABASE %s SET %s" % (db_name, status)
    return wrap_sql_with_use_master_rollback(sql)


def set_db_offline(conn, db_name: str):
    sql = _build_set_db_status(db_name, "OFFLINE")
    exec_script(conn, sql)


def set_db_online(conn, db_name: str):
    sql = _build_set_db_status(db_name, "ONLINE")
    exec_script(conn, sql)


def wrap_sql_with_use_master_rollback(sql:str)->str:
    return """
use master
GO
ROLLBACK
GO
%s    
""" % sql