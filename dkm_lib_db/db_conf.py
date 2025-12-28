import traceback
from dataclasses import dataclass
from typing import Optional

from dkm_lib_db.db_driver import DbConn, DbDriver
from dkm_lib_db.db_types import DbConnInfo
from dkm_lib_db.oh_ini_db_file import TOhIniDBFile


@dataclass
class TDbConf(object):
    #def __init__(self, conn:pyodbc.Connection, i_bin:int, binfakt:int, login_name:str):
    conn:Optional[DbConn]
    i_bin:int
    binfakt:int
    login_name:str

    def commit(self):
        if self.conn:
            self.conn.commit()


class TDbConfGuard(object):
    def __init__(self, db_driver:DbDriver,conn_info: DbConnInfo, login_name: str, bin_fakt: int, i_bin: int):
        self.db_driver = db_driver
        self.conn_info = conn_info
        self.dbconf = TDbConf(
            conn=None,
            login_name=login_name,
            binfakt=bin_fakt,
            i_bin=i_bin
        )

    def __enter__(self) -> TDbConf:
        # connection erzeugen
        self.dbconf.conn = self.db_driver.connect(self.conn_info)
        return self.dbconf

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        if self.dbconf.conn:
            self.dbconf.conn.close()
        return True


class TDbConnGuard(object):
    def __init__(self, db_driver:DbDriver,conn_info: DbConnInfo):
        self.db_driver = db_driver
        self.conn_info = conn_info
        self.conn: Optional[DbConn]  = None

    def __enter__(self) -> DbConn:
        # connection erzeugen
        self.conn = self.db_driver.connect(self.conn_info)
        return self.conn

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        if self.conn:
            self.conn.close()
        return True

def create_conn_info_from_ini_file_path(offh_ini_path: str) -> DbConnInfo:
    offh_ini = TOhIniDBFile(offh_ini_path)
    return create_conn_info_from_ini_file(offh_ini)


def create_conn_info_from_ini_file(oh_ini_db_file: TOhIniDBFile) -> DbConnInfo:
    #return TConnInfo(ohIniDBFile.host, ohIniDBFile.userName, ohIniDBFile.password, ohIniDBFile.database)
    return DbConnInfo(
        host = oh_ini_db_file.host,
        db_name = oh_ini_db_file.database,
        user = oh_ini_db_file.userName,
        pwd = oh_ini_db_file.password,
        db_type = oh_ini_db_file.db_type
    )


