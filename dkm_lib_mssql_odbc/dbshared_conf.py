import traceback
from dataclasses import dataclass

import pyodbc # type: ignore

from dkm_lib_odbc import odbc_util
from dkm_lib_odbc.odbc_util import TOdbcConnInfo
from dkm_lib_odbc.oh_ini_db_file import get_test_offh_ini_file_path, TOhIniDBFile


@dataclass
class IDbConf:
    conn:pyodbc.Connection
    bin:int
    bin_fakt:int
    login_name:str

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()




class TUseDkmConf(object):
    def __init__(self, conn_info: TOdbcConnInfo, i_bin:int, bin_fakt:int, login_name:str):
        self.conn_info = conn_info
        self.bin = i_bin
        self.bin_fakt = bin_fakt
        self.login_name = login_name

    def __enter__(self) -> IDbConf:
        # connection erzeugen
        self.conf = IDbConf(
            conn=odbc_util.odbc_connect(self.conn_info.conn_str),
            bin=self.bin,
            bin_fakt=self.bin_fakt,
            login_name=self.login_name
        )
        return self.conf

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        if self.conf:
            self.conf.conn.close()
        return True
#
#
#
#https://towardsdatascience.com/mastering-memoization-in-python-dcdd8b435189


def using_dkm_conf(offh_ini_path: str, login_name:str, use_cached:bool=True) -> TUseDkmConf:
    if use_cached:
        db_ini_file_obj = TOhIniDBFile(offh_ini_path)
    else:
        db_ini_file_obj = TOhIniDBFile(offh_ini_path)

    conn_info = odbc_util.create_conn_info_from_ini_file(db_ini_file_obj)
    return TUseDkmConf(
        conn_info = conn_info,
        i_bin = db_ini_file_obj.bin,
        bin_fakt = db_ini_file_obj.bin_fakt,
        login_name = login_name
    )


def using_dkm_conf_test():
    return using_dkm_conf(get_test_offh_ini_file_path(),
                          login_name="michael")


def get_next_sq_with_bin(conf:IDbConf, table_name:str)->float:
    new_id = get_next_sq(conf, table_name)
    return (new_id * conf.bin_fakt) + conf.bin


def get_next_sq(conf:IDbConf, table_name:str)->float:
    #     public static double getNextId(@NotNull IDbConf conf ,String tableName) {
    #     final TGenericPtr<BigDecimal> ret = new TGenericPtr<>(null);
    #     final String ptrTableName = tableName;
    sql = f"""
        set NOCOUNT ON
        Insert into {table_name}_sq values ('a')
        select @@IDENTITY as new_id;
    """
    #     String sql = "set NOCOUNT ON\n"
    #             + "Insert into " + ptrTableName + "_sq values ('a')\n"
    #             + "select @@IDENTITY ";
    #     List rows = conf.getEm().sql(sql).resultsRawData(conf.getJDBCConn());
    new_id=[]
    def fn_iter(row):
        new_id.append(row["new_id"])
        return False

    odbc_util.iter_query(conf.conn, sql, fn_iter)

    #     ret.data = (BigDecimal) rows.get(0);
    #     return ret.data.doubleValue();
    # }
    return new_id[0]
