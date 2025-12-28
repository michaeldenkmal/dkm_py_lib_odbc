from dkm_lib_db.db_conf import TDbConnGuard, TDbConfGuard, create_conn_info_from_ini_file
from dkm_lib_db.db_driver import DbDriver
from dkm_lib_db.oh_ini_db_file import TOhIniDBFile, get_test_offh_ini_file_path


def using_db_conn_test(db_driver:DbDriver) -> TDbConnGuard:
    return using_db_conn(db_driver, get_test_offh_ini_file_path())

def using_db_conf_test(db_driver:DbDriver) -> TDbConfGuard:
    return using_db_conf(db_driver ,get_test_offh_ini_file_path(), login_name="michael")


def using_db_conn(db_driver:DbDriver,offh_ini_path: str) -> TDbConnGuard:
    db_ini_file_obj = TOhIniDBFile(offh_ini_path)
    #conn_info = createConnInfoFromIniFile(db_ini_file_obj)
    conn_info = create_conn_info_from_ini_file(db_ini_file_obj)
    return TDbConnGuard(db_driver,conn_info)


def using_db_conf(db_driver:DbDriver,offh_ini_path: str, login_name: str) -> TDbConfGuard:
    db_ini_file_obj = TOhIniDBFile(offh_ini_path)
    conn_info = create_conn_info_from_ini_file(db_ini_file_obj)
    return TDbConfGuard(
        db_driver=db_driver,
        conn_info=conn_info,
        login_name=login_name,
        bin_fakt=db_ini_file_obj.bin_fakt,
        i_bin=db_ini_file_obj.bin
    )




