import dataclasses
from typing import List, Any, Dict
from pyodbc import Connection
from dkm_lib_mssql_odbc.dbshared_conf import IDbConf
from dkm_lib_mssql_odbc.mssql_merge_sql import BuildMergeStmtParams, build_merge_stmt
from dkm_lib_mssql_odbc.mssql_types import SqlAndParams, TDC, PrimaryKeyInfo

#https://docs.python.org/3/library/dataclasses.html
from dkm_lib_mssql_odbc.oh_calced_bin_pk_base_intf import TOCB
from dkm_lib_odbc import odbc_util


def build_dbparams(row:TDC)->List[Any]:
    ret :List[Any]=[]
    for field in dataclasses.fields(row):
        value =getattr(row,field.name)
        #print(field)
        #print(f"{field.name} = {value}")
        ret.append(value)
    return ret


def build_sql_and_params_data_row(row:TDC, merge_params:BuildMergeStmtParams)->SqlAndParams:
    sql = build_merge_stmt(merge_params)
    #print(sql)
    db_params = build_dbparams(row)
    return SqlAndParams(
        sql= sql,
        dbparams=db_params
    )


def build_sql_and_params_data_row_ocb(row:TOCB, merge_params:BuildMergeStmtParams)->SqlAndParams:
    merge_params.changeResultIdDataType= "BigInt"
    merge_params.autoIncCol= "local_id"

    sql = build_merge_stmt(merge_params)
    #print(sql)
    db_params = build_dbparams(row)
    return SqlAndParams(
        sql= sql,
        dbparams=db_params
    )


def merge_data_row(conn:Connection, row:TDC, merge_params:BuildMergeStmtParams):
    sql_and_params = build_sql_and_params_data_row(row, merge_params)
    odbc_util.exec_query(conn, sql_and_params.sql, *sql_and_params.dbparams)


def merge_data_row_ocb(conf:IDbConf, row:TOCB, merge_params:BuildMergeStmtParams)->TOCB:
    sql_and_params = build_sql_and_params_data_row_ocb(row, merge_params)
    #print(f"\n\n@@{sql_and_params.sql}@@\n\n")
    #mssqlUtil.execQuery(conn, sql_and_params.sql, *sql_and_params.dbparams)
    new_id =[]

    def fn_on_row_found(r):
        new_id.append(r["Id"])
        return False

    odbc_util.iter_query(conf.conn, sql_and_params.sql, fn_on_row_found, *sql_and_params.dbparams)

    if len(new_id)>0:
        row.id = (new_id[0] * conf.bin_fakt) + conf.bin
        row.local_id = new_id[0]
    else:
        raise Exception(f"es wurden keine Zeile zurückgegeben:row={row}, mergeparams={merge_params}")

    return row


def get_using_source_select_cols(row:TDC)->List[str]:
    ret :List[str]=[]
    for field in dataclasses.fields(row):
        ret.append(field.name)

    return ret


def build_std_merge_info(clazz, table:str, primary_key:str)->BuildMergeStmtParams:
    using_source_select_cols = get_using_source_select_cols(clazz)
    update_cols = filter(lambda x: x != primary_key, using_source_select_cols)
    return BuildMergeStmtParams(
        table = table,
        usingSourceSelectCols = using_source_select_cols,
        primaryKeyFields =[primary_key],
        updateColumns = list(update_cols),
        insertColumns = using_source_select_cols
    )


def build_del_data_row_sql(table_name:str,primary_key_info:PrimaryKeyInfo)->SqlAndParams:
    sql = f"delete from {table_name}"
    where_sql_and_params = primary_key_info_to_where_exprs(primary_key_info)
    sql = f"{sql} where {where_sql_and_params.sql}"
    return SqlAndParams(sql= sql, dbparams=where_sql_and_params.dbparams)


def del_data_row(conn, row:TDC, table_name:str, primary_keys:Dict[str, Any]):
    sql_and_params = build_del_data_row_sql(table_name, primary_keys)
    try:
        odbc_util.exec_query(conn,sql_and_params.sql,*sql_and_params.dbparams)
    except Exception as e:
        raise Exception(f"{e} bei del_data_row:row={row}, sql_and_params={sql_and_params}")


def primary_key_info_to_where_exprs(primary_key_info:PrimaryKeyInfo) -> SqlAndParams:
    pk_where_exprs = []
    dbparams = []
    for pkfield in primary_key_info.keys():
        pk_where_exprs.append(f"{pkfield} = %s")
        dbparams.append(primary_key_info[pkfield])
    where_exprs = " AND ".join(pk_where_exprs)
    return SqlAndParams(
        sql=where_exprs, dbparams=dbparams
    )


def primary_key_info_to_where_sql(primary_keys:List[str])->str:
    pk_where_exprs = []
    for pkfield in primary_keys:
        pk_where_exprs.append(f"{pkfield} = %s")
    where_exprs = " AND ".join(pk_where_exprs)
    return where_exprs
