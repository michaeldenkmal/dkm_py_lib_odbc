import dataclasses
from typing import List, Callable, Optional

from dkm_lib_mssql_odbc.mssql_merge_sql import build_merge_stmt, BuildMergeStmtParams
from dkm_lib_orm import orm_util

from dkm_lib_orm.__oh_calced_bin_pk_base import OhCalcedBinPkBase
from dkm_lib_orm.orm_util import PkAndUqKeysRes


# noinspection PyPep8Naming
def build_build_merge_stmt_params(clazz_or_inst)->BuildMergeStmtParams:
    table: str = orm_util.get_table_name(clazz_or_inst)
    usingSourceSelectCols: List[str] =[]
    primaryKeyFields: List[str] =[]
    updateColumns: List[str] =[]
    insertColumns: List[str]=[]
    uniqueKeyFields: List[str] =[]
    autoIncCol: Optional[str] =None
    _changeResultIdDataType: Optional[str] = None

    for field in dataclasses.fields(clazz_or_inst):
        sql_field_name = orm_util.get_sql_field_name_from_dataclass_field(field)
        usingSourceSelectCols.append(sql_field_name)
        is_pk = orm_util.get_meta_pk(field.metadata)
        if is_pk:
            primaryKeyFields.append(sql_field_name)

        exclude_in_upd = orm_util.get_meta_exclude_in_update(field.metadata)
        if not exclude_in_upd:
            updateColumns.append(sql_field_name)

        exclude_in_ins = orm_util.get_meta_exclude_in_insert(field.metadata)
        if not exclude_in_ins:
            insertColumns.append(sql_field_name)

        is_uq_key = orm_util.get_meta_uq_key(field.metadata)
        if is_uq_key:
            uniqueKeyFields.append(sql_field_name)

        is_auto_inc = orm_util.get_meta_auto_inc(field.metadata)
        if is_auto_inc:
            autoIncCol = sql_field_name

    changeResultIdDataType="BigInt"
    return  BuildMergeStmtParams(
        table,  usingSourceSelectCols,  primaryKeyFields,
        updateColumns, insertColumns, uniqueKeyFields,
        autoIncCol,  changeResultIdDataType
    )


def get_merge_sql(clazz_or_inst: OhCalcedBinPkBase)->str:
    return  build_merge_stmt(build_build_merge_stmt_params(clazz_or_inst))


def build_where_exprs(fields_list:List[str])->str:
    where_exprs:List[str]=[]
    for delete_where_field in fields_list:
        where_exprs.append("%s =%%s" % (delete_where_field))

    return " AND ".join(where_exprs)


# noinspection PyPep8Naming
def get_delete_sql(inst:OhCalcedBinPkBase)->str:
    table: str = orm_util.get_table_name(inst)
    pkAndUqKeysRes:PkAndUqKeysRes = orm_util.extract_pk_and_uqs(inst)
    delete_where_fields:List[str]
    if len(pkAndUqKeysRes.uqkeys) !=0:
        delete_where_fields = pkAndUqKeysRes.uqkeys
    else:
        delete_where_fields = pkAndUqKeysRes.pks

    return "delete from %s where %s" % (table, build_where_exprs(delete_where_fields))


def get_select_expr(clazz_or_inst, fn_get_fields_list:Callable[[BuildMergeStmtParams], List[str]])->str:
    merge_params = build_build_merge_stmt_params(clazz_or_inst)
    sel_field_expr = ",".join(merge_params.usingSourceSelectCols)
    where_fields :List[str] = fn_get_fields_list(merge_params)
    if (len(where_fields)==0):
        raise Exception("where_fields==0 bei %s" % merge_params)

    return "SELECT %s from %s where %s " % (sel_field_expr, merge_params.table, build_where_exprs(where_fields))



def get_select_by_pk_sql(clazz_or_inst)->str:
    return get_select_expr(clazz_or_inst, lambda merge_params: merge_params.primaryKeyFields)


def get_select_by_uq_sql(clazz_or_inst)->str:
    return get_select_expr(clazz_or_inst, lambda merge_params: merge_params.uniqueKeyFields)
