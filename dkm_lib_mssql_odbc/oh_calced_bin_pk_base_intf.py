from dataclasses import dataclass
from datetime import datetime
from typing import List, TypeVar, Optional

from dkm_lib_mssql_odbc import mssql_crud_util
from dkm_lib_mssql_odbc.dbshared_conf import IDbConf
from dkm_lib_mssql_odbc.mssql_merge_sql import BuildMergeStmtParams


@dataclass
class OhCalcedBinPkBaseIntf:
    local_id: Optional[int] = None
    local_bin: Optional[int] = None
    local_binfakt: Optional[int] = None
    id: Optional[int] = None
    lu: Optional[datetime] = None
    created_on: Optional[datetime] = None
    login_name: Optional[str] = None

TOCB = TypeVar("TOCB", bound=OhCalcedBinPkBaseIntf)


def build_ocb_merge_stmt_params(dataclazz,table:str,
                                  unique_key_fields: Optional[List[str]])->BuildMergeStmtParams:

    exclude_cols =["id","local_id"]

    def fn_filter(col):
        return not exclude_cols.__contains__(col)

    all_cols = mssql_crud_util.get_using_source_select_cols(dataclazz)
    cols_wo_id = list(filter(fn_filter, all_cols))

    return BuildMergeStmtParams(
        table = table,
        usingSourceSelectCols = all_cols,
        primaryKeyFields = ["id"],
        updateColumns = cols_wo_id,
        insertColumns = cols_wo_id,
        uniqueKeyFields = unique_key_fields,
        autoIncCol = "id",
        changeResultIdDataType ="bigint"
    )


def set_ocb_def_fields( conf:IDbConf, row:TOCB)->TOCB:
    lu = datetime.now()
    if row.created_on is None:
        row.created_on = lu
    row.lu = lu
    row.login_name = conf.login_name
    row.local_bin = conf.bin
    row.local_binfakt = conf.bin_fakt
    return row

