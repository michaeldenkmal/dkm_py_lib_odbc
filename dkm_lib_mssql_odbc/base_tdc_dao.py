import dataclasses
from abc import ABC, abstractmethod
from typing import List, Any, TypeVar, Generic, Optional

from dkm_lib_mssql_odbc import mssql_merge_sql, mssql_crud_util
from dkm_lib_odbc import odbc_util
from dkm_lib_odbc.odbc_util import TDbConf
from dkm_lib_mssql_odbc.mssql_merge_sql import BuildMergeStmtParams
from dkm_lib_mssql_odbc.mssql_types import TDC, PrimaryKeyInfo
from dkm_lib_mssql_odbc.norm_util import fill_recs_data

T = TypeVar("T", bound=dataclasses.dataclass)  # T darf nur BaseModel oder Subklassen sein)


class BaseTdcDao(ABC, Generic[T]):

    merge_params: BuildMergeStmtParams
    merge_sql: str
    delete_sql: str

    def __init__(self):
        self.merge_params = self.build_merge_info()
        self.merge_sql = mssql_merge_sql.build_merge_stmt(self.merge_params)
        where_expr = mssql_crud_util.primary_key_info_to_where_sql(self.merge_params.primaryKeyFields)
        self.delete_sql = f"delete from {self.merge_params.table} where {where_expr}"

    @abstractmethod
    def build_merge_info(self) -> BuildMergeStmtParams:
        pass

    @abstractmethod
    def create_new_row(self) -> T:
        pass

    @abstractmethod
    def get_pk_values(self, row: T) -> PrimaryKeyInfo:
        pass

    def save(self, conf:TDbConf, row:T)->T:
        db_params = self.get_merge_params(row)
        odbc_util.exec_query(conf.conn, self.merge_sql, *db_params)
        return row

    def select_sel_columns(self)->List[str]:
        return self.merge_params.usingSourceSelectCols

    @staticmethod
    def get_merge_params(row:TDC)->List[Any]:
        return mssql_crud_util.build_dbparams(row)

    def get_sel_and_from_part(self)->str:
        selcolsexpr = ",".join(self.select_sel_columns())
        return f"select {selcolsexpr} from {self.merge_params.table}"

    def query_to_list(self, conf:TDbConf, sql:str,dbparams:List[Any])->List[T]:
        return fill_recs_data(conf.conn, sql, self.create_new_row, *dbparams)

    def query_first(self, conf:TDbConf, sql:str, dbparams:List[Any])->Optional[T]:
        rows = self.query_to_list(conf,sql, dbparams)
        if (len(rows)>0):
            return rows[0]
        else:
            return None

    def query_by_where_expr(self, conf:TDbConf, where_sql:str, dbparams:List[Any])->List[T]:
        sql_sel_from = self.get_sel_and_from_part()
        sql= f"{sql_sel_from} where {where_sql}"
        return self.query_to_list(conf, sql, dbparams)

    def query_by_where_expr_first(self, conf:TDbConf, where_sql:str, dbparams:List[Any])->Optional[T]:
        rows= self.query_by_where_expr(conf,where_sql,dbparams)
        if (len(rows)>0):
            return rows[0]
        else:
            return None

    def delete(self,conf:TDbConf, row:T):
        dbparams = self.get_pk_values(row)
        odbc_util.exec_query(conf.conn, self.delete_sql, *dbparams)


