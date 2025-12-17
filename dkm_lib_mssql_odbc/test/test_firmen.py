from dataclasses import dataclass
from typing import Optional

from dkm_lib_mssql_odbc import mssql_crud_util
from dkm_lib_mssql_odbc.dbshared_conf import IDbConf
from dkm_lib_mssql_odbc.mssql_merge_sql import BuildMergeStmtParams
from dkm_lib_mssql_odbc.oh_calced_bin_pk_base_intf import OhCalcedBinPkBaseIntf, set_ocb_def_fields, \
    build_ocb_merge_stmt_params

"""
CREATE TABLE [dbo].[test_firmen](
	[local_id] [bigint] IDENTITY(1,1) NOT NULL,
	[local_bin] [int] NOT NULL,
	[local_binfakt] [int] NOT NULL,
	[id]  AS ([local_id]*[local_binfakt]+[local_bin]) PERSISTED NOT NULL,
	[lu] [datetime] NOT NULL,
	[created_on] [datetime] NOT NULL,
	[login_name] [varchar](10) NOT NULL,
	suchbegriff varchar(100) not null,
	bez1 varchar(70) not null,
	bez2 varchar(70)
 CONSTRAINT [test_firmen_pk] PRIMARY KEY 
(
	[id] ASC
)) 
GO
"""


@dataclass
class TestFirmen(OhCalcedBinPkBaseIntf):
    suchbegriff: Optional[str] = None
    bez1: Optional[str] = None
    bez2: Optional[str] = None

TABLE = "test_firmen"

TestFirmen_bmsp: BuildMergeStmtParams = build_ocb_merge_stmt_params(
    dataclazz=TestFirmen, table=TABLE, unique_key_fields=["suchbegriff"]
)


def create_new(suchbegriff: str, bez1: str, bez2: Optional[str] = None) -> TestFirmen:
    row = TestFirmen()
    row.suchbegriff = suchbegriff
    row.bez1 = bez1
    row.bez2 = bez2
    return row


def save(conf: IDbConf, row: TestFirmen) -> TestFirmen:
    set_ocb_def_fields(conf, row)
    return mssql_crud_util.merge_data_row_ocb(conf, row, TestFirmen_bmsp)
