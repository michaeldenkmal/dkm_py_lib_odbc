# -*- coding: utf-8 -*-
# alter Version aus dkmlib kopiert
from typing import Any, Set, Optional, List, Protocol

import pyodbc  # type ignore
# import mssqlMergeSql
from dkm_lib_pure import dkm_reflect_util

import datetime
import decimal

from dkm_lib_mssql_odbc import mssql_merge_sql
from dkm_lib_mssql_odbc.dbshared_conf import IDbConf
from dkm_lib_mssql_odbc.mssql_merge_sql import BuildMergeStmtParams
from dkm_lib_odbc import odbc_util
from dkm_lib_odbc.odbc_util import TDbConf, get_dict_rows, get_long_val_from_row, iter_query
from dkm_lib_orm import orm_util

MAP_MERGE_SQL_STMTS = dict()


class TOrmSchema(object):
    def __init__(self, table_name: str,
                 pks: Optional[List[str]] = None,
                 use_bin:bool=True,
                 uq_keys: Optional[List[str]] = None,
                 auto_inc_col:Optional[str]=None,
                 change_result_id_data_type:Optional[str]=None):

        self.table_name = table_name
        if pks is None:
            self.pks = ["id"]
        else:
            self.pks = pks

        self.useBin = use_bin
        self.uqKeys = uq_keys
        self.autoIncCol = auto_inc_col
        self.changeResultIdDataType = change_result_id_data_type


def get_merge_sql_by_tab(tab):
    if tab in MAP_MERGE_SQL_STMTS:
        return MAP_MERGE_SQL_STMTS.get(tab)
    return None


def add_merge_sql(tab, sql):
    MAP_MERGE_SQL_STMTS[tab] = sql


def get_orm_query_result(conn, sql, clazz, *params):
    ret = []

    def on_row(row_map):
        row = orm_util.fill_orm_obj(clazz, row_map)
        ret.append(row)
        return True

    iter_query(conn, sql, on_row, params)
    return ret


def get_orm_first_row(conn, sql, clazz, *params) -> Optional[Any]:
    """
    rückgabe None, wenn nichts gefunden
    :param conn:
    :param sql:
    :param clazz:
    :return:
    """
    rows = []

    def on_row(row_map):
        rows.append(orm_util.fill_orm_obj(clazz, row_map))
        return False

    odbc_util.iter_query(conn, sql, on_row, params)
    if len(rows) > 0:
        return rows[0]
    return None


def get_by_id(conn, sql, clazz, schema, pk_value):
    assert (isinstance(sql, str))
    assert (isinstance(schema, TOrmSchema))
    assert (schema.pks is not None)
    assert (len(schema.pks) > 0)
    sql = "select * from %s where %s= %%s " % (schema.table_name, schema.pks[0])
    return get_orm_first_row(conn, sql, clazz, pk_value)


def _get_cols_from_orm_clazz(row):
    return list(row.__dict__.keys())


def _get_update_cols_from_orm_class(row, orm_schema):
    all_cols = _get_cols_from_orm_clazz(row)
    pks = orm_schema.pks
    return [x for x in all_cols if not pks.__contains__(x)]


def _get_insert_cols_from_orm_class(row, orm_schema):
    all_cols = _get_cols_from_orm_clazz(row)
    if orm_schema.autoIncCol is None:
        return all_cols
    return [x for x in all_cols if not x == orm_schema.autoIncCol]


# noinspection PyPep8Naming
def _build_merge_sql(row, orm_schema):
    assert (isinstance(orm_schema, TOrmSchema))
    table = orm_schema.table_name
    usingSourceSelectCols = _get_cols_from_orm_clazz(row)
    primaryKeyFields = orm_schema.pks
    updateColumns = _get_update_cols_from_orm_class(row, orm_schema)
    insertColumns = _get_insert_cols_from_orm_class(row, orm_schema)
    uniqueKeyFields = orm_schema.uqKeys
    autoIncCol = orm_schema.autoIncCol
    changeResultIdDataType = orm_schema.changeResultIdDataType
    return mssql_merge_sql.build_merge_stmt(
        BuildMergeStmtParams(
            table, usingSourceSelectCols, primaryKeyFields,
            updateColumns, insertColumns, uniqueKeyFields,
            autoIncCol, changeResultIdDataType))


def get_merge_sql(orm_clazz, orm_schema):
    assert (isinstance(orm_schema, TOrmSchema))
    ret = get_merge_sql_by_tab(orm_schema.table_name)
    if ret is None:
        ret = _build_merge_sql(orm_clazz, orm_schema)
        add_merge_sql(orm_schema.table_name, ret)
    return ret


"""
String sql = "set NOCOUNT ON\n"
                    + "Insert into " + ptrTableName + "_sq values ('a')\n"
                    + "select @@IDENTITY ";
"""


def get_next_id_sq(conn, table_name):
    sql = """
    set NOCOUNT ON
    Insert into %s_sq values ('a')
    select @@IDENTITY as nextId
    """ % table_name
    ret = []

    def on_row(row):
        ret.append(row["nextId"])
        return False

    odbc_util.iter_query(conn, sql, on_row)
    return ret[0]


def get_next_id_sq_with_bin(conf: IDbConf, table_name:str):
    new_id = get_next_id_sq(conf.conn, table_name)
    return (new_id * conf.bin_fakt) + conf.bin


def save_with_get_next_id_and_bin_fakt_login_name_and_lu(conf, row, schema):
    assert (isinstance(schema, TOrmSchema))
    if row.id is None:
        row.id = get_next_id_sq_with_bin(conf, schema.table_name)
    row.login_name = conf.loginName
    row.lu = datetime.datetime.now()
    return save(conf, row, schema)


def set_login_name_and_lu_in_row(conf, row):
    row.login_name = conf.loginName
    row.lu = datetime.datetime.now()


def row_to_params(row):
    pars_list = []
    for v in row.__dict__.values():
        pars_list.append(v)
    return tuple(pars_list)


def save(conf, row, schema):
    merge_sql = get_merge_sql(row, schema)
    params = row_to_params(row)
    try:
        # * damit das Tuple aufgelöst wird
        odbc_util.exec_query(conf.conn, merge_sql, *params)
    except pyodbc.ProgrammingError as ex:
        szpars = []
        idx = 0
        for param in params:
            szpars.append("%d: %s" % (idx, param))
            idx += 1
        raise Exception("%s: bei sql '%s', params:\n%s" % (ex, merge_sql, "\n".join(szpars)))
    return row


# noinspection PyListCreation
def gen_delete_sql(row, schema):
    assert (isinstance(schema, TOrmSchema))
    params = []
    sb = []
    sb.append("delete from %s where" % schema.table_name)
    sbw = []
    if schema.pks is not None:
        for pk in schema.pks:
            sbw.append("%s= %%s " % pk)
            params.append(row[pk])
    sb.append(" AND ".join(sbw))
    return "\n".join(sb), params


def delete(conf, row, schema):
    sql, params = gen_delete_sql(row, schema)
    odbc_util.exec_query(conf.conn, sql, tuple(params))


"""
    public static double getNextMaxId(IDbConf conf, String tablename, String pk, boolean withbin) {
        String SQL = String.format(
                "select max(coalesce(%s,0)) from %s", pk, tablename);
        Double curmax;

        long curIntId;
        int curBin = conf.getBin();
        int curBinFakt = conf.getBinFakt();

        List rows = TDbUtil.getQueryResult(conf, SQL, null);

        if (rows.isEmpty()) {
            curmax = 0.0;
        } else {
            curmax = (Double) rows.get(0);
            if (curmax == null) {
                curmax = 0.0;
            }
        }

        if (withbin) {
            if (curmax == 0.0) {
                curIntId = 0;
            } else {
                curIntId = curmax.longValue();
                curIntId = (curIntId / curBinFakt);
            }
            curIntId = curIntId + 1;
            return (curIntId * curBinFakt) + curBin;
        } else {
            return curmax + 1.0;
        }
    }
"""


# noinspection PyPep8Naming
def get_next_max_id(conf: TDbConf, table_name: str, pk: str, with_bin: bool):
    sql = "select max(coalesce(%s,0)) maxId from %s" % (pk, table_name)
    curBin = conf.i_bin
    curBinFakt = conf.binfakt
    # List rows = TDbUtil.getQueryResult(conf, SQL, null);
    rows = get_dict_rows(conf.conn, sql)
    # if (rows.isEmpty()) {
    if len(rows) == 0:
        return 0
    else:
        row = rows[0]
        curmax = get_long_val_from_row(row, "maxId")
        if curmax is None:
            curmax = 0

        if with_bin:
            if curmax == 0:
                curIntId = 0
            else:
                # curIntId = curmax.longValue();
                curIntId = curmax
                curIntId = (curIntId / curBinFakt)
            curIntId = curIntId + 1
            return (curIntId * curBinFakt) + curBin
        else:
            return curmax + 1.0


def get_lu():
    return datetime.datetime.now()


class ProtRecIdLuLoginName(Protocol):
    id: Optional[float]
    lu: Optional[datetime]
    login_name: Optional[str]



def set_fields_in_sq_id_lu_login_name_rec(conf:IDbConf, table_name:str, rec:ProtRecIdLuLoginName):
    if (rec.id == 0) or (rec.id is None):
        rec.id = get_next_id_sq_with_bin(conf, table_name)
    rec.lu = get_lu()
    rec.login_name = conf.login_name


class TColType(object):

    def __init__(self, col_type):
        self.col_type = col_type

    def __repr__(self):
        return self.col_type


class TColTypes:
    STRING = TColType("string")
    INT = TColType("int")
    FLOAT = TColType("float")
    NUMERIC = TColType("numeric")
    BOOL = TColType("bool")
    DATETIME = TColType("datetime")

    @staticmethod
    def is_valid_col_type(col_type):
        assert (isinstance(col_type, TColType))
        col_types = [cd for cd in list(TColTypes.__dict__.values()) if
                     isinstance(cd, TColType) and cd.col_type == col_type.col_type]
        return col_types.__len__() > 0


class TColDef(object):

    def __init__(self, col_name, col_type=None, col_size=None, not_null=None):
        assert (isinstance(col_name, str))
        self.col_name = col_name
        self.col_type = TColTypes.STRING
        if col_type is not None:
            assert (TColTypes.is_valid_col_type(col_type))
            self.col_type = col_type

        if col_size is not None:
            assert (isinstance(col_size, int))
            self.col_size = col_size
        else:
            if self.col_type == TColTypes.STRING:
                self.size = 255

        self.not_null = False
        if not_null is not None:
            assert (isinstance(not_null, bool))
            self.not_null = not_null


class TPkDef(object):

    def __init__(self, colnames: Set[str]):
        self.colnames = colnames


class TTableDef(object):

    def __init__(self, table_name, col_defs, pk_cols=None):
        assert (isinstance(table_name, str))
        self.table_name = table_name
        # assert (isinstance(col_defs, dict))
        self.col_defs = dict()
        for key in list(col_defs.keys()):
            col_def = col_defs[key]
            if (isinstance(col_def, TColDef)):
                self.col_defs[key] = col_def
        self.pk_cols = ["id"]
        if pk_cols is not None:
            assert (isinstance(pk_cols, list))
            for pk in pk_cols:
                assert (isinstance(pk, str))
            self.pk_cols = pk_cols


def _validate_field_string(field_value):
    return isinstance(field_value, str)


def _validate_field_int(field_value):
    return isinstance(field_value, int)


def _validate_field_float(field_value):
    return isinstance(field_value, float)


def _validate_field_numeric(field_value):
    return isinstance(field_value, decimal.Decimal)


def _validate_field_bool(field_value):
    return isinstance(field_value, bool)


def _validate_field_datetime(field_value):
    return isinstance(field_value, datetime.datetime)


COL_DEF_VALIDATOR = {
    TColTypes.STRING.col_type: _validate_field_string,
    TColTypes.INT.col_type: _validate_field_int,
    TColTypes.FLOAT.col_type: _validate_field_float,
    TColTypes.NUMERIC.col_type: _validate_field_numeric,
    TColTypes.BOOL.col_type: _validate_field_bool,
    TColTypes.DATETIME.col_type: _validate_field_bool
}


class EOrmDAOValidatorErr(Exception):
    pass


def sort_col_names(col_names):
    assert (isinstance(col_names, list))
    assert (dkm_reflect_util.isListOf(col_names, str))
    return sorted(col_names)


def col_names_wo_pk(col_defs, pk_defs):
    assert (isinstance(col_defs, dict))
    assert (dkm_reflect_util.isListOf(list(col_defs.values()), TColDef))
    assert (isinstance(pk_defs, list))
    assert (dkm_reflect_util.isListOf(pk_defs, str))
    return [col_name for col_name in col_names_from_col_defs(col_defs) if not pk_defs.__contains__(col_name)]


def col_names_from_col_defs(col_defs):
    assert (isinstance(col_defs, dict))
    assert (dkm_reflect_util.isListOf(list(col_defs.values()), TColDef))
    return sort_col_names([col_def.col_name for col_def in list(col_defs.values())])


def get_sorted_row_values(row):
    assert (isinstance(row, object))
    keys = sorted(row.__dict__.keys())
    values = []
    for key in keys:
        values.append(row.__getattribute__(key))
    return values


class TOrmDAO(object):

    def __init__(self, table_def, row_clazz, use_sq_4_pk=None, use_bin=None, auto_stamp=None):
        assert (isinstance(table_def, TTableDef))
        self.table_def = table_def
        self.row_clazz = row_clazz
        self.use_sq_4_pk = True
        if use_sq_4_pk is not None:
            assert (isinstance(use_sq_4_pk, bool))
            self.use_sq_4_pk = use_sq_4_pk
        self.use_bin = True
        if use_bin is not None:
            assert (isinstance(use_bin, bool))
            self.use_bin = use_bin
        self.auto_stamp = True
        if auto_stamp is not None:
            assert (isinstance(auto_stamp, bool))
            self.auto_stamp = auto_stamp
        self._merge_sql = None

    def validate_fields(self, row):
        for col_def in self.table_def.col_defs:
            try:
                field_value = row.__dict__[col_def.col_name]
            except KeyError:
                field_value = None
            if not self.validate_field_value(col_def, field_value):
                return False
        return True

    @staticmethod
    def validate_field_value(col_def, field_value):
        assert (isinstance(col_def, TColDef))
        if col_def.not_null:
            if col_def.col_type == TColTypes.STRING:
                if field_value == "":
                    raise EOrmDAOValidatorErr("%s darf nicht null sein" % col_def.col_name)
            else:
                if field_value is None:
                    raise EOrmDAOValidatorErr("%s darf nicht null sein" % col_def.col_name)
        if field_value is None:
            return True
        if col_def.col_type == TColTypes.STRING:
            if len(field_value) > col_def.col_size:
                raise EOrmDAOValidatorErr("Wert von Field %s ist zu lang: maximal %d ist aber %d: Wert: %s"
                                          % (col_def.col_name, col_def.col_size, len(field_value), field_value)
                                          )
        try:
            if not COL_DEF_VALIDATOR[col_def.col_type](field_value):
                raise EOrmDAOValidatorErr(
                    "Field %s hat nicht den Type %s bei wert %s" % (col_def.col_name, col_def.col_type, field_value))
        except KeyError:
            raise EOrmDAOValidatorErr("FieldType %s fehler cin COL_DEF_VALIDATOR map" % col_def.col_type)

    def save(self, conf, row):
        assert (isinstance(row, self.row_clazz))
        if (row.id is None) or (row.id == 0):
            if self.use_sq_4_pk:
                if self.use_bin:
                    row.id = get_next_id_sq_with_bin(conf, self.table_def.table_name)
                else:
                    row.id = get_next_id_sq(conf.conn, self.table_def.table_name)
        if self.auto_stamp:
            row.lu = get_lu()
            row.login_name = conf.loginName
        params = get_sorted_row_values(row)
        odbc_util.exec_query(conf.conn, self.get_merge_sql(), *params)
        return row

    def get_merge_sql(self):
        if self._merge_sql is not None:
            return self._merge_sql
        self._merge_sql = mssql_merge_sql.build_merge_stmt(
            BuildMergeStmtParams(
                table=self.table_def.table_name,
                usingSourceSelectCols=col_names_from_col_defs(self.table_def.col_defs),
                primaryKeyFields=self.table_def.pk_cols,
                updateColumns=col_names_wo_pk(self.table_def.col_defs, self.table_def.pk_cols),
                insertColumns=col_names_from_col_defs(self.table_def.col_defs)
            )
        )
        return self._merge_sql

    def delete_by_id(self, conn:pyodbc.Connection, i_id):
        sql = "DELETE FROM %s WHERE ID=%%s" % (self.table_def.table_name)
        odbc_util.exec_query(conn, sql, i_id)
