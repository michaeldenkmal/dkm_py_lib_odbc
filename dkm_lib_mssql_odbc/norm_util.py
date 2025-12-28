import logging
from dataclasses import dataclass, fields
from typing import Dict, List, Any, Callable, Optional, TypeVar, Type

from dkm_lib_db.db_util import get_dict_rows, get_long_val_from_row
from dkm_lib_mssql_odbc import mssql_crud_util, mssql_types

T = TypeVar("T")

@dataclass
class TFillRecDataOpts:
    ignored_fields:Optional[List[str]] =None
    prop_getter_map: Optional[Dict[str, Callable[[Dict, Any], Any]]] =None


# def fetch_rows_lazy(
#         conn:DbConn,
#         sql: str,
#         params: Sequence[Any] | None,
#         dataclazz: Type[T],
#     *,
#         batch_size: int = 5000,
#         fn_mapping:Optional[Callable[[str,any],any]] = None,
# ) -> Iterator[T]:
#     """
#     Führt eine SQL-Query aus und gibt Dataclass-Instanzen zurück.
#     Holt Daten in Batches, damit auch sehr große Tabellen streamend
#     verarbeitet werden können.
#     :param fn_mapping: bekommt colName and rawColValue und erwartet denn wert als rückgabe
#     wenn nichts zu ändern war, dann den übergebenen Wert wieder zurückgeben
#     """
#     # Feldnamen aus der Dataclass (nur die nehmen wir auf)
#     #  baut ein Set Comprehension.
#     # noinspection Mypy
#     dc_fields: set[str] = {f.name for f in fields( dataclazz)} # type: ignore[arg-type]
#
#     with conn.cursor() as cur:
#         try:
#             cur.execute(sql, params or [])
#         except ProgrammingError as e:
#             errmsg = f"{e} bei sql={sql} params={params}"
#             raise ProgrammingError(errmsg) from e
#
#         colnames = [c[0] for c in cur.description]
#
#         while True:
#             rows = cur.fetchmany(batch_size)
#             if not rows:
#                 break
#
#             for row in rows:
#                 # 1. Dict aus Spaltenname -> Wert aufbauen
#                 row_dict = {}
#                 for col, val in zip(colnames, row):
#                     if col in dc_fields:
#                         if fn_mapping:
#                             val = fn_mapping(col,val)
#                         row_dict[col] = val
#                     # else: Feld ignorieren (nicht in Dataclass definiert)
#
#                 # 2. Dataclass-Instanz bauen
#                 yield dataclazz(**row_dict)
#

def _fill_rec_data(raw_row, dataclazz: Type[T] ,opts:TFillRecDataOpts, field_mapping:Dict[str,str]):

    def is_ignored_field(field:str)->bool:
        if not opts.ignored_fields:
            return False
        return opts.ignored_fields.__contains__(field)

    def handle_prog_getter(field:str, cur_value:Any)->Optional[Any]:
        if not opts.prop_getter_map:
            return None
        fn_new_value:Callable[[Dict,Any],Any] = opts.prop_getter_map.get(field)
        if not fn_new_value:
            return None
        return fn_new_value(raw_row,cur_value)

    dc_fields: set[str] = {f.name for f in fields(dataclazz)}
    dc_dict = {}
    for key in dc_fields:
        if not (key.startswith("__") and key.endswith("__")) and (not is_ignored_field(key)):
            try:
                value = raw_row[key]
            except KeyError:
                uc_key = field_mapping.get(key.upper())
                try:
                    value = raw_row[uc_key]
                except KeyError as e:
                    msg = "%s: key=%s, lc_key=%s" % (e, key, uc_key)
                    logging.error(msg)
                    raise Exception(msg)
            except TypeError as e:
                msg ="%s: key=%s" % (e, key)
                print (msg)
                raise Exception(msg)
            new_value=handle_prog_getter(key, value)
            if new_value:
                value = new_value
            #r.__setattr__(key, value)
            dc_dict[key] = value
    return dataclazz(**dc_dict)


def fill_recs_data(conn, sql, dataclazz: Type[T] , *params):
    return fill_recs_data_with_opts(conn, sql, dataclazz, TFillRecDataOpts(),*params)


def fill_recs_data_with_opts(conn, sql, dataclazz: Type[T],opts:TFillRecDataOpts,*params):
    recs =[]
    rows = get_dict_rows(conn, sql, *params)
    fieldmapping =None
    for row in rows:
        # r = TVeranstRechRec()
        # for key in r.__dict__.keys():
        #    r.__setattr__(key, row[key])
        if not fieldmapping:
            fieldmapping ={}
            for key in row.keys():
                fieldmapping[key.upper()] = key

        recs.append(_fill_rec_data(
            raw_row=row, dataclazz=dataclazz, opts=opts,field_mapping=fieldmapping)
        )
    return recs


def get_select_fields_list(dataclazz):
    cols = mssql_crud_util.get_using_source_select_cols(dataclazz)
    return ",".join(cols)


def find_entity_by_id (conn, table_name:str, primary_key_info:mssql_types.PrimaryKeyInfo,
                       fn_create_rec:Callable[[],mssql_types.TDC])->Optional[mssql_types.TDC]:
    row = fn_create_rec()
    where_expr_snp = mssql_crud_util.primary_key_info_to_where_exprs(primary_key_info)
    sql =f"select {get_select_fields_list(row)} from {table_name} where {where_expr_snp.sql}  "
    rows = fill_recs_data(conn, sql,fn_create_rec, *where_expr_snp.dbparams)
    if len(rows)> 0:
        return rows[0]
    return None


# noinspection PyPep8Naming
def get_next_max_id(conn, table_name:str, pk_name:str, with_bin:bool=False, i_bin:int=0, bin_fakt=0 )->float|int:
    sql = "select max(coalesce(%s,0)) maxId from %s" % (pk_name, table_name)
    # List rows = TDbUtil.getQueryResult(conf, SQL, null);
    rows = get_dict_rows(conn, sql)
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
                #curIntId = curIntId *(bin / bin_fakt)
            curIntId = curIntId + 1
            return (curIntId * bin_fakt) + i_bin
        else:
            return curmax + 1.0




