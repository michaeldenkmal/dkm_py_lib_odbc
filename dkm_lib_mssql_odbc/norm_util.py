import logging
from dataclasses import dataclass
from typing import Dict, List, Any, Callable, Optional

from dkm_lib_mssql_odbc import  mssql_crud_util, mssql_types
from dkm_lib_odbc import odbc_util


@dataclass
class TFillRecDataOpts:
    ignored_fields:Optional[List[str]] =None
    prop_getter_map: Optional[Dict[str, Callable[[Dict, Any], Any]]] =None


def _fill_rec_data(raw_row,r, opts:TFillRecDataOpts, field_mapping:Dict[str,str]):

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

    for key in r.__dict__.keys():
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
            r.__setattr__(key, value)
    return r


def fill_recs_data(conn, sql, fn_create_rec, *params):
    return fill_recs_data_with_opts(conn, sql, fn_create_rec, TFillRecDataOpts(),*params)


def fill_recs_data_with_opts(conn, sql, fn_create_rec, opts:TFillRecDataOpts,*params):
    recs =[]
    rows = odbc_util.get_dict_rows(conn, sql, *params)
    fieldmapping =None
    for row in rows:
        # r = TVeranstRechRec()
        # for key in r.__dict__.keys():
        #    r.__setattr__(key, row[key])
        if not fieldmapping:
            fieldmapping ={}
            for key in row.keys():
                fieldmapping[key.upper()] = key

        recs.append(_fill_rec_data(row, fn_create_rec(),opts,fieldmapping))
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
    rows = odbc_util.get_dict_rows(conn, sql)
    # if (rows.isEmpty()) {
    if len(rows) == 0:
        return 0
    else:
        row = rows[0]
        curmax = odbc_util.get_long_val_from_row(row, "maxId")
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




