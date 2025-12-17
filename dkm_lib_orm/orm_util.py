import dataclasses
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import List, Dict, Any, Optional


@dataclass
class FkMapping:
    fk_table_name:str
    fk_field_name:str="id"




class _MetaKeyNames(Enum):
    PRIMARY_KEY="pk"
    SQL_TYPE="sql_Type"
    FIELD_LEN="field_len"
    FIELD_NAME="field_name"
    NOT_NULL="not_null"
    DEFAULT_VALUE="default_value"
    AUTO_INC="auto_inc"
    UQ_KEY="uq_key"
    FOREIGN_KEY="fk_key"
    EXCLUDE_IN_INSERT="included_in_insert"
    EXCLUDE_IN_UPDATE="inlcude_in_update"
    COMPUTED_FORMULA="computed_formula"
    DATE_ONLY="date_only"


_ORM_TABLE_NAME="___DKM_ORM_TABLE_NAME___"
_ORM_FK_MAPPING="___DKM_ORM_FK_MAPPING___"



class SqlType(Enum):
    VARCHAR="varchar"
    DOUBLE="double"
    DATETIME="datetime"
    INT="int"
    BIG_INT="bigint"
    DECIMAL="DECIMAL"
    BOOL="bool"


def set_meta_pk(meta_map:dict, value:bool):
    meta_map[_MetaKeyNames.PRIMARY_KEY.value]=value

def get_meta_pk(meta_map:MappingProxyType)->bool:
    try:
        return meta_map[_MetaKeyNames.PRIMARY_KEY.value]
    except KeyError:
        return False


def set_meta_sql_type(meta_map:dict, sql_type:SqlType):
    meta_map[_MetaKeyNames.SQL_TYPE.value] = sql_type


def get_meta_sql_type(meta_map:MappingProxyType)->SqlType:
    try:
        return meta_map[_MetaKeyNames.SQL_TYPE.value]
    except KeyError:
        return SqlType.VARCHAR


def set_meta_field_len(meta_map:dict, field_len:int):
    meta_map[_MetaKeyNames.FIELD_LEN.value] = field_len


def get_meta_field_len(meta_map:MappingProxyType)->int:
    try:
        return meta_map[_MetaKeyNames.FIELD_LEN.value]
    except KeyError:
        return 0


#field_name: str = None,
def set_meta_field_name(meta_map:dict, field_name:str):
    meta_map[_MetaKeyNames.FIELD_NAME.value] = field_name


def get_meta_field_name(meta_map:MappingProxyType)->str:
    try:
        return meta_map[_MetaKeyNames.FIELD_NAME.value]
    except KeyError:
        return ""


def lower_case_keys(a_map):
    assert isinstance(a_map, dict)
    ret = dict()
    for key in list(a_map.keys()):
        lc_key = key.lower()
        ret[lc_key] = a_map[key]
    return ret


def fill_orm_obj(clazz, row_map):
    """
    befüllt eine Klass anhand der Properties von obj
    :param clazz:
    :param row_map:
    :return:
    """
    ret = clazz()
    lc_key_row_map = lower_case_keys(row_map)
    for key in list(ret.__dict__.keys()):
        try:
            lc_key = key.lower()
            value = lc_key_row_map.get(lc_key, None)
        except KeyError:
            value = None
        ret.__setattr__(key, value)
    return ret




#not_null: bool = False,
def set_meta_not_null(meta_map:dict, not_null:bool):
    meta_map[_MetaKeyNames.NOT_NULL.value] = not_null


def get_meta_not_null(meta_map:MappingProxyType)->bool:
    try:
        return meta_map[_MetaKeyNames.NOT_NULL.value]
    except KeyError:
        return False


#default_value: str = None
def set_meta_default_value(meta_map:dict, default_value:str):
    meta_map[_MetaKeyNames.DEFAULT_VALUE.value] = default_value


def get_meta_default_value(meta_map:MappingProxyType)->str:
    try:
        return meta_map[_MetaKeyNames.DEFAULT_VALUE.value]
    except KeyError:
        return ""

#    UQ_KEY="uq_key"
def set_meta_uq_key(meta_map:Dict[str,Any], value:bool):
    meta_map[_MetaKeyNames.UQ_KEY.value]=value


def get_meta_uq_key(meta_map:MappingProxyType)->bool:
    try:
        return meta_map[_MetaKeyNames.UQ_KEY.value]
    except KeyError:
        return False


#    FOREIGN_KEY="fk_key"
def set_meta_fk_key(meta_map:Dict[str, Any], value:FkMapping):
    meta_map[_MetaKeyNames.FOREIGN_KEY.value] = value


def get_meta_fk_key(meta_map:MappingProxyType) -> Optional[FkMapping]:
    try:
        return meta_map[_MetaKeyNames.FOREIGN_KEY.value]
    except KeyError:
        return None


# auto_inc
def set_meta_auto_inc(meta_map:dict, value:bool):
    meta_map[_MetaKeyNames.AUTO_INC.value] = value


def get_meta_auto_inc(meta_map:MappingProxyType)->bool:
    try:
        return meta_map[_MetaKeyNames.AUTO_INC.value]
    except KeyError:
        return False


# exclude_in_insert
def set_meta_exclude_in_insert(meta_map:Dict[str, Any], value:bool):
    meta_map[_MetaKeyNames.EXCLUDE_IN_INSERT.value] = value


def get_meta_exclude_in_insert(meta_map:MappingProxyType)->bool:
    try:
        return meta_map[_MetaKeyNames.EXCLUDE_IN_INSERT.value]
    except KeyError:
        return False


# exclude_in_update
def set_meta_exclude_in_update(meta_map:Dict[str, Any], value:bool):
    meta_map[_MetaKeyNames.EXCLUDE_IN_UPDATE.value] = value


def get_meta_exclude_in_update(meta_map:MappingProxyType)->bool:
    try:
        return meta_map[_MetaKeyNames.EXCLUDE_IN_UPDATE.value]
    except KeyError:
        return False


#COMPUTED_FORMULA
def set_meta_computed_formula(meta_map:Dict[str, Any], formula:str):
    meta_map[_MetaKeyNames.COMPUTED_FORMULA.value] = formula


def get_meta_computed_formula(meta_map:MappingProxyType)->str:
    try:
        return meta_map[_MetaKeyNames.COMPUTED_FORMULA.value]
    except KeyError:
        return ""


#DATE_ONLY
def set_meta_date_only(meta_map:Dict[str, Any], value:bool):
    meta_map[_MetaKeyNames.DATE_ONLY.value] = value


def get_meta_date_only(meta_map:MappingProxyType)->bool:
    try:
        return meta_map[_MetaKeyNames.DATE_ONLY.value]
    except KeyError:
        return False


def orm_field(pk:bool=False, sql_type:SqlType=SqlType.VARCHAR,field_len:int=0,
                    field_name:str=None,
                    not_null:bool=False,
                    default_value:str=None,
                    uq_key_field:bool=False,
                    fk:FkMapping=None,
                    auto_inc:bool=False,
                    exclude_in_insert:bool=False,
                    exclude_in_update:bool=False,
                    computed_formula:str="",
                    date_only:bool = False
                   ):
    return field(init=False,default=None,metadata=build_meta_map(pk=pk,
                                         sql_type=sql_type,
                                         field_len=field_len,
                                         field_name=field_name,
                                         not_null=not_null,
                                         default_value=default_value,
                                         uq_key_field=uq_key_field,
                                         fk_mapping=fk,
                                         auto_inc=auto_inc,
                                         exclude_in_insert=exclude_in_insert,
                                         exclude_in_update=exclude_in_update,
                                         computed_formula=computed_formula,
                                         date_only = date_only
                                         ))


def build_meta_map(pk:bool=False, sql_type:SqlType=SqlType.VARCHAR,field_len:int=0,
                   field_name:str=None,
                   not_null:bool=False,
                   default_value:str=None,
                   uq_key_field:bool=False,
                   fk_mapping:FkMapping=None,
                   auto_inc:bool=False,
                   exclude_in_insert: bool = False,
                   exclude_in_update: bool = False,
                   computed_formula: str = "",
                   date_only:bool = False
                   )->Dict[str,Any]:
    ret =dict()
    if pk:
        set_meta_pk(ret,True)
    if sql_type.VARCHAR.value != sql_type.value:
        set_meta_sql_type(ret,sql_type)
    if field_len !=0:
        set_meta_field_len(ret, field_len)
    if field_name:
        set_meta_field_name(ret, field_name)
    if not_null:
        set_meta_not_null(ret, not_null)
    if default_value:
        set_meta_default_value(ret, default_value)
    if uq_key_field:
        set_meta_uq_key(ret, uq_key_field)
    if fk_mapping:
        set_meta_fk_key(ret, fk_mapping)
    if auto_inc:
        set_meta_auto_inc(ret, auto_inc)
    if exclude_in_insert:
        set_meta_exclude_in_insert(ret, exclude_in_insert)
    if exclude_in_update:
        set_meta_exclude_in_update(ret, exclude_in_update)
    if computed_formula:
        set_meta_computed_formula(ret, computed_formula)
    if date_only:
        set_meta_computed_formula(ret, str(date_only))
    return ret


# noinspection PyShadowingNames
def is_meta_pk_field(field:dataclasses.Field)->bool:
    return get_meta_pk(field.metadata)


def get_pks_from_orm_class(cls)->List[str]:
    ret =[]
    fields = dataclasses.fields(cls)
    for afield in fields:
        if is_meta_pk_field(afield):
            ret.append(afield.name)
    return ret





def dkm_orm_class(_cls=None, table_name:str=None, fks:List[FkMapping]=None):
    def wrapper(cls):
        if table_name:
            setattr(cls,_ORM_TABLE_NAME, table_name)
        if fks:
            setattr(cls,_ORM_FK_MAPPING, fks)
        return cls
    return wrapper


def get_table_name(cls_or_inst)->str:
    return getattr(cls_or_inst, _ORM_TABLE_NAME)

## TESTE Start here

@dataclass
@dkm_orm_class(table_name="kk_variable")
class KkVariable:
    v:str=field(metadata=build_meta_map(pk=True))
    w:str





def test_kk_varible():

    def get_pks(row):
        print(",".join(get_pks_from_orm_class(row)))


    my_row = KkVariable(v="Kurzame",w="wurscht")
    get_pks(my_row)
    get_pks(KkVariable)
    print(get_table_name(my_row))
    print(get_table_name(KkVariable))


#test_kk_varible()


def get_sql_field_name_from_dataclass_field(dataclass_field:dataclasses.Field)->str:
    ret = get_meta_field_name(dataclass_field.metadata)
    if not ret:
        ret = dataclass_field.name
    return ret


@dataclass
class PkAndUqKeysRes:
    pks:List[str]
    uqkeys:List[str]


# noinspection PyShadowingNames
def extract_pk_and_uqs(clazz_or_inst):
    pks:List[str]=[]
    uqkeys:List[str]=[]
    for field in dataclasses.fields(clazz_or_inst):
        sql_field_name = get_sql_field_name_from_dataclass_field(field)
        is_pk = get_meta_pk(field.metadata)
        if is_pk:
            pks.append(sql_field_name)

        is_uq_key = get_meta_uq_key(field.metadata)
        if is_uq_key:
            uqkeys.append(sql_field_name)

    return PkAndUqKeysRes(pks, uqkeys)


# noinspection PyShadowingNames
def get_sql_field_names(clazz_or_inst)->List[str]:
    ret=[]
    for g_field in dataclasses.fields(clazz_or_inst):
        sql_field_name = get_sql_field_name_from_dataclass_field(g_field)
        ret.append(sql_field_name)
    return ret

