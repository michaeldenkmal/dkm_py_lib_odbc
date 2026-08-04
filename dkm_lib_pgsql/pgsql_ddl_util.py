from dataclasses import dataclass
from typing import List, Dict, Callable

from jinja2.nodes import Literal

PGSQL_DATA_TYPE = Literal["BIGINT", "INTEGER", "TIMESTAMP", "VARCHAR", "XML"]


@dataclass(slots=True)
class DDLCol:
    col_name: str
    data_type: PGSQL_DATA_TYPE
    size: int
    default_expr: str
    not_null: bool






class DDLCol_h:
    @staticmethod
    def str_col(name: str, size: int, not_null=False, default_expr="") -> DDLCol:
        return DDLCol(col_name=name,
                      data_type="VARCHAR",
                      size=size,
                      default_expr=default_expr,
                      not_null=not_null
                      )

    @staticmethod
    def bigint_col(name: str, *, not_null=False, default_expr=None) -> DDLCol:
        return DDLCol(
            col_name=name,
            data_type="BIGINT",
            default_expr=default_expr,
            not_null=not_null,
            size=0
        )

    @staticmethod
    def int_col(name: str, *, not_null=False, default_expr=None) -> DDLCol:
        return DDLCol(
            col_name=name,
            data_type="INTEGER",
            default_expr=default_expr,
            not_null=not_null,
            size=0
        )

    @staticmethod
    def ts_col(name: str, *, not_null=False, default_expr=None) -> DDLCol:
        return DDLCol(
            col_name=name,
            data_type="TIMESTAMP",
            default_expr=default_expr,
            not_null=not_null,
            size=0
        )

    @staticmethod
    def xml_col(name: str, *, not_null=False, default_expr=None) -> DDLCol:
        return DDLCol(
            col_name=name,
            data_type="XML",
            default_expr=default_expr,
            not_null=not_null,
            size=0
        )

@dataclass(slots=True)
class DDLPrimaryKey:
    pk_name: str
    fields: List[str]


@dataclass(slots=True)
class DDLUniqueKey:
    uq_name: str
    fields: List[str]


@dataclass(slots=True)
class DDLForeignKey:
    fk_name: str
    col_names: List[str]
    fk_table_name: str
    fk_col_names: List[str]


class DDLBuildExpr_h:
    @staticmethod
    def build_col(cdef:DDLCol) -> str:
        parts =[]
        parts.append("{cdef.col_name} {cdef.data_type}")
        if cdef.size>0:
            parts.append("({cdef.size})")
        if cdef.not_null:
            parts.append(" NOT NULL ")
        if cdef.default_expr:
            parts.append(f" DEFAULT {cdef.default_expr}")
        return "".join(parts)

    @staticmethod
    def build_pk(pk:DDLPrimaryKey) -> str:
        # CONSTRAINT ga_su_mo_pk PRIMARY KEY (id),
        field_exprs = ",".join(pk.fields)
        return f"CONSTRAINT {pk.pk_name} PRIMARY KEY (id),{field_exprs}"

    @staticmethod
    def build_uq(uq:DDLUniqueKey) -> str:
        # CONSTRAINT ga_su_mo_pk PRIMARY KEY (id),
        field_exprs = ",".join(uq.fields)
        return f"CONSTRAINT {uq.pk_name} UNIQUE KEY (id),{field_exprs}"

    @staticmethod
    def build_fk(fk:DDLForeignKey) -> str:
        # CONSTRAINT ga_su_mo_pk PRIMARY KEY (id),
        own_field_exprs = ",".join(fk.fields)
        ref_field_exprs =",".join(fk.fk_col_names)

        return f"""CONSTRAINT {fk.pk_name} 
          Foreign KEY ({own_field_exprs})
          references {fk.fk_table_name} ({ref_field_exprs}
        """
"""
CREATE TABLE ga_su_mo
(
    local_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    local_bin     INTEGER NOT NULL,
    local_binfakt INTEGER NOT NULL,

    id            BIGINT GENERATED ALWAYS AS (
        local_id * local_binfakt::bigint + local_bin::bigint
) STORED,

    lu           TIMESTAMP NOT NULL,
    created_on   TIMESTAMP NOT NULL,
    login_name   VARCHAR(10) NOT NULL,
    gaid         DOUBLE PRECISION NOT NULL,
    data_xml     XML,
    su_tab_name  VARCHAR(120) NOT NULL,
    foegeb_id    BIGINT NOT NULL,
    copy_src_id  BIGINT,

        CONSTRAINT ga_su_mo_pk PRIMARY KEY (id),
    CONSTRAINT ga_su_mo_uq UNIQUE (gaid, su_tab_name)
);
"""



@dataclass(slots=True)
class DDLLocalBinPkTable:
    table_name: str
    cols_defs: List[DDLCol]
    unique_key: DDLUniqueKey
    fk_keys: List[DDLForeignKey]


def build_ddl_local_bin_pk_table(table_def: DDLLocalBinPkTable) -> str:
    sb = []
    #    """CREATE TABLE ga_su_mo (
    sb.append(f"CREATE TABLE {table_def.table_name} (")
    sb.append("""
        local_id      BIGINT GENERATED ALWAYS AS IDENTITY,
        local_bin     INTEGER NOT NULL,
        local_binfakt INTEGER NOT NULL,
        id BIGINT GENERATED ALWAYS AS (
            local_id * local_binfakt::bigint + local_bin::bigint
        ) STORED,
    
        lu           TIMESTAMP NOT NULL,
        created_on   TIMESTAMP NOT NULL, 
        login_name   VARCHAR(10) NOT NULL,
        """)
    col_def_exprs =[]
    for col_def in table_def.cols_defs:
        col_def_exprs.append(DDLBuildExpr_h.build_col(col_def))
    sb.append(",\n".join(col_def_exprs))

    sb.append(" CONSTRAINT ga_su_mo_pk PRIMARY KEY (id)")
    sb.append(DDLBuildExpr_h.build_uq(table_def.unique_key))
    for fk_def in table_def.fk_keys:
        sb.append(DDLBuildExpr_h.build_fk(fk_def))
    return "\n".join(sb)