from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import datetime as dt
import decimal
import uuid
import keyword
import re

import psycopg


# Schnelles Basis-Mapping (OID -> Python-Typ)
_PG_OID_TO_PYTYPE: dict[int, str] = {
    16: "bool",
    20: "int", 21: "int", 23: "int",
    700: "float", 701: "float",
    1700: "decimal.Decimal",
    18: "str", 19: "str", 25: "str", 1042: "str", 1043: "str",
    1082: "dt.date",
    1114: "dt.datetime",
    1184: "dt.datetime",
    1083: "dt.time",
    1266: "dt.time",
    1186: "dt.timedelta",
    2950: "uuid.UUID",
    17: "bytes",
    114: "Any",     # json
    3802: "Any",    # jsonb
}


def _sanitize_identifier(name: str) -> str:
    s = (name or "").strip()
    s = re.sub(r"[^\w]", "_", s)
    if not s:
        s = "field"
    if s[0].isdigit():
        s = f"f_{s}"
    if keyword.iskeyword(s):
        s = f"{s}_"
    return s


def _pytype_for_oid(oid: int) -> str:
    return _PG_OID_TO_PYTYPE.get(int(oid), "Any")


def _split_schema_table(schema_table: str) -> tuple[Optional[str], str]:
    """
    Accepts 'table' or 'schema.table' and returns (schema|None, table).
    (Einfach gehalten; keine vollständige Quote-Parser-Logik.)
    """
    st = schema_table.strip().strip('"')
    if "." in st:
        schema, table = st.split(".", 1)
        return schema.strip('"'), table.strip('"')
    return None, st


def generate_dataclass_from_table(
    conn: psycopg.Connection,
    table: str,
    *,
    class_name: str = "Row",
    optional_mode: str = "auto",   # "auto" | "all" | "none"
    slots: bool = True,
) -> str:
    """
    Erzeugt eine Dataclass aus einer Tabelle (ohne Daten zu laden).
    Nullability wird aus pg_attribute.attnotnull bestimmt (schnell & korrekt).

    optional_mode:
      - "auto": Optional nur wenn Spalte nullable ist (attnotnull = false)
      - "all":  alles Optional
      - "none": nichts Optional
    """
    if optional_mode not in ("auto", "all", "none"):
        raise ValueError("optional_mode must be 'auto', 'all', or 'none'")

    schema, tab = _split_schema_table(table)

    if schema:
        sql = """
            SELECT
                a.attname,
                a.atttypid::int,
                a.attnotnull
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        args = (schema, tab)
    else:
        sql = """
            SELECT
                a.attname,
                a.atttypid::int,
                a.attnotnull
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = current_schema()
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        args = (tab,)

    with conn.cursor() as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()

    if not rows:
        qual = f"{schema}.{tab}" if schema else tab
        raise ValueError(f"Tabelle nicht gefunden oder keine Spalten: {qual}")

    # fields: (py_name, py_type, orig_name)
    fields: list[tuple[str, str, str]] = []
    for attname, atttypid, attnotnull in rows:
        orig_name = str(attname)
        py_name = _sanitize_identifier(orig_name)
        base_type = _pytype_for_oid(int(atttypid))

        if optional_mode == "all":
            py_type = f"Optional[{base_type}]"
        elif optional_mode == "none":
            py_type = base_type
        else:
            # auto: attnotnull=True => not Optional; False => Optional
            is_nullable = (not bool(attnotnull))
            py_type = f"Optional[{base_type}]" if is_nullable else base_type

        fields.append((py_name, py_type, orig_name))

    return _render_dataclass_code(fields, class_name=class_name, slots=slots)


def _render_dataclass_code(
    fields: list[tuple[str, str, str]],
    *,
    class_name: str,
    slots: bool,
) -> str:
    needs_optional = any(t.startswith("Optional[") for _, t, _ in fields)
    needs_dt = any("dt." in t for _, t, _ in fields)
    needs_decimal = any("decimal.Decimal" in t for _, t, _ in fields)
    needs_uuid = any("uuid.UUID" in t for _, t, _ in fields)
    needs_any = any(re.search(r"\bAny\b", t) for _, t, _ in fields)

    lines: list[str] = []
    lines.append("from __future__ import annotations")
    lines.append("")
    lines.append("from dataclasses import dataclass")

    typing_bits = []
    if needs_any:
        typing_bits.append("Any")
    if needs_optional:
        typing_bits.append("Optional")
    if typing_bits:
        lines.append(f"from typing import {', '.join(sorted(set(typing_bits)))}")

    if needs_dt:
        lines.append("import datetime as dt")
    if needs_decimal:
        lines.append("import decimal")
    if needs_uuid:
        lines.append("import uuid")

    lines.append("")
    lines.append("")
    slots_txt = ", slots=True" if slots else ""
    lines.append(f"@dataclass{slots_txt}")
    lines.append(f"class {class_name}:")

    for py_name, py_type, orig_name in fields:
        if py_name != orig_name:
            lines.append(f"    {py_name}: {py_type}  # column={orig_name!r}")
        else:
            lines.append(f"    {py_name}: {py_type}")

    return "\n".join(lines)
