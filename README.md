# installation

```powershell
Get-OdbcDriver | Where-Object Name -like "*SQL*"

```

da sollte der Sql Server 18 dabei sein.

wenn nicht dann

```powershell
.\install\msodbcsql.msi

```
# bespiel für DAO

```python
@dataclass
class MitarbeiterRow:
    NR:Optional[float]=None
    MNAME:Optional[str]=None
    MPASSWORD:Optional[str]=None
    MLOGIN:Optional[str]=None
    HGR_ID:Optional[float]=None
    SZEMAIL:Optional[str]=None
    bAktiv:Optional[str]=None
    pe_id:Optional[float]=None


class MitarbeiterDAO(BaseTdcDao[MitarbeiterRow] ):

    def create_new_row(self) -> MitarbeiterRow:
        return MitarbeiterRow()

    def get_pk_values(self, row: MitarbeiterRow) -> PrimaryKeyInfo:
        return {
            "NR":row.NR
        }

    def build_merge_info(self) -> BuildMergeStmtParams:
        return mssql_crud_util.build_std_merge_info(
            clazz=MitarbeiterRow, primary_key="nr", table="mitarbeiter"
        )

    def save(self, conf: TDbConf, row: MitarbeiterRow) -> MitarbeiterRow:
        nr = row.NR
        if (nr==0) or not (nr):
            row.NR = norm_util.get_next_max_id(
                conn=conf.conn,
                table_name=self.merge_params.table,
                pk_name="NR"
            )
        db_params = self.get_merge_params(row)
        mssqlUtil.execQuery(conf.conn, self.merge_sql, *db_params)
        return row

    def get_by_login_name(self, conf:TDbConf, login_name:str)->MitarbeiterRow:
        where="mlogin = %s"
        return self.query_by_where_expr_first(conf, where, [login_name])


```

# bespiel der Verwendung:

```python
    with mssqlUtil.using_odbc_conn(ohIniDBFile.get_test_offh_ini_file_path()) as conn:
    sql = "select v,w from KK_VARIABLE where v like %s order by 1"
rows = mssqlUtil.get_dict_rows(conn, sql, "A%")
for row in rows:
    print("%s = %s" % (row['v'], row['w']))

```

# norm Bespiel

```python
from dkm_lib_mssql.norm_util import fill_recs_data
@dataclass()
class TVeranstRechRec:
    fmt_rech_num:str=""
    rech_num:str=""
    uq_num:str=""
    veranstalt_name:str=""
    findet_statt_von:datetime or None=None
    nname:str=""
    vname:str=""
    gp_sb:str=""
    status_c:str=""
    rech_date:datetime or None=None


def get_veranst_rech_by_zr(conn, zr:TZeitraum)->List[TVeranstRechRec]:
    sql = """
        declare @von date= %s
        declare @bis date= %s

        select r.fmt_rech_num, r.rech_num, vera.uq_num, vera.veranstalt_name , vera.findet_statt_von,
                    anm.nname, anm.vname, anm.gp_sb, vera.status_c, r.rech_date
                from nmw_rech r
                inner join nmw_anmeld anm on r.id =anm.rech_id
                inner join nmw_veranstaltung vera on vera.id = anm.veranstalt_id
                where cast(rech_date as date) >=@von
                and cast (rech_date as date) <=@bis
                and r.rech_num_kr=2
                order by r.fmt_rech_num
    """
    return fill_recs_data(conn, sql, lambda x: TVeranstRechRec(), zr.von, zr.bis)

```

# SAVE Bespiel

```python
@dataclass
class Person:
    id: float
    nname: str


PERSON_MERGE_INFO = mssql_crud_util.build_std_merge_info(
    clazz=Person, primary_key="id", table="person"
)


def test_merge_data_row(self):
    pers = Person(id=2, nname="NName2")

    def get_saved_row() -> Optional[Person]:
        with mssqlUtil.using_dkm_test_conn() as conn:
            sql = f"select  {norm_util.get_select_fields_list(Person)} from pers where id = %s"
            return norm_util.find_entity_by_id(conn, table_name="person", primary_key_info={"id": 2},
                                               fn_create_rec=lambda x: pers
                                               )

    with mssqlUtil.using_dkm_test_conn() as conn:
        mssql_crud_util.merge_data_row(conn, pers, PERSON_MERGE_INFO)

    saved_row = get_saved_row()
    self.assertEqual(pers, saved_row)

    with mssqlUtil.using_dkm_test_conn() as conn:
        mssql_crud_util.del_data_row(conn, pers, "person", {"id": 2.0})

    saved_row = get_saved_row()
    self.assertTrue(saved_row is None)


def test_merge_data_row_oh_calced_bin_pk(self):
    row = test_firmen.create_new(
        suchbegriff="sb",
        bez1="bez1",
        bez2="bez2"
    )
    print(row)
    with using_test_conf() as conf:
        saved_row = test_firmen.save(conf, row)
        self.assertTrue(saved_row.id is not None)
        self.assertTrue(saved_row.local_id is not None)
        self.assertTrue(saved_row.login_name is not None)
        self.assertEqual(saved_row.local_bin, conf.bin)
        self.assertEqual(saved_row.local_binfakt, conf.bin_fakt)
        conf.commit()

```

