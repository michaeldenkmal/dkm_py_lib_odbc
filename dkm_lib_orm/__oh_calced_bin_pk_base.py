import datetime
from dataclasses import dataclass

from dkm_lib_orm.orm_util import orm_field, dkm_orm_class


@dataclass
@dkm_orm_class()
class OhCalcedBinPkBase:
    """
            final val LOCAL_ID = addFieldDefLong(new FieldDefValLong[T](
            name = "local_id", autoInc = true, notNull = true, includedInInsert = false, includedInUpate = false,
            getter = (r) => row(r).getLocalId, setter = (r, v) => row(r).setLocalId(v)
        ))
    """
    local_id:int = orm_field(auto_inc=True, not_null=True, exclude_in_insert=True, exclude_in_update=True)
    """
        val LOCAL_BIN = addFieldDefInt(new FieldDefValInt[T](name = "local_bin", notNull = true,
            getter = (r) => row(r).getLocalBin,
            setter = (r, v) => row(r).setLocalBin(v),
            includedInInsert = true, includedInUpate = false
        ))
    """
    local_bin:int = orm_field(not_null=True, exclude_in_update=True)
    """
        //        @IPoAnnFieldDef(
        //            name = "local_binfakt",
        //            fieldType = EnFieldType.INT,
        //            notNull = true
        //        )
        //        private int localBinfakt;
        val LOCAL_BINFAKT = addFieldDefInt(new FieldDefValInt[T](name = "local_binfakt", notNull = true,
            getter = (r) => row(r).getLocalBinfakt,
            setter = (r, v) => row(r).setLocalBinfakt(v),
            includedInInsert = true, includedInUpate = false
        ))
    """
    local_binfakt:int = orm_field(not_null=True,exclude_in_update=True)
    """

        //        @IPoAnnFieldDef(
        //            name = "id",
        //            fieldType = EnFieldType.BIGINT,
        //            notNull = true,
        //            computeFormula = "([local_id]*[local_binfakt]+[local_bin])",
        //            isPersisted = true,
        //            isIncludedInInsert = false,
        //            isIncludedInUpate = false
        //        )
        //        private Long id;
        val ID = addFieldDefLong(new FieldDefValLong[T](
            name = "id", notNull = true, computedFormula = "([local_id]*[local_binfakt]+[local_bin])",
            getter = (r) => row(r).getId, setter = (r, v) => row(r).setId(v),
            includedInInsert = false, includedInUpate = false
        ))
    """
    id:int = orm_field(pk=True, not_null=True, computed_formula="([local_id]*[local_binfakt]+[local_bin])",
                   exclude_in_insert=True, exclude_in_update=True)
    """
        //        @IPoAnnFieldDef(
        //            name = "lu",
        //            fieldType = EnFieldType.DATETIME,
        //            notNull = true
        //        )
        //        private Date lu;
        val LU = addFieldDefDate(new FieldDefValDate[T](
            name = "lu", notNull = true, dateOnly = false,
            getter = (r) => row(r).getLu, setter = (r, v) => row(r).setLu(v)
        ))
    """
    lu:datetime.datetime = orm_field(not_null=True)
    """

        //        @IPoAnnFieldDef(
        //            name = "created_on",
        //            fieldType = EnFieldType.DATETIME,
        //            notNull = true,
        //            isIncludedInUpate = false
        //        )
        //        private Date createdOn;
        val CREATED_ON = addFieldDefDate(new FieldDefValDate[T](
            name = "created_on", notNull = true, dateOnly = false,
            getter = (r) => row(r).getCreatedOn, setter = (r, v) => row(r).setCreatedOn(v),
            includedInUpate = false
        ))
    """
    created_on:datetime.datetime = orm_field(not_null=True, exclude_in_update=True)
    """        //        @IPoAnnFieldDef(
        //            name = "login_name",
        //            fieldType = EnFieldType.STRING,
        //            size = 10
        //        )
        var LOGIN_NAME = addFieldDefStr(new FieldDefValString[T](
            name = "login_name", size = 10, notNull = true,
            getter = (r) => row(r).getLoginName, setter = (r, v) => row(r).setLoginName(v)
        ))
    """
    login_name:str = orm_field(not_null=True, field_len=10)
