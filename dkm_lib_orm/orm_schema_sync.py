"""
    public static enum EnSchemaAction {
        NEW_TABLE,
        NEW_COLUMN, CHANGE_COLUMN, DROP_COLUMN,
        NEW_CONSTRAINT, CHANGE_CONSTRAINT,DROP_CONSTRAINT,
    }
"""
from dataclasses import dataclass
from enum import StrEnum

from dkm_lib_orm.field_def import IFieldDef


class EnSchemaAction(StrEnum):
    NEW_TABLE="NEW_TABLE",
    NEW_COLUMN="NEW_COLUMN"
    CHANGE_COLUMN="CHANGE_COLUMN"
    DROP_COLUMN="DROP_COLUMN"
    NEW_CONSTRAINT="NEW_CONSTRAINT"
    CHANGE_CONSTRAINT="CHANGE_CONSTRAINT"
    DROP_CONSTRAINT="DROP_CONSTRAINT"


"""
    public static enum EnColumnChangeAction {
        CHANGE_DATA_TYPE,
        CHANGE_DATA_LEN,
        CHANGE_NOT_NULL,
        CHANGE_PREC,
        CHANGE_SCALE,
        CHANGE_CHECK_CONSTRAINT,
        CHANGE_AUTO_INC,
        CHANGE_SEED,
        CHANGE_INCREMENT,
        CHANGE_COMPUTE_FORMULA,
        CHANGE_PERSISTED,
        NO_ACTION
    }

"""

class EnColumnChangeAction(StrEnum):
    CHANGE_DATA_TYPE="CHANGE_DATA_TYPE"
    CHANGE_DATA_LEN="CHANGE_DATA_LEN"
    CHANGE_NOT_NULL="CHANGE_NOT_NULL"
    CHANGE_PREC="CHANGE_PREC"
    CHANGE_SCALE="CHANGE_SCALE"
    CHANGE_CHECK_CONSTRAINT="CHANGE_CHECK_CONSTRAINT"
    CHANGE_AUTO_INC="CHANGE_AUTO_INC"
    CHANGE_SEED="CHANGE_SEED"
    CHANGE_INCREMENT="CHANGE_INCREMENT"
    CHANGE_COMPUTE_FORMULA="CHANGE_COMPUTE_FORMULA"
    CHANGE_PERSISTED="CHANGE_PERSISTED"
    NO_ACTION="NO_ACTION"



#public static class TCompareFieldDefResult {

@dataclass
class TCompareFieldDefResult:
    #        public TCompareFieldDefResult(IFieldDef dbFieldDef, IFieldDef pojoFieldDef) {
    #             this._dbFieldDef = dbFieldDef;
    #             this._pojoFieldDef = pojoFieldDef;
    #        private final IFieldDef _dbFieldDef;
    db_field_def:IFieldDef
    #        private final IFieldDef _pojoFieldDef;
    pojo_field_def:IFieldDef

    #private boolean flag_fieldType=false;
    flag_fieldType:bool=False

    #    //        EnFieldType getFieldType();
    #    //        // Size when String
    #    //    int getSize();
    #    private boolean flag_size=false;
    flag_size:bool = False
    #    //    // nur wenn Decimal
    #    //    int getPrec();
    #    private boolean flag_prec=false;
    flag_prec:bool =False

    #    //    // nur wenn Decimal
    #    //    int getScale();
    #    private boolean flag_scale=false;
    flag_scale:bool= False

    #    //    // CheckConstraint
    #    //    String checkConstraint();
    #    private boolean flag_checkConstraint=false;
    flag_check_constraint:bool=False

    #    //    // not null?
    #    //    boolean isNotNull();
    #    private boolean flag_notNull=false;
    flag_not_null:bool=False


    #    //
    #    //    boolean isAutoInc();
    #    private boolean flag_isAutoInc=false;
    flag_is_auto_inc=False

    #    //
    #    //    long isSeed();
    #    private boolean flag_seed=false;
    flag_seed:bool=False

    #    //    long isIncrement();
    #    private boolean flag_increment=false;#

    #    //    String getComputeFormula();
    #    private boolean flag_computeFormula=false;
    #    //
    #    //    boolean isPersisted() ;
    #    private boolean flag_isPersisted=false;

    """    public boolean changesFound() {
            boolean[] flags = new boolean[] {
                flag_fieldType , flag_size , flag_prec, flag_scale, flag_checkConstraint, flag_notNull,
                flag_isAutoInc, flag_seed, flag_increment, flag_computeFormula, flag_isPersisted
            };
            for (boolean flag:flags) {
                if (flag) {
                    return true;
                }
            }
            return false;
        }

"""