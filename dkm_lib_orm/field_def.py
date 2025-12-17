#public interface IFieldDef {
from dataclasses import dataclass


@dataclass
class IFieldDef:
    pass
"""
    // Name
    @NotNull
    String getName();
    // Type
    @NotNull
    EnFieldType getFieldType();
    // Size when String
    int getSize();
    // nur wenn Decimal
    int getPrec();
    // nur wenn Decimal
    int getScale();
    // not null?
    boolean isNotNull();

    boolean isAutoInc();

    long isSeed();
    long isIncrement();

    String getComputeFormula();

    boolean isPersisted() ;

    String getDefaultExpr();

    public static boolean NOT_NULL=true;
    public static boolean NULL_ALLOWED=false;

    public static IFieldDef createFromPoAnnFieldDef(@NotNull final IPoAnnFieldDef annFieldDef) {
        return new IFieldDef() {
            @Override
            @NotNull
            public String getName() {
                return annFieldDef.name();
            }

            @Override
            @NotNull
            public EnFieldType getFieldType() {
                return annFieldDef.fieldType();
            }

            @Override
            public int getSize() {
                return annFieldDef.size();
            }

            @Override
            public int getPrec() {
                return annFieldDef.prec();
            }

            @Override
            public int getScale() {
                return annFieldDef.scale();
            }

            @Override
            public boolean isNotNull() {
                return annFieldDef.notNull();
            }

            @Override
            public boolean isAutoInc() {
                return annFieldDef.isAutoInc();
            }

            @Override
            public long isSeed() {
                return annFieldDef.seed();
            }

            @Override
            public long isIncrement() {
                return annFieldDef.increment();
            }

            @Override
            public String getComputeFormula() {
                return annFieldDef.computeFormula();
            }

            @Override
            public boolean isPersisted() {
                return annFieldDef.isPersisted();
            }

            @Override
            public String getDefaultExpr() {
                return annFieldDef.defaultExpr();
            }
        };
    }

    public static String toString(@NotNull IFieldDef fieldDef) {

        StringBuilder sb = new StringBuilder();

        // Name
        sb.append(String.format("getName=%s", fieldDef.getName())).
                append(String.format("type=%s", fieldDef.getFieldType())).
                append(String.format("getSize=%d", fieldDef.getSize())).
                append(String.format("getPrec=%d", fieldDef.getPrec())).
                append(String.format("getScale=%d", fieldDef.getScale())).
                append(String.format("isNotNull=%s", Tmwunit.bool2str(fieldDef.isNotNull()))).
                append(String.format("isAutoInc=%s", Tmwunit.bool2str(fieldDef.isAutoInc()))).
                append(String.format("isSeed=%d", fieldDef.isSeed())).
                append(String.format("isIncrement=%d", fieldDef.isIncrement())).
                append(String.format("getComputeFormula='%s'", fieldDef.getComputeFormula())).
                append(String.format("isPersisted=%s", Tmwunit.bool2str(fieldDef.isPersisted())));
        return sb.toString();
    }



}

"""