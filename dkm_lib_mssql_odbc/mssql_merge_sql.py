from dataclasses import dataclass
from typing import List, Optional


@dataclass
class BuildMergeStmtParams:
    table: str
    usingSourceSelectCols: List[str]
    primaryKeyFields: List[str]
    updateColumns: List[str]
    insertColumns: List[str]
    uniqueKeyFields: Optional[List[str]] = None
    autoIncCol: Optional[str] = None
    changeResultIdDataType: Optional[str] = None


# public static String buildMergeStmt(IOnBuildMergeOpts opts) {
# noinspection PyListCreation
def build_merge_stmt(params:BuildMergeStmtParams)->str:
    return _build_merge_stmt(table=params.table, using_source_select_cols=params.usingSourceSelectCols,
                             primary_key_fields=params.primaryKeyFields,
                             update_columns=params.updateColumns,
                             insert_columns=params.insertColumns,
                             unique_key_fields=params.uniqueKeyFields,
                             auto_inc_col=params.autoIncCol, change_result_id_data_type=params.changeResultIdDataType)


# noinspection PyListCreation
def _build_merge_stmt(table:str, using_source_select_cols:List[str], primary_key_fields:List[str],
                      update_columns:List[str], insert_columns:List[str],
                      unique_key_fields: Optional[List[str]],
                      auto_inc_col:Optional[str], change_result_id_data_type:Optional[str])->str:

    sb:List[str] = []
    sb.append(_build_head(table, change_result_id_data_type or ""))
    sb.append(_build_using(using_source_select_cols))
    sb.append(_build_on(primary_key_fields, unique_key_fields))
    sb.append("WHEN MATCHED THEN")
    sb.append(_build_update(update_columns))
    sb.append("WHEN NOT MATCHED THEN")
    sb.append(_buildInsert(insert_columns))
    if auto_inc_col:
        sb.append(_buildOutput(auto_inc_col))
        sb.append("select * from @ChangeResult")
    else:
        sb.append(";")
    # return TListUtil.joinListOfStr(sb,"\n");
    return "\n".join(sb)


# static String buildHead(final String tableName, final String changeResultIdDataType) {
def _build_head(table_name:str, change_result_id_datatype:str):
    if (change_result_id_datatype is None) or (change_result_id_datatype == ""):
        # return String.format(
        # "MERGE %s AS target\n", tableName);
        return "MERGE %s AS target\n" % table_name
    else:
        return """
        DECLARE @ChangeResult TABLE (ChangeType VARCHAR(10), Id %s)
        MERGE %s AS target
        """ % (change_result_id_datatype, table_name)


# static String buildUsing(String[] usingSourceSelectCols, boolean delphiParamMarker) {
# noinspection PyPep8Naming
def _build_using(using_source_select_cols:List[str]):
        # // USING (SELECT  @id, @bez_C, @suchbegriff_C, @local_bin, @local_binfakt, @lu, @created_on, @login_name) as Source ( id, bez_C, suchbegriff_C, local_bin, local_binfakt, lu, created_on, login_name)
        # Arrays.stream(usingSourceSelectCols).forEach(c-> listParamMarker.add(String.format(":%s", c)));
        # }
        listParamMarker = []
        # noinspection PyUnusedLocal
        for usingSourceSelectCol in using_source_select_cols:
            listParamMarker.append("%s")
        # return String.format("USING (SELECT %s) as Source (%s)", TListUtil.joinListOfStr(listParamMarker,","),
                # TListUtil.joinArrOfStr(usingSourceSelectCols,","));
        return "USING (SELECT %s) as Source (%s)" % (",".join(listParamMarker), ",".join(using_source_select_cols))


# static String _buildOn(String[] fields) {
def _build_on_sub(fields:Optional[List[str]]):
    if fields is None:
        return ""

    assert(isinstance(fields, list))
    # // target.id = source.id and target.id1 = source.id1
    # List<String> sb = new ArrayList<>();
    sb =[]
    # Arrays.stream(fields)
            # .filter(field-> !u.nvlStr(field,"").isEmpty())
            # .forEach(field-> sb.add(String.format("target.%s = source.%s ",field, field)));
    for field in fields:
        if field:
            sb.append("target.%s = source.%s " % (field, field))
    if len(sb) == 0:
        return ""
    # return "(" + TListUtil.joinListOfStr(sb, " and ") +")";
    return "(" + " and ".join(sb) + ")"


# static String buildOn(String[] primaryKeys, String [] uniqueKeys) {
# noinspection PyPep8Naming
def _build_on(primary_keys:List[str], unique_keys: Optional[List[str]]):
    #  // ON ( (target.id = source.id) or (target.suchbegriff_C = source.suchbegriff_C ))
    # String pkSql = _buildOn(primaryKeys);
    pkSql =_build_on_sub(primary_keys)
    # String uqSql = _buildOn(uniqueKeys);
    uqSql = _build_on_sub(unique_keys)
    # List<String> parts = new ArrayList<>();
    parts=[]
    # if ((primaryKeys!=null) && (primaryKeys.length>0)) {
    if pkSql:
        parts.append(pkSql)
    if uqSql:
        parts.append(uqSql)
    # return "ON (" + TListUtil.joinListOfStr(parts," or ") + ")";
    return "ON (" + " or ".join(parts) +  ")"


# static String buildUpdate(String[] updateColumns) {
# noinspection PyPep8Naming
def _build_update(update_columns):
    # // UPDATE SET
    # // bez_C = source.bez_C,
    # List<String> lstStmt= new ArrayList<>();
    lstStmt = []
    # for (String updateCol:updateColumns) {
        # lstStmt.add(String.format("%s = source.%s", updateCol, updateCol));
    # }
    for updateCol in update_columns:
        lstStmt.append("%s = source.%s" % (updateCol, updateCol))
    # return String.format("UPDATE SET %s", TListUtil.joinListOfStr(lstStmt,",\n"));
    return "UPDATE SET %s" % (",\n".join(lstStmt))



# static String buildInsert(String[] insertColumns) {
# noinspection PyPep8Naming
def _buildInsert(insertColumns):
    # // INSERT (bez_C, suchbegriff_C, local_bin)
    # // VALUES (source.bez_C, source.suchbegriff_C, source.local_bin)

    lstColNames = []
    lstValNames = []

    for insertCol in insertColumns:
        lstColNames.append(insertCol)
        lstValNames.append("source.%s" % insertCol)
    # return String.format("INSERT (%s) \n VALUES (%s)" , TListUtil.joinListOfStr(lstColNames,", "),
            # TListUtil.joinListOfStr(lstValNames, ", "));
    return "INSERT (%s) \n VALUES (%s)"  % ( ", ".join(lstColNames), ", ".join(lstValNames) )


# noinspection PyPep8Naming
def _buildOutput( autoIncCol):
        return "OUTPUT $action, inserted.%s as newId into @ChangeResult;" % autoIncCol

