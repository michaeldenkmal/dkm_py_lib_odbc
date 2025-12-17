clear
#$env:PYTHONPATH = "D:\projekte\dkm\dkm_py\dkm_py_dkm_fakt"
# python -c "import appdkmfakt.settings"    # sollte jetzt funktionieren
$temp_file = JOIN-Path -Path ($env:TEMP) -ChildPath "dkm-py-dkm-lib-mssql-odbc.out"
$py = Join-Path -Path ($PSScriptRoot) -ChildPath ".\venv\Scripts\python.exe"

function run_my_py($package_name) {
    & $py -m mypy --config-file mypy.ini -p $package_name --install-types
}
run_my_py -package_name dkm_lib_mssql_odbc > $temp_file
run_my_py -package_name dkm_lib_orm_odbc >> $temp_file
#mypy --config-file mypy.ini -p dkm_lib_orm_odbc >> $temp_file
start "C:\Program Files\Notepad++\notepad++.exe" $temp_file
type $temp_file
