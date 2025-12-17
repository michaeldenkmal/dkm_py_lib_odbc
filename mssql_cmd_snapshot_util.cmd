set mydir=%~dp0
set python=%mydir%\venv\Scripts\python.exe
set PYTHON_PATH=%mydir%
set action=%1
set snapshot_name=%2

cd /D %mydir%
%python% mssql_cmd_snapshot_util.py %action% %snapshot_name%
