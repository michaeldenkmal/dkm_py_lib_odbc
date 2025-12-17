set mydir=%~dp0
set python=%mydir%\venv\Scripts\python.exe
set PYTHON_PATH=%mydir%

set OFFH_INI=%1
set ACTION=%2
set DBNAME=%3


cd /D %mydir%
%python% mssql_cmd_session_util.py %OFFH_INI% %ACTION% %DBNAME%
