@echo off
set PROJECT_NAME=%1
set VERSION=%2
if "%PROJECT_NAME%"=="" goto :syntax
if "%VERSION%"=="" set VERSION=master


set REG_KEY_PATH=HKCU\Software\Denkmal\github
set REG_VALUE=PersonalToken

for /f "tokens=3" %%a in ('reg query %REG_KEY_PATH%  /V %REG_VALUE% ') do set GITHUB_TOKEN=%%a
echo %GITHUB_TOKEN%
cd ..pause


set curdir=%CD%

set pip=%curdir%/venv/Scripts/pip.exe

if exist %pip% goto :pip_path_exists
echo %pip% existiert nicht - Script muss immer aus dem Root Verzeichnis gestartet werden also pip_git_scripts\pipInstallFromGit
exit /B 1
:pip_path_exists




:$pip install  git+https://${GITHUB_TOKEN}@github.com/michaeldenkmal/${PROJECT_NAME}.git@${xVERSION}

%pip% install git+https://%GITHUB_TOKEN%@github.com/michaeldenkmal/%PROJECT_NAME%.git@%VERSION%
goto :EOF

:syntax
echo pipinstallFromGit dkm-py-lib-pure [master]
goto :EOF