if "%1"=="" goto :eof
set REG_KEY_PATH=HKCU\Software\Denkmal\github
set REG_VALUE=PersonalToken
reg add %REG_KEY_PATH% /V %REG_VALUE% /d %1