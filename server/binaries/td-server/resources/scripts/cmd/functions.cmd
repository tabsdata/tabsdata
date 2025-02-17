rem
rem Copyright 2024 Tabs Data Inc.
rem

rem function to check execution error
:check_error
if %errorlevel% neq 0 (
    echo Command failed with status %errorlevel%
    exit %errorlevel%
)
goto :eof

rem function to sleep some time
:td_sleep
set "duration=%~1"
timeout /t %duration% /nobreak > nul
goto :eof