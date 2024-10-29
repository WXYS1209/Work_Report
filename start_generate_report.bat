@echo off

set PYTHON_PATH=python

REM Parse arguments
if "%1"=="" goto usage

set REPORT=%1

set SCRIPT_PATH="./%REPORT%/main.py"

if not exist %SCRIPT_PATH% goto error_file

%PYTHON_PATH% %SCRIPT_PATH%
if %errorlevel% neq 0 goto error

goto end

:usage
echo Usage: start_generate_report.bat REPORT
echo=
echo REPORT options: 
echo Five_Leaue
echo=

:error_file
echo No such directory.
goto end

:error
goto end

:end
pause
