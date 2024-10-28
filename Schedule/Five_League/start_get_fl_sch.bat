@echo off
REM 检查是否请求了帮助或未提供参数
if "%1"=="" (
    echo Please provide the starting round number and ending round number.
    echo For help, use: start_get_fl_sch.bat help
    exit /b
)

if /i "%1"=="help" (
    echo Usage: start_get_fl_sch.bat [round_num_start] [round_num_end] [optional: season]
    echo Example: start_get_fl_sch.bat 5 10 2024-2025
    echo This script runs a Python program to scrape football matches for the given round range and optional season.
    exit /b
)

REM 检查是否提供了第二个参数（轮次结束）
if "%2"=="" (
    echo Please provide the ending round number.
    exit /b
)

REM 检查是否提供了第三个参数（赛季），如果没有，则设置默认值为 2024-2025
set season=%3
if "%3"=="" (
    set season=2024-2025
)

REM 执行第一个 Python 脚本并检查是否成功运行
python get_five_league_sch.py %season% %1 %2

REM 检查第一个 Python 脚本的退出状态
if %errorlevel% neq 0 (
    echo The first Python script encountered an error. Stopping the process.
    exit /b
)

REM 如果第一个 Python 脚本成功运行，则继续执行第二个 Python 脚本
python clean_five_league_sch.py

REM 提示任务完成
echo Both Python scripts ran successfully.
pause
