@echo off
chcp 65001 >nul
cd /d "%~dp0quiet_patterns"

:loop
echo [%date% %time%] Starting daemon... >> daemon_watcher.log
call ..\.venv\Scripts\python.exe main.py --daemon >> quiet_patterns_daemon.log 2>&1
echo [%date% %time%] Daemon exited with code %errorlevel% >> daemon_watcher.log
timeout /t 30 /nobreak >nul
goto loop
