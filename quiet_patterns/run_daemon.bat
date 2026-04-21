@echo off
chcp 65001 >nul
cd /d "%~dp0quiet_patterns"
call ..\.venv\Scripts\python.exe main.py --daemon >> quiet_patterns_daemon.log 2>&1
