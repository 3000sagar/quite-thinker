@echo off
chcp 65001 >nul
echo Installing Quiet Patterns daemon watcher as Windows startup task...
schtasks /create /tn "QuietPatternsDaemon" /tr "\"%cd%\daemon_watcher.bat\"" /sc onlogon /rl limited /f
echo.
echo Daemon watcher installed! It will start automatically when you log in.
echo To uninstall: schtasks /delete /tn "QuietPatternsDaemon" /f
echo.
echo Log file: quiet_patterns\daemon_watcher.log
echo Daemon log: quiet_patterns\quiet_patterns_daemon.log
pause
