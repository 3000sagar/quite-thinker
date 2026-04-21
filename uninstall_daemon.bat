@echo off
chcp 65001 >nul
schtasks /delete /tn "QuietPatternsDaemon" /f
echo Task removed.
pause
