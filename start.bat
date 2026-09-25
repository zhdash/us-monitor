@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "PY=C:\Users\jones\.workbuddy\binaries\python\versions\3.13.12\python.exe"
if not exist "%PY%" set "PY=python"

echo ==========================================
echo   US Stock Monitor  (amount TOP30 / 60m)
echo   URL: http://127.0.0.1:8021
echo   Press Ctrl+C to stop
echo ==========================================
echo.

"%PY%" app.py
pause
