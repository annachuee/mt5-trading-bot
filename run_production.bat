@echo off
setlocal
cd /d "%~dp0"

if not exist ".env" (
  echo Missing .env. Copy .env.example to .env and set credentials and risk limits.
  exit /b 2
)

where py >nul 2>nul
if errorlevel 1 (
  echo Python Launcher for Windows was not found. Install 64-bit Python 3.12.
  exit /b 3
)

if not exist ".venv\Scripts\python.exe" py -3.12 -m venv .venv
if errorlevel 1 exit /b 4

".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 exit /b 5
".venv\Scripts\python.exe" -m pip install -r requirements.txt -r requirements-mt5-windows.txt
if errorlevel 1 exit /b 6

".venv\Scripts\python.exe" mt5_production.py
exit /b %errorlevel%
