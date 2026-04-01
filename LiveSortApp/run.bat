@echo off
setlocal
cd /d "%~dp0"

echo [LIVESORT_SYS] Checking Environment...

:: Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.9+ and add to PATH.
    pause
    exit /b
)

:: Create Virtual Environment if not exists
if not exist "venv" (
    echo [LIVESORT_SYS] Creating Virtual Environment...
    python -m venv venv
)

:: Activate Venv and Install dependencies
echo [LIVESORT_SYS] Installing Dependencies...
call .\venv\Scripts\activate
pip install -r requirements.txt

:: Create necessary directories
if not exist "music_files" mkdir "music_files"
if not exist "static" mkdir "static"
if not exist "templates" mkdir "templates"

:: Start Server
echo [LIVESORT_SYS] Launching Application...
echo Access at: http://localhost:8000
python main.py

pause
