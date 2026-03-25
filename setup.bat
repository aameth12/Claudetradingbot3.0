@echo off
echo ============================================
echo  AI Trading Bot - Setup
echo ============================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Install Python 3.11+ from python.org
    pause
    exit /b 1
)

:: Create virtual environment
echo Creating virtual environment...
python -m venv venv
call venv\Scripts\activate.bat

:: Install dependencies
echo Installing dependencies...
pip install -r requirements.txt

:: Create .env if it doesn't exist
if not exist .env (
    echo.
    echo Creating .env file...
    set /p BOT_TOKEN="Enter your Telegram Bot Token: "
    set /p CHAT_ID="Enter your Telegram Chat ID: "
    echo TELEGRAM_BOT_TOKEN=%BOT_TOKEN%> .env
    echo TELEGRAM_CHAT_ID=%CHAT_ID%>> .env
    echo .env created.
) else (
    echo .env already exists, skipping.
)

:: Create logs directory
if not exist logs mkdir logs

echo.
echo Setup complete! Run 'run.bat' to start the bot.
pause
