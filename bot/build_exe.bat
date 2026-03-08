@echo off
REM Build bot.exe from the combined bot (run_bot.py + el_gordo, euromillones, la_primitiva).
REM Run this from the bot folder:  build_exe.bat
REM Requires: Python with pip, then  pip install pyinstaller

cd /d "%~dp0"

echo Installing PyInstaller if needed...
pip install pyinstaller --quiet

echo Building bot.exe ...
pyinstaller --noconfirm bot.spec

if exist "dist\bot.exe" (
    echo.
    echo Done. bot.exe is in: dist\bot.exe
    echo Copy dist\bot.exe and .env to your PC; run bot.exe from the same folder as .env.
) else (
    echo Build failed.
    exit /b 1
)
