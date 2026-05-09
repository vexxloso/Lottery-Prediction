@echo off
REM Build FullWheelViewer.exe from viewer_app/app.py
REM Run from repo root:  build_viewer.bat
REM Requires: Python with pip

cd /d "%~dp0"

echo Installing dependencies...
pip install pyinstaller openpyxl PySide6 requests --quiet

echo.
echo Building FullWheelViewer.exe ...
python -m PyInstaller --noconfirm FullWheelViewer.spec

if exist "dist\FullWheelViewer.exe" (
    echo.
    echo Done. FullWheelViewer.exe is in: dist\FullWheelViewer.exe
    echo Run it from any folder. Set FULLWHEEL_API_BASE env var if backend is not on http://213.165.78.107
) else (
    echo.
    echo Build failed. Check output above for errors.
    exit /b 1
)

pause
