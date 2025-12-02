@echo off
set "UV_VERSION=0.6.3"
set "UV_URL=https://github.com/astral-sh/uv/releases/download/%UV_VERSION%/uv-x86_64-pc-windows-msvc.zip"
set "UV_EXE=uv.exe"

:: Check if uv.exe exists
if exist "%UV_EXE%" (
    goto :Launch
)

echo [Info] uv.exe not found. Downloading...

:: Download uv
powershell -Command "Invoke-WebRequest -Uri '%UV_URL%' -OutFile 'uv.zip'"

:: Extract
echo [Info] Extracting...
powershell -Command "Expand-Archive -Path 'uv.zip' -DestinationPath '.'"

:: Move from folder (it extracts to a folder) usually
:: uv-x86_64-pc-windows-msvc/uv.exe
if exist "uv-x86_64-pc-windows-msvc\uv.exe" (
    move "uv-x86_64-pc-windows-msvc\uv.exe" .
    rmdir /S /Q "uv-x86_64-pc-windows-msvc"
)

:: Clean up
del "uv.zip"

:Launch
echo [Info] Starting ReconLab...
"%UV_EXE%" run main.py

if %ERRORLEVEL% NEQ 0 (
    echo [Error] Application crashed.
    pause
)
