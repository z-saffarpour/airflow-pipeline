@echo off
REM =============================================================================
REM Airflow Security Setup Script (Windows)
REM =============================================================================
REM This script helps with initial security configuration
REM Usage: setup-security.bat
REM =============================================================================

setlocal enabledelayedexpansion

echo.
echo =============================================================================
echo Airflow Security Setup
echo =============================================================================
echo.

REM Check if .env exists
if not exist ".env" (
    echo [1/5] Creating .env file from template...
    if exist ".env.example" (
        copy .env.example .env >nul
        echo [OK] .env created
    ) else (
        echo [ERROR] .env.example not found!
        exit /b 1
    )
) else (
    echo [1/5] .env file already exists (skipping)
)

REM Check Python availability
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ERROR] Python is required for key generation
    echo Please install Python or generate keys manually using:
    echo python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    exit /b 1
)

echo.
echo [2/5] Generating security keys...
echo.

REM Generate FERNET_KEY
echo Generating FERNET_KEY...
for /f "tokens=*" %%a in ('python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"') do set FERNET_KEY=%%a

if "%FERNET_KEY%"=="" (
    echo [ERROR] Failed to generate FERNET_KEY
    exit /b 1
) else (
    echo [OK] FERNET_KEY generated
    python -c "
import re
with open('.env', 'r') as f:
    content = f.read()
content = re.sub(r'^AIRFLOW__CORE__FERNET_KEY=.*$', 'AIRFLOW__CORE__FERNET_KEY=%FERNET_KEY%', content, flags=re.MULTILINE)
with open('.env', 'w') as f:
    f.write(content)
"
)

REM Generate JWT_SECRET
echo Generating JWT_SECRET...
for /f "tokens=*" %%a in ('python -c "import os; print(os.urandom(32).hex())"') do set JWT_SECRET=%%a
echo [OK] JWT_SECRET generated

REM Generate WEBSERVER_SECRET_KEY
echo Generating WEBSERVER_SECRET_KEY...
for /f "tokens=*" %%a in ('python -c "import os; print(os.urandom(32).hex())"') do set WEBSERVER_SECRET=%%a
echo [OK] WEBSERVER_SECRET_KEY generated

REM Generate passwords using Python secrets module
echo.
echo [3/5] Generating database passwords...
echo.

echo Generating POSTGRES_PASSWORD...
for /f "tokens=*" %%a in ('python -c "import base64, os; print(base64.b64encode(os.urandom(24)).decode())"') do set POSTGRES_PASS=%%a
echo [OK] POSTGRES_PASSWORD generated

echo Generating REDIS_PASSWORD...
for /f "tokens=*" %%a in ('python -c "import base64, os; print(base64.b64encode(os.urandom(24)).decode())"') do set REDIS_PASS=%%a
echo [OK] REDIS_PASSWORD generated

echo.
echo [4/5] Setting file permissions...
echo.

REM Set .env file to read-only for current user (Windows ICACLS)
echo Securing .env file...
icacls .env /inheritance:r /grant:r "%USERNAME%:F" >nul 2>&1
echo [OK] .env file permissions set

echo.
echo [5/5] Verifying .gitignore...
echo.

if exist ".gitignore" (
    findstr /M /C:".env" .gitignore >nul
    if errorlevel 1 (
        echo Adding .env to .gitignore...
        echo .env>> .gitignore
        echo [OK] .env added to .gitignore
    ) else (
        echo [OK] .env is already in .gitignore
    )
) else (
    echo [WARNING] .gitignore not found
)

echo.
echo =============================================================================
echo [SUCCESS] Security setup completed!
echo =============================================================================
echo.

echo IMPORTANT NEXT STEPS:
echo.
echo 1. Review and update .env file with your specific configuration:
echo    - MSSQL_HOST: Your SQL Server hostname
echo    - MSSQL_USERNAME: SQL Server username
echo    - POSTGRES_USER: Database user (if changing from 'airflow')
echo    - Other application-specific settings
echo.
echo 2. Verify Kerberos configuration:
echo    - Review config/krb5.conf
echo    - Ensure KDC is accessible: okdc10003.okco.ir
echo.
echo 3. Start the services:
echo    docker-compose -f docker-compose.winauth.yml up -d
echo.
echo 4. Access Airflow UI:
echo    http://localhost:9280
echo.
echo 5. Default credentials (set during first run):
echo    Username: airflow
echo    Password: Check _AIRFLOW_WWW_USER_PASSWORD in .env
echo.
echo For detailed security information, see SECURITY_GUIDE.md
echo.

endlocal
