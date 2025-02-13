REM 
REM Tests JDBC Driver on Windows
REM
setlocal
setlocal EnableDelayedExpansion
python -m venv venv
call venv\scripts\activate
pip install -U snowflake-connector-python

cd %GITHUB_WORKSPACE%

if "%CLOUD_PROVIDER%"=="AZURE" (
  set ENCODED_PARAMETERS_FILE=.github/workflows/parameters_azure.json.gpg
) else if "%CLOUD_PROVIDER%"=="GCP" (
  set ENCODED_PARAMETERS_FILE=.github/workflows/parameters_gcp.json.gpg
) else if "%CLOUD_PROVIDER%"=="AWS" (
  set ENCODED_PARAMETERS_FILE=.github/workflows/parameters_aws.json.gpg
) else (
  echo === unknown cloud provider
  exit /b 1
)

gpg --quiet --batch --yes --decrypt --passphrase=%PARAMETERS_SECRET% --output parameters.json %ENCODED_PARAMETERS_FILE%

REM DON'T FORGET TO include @echo off here or the password may be leaked!
echo @echo off>parameters.bat
jq -r ".testconnection | to_entries | map(\"set \(.key)=\(.value)\") | .[]" parameters.json >> parameters.bat
call parameters.bat
if %ERRORLEVEL% NEQ 0 (
    echo === failed to set the test parameters
    exit /b 1
)
echo @echo off>parametersorg.bat
jq -r ".orgconnection | to_entries | map(\"set \(.key)=\(.value)\") | .[]" parameters.json >> parametersorg.bat
call parametersorg.bat
if %ERRORLEVEL% NEQ 0 (
    echo === failed to set the org parameters
    exit /b 1
)
set SNOWFLAKE_TEST_SCHEMA=%RUNNER_TRACKING_ID:-=_%_%GITHUB_SHA%
set TARGET_SCHEMA_NAME=%SNOWFLAKE_TEST_SCHEMA%

echo [INFO] Account:   %SNOWFLAKE_TEST_ACCOUNT%
echo [INFO] User   :   %SNOWFLAKE_TEST_USER%
echo [INFO] Database:  %SNOWFLAKE_TEST_DATABASE%
echo [INFO] Schema:    %SNOWFLAKE_TEST_SCHEMA%
echo [INFO] Warehouse: %SNOWFLAKE_TEST_WAREHOUSE%
echo [INFO] Role:      %SNOWFLAKE_TEST_ROLE%
echo [INFO] PROVIDER:  %CLOUD_PROVIDER%

echo [INFO] Creating schema %SNOWFLAKE_TEST_SCHEMA%
pushd %GITHUB_WORKSPACE%\ci\container
python create_schema.py
popd

REM setup log

set CLIENT_LOG_DIR_PATH=%GITHUB_WORKSPACE%\jenkins_rt_logs
echo "[INFO] CLIENT_LOG_DIR_PATH=%CLIENT_LOG_DIR_PATH%"

set CLIENT_LOG_FILE_PATH=%CLIENT_LOG_DIR_PATH%\ssnowflake_ssm_rt.log
echo "[INFO] CLIENT_LOG_FILE_PATH=%CLIENT_LOG_FILE_PATH%"

set CLIENT_KNOWN_SSM_FILE_PATH=%CLIENT_LOG_DIR_PATH%\rt_jenkins_log_known_ssm.txt
echo "[INFO] CLIENT_KNOWN_SSM_FILE_PATH=%CLIENT_KNOWN_SSM_FILE_PATH%"

REM To close log analyze, just set ENABLE_CLIENT_LOG_ANALYZE to not "true", e.g. "false".
set ENABLE_CLIENT_LOG_ANALYZE=true

REM The new complex password we use for jenkins test
set SNOWFLAKE_TEST_PASSWORD_NEW="ThisIsRandomPassword123!"

set LOG_PROPERTY_FILE=%GITHUB_WORKSPACE%\src\test\resources\logging.properties

echo "[INFO] LOG_PROPERTY_FILE=%LOG_PROPERTY_FILE%"

set CLIENT_DRIVER_NAME=JDBC

powershell -Command "(Get-Content %LOG_PROPERTY_FILE%) | Foreach-Object { $_ -replace '^java.util.logging.FileHandler.pattern.*', 'java.util.logging.FileHandler.pattern = %CLIENT_LOG_FILE_PATH%' } | Set-Content %LOG_PROPERTY_FILE%"

echo "[INFO] Create log directory"

IF NOT EXIST %CLIENT_LOG_DIR_PATH% MD %CLIENT_LOG_DIR_PATH% 2>nul

echo "[INFO] Delete ssm file"
IF EXIST "%CLIENT_KNOWN_SSM_FILE_PATH%" DEL /F /Q "%CLIENT_KNOWN_SSM_FILE_PATH%"

echo "[INFO] Create ssm file"
echo.>"%CLIENT_KNOWN_SSM_FILE_PATH%"

echo "[INFO] Finish log setup"
REM end setup log

for /F "tokens=1,* delims==" %%i in ('set ^| findstr /I /R "^SNOWFLAKE_[^=]*$" ^| findstr /I /V /R "^SNOWFLAKE_PASS_[^=]*$" ^| sort') do (
  echo %%i=%%j
)

echo [INFO] Starting hang_webserver.py 12345
pushd %GITHUB_WORKSPACE%\ci\container
start /b python hang_webserver.py 12345 > hang_webserver.out 2>&1
popd

echo [INFO] Testing

set MVNW_EXE=%GITHUB_WORKSPACE%\mvnw.cmd

REM Avoid connection timeouts
set MAVEN_OPTS="-Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Dmaven.wagon.http.retryHandler.class=standard -Dmaven.wagon.http.retryHandler.count=3 -Dmaven.wagon.httpconnectionManager.ttlSeconds=120"
echo "MAVEN OPTIONS %MAVEN_OPTS%"

REM Avoid connection timeout on plugin dependency fetch or fail-fast when dependency cannot be fetched
cmd /c %MVNW_EXE% --batch-mode --show-version dependency:go-offline

set SF_ENABLE_EXPERIMENTAL_AUTHENTICATION=true

if "%JDBC_TEST_SUITES%"=="FipsTestSuite" (
    pushd FIPS
    echo "[INFO] Run Fips tests"
    cmd /c %MVNW_EXE% -B -DjenkinsIT ^
        -Djava.io.tmpdir=%GITHUB_WORKSPACE% ^
        -Djacoco.skip.instrument=false ^
        -DintegrationTestSuites=FipsTestSuite ^
        -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn ^
        -Dnot-self-contained-jar ^
        verify ^
        --batch-mode --show-version > log.txt & type log.txt
    echo "[INFO] Check for test execution status"
    find /i /c "BUILD FAILURE" log.txt > NUL
    set isfound=!errorlevel!
    if !isfound! equ 0 (
        echo [ERROR] Failed run %%a test
        exit /b 1
    ) else (
        echo [INFO] Success run %%a test
    )
    popd
) else (
    echo "[INFO] Run %JDBC_TEST_SUITES% tests"
    cmd /c %MVNW_EXE% -B -DjenkinsIT ^
        -Djava.io.tmpdir=%GITHUB_WORKSPACE% ^
        -Djacoco.skip.instrument=false ^
        -DintegrationTestSuites="%JDBC_TEST_SUITES%" ^
        -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn ^
        -Dnot-self-contained-jar %ADDITIONAL_MAVEN_PROFILE% ^
        verify ^
        --batch-mode --show-version > log.txt & type log.txt
    echo "[INFO] Check for test execution status"
    find /i /c "BUILD FAILURE" log.txt > NUL
    set isfound=!errorlevel!
    if !isfound! equ 0 (
        echo [ERROR] Failed run %%a test
        exit /b 1
    ) else (
        echo [INFO] Success run %%a test
    )
)

echo [INFO] Dropping schema %SNOWFLAKE_TEST_SCHEMA%
pushd %GITHUB_WORKSPACE%\ci\container
python drop_schema.py
popd
