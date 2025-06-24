@echo off
echo ========================================
echo Copy JAR package to Doris containers
echo ========================================

echo.
echo 1. Checking if JAR file exists...
if not exist "doris-udf-demo.jar" (
    echo Error: doris-udf-demo.jar file not found
    echo Please run build.bat first to compile the project
    pause
    exit /b 1
)

echo JAR file exists, continuing...

echo.
echo 2. Creating directory in FE container...
docker exec doris-docker-fe-1 mkdir -p /opt/apache-doris/jdbc_drivers
if %errorlevel% neq 0 (
    echo Warning: Failed to create directory in FE container, container may not exist or not running
)

echo.
echo 3. Creating directory in BE container...
docker exec doris-docker-be-1 mkdir -p /opt/apache-doris/jdbc_drivers
if %errorlevel% neq 0 (
    echo Warning: Failed to create directory in BE container, container may not exist or not running
)

echo.
echo 4. Copying JAR package to FE container...
docker cp doris-udf-demo.jar doris-docker-fe-1:/opt/apache-doris/jdbc_drivers/
if %errorlevel% neq 0 (
    echo Error: Failed to copy JAR package to FE container
    pause
    exit /b 1
) else (
    echo Successfully copied JAR package to FE container
)

echo.
echo 5. Copying JAR package to BE container...
docker cp doris-udf-demo.jar doris-docker-be-1:/opt/apache-doris/jdbc_drivers/
if %errorlevel% neq 0 (
    echo Error: Failed to copy JAR package to BE container
    pause
    exit /b 1
) else (
    echo Successfully copied JAR package to BE container
)

echo.
echo ========================================
echo JAR package copy completed!
echo ========================================
echo.
echo Next, execute the following SQL in Doris to register the UDF:
echo.
echo CREATE FUNCTION window_funnel(INT, INT, STRING, STRING) RETURNS STRING PROPERTIES (
echo   "file"="file:///opt/apache-doris/jdbc_drivers/doris-udf-demo.jar",
echo   "symbol"="org.apache.doris.udf.WindowFunnel",
echo   "always_nullable"="true",
echo   "type"="JAVA_UDF"
echo );
echo.
pause 