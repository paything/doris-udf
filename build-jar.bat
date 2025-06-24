@echo off
echo Creating JAR file...

REM Create JAR file from compiled classes
jar cf doris-udf-demo.jar -C target\classes .

if %ERRORLEVEL% EQU 0 (
    echo JAR file created successfully: doris-udf-demo.jar
    echo File size:
    dir doris-udf-demo.jar
) else (
    echo Failed to create JAR file
    pause
) 