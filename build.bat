@echo off
echo Compiling Doris UDF project...

REM Create output directory
if not exist "target\classes" mkdir "target\classes"

REM Compile Java files with UTF-8 encoding (main + test)
javac -encoding UTF-8 -d target\classes -cp . src\main\java\org\apache\doris\udf\*.java src\test\java\org\apache\doris\udf\*.java

java -cp target\classes org.apache.doris.udf.WindowFunnelTest

jar cf doris-udf-demo.jar -C target\classes .

if %ERRORLEVEL% EQU 0 (
    echo Compilation successful!
    echo Compiled files are in target\classes directory
    echo JAR file created successfully: doris-udf-demo.jar
    echo File size:
    dir doris-udf-demo.jar
) else (
    echo Compilation failed!
    pause
)