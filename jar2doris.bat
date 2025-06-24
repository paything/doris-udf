@echo off
echo ========================================
echo 将JAR包复制到Doris容器中
echo ========================================

echo.
echo 1. 检查JAR文件是否存在...
if not exist "target\doris-udf-demo.jar" (
    echo 错误：找不到 doris-udf-demo.jar 文件
    echo 请先运行 build.bat 编译项目
    pause
    exit /b 1
)

echo JAR文件存在，继续执行...

echo.
echo 2. 在FE容器中创建目录...
docker exec doris-docker-fe-1 mkdir -p /opt/apache-doris/jdbc_drivers
if %errorlevel% neq 0 (
    echo 警告：在FE容器中创建目录失败，可能容器不存在或未运行
)

echo.
echo 3. 在BE容器中创建目录...
docker exec doris-docker-be-1 mkdir -p /opt/apache-doris/jdbc_drivers
if %errorlevel% neq 0 (
    echo 警告：在BE容器中创建目录失败，可能容器不存在或未运行
)

echo.
echo 4. 复制JAR包到FE容器...
docker cp target\doris-udf-demo.jar doris-docker-fe-1:/opt/apache-doris/jdbc_drivers/
if %errorlevel% neq 0 (
    echo 错误：复制JAR包到FE容器失败
    pause
    exit /b 1
) else (
    echo 成功复制JAR包到FE容器
)

echo.
echo 5. 复制JAR包到BE容器...
docker cp target\doris-udf-demo.jar doris-docker-be-1:/opt/apache-doris/jdbc_drivers/
if %errorlevel% neq 0 (
    echo 错误：复制JAR包到BE容器失败
    pause
    exit /b 1
) else (
    echo 成功复制JAR包到BE容器
)

echo.
echo ========================================
echo JAR包复制完成！
echo ========================================
echo.
echo 接下来需要在Doris中执行以下SQL来注册UDF：
echo.
echo CREATE FUNCTION window_funnel(BIGINT, BIGINT, STRING, STRING) RETURNS STRING PROPERTIES (
echo   "file"="file:///opt/apache-doris/jdbc_drivers/doris-udf-demo.jar",
echo   "symbol"="org.apache.doris.udf.WindowFunnel",
echo   "always_nullable"="true",
echo   "type"="JAVA_UDF"
echo );
echo.
pause 