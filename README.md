# Doris UDF - WindowFunnel 漏斗分析函数

这是一个用于Apache Doris的Java UDF函数，专门用于执行漏斗分析。该函数可以分析用户行为序列，识别符合特定步骤顺序的路径，并返回时间戳、时间差和标签信息。

## 功能特性

- 支持多步骤漏斗分析（不限步骤数）
- 可配置的时间窗口和步骤间隔
- 灵活的分析模式
- 支持标签分组，标签自动从事件字符串提取
- 毫秒级时间戳精度
- 支持多条漏斗路径输出
- 支持混合时间格式（毫秒级和秒级）

## 项目结构

```
doris-udf/
├── src/
│   ├── main/java/org/apache/doris/udf/
│   │   ├── UDF.java                    # UDF基类
│   │   └── WindowFunnel.java           # 漏斗分析UDF实现
│   └── test/java/org/apache/doris/udf/
│       └── WindowFunnelTest.java       # 测试类
├── pom.xml                             # Maven配置文件
├── README.md                           # 项目说明
├── build.bat                           # 编译脚本
├── build-jar.bat                       # JAR打包脚本
├── jar2doris.bat                       # JAR包部署到Doris容器脚本
├── doris-udf-demo.jar                  # 编译生成的JAR包
└── target/                             # 编译输出目录
```

## 快速开始

### 1. 编译与打包

```bash
build.bat         # 编译Java代码并生成JAR包
```

### 2. 部署到Doris容器

```bash
jar2doris.bat     # 将JAR包复制到Doris的FE和BE容器中
```

### 3. 在Doris中注册UDF

```sql
CREATE FUNCTION window_funnel(INT, INT, STRING, STRING) 
RETURNS STRING 
PROPERTIES (
    "file"="file:///opt/apache-doris/jdbc_drivers/doris-udf-demo.jar",
    "symbol"="org.apache.doris.udf.WindowFunnel",
    "always_nullable"="true",
    "type"="JAVA_UDF"
);
```

### 4. 使用示例

```sql
with event_log as (
select '2024-01-01 00:00:00.123' as dt,'payne' as uid,'reg' as event,'tiktok' as group0
union all
select '2024-01-01 00:00:01.345' as dt,'payne' as uid,'iap' as event,'tiktok' as group0
union all
select '2024-01-01 00:00:03.111' as dt,'payne' as uid,'chat' as event,'tiktok' as group0
union all
select '2024-01-01 00:00:00.012' as dt,'cjt' as uid,'reg' as event,'fb' as group0
union all
select '2024-01-01 00:00:01.001' as dt,'cjt' as uid,'iap' as event,'fb' as group0
union all
select '2024-01-01 00:00:02.434' as dt,'cjt' as uid,'chat' as event,'fb' as group0
)
SELECT 
    uid,
    window_funnel(10, 1, 'strict', funnel_track) as funnel_result
FROM (
    SELECT 
        uid,
        group_concat(event_string) as funnel_track
    FROM (
    select 
        uid,
        concat(dt,'#'
        ,event='reg'
        ,'@',event='iap'
        ,'@',event='chat'
        ,'#',group0
        ) as event_string
    from event_log
) t1
    GROUP BY uid
) t;
```

## 详细使用说明

### UDF函数签名

```sql
window_funnel(
    分析窗口时长秒,
    步骤间隔时长秒,
    分析模式,
    事件字符串
)
```

### 参数说明

- **分析窗口时长秒** (INT): 漏斗分析的时间窗口，单位为秒
- **步骤间隔时长秒** (INT): 相邻步骤之间的最大时间间隔，单位为秒
- **分析模式** (STRING): 分析模式，如 "strict"（严格模式）
- **事件字符串** (STRING): 多条事件拼接的字符串，格式见下

### 输入数据格式

事件字符串格式：`时间戳#事件1@事件2@...@事件N#标签`

示例（3步）：
```
2024-01-01 00:00:00.123#1@0@0#tiktok,2024-01-01 00:00:01.345#0@1@0#fb,2024-01-01 00:00:03.111#0@0@1#wechat
```
示例（20步）：
```
2024-01-01 00:00:00.000#1@0@0@...@0#step1,2024-01-01 00:00:00.100#0@1@0@...@0#step2,...,2024-01-01 00:00:01.900#0@0@...@1#step20
```

- `时间戳`: 格式为 `YYYY-MM-DD HH:mm:ss.SSS` 或 `YYYY-MM-DD HH:mm:ss`
- `事件1@事件2@...@事件N`: N个步骤的标志位，1表示发生，0表示未发生
- `标签`: 每条事件的标签（如渠道、国家、步骤名等）

### 输出格式

返回JSON格式的二维数组，每个元素为一条完整路径：
```
[
  [第一个事件的时间戳, 步骤2-1时间差, 步骤3-2时间差, ..., ["标签1","标签2",...]],
  ...
]
```

示例输出（3步）：
```
[
  [1704038400123,1222,1766,["tiktok","fb","wechat"]],
  [1706716800123,1222,1766,["tiktok","fb","wechat"]]
]
```
示例输出（20步）：
```
[
  [1704038400000,100,100,...,100,["step1","step2",...,"step20"]]
]
```

## 部署说明

### 使用jar2doris.bat脚本

1. **前提条件**：
   - Docker容器已启动
   - 容器名称分别为：`doris-docker-fe-1` 和 `doris-docker-be-1`
   - 已运行 `build.bat` 生成JAR包

2. **执行部署**：
   ```bash
   jar2doris.bat
   ```

3. **脚本功能**：
   - 检查JAR文件是否存在
   - 在FE和BE容器中创建目录 `/opt/apache-doris/jdbc_drivers/`
   - 复制JAR包到两个容器中
   - 显示注册UDF的SQL语句

### 手动部署

如果容器名称不同，可以手动执行：

```bash
# 在be、fe创建文件夹
docker exec <fe-container-name> mkdir -p /opt/apache-doris/jdbc_drivers
docker exec <be-container-name> mkdir -p /opt/apache-doris/jdbc_drivers

# 把jar包复制到对应文件夹
docker cp doris-udf-demo.jar <fe-container-name>:/opt/apache-doris/jdbc_drivers/
docker cp doris-udf-demo.jar <be-container-name>:/opt/apache-doris/jdbc_drivers/
```

## 构建JAR包

```bash
build.bat         # 编译Java代码并生成JAR包
```

生成的JAR包位于：`doris-udf-demo.jar`

## 测试

运行测试验证功能：

```bash
build.bat
```

测试包括：
- 3步骤多路径测试
- 20步骤单路径测试
- 混合时间格式测试（毫秒级和秒级）

## 注意事项

1. 确保Doris集群支持Java UDF
2. JAR包需要上传到Doris可访问的路径
3. 函数注册需要相应权限
4. 输入数据格式必须严格遵循规范
5. 支持多路径、多步骤、毫秒级时间差
6. 支持混合时间格式，自动识别毫秒级和秒级时间戳