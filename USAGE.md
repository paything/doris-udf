# Doris WindowFunnel UDF 使用说明

## 概述

`WindowFunnel` 是一个用于Doris的Java UDF函数，用于执行漏斗分析。该函数可以分析用户行为序列，识别符合特定步骤顺序的路径，并返回时间戳和标签信息。

## 函数签名

```sql
window_funnel(
    分析窗口时长秒,
    步骤间隔时长秒,
    分析模式,
    事件字符串,
    标签列
)
```

## 参数说明

- **分析窗口时长秒** (Integer): 漏斗分析的时间窗口，单位为秒
- **步骤间隔时长秒** (Integer): 相邻步骤之间的最大时间间隔，单位为秒
- **分析模式** (String): 分析模式，如 "strict"（严格模式）
- **事件字符串** (String): 格式化的用户事件数据
- **标签列** (String): 可选的标签信息，用逗号分隔

## 输入数据格式

事件字符串格式：`时间戳#事件1@事件2@事件3#分组`

示例：
```
2024-01-01 00:00:00.123#1@0@0#tiktok,2024-01-01 00:00:01.345#0@1@0#tiktok,2024-01-01 00:00:03.111#0@0@1#tiktok
```

其中：
- `时间戳`: 格式为 `YYYY-MM-DD HH:mm:ss.SSS`
- `事件1@事件2@事件3`: 三个事件的标志位，1表示发生，0表示未发生
  - 事件1: 注册事件 (reg)
  - 事件2: 内购事件 (iap)  
  - 事件3: 聊天事件 (chat)
- `分组`: 用户分组信息

## 输出格式

返回JSON格式的数组，每个元素包含：
- 第一个事件的时间戳（毫秒）
- 后续步骤的时间差（毫秒）
- 标签数组

示例输出：
```json
[
  [1727752682000,0,0,["hk","cn","de"]],
  [1727814050000,0,0,["hk","cn","de"]],
  [1727785225000,0,0,["hk","cn","de"]]
]
```

## 在Doris中注册UDF

```sql
CREATE FUNCTION window_funnel(int, int, string, string) 
RETURNS string 
PROPERTIES (
    "file"="file:///path/to/doris-udf-demo-1.0.0-jar-with-dependencies.jar",
    "symbol"="org.apache.doris.udf.WindowFunnel",
    "always_nullable"="true",
    "type"="JAVA_UDF"
);
```

## 使用示例

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
    FROM event_log
    GROUP BY uid
) t;
```

## 构建JAR包

```bash
mvn clean package
```

生成的JAR包位于：`target/doris-udf-demo-1.0.0-jar-with-dependencies.jar`

## 测试

运行测试类验证功能：

```bash
mvn exec:java -Dexec.mainClass="org.apache.doris.udf.WindowFunnelTest"
```

## 注意事项

1. 确保Doris集群支持Java UDF
2. JAR包需要上传到Doris可访问的路径
3. 函数注册需要相应权限
4. 输入数据格式必须严格遵循规范
5. 时间戳解析支持毫秒精度 