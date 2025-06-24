# Doris UDF - WindowFunnel 漏斗分析函数

这是一个用于Apache Doris的Java UDF函数，专门用于执行漏斗分析。该函数可以分析用户行为序列，识别符合特定步骤顺序的路径，并返回时间戳和标签信息。

## 功能特性

- 支持多步骤漏斗分析（注册 → 内购 → 聊天）
- 可配置的时间窗口和步骤间隔
- 灵活的分析模式
- 支持标签分组
- 毫秒级时间戳精度

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
└── USAGE.md                            # 详细使用说明
```

## 快速开始

### 1. 构建项目

```bash
mvn clean package
```

### 2. 在Doris中注册UDF

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

### 3. 使用示例

```sql
-- 使用您提供的数据格式
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

## 输入数据格式

事件字符串格式：`时间戳#事件1@事件2@事件3#分组`

示例：
```
2024-01-01 00:00:00.123#1@0@0#tiktok,2024-01-01 00:00:01.345#0@1@0#tiktok,2024-01-01 00:00:03.111#0@0@1#tiktok
```

## 输出格式

返回JSON格式的数组：
```json
[
  [1727752682000,0,0,["hk","cn","de"]],
  [1727814050000,0,0,["hk","cn","de"]],
  [1727785225000,0,0,["hk","cn","de"]]
]
```

## 详细文档

更多详细信息请参考 [USAGE.md](USAGE.md) 文件。

## 测试

运行测试验证功能：

```bash
mvn exec:java -Dexec.mainClass="org.apache.doris.udf.WindowFunnelTest"
```

## 注意事项

1. 确保Doris集群支持Java UDF
2. JAR包需要上传到Doris可访问的路径
3. 函数注册需要相应权限
4. 输入数据格式必须严格遵循规范