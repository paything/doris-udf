-- 如果换了jar包，先删除再创建
DROP FUNCTION window_funnel(INT, INT, STRING, STRING) 

-- 创建udf
CREATE FUNCTION window_funnel(INT, INT, STRING, STRING) RETURNS STRING PROPERTIES (
  "file"="file:///opt/apache-doris/jdbc_drivers/doris-udf-demo.jar",
  "symbol"="org.apache.doris.udf.WindowFunnel",
  "always_nullable"="true",
  "type"="JAVA_UDF"
);

-- 测试语句
with event_log as (
select '2024-01-01 00:00:00.123' as dt,'payne' as uid,'reg' as event,'tiktok' as group0
union all
select '2024-01-01 00:00:01.345' as dt,'payne' as uid,'iap' as event,'tiktok' as group0
union all
select '2024-01-01 00:00:03.111' as dt,'payne' as uid,'chat' as event,'tiktok' as group0
union all
select '2024-02-01 00:00:00.012' as dt,'payne' as uid,'reg' as event,'fb' as group0
union all
select '2024-02-01 00:00:01.001' as dt,'payne' as uid,'iap' as event,'fb' as group0
union all
select '2024-02-01 00:00:02.434' as dt,'payne' as uid,'chat' as event,'fb' as group0
union all
select '2024-01-01 00:00:00.012' as dt,'cjt' as uid,'reg' as event,'fb' as group0
union all
select '2024-01-01 00:00:01.001' as dt,'cjt' as uid,'iap' as event,'fb' as group0
union all
select '2024-01-01 00:00:02.434' as dt,'cjt' as uid,'chat' as event,'fb' as group0
)
SELECT 
    uid,
    kuro_comments_ai.window_funnel(10, 1, 'strict', funnel_track) as funnel_result
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