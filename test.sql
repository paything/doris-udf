-- 如果换了jar包，先删除再创建
DROP GLOBAL FUNCTION IF EXISTS window_funnel_track(INT, INT, STRING, STRING) ;
DROP GLOBAL FUNCTION IF EXISTS session_agg(STRING, STRING, STRING, INT, INT, STRING) ;

-- 进入mysql，在mysql里面创建函数，如果用了GLOBAL就不需要进入mysql
-- USE mysql;

-- 创建window_funnel_track UDF
CREATE GLOBAL FUNCTION window_funnel_track(INT, INT, STRING, STRING) RETURNS STRING PROPERTIES (
  "file"="file:///opt/apache-doris/jdbc_drivers/doris-udf-demo.jar",
  "symbol"="org.apache.doris.udf.WindowFunnel",
  "always_nullable"="true",
  "type"="JAVA_UDF"
);

-- 创建session_agg UDF
CREATE GLOBAL FUNCTION session_agg(STRING, STRING, STRING, INT, INT, STRING) RETURNS STRING PROPERTIES (
  "file"="file:///opt/apache-doris/jdbc_drivers/doris-udf-demo.jar",
  "symbol"="org.apache.doris.udf.SessionAgg",
  "always_nullable"="true",
  "type"="JAVA_UDF"
);


-- 测试不同分析模式
-- 1. default模式测试（允许时间相等）
with event_log as (
select '2024-01-01 00:00:00.123' as dt,'payne' as uid,'reg' as event,'tiktok#1' as group0
union all
select '2024-01-01 00:00:01.345' as dt,'payne' as uid,'iap' as event,'tiktok#1' as group0
union all
select '2024-01-01 00:00:03.111' as dt,'payne' as uid,'chat' as event,'tiktok#1' as group0
union all
select '2024-02-01 00:00:00.012' as dt,'payne' as uid,'reg' as event,'fb@,#2' as group0
union all
select '2024-02-01 00:00:01.001' as dt,'payne' as uid,'iap' as event,'fb@,#2' as group0
union all
select '2024-02-01 00:00:02.434' as dt,'payne' as uid,'chat' as event,'fb' as group0
union all
select '2024-01-01 00:00:00.012' as dt,'cjt' as uid,'reg' as event,'f@#,b' as group0
union all
select '2024-01-01 00:00:01.001' as dt,'cjt' as uid,'iap' as event,'f@#,@#,b' as group0
union all
select '2024-01-01 00:00:02.434' as dt,'cjt' as uid,'chat' as event,'fb' as group0
union all
select '2024-01-02 00:00:00.012' as dt,'cjt' as uid,'reg' as event,'f@#,b' as group0
union all
select '2024-01-02 00:00:03.012' as dt,'cjt' as uid,'iap' as event,'f@#,b' as group0
)
, track_udf as (
    SELECT 
        uid,
        group0 as link_col, --关联属性列，不需要就去掉
        window_funnel_track(
            10, 
            5, 
            'default', 
            group_concat(_event_string)
        ) as funnel_result
    FROM (
        select 
            *,
            concat(dt,'#'
            ,event='reg'
            ,'@',event='iap'
            ,'@',event='chat'
            ,'#',group0
            ) as _event_string
        from event_log
    ) t1
    GROUP BY 
    uid
    ,link_col  --关联属性列，不需要就去掉
)
select * 
    from 
    track_udf
    lateral view EXPLODE(cast(funnel_result as ARRAY<varchar>)) tmp as e1
;

-- 2. backtrack模式测试（允许倒序）
with event_log_backtrack as (
select '2024-01-01 00:00:00.000' as dt,'user2' as uid,'reg' as event,'te@st1' as group0
union all
select '2024-01-01 00:00:00.200' as dt,'user2' as uid,'chat' as event,'te#st2' as group0
union all
select '2024-01-01 00:00:00.300' as dt,'user2' as uid,'iap' as event,'te,st3' as group0
)
SELECT 
    'backtrack_mode' as test_type,
    uid,
    window_funnel_track(10, 5, 'backtrack', funnel_track) as funnel_result
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
    from event_log_backtrack
) t1
    GROUP BY uid
) t;

-- 3. increase模式测试（严格递增）
with event_log_increase as (
select '2024-01-01 00:00:00.000' as dt,'user3' as uid,'reg' as event,'te@st1' as group0
union all
select '2024-01-01 00:00:00.000' as dt,'user3' as uid,'iap' as event,'te#st2' as group0
union all
select '2024-01-01 00:00:00.100' as dt,'user3' as uid,'chat' as event,'te,st3' as group0
)
SELECT 
    'increase_mode' as test_type,
    uid,
    window_funnel_track(10, 5, 'increase', funnel_track) as funnel_result
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
    from event_log_increase
) t1
    GROUP BY uid
) t;


-- 4. 测试直接输出分析结果
with event_log as (
select '2024-01-01 00:00:00.123' as dt,'payne' as uid,'reg' as event,'tiktok#1' as group0
union all
select '2024-01-01 00:00:01.345' as dt,'payne' as uid,'iap' as event,'tiktok#1' as group0
union all
select '2024-01-01 00:00:03.111' as dt,'payne' as uid,'chat' as event,'tiktok#1' as group0
union all
select '2024-02-01 00:00:00.012' as dt,'payne' as uid,'reg' as event,'fb@,#2' as group0
union all
select '2024-02-01 00:00:01.001' as dt,'payne' as uid,'iap' as event,'fb@,#2' as group0
union all
select '2024-02-01 00:00:02.434' as dt,'payne' as uid,'chat' as event,'fb' as group0
union all
select '2024-01-01 00:00:00.012' as dt,'cjt' as uid,'reg' as event,'f@#,b' as group0
union all
select '2024-01-01 00:00:01.001' as dt,'cjt' as uid,'iap' as event,'f@#,@#,b' as group0
union all
select '2024-01-01 00:00:02.434' as dt,'cjt' as uid,'chat' as event,'fb' as group0
union all
select '2024-01-02 00:00:00.012' as dt,'cjt' as uid,'reg' as event,'f@#,b' as group0
union all
select '2024-01-02 00:00:03.012' as dt,'cjt' as uid,'iap' as event,'f@#,b' as group0
)
, track_udf as (
    SELECT 
        uid,
        group0 as link_col, --关联属性列，不需要就去掉
        window_funnel_track(
            10, 
            5, 
            'default', 
            group_concat(_event_string)
        ) as funnel_result
    FROM (
        select 
            *,
            concat(dt,'#'
            ,event='reg'
            ,'@',event='iap'
            ,'@',event='chat'
            ,'#',group0
            ) as _event_string
        from event_log
    ) t1
    GROUP BY 
    uid
    ,link_col  --关联属性列，不需要就去掉
)
,track_explode as (
select * from (
    select uid
    ,group0
    ,funnel_result
    ,funnel_path
    ,ifnull(duration_sum_second,0) as duration_sum_second
    ,step_count
    ,max(step_count) over(partition by uid,funnel_result) as funnel_track_max_step
    ,row_number() over(partition by uid,group0 order by funnel_path) as path_rank
    from (     
        select 
            uid
            ,link_col
            ,funnel_result
            ,e1 as funnel_path
            ,cast(cast(array_slice(cast(e1 as ARRAY<varchar>),-1,1) as array<string>)[1] as array<string>)[1] as group0
            ,abs(array_sum(array_popfront(array_popback(cast(e1 as ARRAY<bigint>)))))/1000 as duration_sum_second
            ,array_size(cast(e1 as ARRAY<bigint>))-1 as step_count
            from 
            track_udf
            lateral view EXPLODE(cast(funnel_result as ARRAY<varchar>)) tmp as e1
        ) t1
) t2 where 1=1 
--只保留最深的漏斗时，加上这个过滤，否则去掉
and step_count=funnel_track_max_step
--只保留最深的漏斗时，加上这个过滤，否则去掉
)

----1个用户有N个路径满足诉求时，有4个处理模式
,path_filter_mode as (
--模式1，全路径保留，无需任何处理
-- select 
-- uid
-- ,group0
-- ,array_popfront(cast(funnel_path as ARRAY<bigint>)) as funnel_track 
-- from track_explode

--模式2，只保留第一条路径，按时间排序后最早的第一条
-- select 
-- uid
-- ,group0
-- ,array_popfront(cast(funnel_path as ARRAY<bigint>)) as funnel_track 
-- from track_explode
-- where path_rank=1

--模式3/4保留1条整体耗时最长/最短的路径
select 
uid
,group0
,array_popfront(cast(max_by(funnel_path,duration_sum_second) as ARRAY<bigint>)) as funnel_track 
-- ,array_popfront(cast(min_by(funnel_path,duration_sum_second) as ARRAY<bigint>)) as funnel_track 
from track_explode
group by uid,group0
)

-- select * from path_filter_mode


select
  ifnull (group0, 'Total') as group0,
  funnel_level,
  count(distinct uid) as value,
  count(1) as path_count,
  round(avg(duration), 1) as `avg`,
  cast(percentile(duration, 0) as int) as p0,
  cast(percentile(duration, 0.25) as int) as p25,
  cast(percentile(duration, 0.5) as int) as p50,
  cast(percentile(duration, 0.75) as int) as p75,
  cast(percentile(duration, 0.90) as int) as p100,
  percentile(duration, 0.75) + 1.5 * (percentile(duration, 0.75) - percentile(duration, 0.25)) as 'Q3+1_5IQR',
  percentile(duration, 0.25) -1.5 * (percentile(duration, 0.75) - percentile(duration, 0.25)) as 'Q1-1_5IQR'
from
  (
    SELECT
      uid,
      group0,
      funnel_track,
      abs(funnel_track[e1] / 1000) as duration,
      e1 as funnel_level
    FROM
      path_filter_mode 
      lateral VIEW explode (array_enumerate (funnel_track)) tmp1 AS e1
  ) t
where
  funnel_level >= 1
group by
  grouping sets ((funnel_level), (group0, funnel_level))
order by
  ifnull (group0, 'Total'),
  funnel_level
;


-- 测试session_agg

with event_log as (
select '2024-01-01 00:00:00.123' as dt,'payne' as uid,'reg' as event,'tiktok#1' as group0,'cn' as group1
union all
select '2024-01-01 00:00:01.345' as dt,'payne' as uid,'iap' as event,'tiktok#1' as group0,'cn' as group1
union all
select '2024-01-01 00:00:03.111' as dt,'payne' as uid,'chat' as event,'tiktok#1' as group0,'cn' as group1
union all
select '2024-02-01 00:00:00.012' as dt,'payne' as uid,'reg' as event,'fb@,#2' as group0,'cn' as group1
union all
select '2024-02-01 00:00:01.001' as dt,'payne' as uid,'iap' as event,'fb@,#2' as group0,'cn' as group1
union all
select '2024-02-01 00:00:02.434' as dt,'payne' as uid,'chat' as event,'fb' as group0,'cn' as group1
union all
select '2024-01-01 00:00:00.012' as dt,'cjt' as uid,'reg' as event,'f@#,b' as group0,'cn' as group1
union all
select '2024-01-01 00:00:01.001' as dt,'cjt' as uid,'iap' as event,'f@#,@#,b' as group0,'cn' as group1
union all
select '2024-01-01 00:00:02.434' as dt,'cjt' as uid,'chat' as event,'fb' as group0,'cn' as group1
union all
select '2024-01-02 00:00:00.012' as dt,'cjt' as uid,'reg' as event,'f@#,b' as group0,'cn' as group1
union all
select '2024-01-02 00:00:03.012' as dt,'cjt' as uid,'iap' as event,'f@#,b' as group0,'cn' as group1
union all
select '2024-01-01 00:00:00' as dt,'aki' as uid,'reg' as event,'fb' as group0,'' as group1
union all
select '2024-01-01 00:30:00' as dt,'aki' as uid,'event_1' as event,'tt' as group0,'' as group1
union all
select '2024-01-01 00:59:00' as dt,'aki' as uid,'event_2' as event,'fb' as group0,'' as group1
union all
select '2024-01-01 00:40:00' as dt,'aki' as uid,'event_1.1' as event,'tt' as group0,'' as group1
union all
select '2024-01-01 00:40:00' as dt,'aki' as uid,'event_1.1' as event,'fb' as group0,'' as group1
union all
select '2024-01-01 00:40:00' as dt,'aki' as uid,'reg' as event,'tt' as group0,'' as group1
union all
select '2024-01-01 00:40:00' as dt,'aki' as uid,'reg' as event,'fb' as group0,'' as group1
)
,session_udf as (
select 
    e1,
    count(1) as value
    from (
        select * from (
                select 
                path_uid,
                session_agg(
                    'iap',           -- 标记事件名称
                    'start',         -- 标记事件类型：start/end
                    group_concat(path), -- 事件字符串
                    30*60,           -- 会话间隔时长（秒）
                    10,              -- 会话最大步数
                    'session_uniq_with_group' --分析模式：default,cons_uniq,session_uniq,cons_uniq_with_group,session_uniq_with_group
                ) as session_result
                from (
                        select 
                            json_array(dt,event,group0,group1)  as path,
                            uid as path_uid
                                from 
                            event_log
                ) t1
                group by 
                path_uid
            ) t2 lateral view EXPLODE(cast(session_result as ARRAY<varchar>)) tmp as e1
    ) t3
    group by e1       
)



-- 分组项大于某个数量时，把长尾分组项转为其他，if 里面rank1<的值就是控制分组数量
,sankey_dim as (
SELECT
    e2,
    IF (rank1 < 2 or JSON_LENGTH(e2_json)=2
        ,e2_json
        ,JSON_EXTRACT(e2_json,'$.[0]','$.[1]')
    ) AS e3
    ,rank1
    ,VALUE
FROM
    (
    select 
    index1
    ,e2
    ,JSON_PARSE(e2) as e2_json
    ,value
    ,row_number() over (partition by index1 order by value desc) as rank1
    from (
            select 
            cast(e2 as array<int>)[1] as index1
            ,e2
            ,sum(value) as value 
            from (
                select 
                    *    
                from 
                    session_udf
                    lateral view EXPLODE(cast(e1 as ARRAY<varchar>)) tmp as e2
            ) t group by e2
        ) t3
    ) t4
) 

-- select * from sankey_dim
-- 把一些分组项转成其他，下面的结果留空就是转成其他了
select
-- t1.e1,
-- t1.e2,
-- sankey_dim.e3,
-- cast(t1.e2 as array<int>)[1] as col_num,
group_concat(sankey_dim.e3 order by t1.value,cast(t1.e2 as array<int>)[1]) as e1,
t1.value
from
  (
    select
      *
    from
      session_udf lateral view EXPLODE(cast(e1 as ARRAY<varchar>)) tmp as e2
  ) t1
  left join sankey_dim on t1.e2 = sankey_dim.e2
group by
  t1.e1,
  t1.value

;