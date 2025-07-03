<script setup>
import { ref, nextTick, watch, onMounted, onUnmounted, computed } from 'vue'
import * as VTable from '@visactor/vtable';
import VChart from '@visactor/vchart';

// ====== 全局API地址配置 ======
// 自动读取本机IP（开发环境），如需指定远程IP请手动覆盖
const API_BASE_URL = `http://${window.location.hostname}:5012`;
// 生产环境可改为：const API_BASE_URL = 'http://10.0.159.131:5012';
// ============================

// 漏斗分析参数标签
const FUNNEL_FIELD_LABELS = {
  window_seconds: '时间窗口(秒)',
  session_interval_seconds: '步骤间隔(秒)',
  mode: '分析模式',
  keep_deepest: '保留最深漏斗',
  path_filter_mode: '多路径处理模式'
};

// 会话聚合参数标签
const SESSION_FIELD_LABELS = {
  mark_event: '标记事件',
  mark_type: '标记类型',
  session_interval_seconds: '会话间隔(秒)',
  max_steps: '最大步数',
  max_group_count: '最大分组数',
  replace_value: '替换值',
  session_mode: '会话模式'
};

// 漏斗分析默认参数
const defaultFunnelParams = () => ({
  window_seconds: 60,
  session_interval_seconds: 30,
  mode: 'default',
  keep_deepest: true,
  path_filter_mode: 'max_duration' // all_paths, first_path, max_duration, min_duration
});

// 会话聚合默认参数
const defaultSessionParams = () => ({
  mark_event: 'reg',
  mark_type: 'start',
  session_interval_seconds: 1800,
  max_steps: 10,
  max_group_count: 2,
  replace_value: 'replace_value',
  session_mode: 'session_uniq_with_group'
});

const funnelParams = ref(defaultFunnelParams());
const sessionParams = ref(defaultSessionParams());

const loading1 = ref(false);
const loading2 = ref(false);
const errorMsg1 = ref('');
const errorMsg2 = ref('');
const result1 = ref(null);
const result2 = ref(null);

const sidebarCollapsed = ref(false);

// 图表数据
const funnelChartData = ref(null);
const funnelBoxData = ref(null);
const sankeyChartData = ref(null);

// 漏斗分析SQL构建
function buildFunnelSQL(params) {
  const keepDeepestFilter = params.keep_deepest ? 'and step_count=funnel_track_max_step' : '';
  
  let pathFilterSQL = '';
  switch (params.path_filter_mode) {
    case 'all_paths':
      pathFilterSQL = `
select 
uid
,group0
,array_popfront(cast(funnel_path as ARRAY<bigint>)) as funnel_track 
from track_explode
      `;
      break;
    case 'first_path':
      pathFilterSQL = `
select 
uid
,group0
,array_popfront(cast(funnel_path as ARRAY<bigint>)) as funnel_track 
from track_explode
where path_rank=1
      `;
      break;
    case 'max_duration':
      pathFilterSQL = `
select 
uid
,group0
,array_popfront(cast(max_by(funnel_path,duration_sum_second) as ARRAY<bigint>)) as funnel_track 
from track_explode
group by uid,group0
      `;
      break;
    case 'min_duration':
      pathFilterSQL = `
select 
uid
,group0
,array_popfront(cast(min_by(funnel_path,duration_sum_second) as ARRAY<bigint>)) as funnel_track 
from track_explode
group by uid,group0
      `;
      break;
  }

  return `
-- 漏斗分析SQL
with track_udf as (
    SELECT 
        uid,
        -- group0 as link_col,
        window_funnel_track(
            ${params.window_seconds}, 
            ${params.session_interval_seconds}, 
            '${params.mode}', 
            group_concat(_event_string)
        ) as funnel_result
    FROM (
    select 
            *,
            concat(dt,'#'
            ,event='reg'
            ,'@',event='iap'
            ,'@',event='chat'
            ,'#',group1
            ) as _event_string
        from doris_udf_test.user_event_log
    ) t1
    GROUP BY 
    uid
    --,link_col
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
            --,link_col
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
${keepDeepestFilter}
)
,path_filter_mode as (
${pathFilterSQL}
)
select
  ifnull (group0, 'Total') as group0,
  funnel_level,
  count(distinct uid) as value,
  count(1) as path_count,
  round(avg(duration), 1) as avg_duration,
  cast(percentile(duration, 0) as int) as p0,
  cast(percentile(duration, 0.25) as int) as p25,
  cast(percentile(duration, 0.5) as int) as p50,
  cast(percentile(duration, 0.75) as int) as p75,
  cast(percentile(duration, 0.90) as int) as p90,
  cast(percentile(duration, 1) as int) as p100,
  percentile(duration, 0.75) + 1.5 * (percentile(duration, 0.75) - percentile(duration, 0.25)) as Q3_plus_1_5IQR,
  percentile(duration, 0.25) -1.5 * (percentile(duration, 0.75) - percentile(duration, 0.25)) as Q1_minus_1_5IQR
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
  `;
}

// 会话聚合SQL构建
function buildSessionSQL(params) {
  return `
-- 会话聚合SQL
with session_udf as (
select 
    path_uid,
    session_agg(
        '${params.mark_event}',
        '${params.mark_type}',
        group_concat(path),
        ${params.session_interval_seconds},
        ${params.max_steps},
        ${params.max_group_count},
        '${params.replace_value}',
        '${params.session_mode}' 
    ) as session_result
    from (
        select 
            json_array(dt,event,group0,group1) as path,
            uid as path_uid
        from doris_udf_test.user_event_log
    ) t1
group by 
    path_uid
)
select 
    e1,
    count(1) as value
from
    session_udf lateral view EXPLODE(cast(session_result as ARRAY<varchar>)) tmp as e1    
group by e1
  `;
}

// 漏斗分析查询
async function handleFunnelQuery() {
  loading1.value = true;
  errorMsg1.value = '';
  result1.value = null;
  funnelChartData.value = null;
  funnelBoxData.value = null;
  
  const sql = buildFunnelSQL(funnelParams.value);
  try {
    const resp = await fetch(`${API_BASE_URL}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql1: sql })
    });
    if (!resp.ok) throw new Error('后端服务异常');
    const data = await resp.json();
    result1.value = data.result1;
    
    // 转换数据用于图表渲染
    if (data.result1 && data.result1.data) {
      transformFunnelData(data.result1.data);
    }
  } catch (e) {
    errorMsg1.value = e.message;
  } finally {
    loading1.value = false;
  }
}

// 会话聚合查询
async function handleSessionQuery() {
  loading2.value = true;
  errorMsg2.value = '';
  result2.value = null;
  sankeyChartData.value = null;
  
  const sql = buildSessionSQL(sessionParams.value);
  try {
    const resp = await fetch(`${API_BASE_URL}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql1: sql })
    });
    if (!resp.ok) throw new Error('后端服务异常');
    const data = await resp.json();
    result2.value = data.result1;
    
    // 转换数据用于桑基图渲染
    if (data.result1 && data.result1.data) {
      transformSankeyData(data.result1.data);
    }
  } catch (e) {
    errorMsg2.value = e.message;
  } finally {
    loading2.value = false;
  }
}

// 转换漏斗数据用于图表渲染
function transformFunnelData(data) {
  console.log('转换漏斗数据:', data);
  
  // 漏斗图数据 - 只使用Total汇总数据
  const funnelData = data
    .filter(row => row.group0 === 'Total')
    .map(row => ({
      step: `步骤${row.funnel_level}`,
      value: row.value,
      conversion: row.funnel_level === 1 ? 100 : 
        Math.round((row.value / data.find(r => r.group0 === 'Total' && r.funnel_level === 1)?.value) * 100)
    }));
  funnelChartData.value = funnelData;
  console.log('漏斗图数据:', funnelData);
  
  // 箱型图数据 - 显示步骤间的间隔时间，而不是每个步骤的耗时
  // 过滤掉最后一个步骤，因为箱型图显示的是步骤间的间隔
  const boxData = data
    .filter(row => {
      // 找到当前分组中最大的步骤数
      const maxStepInGroup = Math.max(...data
        .filter(r => r.group0 === row.group0)
        .map(r => r.funnel_level)
      );
      // 只保留不是最后一个步骤的数据
      return row.funnel_level < maxStepInGroup;
    })
    .map(row => ({
      step: `步骤${row.funnel_level}→${row.funnel_level + 1}`, // 显示步骤间的间隔
      min: row.p0,
      q1: row.p25,
      median: row.p50,
      q3: row.p75,
      max: row.p100,
      avg: row.avg_duration,
      group0: row.group0  // 保留分组信息
    }));
  funnelBoxData.value = boxData;
  console.log('箱型图数据:', boxData);
  
  nextTick(() => {
    console.log('开始渲染漏斗图...');
    renderFunnelChart();
    console.log('开始渲染箱型图...');
    renderBoxChart();
  });
}

// 转换桑基图数据
function transformSankeyData(data) {
  console.log('转换桑基图数据:', data);
  
  // 解析数据格式: [[1,"reg","f@#,b","cn"],[2,"iap","f@#,b","cn"]]
  const nodes = [];
  const links = [];
  const nodeMap = new Map();
  
  data.forEach(row => {
    if (row.e1 && typeof row.e1 === 'string') {
      try {
        // 尝试解析JSON数组格式
        const pathData = JSON.parse(row.e1);
        if (Array.isArray(pathData)) {
          pathData.forEach((item, index) => {
            if (Array.isArray(item) && item.length >= 2) {
              const pathId = item[0];
              const eventName = item[1];
              const nodeName = `${eventName}_${pathId}`;
              
              if (!nodeMap.has(nodeName)) {
                nodeMap.set(nodeName, {
                  name: nodeName,
                  value: row.value || 1
                });
                nodes.push({
                  name: nodeName,
                  value: row.value || 1
                });
              }
            }
          });
          
          // 创建连接关系
          for (let i = 0; i < pathData.length - 1; i++) {
            const currentItem = pathData[i];
            const nextItem = pathData[i + 1];
            
            if (Array.isArray(currentItem) && Array.isArray(nextItem) && 
                currentItem.length >= 2 && nextItem.length >= 2) {
              const sourceName = `${currentItem[1]}_${currentItem[0]}`;
              const targetName = `${nextItem[1]}_${nextItem[0]}`;
              
              links.push({
                source: sourceName,
                target: targetName,
                value: row.value || 1
              });
            }
          }
        }
      } catch (e) {
        console.warn('解析桑基图数据失败:', e);
        // 如果不是JSON格式，直接使用原始数据
        const nodeName = row.e1;
        if (!nodeMap.has(nodeName)) {
          nodeMap.set(nodeName, {
            name: nodeName,
            value: row.value || 1
          });
          nodes.push({
            name: nodeName,
            value: row.value || 1
          });
        }
      }
    }
  });
  
  // 如果没有足够的节点或连接，使用示例数据
  if (nodes.length < 2 || links.length === 0) {
    console.log('使用示例桑基图数据');
    sankeyChartData.value = {
      nodes: [
        { name: '机会人群' },
        { name: '高潜用户0' },
        { name: '高潜用户' },
        { name: '复购忠诚' },
        { name: '首课新单' },
        { name: '副购忠诚' },
        { name: '其他', value: 199999 },
        { name: '首单新客', value: 999 }
      ],
      links: [
        { source: '机会人群', target: '高潜用户', value: 199999 },
        { source: '高潜用户0', target: '高潜用户', value: 299999 },
        { source: '首课新单', target: '高潜用户', value: 399999 },
        { source: '首课新单', target: '复购忠诚', value: 499999 },
        { source: '副购忠诚', target: '高潜用户', value: 599999 }
      ]
      };
    } else {
    sankeyChartData.value = { nodes, links };
  }
  
  console.log('桑基图数据:', sankeyChartData.value);
  
  nextTick(() => {
    renderSankeyChart();
  });
}

// 渲染漏斗图
function renderFunnelChart() {
  const container = document.getElementById('funnel-chart');
  if (!container || !funnelChartData.value) return;
  
  container.innerHTML = '';
  
  const spec = {
    type: 'common',
    width: 600,
    height: 400,
    padding: { right: 80, left: 20 },
    color: {
      type: 'ordinal',
      range: ['#00328E', '#0048AA', '#4E91FF', '#8FC7FF', '#AEE2FF']
    },
    data: [
      {
        id: 'funnel',
        values: funnelChartData.value
      }
    ],
    series: [
      {
        type: 'funnel',
        maxSize: '65%',
        minSize: '10%',
        isTransform: true,
        shape: 'rect',
        funnel: {
          style: {
            cornerRadius: 4,
            stroke: 'white',
            lineWidth: 2
          },
          state: {
            hover: {
              stroke: '#4e83fd',
              lineWidth: 1
            }
          }
        },
        transform: {
          style: {
            stroke: 'white',
            lineWidth: 2
          },
          state: {
            hover: {
              stroke: '#4e83fd',
              lineWidth: 1
            }
          }
        },
        label: {
          visible: true,
          smartInvert: true,
          style: {
            lineHeight: 16,
            limit: Infinity,
            text: datum => [`${datum.step}`, `${datum.value}`]
          }
        },
        outerLabel: {
          visible: true,
          position: 'left',
          style: {
            fontSize: 12
          },
          line: {
            style: {
              lineDash: [4, 4]
            }
          }
        },
        transformLabel: {
          visible: true,
          style: {
            fill: 'black'
          }
        },
        extensionMark: [
          {
            type: 'polygon',
            dataId: 'funnel',
            style: {
              points: (datum, ctx, params, dataView) => {
                const data = dataView.latestData;
                if (!data) return;
                const curIndex = data.findIndex(d => d.step === datum.step);
                if (curIndex === data.length - 1) {
                  return;
                }
                const nextDatum = data[curIndex + 1];
                const firstDatum = data[0];

                const points = ctx.vchart.getChart().getSeriesInIndex(0)[0].getPoints(datum);
                const nextPoints = ctx.vchart.getChart().getSeriesInIndex(0)[0].getPoints(nextDatum);
                const firstPoints = ctx.vchart.getChart().getSeriesInIndex(0)[0].getPoints(firstDatum);

                const tr = points[1];
                const tb = points[2];
                const next_tr = nextPoints[1];
                const first_tr = firstPoints[1];

                const result = [
                  { x: tr.x + 5, y: (tr.y + tb.y) / 2 },
                  { x: first_tr.x + 20, y: (tr.y + tb.y) / 2 },
                  {
                    x: first_tr.x + 20,
                    y: (tr.y + tb.y) / 2 + (next_tr.y - tr.y) - 10
                  },
                  {
                    x: next_tr.x + 5,
                    y: (tr.y + tb.y) / 2 + (next_tr.y - tr.y) - 10
                  }
                ];
                return result;
              },
              cornerRadius: 5,
              stroke: 'rgb(200,200,200)',
              strokeOpacity: 0.5,
              lineWidth: 2,
              closePath: false
            }
          },
          {
            type: 'symbol',
            dataId: 'funnel',
            style: {
              visible: (datum, ctx, params, dataView) => {
                const data = dataView.latestData;
                if (!data) return;
                const curIndex = data.findIndex(d => d.step === datum.step);
                if (curIndex === data.length - 1) {
                  return false;
                }
                return true;
              },
              x: (datum, ctx, params, dataView) => {
                const data = dataView.latestData;
                if (!data) return;
                const curIndex = data.findIndex(d => d.step === datum.step);
                if (curIndex === data.length - 1) {
                  return;
                }
                const nextDatum = data[curIndex + 1];
                const nextPoints = ctx.vchart.getChart().getSeriesInIndex(0)[0].getPoints(nextDatum);
                const next_tr = nextPoints[1];
                return next_tr.x + 5;
              },
              y: (datum, ctx, params, dataView) => {
                const data = dataView.latestData;
                if (!data) return;
                const curIndex = data.findIndex(d => d.step === datum.step);
                if (curIndex === data.length - 1) {
                  return;
                }
                const nextDatum = data[curIndex + 1];
                const points = ctx.vchart.getChart().getSeriesInIndex(0)[0].getPoints(datum);
                const nextPoints = ctx.vchart.getChart().getSeriesInIndex(0)[0].getPoints(nextDatum);

                const tr = points[1];
                const tb = points[2];
                const next_tr = nextPoints[1];

                return (tr.y + tb.y) / 2 + (next_tr.y - tr.y) - 10;
              },
              size: 8,
              scaleX: 0.8,
              symbolType: 'triangleLeft',
              cornerRadius: 2,
              fill: 'rgb(200,200,200)'
            }
          },
          {
            type: 'text',
            dataId: 'funnel',
            style: {
              text: datum => `${datum.step}  ${datum.conversion}%`,
              textAlign: 'left',
              visible: (datum, ctx, params, dataView) => {
                const data = dataView.latestData;
                if (!data) return;
                const curIndex = data.findIndex(d => d.step === datum.step);
                if (curIndex === data.length - 1) {
                  return false;
                }
                return true;
              },
              x: (datum, ctx, params, dataView) => {
                const data = dataView.latestData;
                if (!data) return;

                const firstDatum = data[0];
                const firstPoints = ctx.vchart.getChart().getSeriesInIndex(0)[0].getPoints(firstDatum);
                const tr = firstPoints[1];

                return tr.x + 20 + 10;
              },
              y: (datum, ctx, params, dataView) => {
                const data = dataView.latestData;
                if (!data) return;
                const curIndex = data.findIndex(d => d.step === datum.step);
                if (curIndex === data.length - 1) {
                  return;
                }
                const nextDatum = data[curIndex + 1];
                const points = ctx.vchart.getChart().getSeriesInIndex(0)[0].getPoints(datum);
                const nextPoints = ctx.vchart.getChart().getSeriesInIndex(0)[0].getPoints(nextDatum);

                const tr = points[1];
                const tb = points[2];
                const next_tr = nextPoints[1];

                return ((tr.y + tb.y) / 2 + (next_tr.y - tr.y) - 10 + (tr.y + tb.y) / 2) / 2;
              },
              fontSize: 12,
              fill: 'black'
            }
          }
        ],
        categoryField: 'step',
        valueField: 'value'
      }
    ]
  };
  
  try {
    const vchart = new VChart(spec, { dom: 'funnel-chart' });
    vchart.renderSync();
  } catch (error) {
    console.error('漏斗图渲染失败:', error);
    container.innerHTML = '<div style="padding: 20px; text-align: center; color: #999;">漏斗图渲染失败</div>';
  }
}

// 渲染箱型图
function renderBoxChart() {
  const container = document.getElementById('box-chart');
  if (!container || !funnelBoxData.value) return;
  
  container.innerHTML = '';
  
  // 转换数据格式为VChart需要的格式，支持分组
  const boxData = [];
  
  // 处理分组数据
  funnelBoxData.value.forEach(item => {
    // 如果有分组数据，为每个分组创建一条记录
    if (item.group0 && item.group0 !== 'Total') {
      boxData.push({
        x: item.step,
        y1: item.min,
        y2: item.q1,
        y3: item.median,
        y4: item.q3,
        y5: item.max,
        avg: item.avg,
        group: item.group0
        });
      } else {
      // Total汇总数据
      boxData.push({
        x: item.step,
        y1: item.min,
        y2: item.q1,
        y3: item.median,
        y4: item.q3,
        y5: item.max,
        avg: item.avg,
        group: 'Total'
      });
    }
  });
  
  const spec = {
    type: 'boxPlot',
    width: 1200,
    height: 400,
    padding: { top: 20, right: 20, bottom: 40, left: 60 },
    data: {
      values: boxData
    },
    xField: ['x', 'group'],
    minField: 'y1',
    q1Field: 'y2',
    medianField: 'y3',
    q3Field: 'y4',
    maxField: 'y5',
    seriesField: 'group',
    direction: 'vertical',
    color: ['#62CDFF', '#9E4784', '#FF6B6B', '#4ECDC4', '#45B7D1'],
    title: {
      visible: true,
      text: '各步骤间间隔时间分布'
    },
    legends: {
      visible: true,
      data: data => {
        return data.map(obj => {
          obj.shape.fill = obj.shape.stroke;
          return obj;
        });
      }
    },
    axes: [
      {
        orient: 'left',
        title: { visible: true, text: '间隔时间(秒)' }
      },
      {
        orient: 'bottom',
        title: { visible: true, text: '步骤间间隔' }
      }
    ],
    boxPlot: {
      style: {
        shaftShape: 'line',
        lineWidth: 2
      }
    }
  };
  
  try {
    const vchart = new VChart(spec, { dom: 'box-chart' });
    vchart.renderSync();
  } catch (error) {
    console.error('箱型图渲染失败:', error);
    // 如果箱型图失败，回退到柱状图
    renderBoxChartAsBar();
  }
}

// 箱型图失败时的回退方案 - 柱状图
function renderBoxChartAsBar() {
  const container = document.getElementById('box-chart');
  if (!container || !funnelBoxData.value) return;
  
  container.innerHTML = '';
  
  // 转换数据格式为柱状图格式
  const barData = funnelBoxData.value.map(item => ({
    step: item.step,
    avg: item.avg
  }));
  
  const spec = {
    type: 'bar',
    data: {
      values: barData
    },
    xField: 'step',
    yField: 'avg',
    title: {
      visible: true,
      text: '各步骤间平均间隔时间'
    },
    axes: [
      {
        orient: 'left',
        title: { visible: true, text: '间隔时间(秒)' }
      },
      {
        orient: 'bottom',
        title: { visible: true, text: '步骤间间隔' }
      }
    ]
  };
  
  try {
    const vchart = new VChart(spec, { dom: 'box-chart' });
  vchart.renderSync();
  } catch (error) {
    console.error('柱状图渲染失败:', error);
    container.innerHTML = '<div style="padding: 20px; text-align: center; color: #999;">图表渲染失败</div>';
  }
}

// 渲染桑基图
function renderSankeyChart() {
  const container = document.getElementById('sankey-chart');
  if (!container || !sankeyChartData.value) return;
  
  container.innerHTML = '';
  
  // 检查数据是否有效
  if (!sankeyChartData.value.nodes || sankeyChartData.value.nodes.length === 0) {
    container.innerHTML = '<div style="padding: 20px; text-align: center; color: #999;">暂无路径数据</div>';
    return;
  }
  
  // 如果节点太少，使用饼图
  if (sankeyChartData.value.nodes.length < 2) {
    renderSankeyAsPie();
    return;
  }
  
  const spec = {
    type: 'sankey',
    width: 1200,
    height: 800,
    padding: { top: 20, right: 20, bottom: 20, left: 20 },
    data: [
      {
        id: 'sankeyData',
        values: [
          {
            nodes: sankeyChartData.value.nodes,
            links: sankeyChartData.value.links
          }
        ]
      },
      {
        id: 'sankeyNodes',
        values: sankeyChartData.value.nodes
      }
    ],
    dataId: 'sankeyData',
    categoryField: 'name',
    valueField: 'value',
    sourceField: 'source',
    targetField: 'target',
    nodeKey: 'name',
    dropIsolatedNode: false,
    nodeGap: 2,
    nodeWidth: 120,
    nodeHeight: 80,
    nodeAlign: 'center',
    equalNodeHeight: true,
    // linkOverlap: 'center',
    title: {
      text: '用户路径流向分析',
      subtext: '显示用户在不同事件间的流转情况',
      subtextStyle: {
        fontSize: 12
      }
    },
    label: {
      visible: false,
    style: {
        fontSize: 10
      }
    },
    node: {
      state: {
        hover: {
          stroke: '#333333'
        },
        selected: {
          lineWidth: 1,
          brighter: 1,
          fillOpacity: 0.1
        }
      },
      style: {
        fill: '#1664FF',
        fillOpacity: 0,
        lineWidth: 1,
        stroke: '#1664FF'
      }
    },
    link: {
      style: {
        fill: '#1664FF'
      },
      state: {
        hover: {
          fillOpacity: 1
        },
        selected: {
          fill: '#1664FF',
          stroke: '',
          lineWidth: 1,
          brighter: 1,
          fillOpacity: 0.2
        }
      }
    },
    extensionMark: [
      {
        type: 'text',
        dataId: 'sankeyNodes',
        dataKey: 'name',
      visible: true,
        state: {
          hover: {
            fill: '#1664FF'
          }
        },
        style: {
          stroke: false,
          x: (datum, ctx, elements, dataView) => {
            return ctx.valueToX([datum.name]) + 10;
          },
          y: (datum, ctx, elements, dataView) => {
            return ctx.valueToY([datum.name]) + 10;
          },
          text: (datum, ctx, elements, dataView) => {
            const node = ctx.valueToNode([datum.name]);
            return {
              type: 'rich',
              text: [
                {
                  text: `${datum.name}\n`,
                  fill: '#646475',
                  lineHeight: 18,
                  fontWeight: 500,
                  fontSize: 12
                },
                {
                  text: node.value || 0,
                  fill: '#1d1d2e',
                  lineHeight: 28,
                  fontWeight: 700,
                  fontSize: 28
                }
              ]
            };
          },
          textAlign: 'left'
        }
      },
      {
        type: 'rect',
        dataId: 'sankeyNodes',
        dataKey: 'name',
        visible: true,
        style: {
          fill: '#1664FF',
          x: (datum, ctx, elements, dataView) => {
            return ctx.valueToNode([datum.name]).x1 - 10;
          },
          y: (datum, ctx, elements, dataView) => {
            return ctx.valueToNode([datum.name]).y0;
          },
          y1: (datum, ctx, elements, dataView) => {
            return ctx.valueToNode([datum.name]).y1;
          },
          width: 10
        }
      }
    ]
  };
  
  try {
    const vchart = new VChart(spec, { dom: 'sankey-chart' });
    vchart.renderSync();
  } catch (error) {
    console.error('桑基图渲染失败:', error);
    // 如果桑基图失败，回退到饼图
    renderSankeyAsPie();
  }
}

// 桑基图失败时的回退方案 - 饼图
function renderSankeyAsPie() {
  const container = document.getElementById('sankey-chart');
  if (!container || !sankeyChartData.value) return;
  
  container.innerHTML = '';
  
  // 转换为饼图数据格式
  const pieData = sankeyChartData.value.nodes.map(node => ({
    name: node.name,
    value: 1 // 简单示例，实际应该根据业务逻辑计算
  }));
  
  const spec = {
    type: 'pie',
    data: {
      values: pieData
    },
    categoryField: 'name',
    valueField: 'value',
    title: {
      visible: true,
      text: '事件分布'
    }
  };
  
  try {
    const vchart = new VChart(spec, { dom: 'sankey-chart' });
    vchart.renderSync();
  } catch (error) {
    console.error('饼图渲染失败:', error);
    container.innerHTML = '<div style="padding: 20px; text-align: center; color: #999;">图表渲染失败</div>';
  }
}

// 渲染表格
function renderTable(containerId, data) {
  const container = document.getElementById(containerId);
  if (!container || !data || !data.data) return;
  
  container.innerHTML = '';
  
  const columns = data.column_order.map(key => ({
    field: key,
    title: key,
    width: 100
  }));
  
  const option = {
    records: data.data,
    columns: columns,
    theme: {
      bodyStyle: {
        fontSize: 12
      },
      headerStyle: {
        fontSize: 14
      }
    }
  };
  
  const tableInstance = new VTable.ListTable(container, option);
}

// 监听数据变化，渲染表格和图表
watch(result1, (val) => {
  nextTick(() => {
    renderTable('table1', val);
  });
});

watch(result2, (val) => {
  nextTick(() => {
    renderTable('table2', val);
  });
});

// 统一查询
function handleQuery() {
  handleFunnelQuery();
  handleSessionQuery();
}

onMounted(() => {
  // 检查VChart版本和可用图表类型
  console.log('VChart版本:', VChart.version);
  console.log('VChart可用图表类型:', VChart.getChartTypes ? VChart.getChartTypes() : '无法获取图表类型');
  
  // 测试基本图表类型
  testChartTypes();
  
  handleQuery();
});

// 测试VChart支持的图表类型
function testChartTypes() {
  const testContainer = document.createElement('div');
  testContainer.id = 'test-chart';
  testContainer.style.width = '200px';
  testContainer.style.height = '200px';
  testContainer.style.position = 'absolute';
  testContainer.style.top = '-9999px';
  document.body.appendChild(testContainer);
  
  // 测试柱状图
  const barSpec = {
    type: 'bar',
    data: {
      values: [
        { x: 'A', y: 10 },
        { x: 'B', y: 20 },
        { x: 'C', y: 15 }
      ]
    },
    xField: 'x',
    yField: 'y'
  };
  
  try {
    const barChart = new VChart(barSpec, { dom: 'test-chart' });
    barChart.renderSync();
    console.log('✅ 柱状图支持正常');
  } catch (error) {
    console.error('❌ 柱状图不支持:', error);
  }
  
  // 测试饼图
  const pieSpec = {
    type: 'pie',
    data: {
      values: [
        { name: 'A', value: 10 },
        { name: 'B', value: 20 },
        { name: 'C', value: 15 }
      ]
    },
    categoryField: 'name',
    valueField: 'value'
  };
  
  try {
    const pieChart = new VChart(pieSpec, { dom: 'test-chart' });
    pieChart.renderSync();
    console.log('✅ 饼图支持正常');
  } catch (error) {
    console.error('❌ 饼图不支持:', error);
  }
  
  // 测试漏斗图
  const funnelSpec = {
    type: 'common',
    padding: { right: 80, left: 20 },
    data: [
      {
        id: 'funnel',
        values: [
          { step: '步骤1', value: 100 },
          { step: '步骤2', value: 80 },
          { step: '步骤3', value: 60 }
        ]
      }
    ],
    series: [
      {
        type: 'funnel',
        maxSize: '65%',
        minSize: '10%',
        isTransform: true,
        shape: 'rect',
        categoryField: 'step',
        valueField: 'value'
      }
    ]
  };
  
  try {
    const funnelChart = new VChart(funnelSpec, { dom: 'test-chart' });
    funnelChart.renderSync();
    console.log('✅ 漏斗图支持正常');
  } catch (error) {
    console.error('❌ 漏斗图不支持:', error);
  }
  
  // 测试箱型图
  const boxSpec = {
    type: 'boxPlot',
    data: {
      values: [
        {
          x: 'A',
          y1: 5,
          y2: 8,
          y3: 10,
          y4: 12,
          y5: 20
        },
        {
          x: 'B',
          y1: 3,
          y2: 6,
          y3: 9,
          y4: 15,
          y5: 18
        }
      ]
    },
    xField: 'x',
    minField: 'y1',
    q1Field: 'y2',
    medianField: 'y3',
    q3Field: 'y4',
    maxField: 'y5',
    direction: 'vertical'
  };
  
  try {
    const boxChart = new VChart(boxSpec, { dom: 'test-chart' });
    boxChart.renderSync();
    console.log('✅ 箱型图支持正常');
  } catch (error) {
    console.error('❌ 箱型图不支持:', error);
  }
  
  // 测试桑基图
  const sankeySpec = {
    type: 'sankey',
    data: [
      {
        id: 'sankeyData',
        values: [
          {
            nodes: [
              { name: 'A' },
              { name: 'B' },
              { name: 'C' }
            ],
            links: [
              { source: 'A', target: 'B', value: 10 },
              { source: 'B', target: 'C', value: 8 }
            ]
          }
        ]
      }
    ],
    dataId: 'sankeyData',
    categoryField: 'name',
    valueField: 'value',
    sourceField: 'source',
    targetField: 'target',
    nodeKey: 'name'
  };
  
  try {
    const sankeyChart = new VChart(sankeySpec, { dom: 'test-chart' });
    sankeyChart.renderSync();
    console.log('✅ 桑基图支持正常');
  } catch (error) {
    console.error('❌ 桑基图不支持:', error);
  }
  
  // 清理测试容器
  document.body.removeChild(testContainer);
}
</script>

<template>
  <div class="container">
    <div class="sidebar" v-show="!sidebarCollapsed">
      <div
        class="sidebar-toggle-btn"
        :class="{ collapsed: sidebarCollapsed }"
        @click="sidebarCollapsed = !sidebarCollapsed"
        :aria-label="sidebarCollapsed ? '显示参数' : '隐藏参数'"
      >
        <span class="arrow">◀</span>
      </div>
      <div class="param-row">
        <div class="param-block">
          <h2>漏斗分析参数</h2>
          <div class="param-form">
            <div v-for="(v, k) in funnelParams" :key="k" class="form-row">
              <label :for="'funnel-' + k">{{ FUNNEL_FIELD_LABELS[k] || k }}</label>
              <template v-if="k === 'mode'">
                <select v-model="funnelParams[k]" :id="'funnel-' + k">
                  <option value="default">default</option>
                  <option value="backtrack">backtrack</option>
                  <option value="increase">increase</option>
                </select>
              </template>
              <template v-else-if="k === 'keep_deepest'">
                <input v-model="funnelParams[k]" :id="'funnel-' + k" type="checkbox" />
              </template>
              <template v-else-if="k === 'path_filter_mode'">
                <select v-model="funnelParams[k]" :id="'funnel-' + k">
                  <option value="all_paths">保留所有路径</option>
                  <option value="first_path">保留第一条路径</option>
                  <option value="max_duration">保留耗时最长路径</option>
                  <option value="min_duration">保留耗时最短路径</option>
                </select>
              </template>
              <template v-else>
                <input v-model="funnelParams[k]" :id="'funnel-' + k" :type="typeof v === 'number' ? 'number' : 'text'" step="any" />
              </template>
            </div>
          </div>
        </div>
        <div class="param-block">
          <h2>会话聚合参数</h2>
          <div class="param-form">
            <div v-for="(v, k) in sessionParams" :key="k" class="form-row">
              <label :for="'session-' + k">{{ SESSION_FIELD_LABELS[k] || k }}</label>
              <template v-if="k === 'mark_type'">
                <select v-model="sessionParams[k]" :id="'session-' + k">
                  <option value="start">start</option>
                  <option value="end">end</option>
                </select>
              </template>
              <template v-else-if="k === 'session_mode'">
                <select v-model="sessionParams[k]" :id="'session-' + k">
                  <option value="default">default</option>
                  <option value="cons_uniq">cons_uniq</option>
                  <option value="session_uniq">session_uniq</option>
                  <option value="cons_uniq_with_group">cons_uniq_with_group</option>
                  <option value="session_uniq_with_group">session_uniq_with_group</option>
                </select>
              </template>
              <template v-else>
                <input v-model="sessionParams[k]" :id="'session-' + k" :type="typeof v === 'number' ? 'number' : 'text'" step="any" />
              </template>
            </div>
          </div>
        </div>
      </div>
      <button class="query-btn" @click="handleQuery" :disabled="loading1 || loading2">
        {{ loading1 || loading2 ? '查询中...' : '查询' }}
      </button>
      <div v-if="errorMsg1" style="color:red;margin-top:8px;">{{ errorMsg1 }}</div>
      <div v-if="errorMsg2" style="color:red;margin-top:8px;">{{ errorMsg2 }}</div>
    </div>
    
    <div
      v-if="sidebarCollapsed"
      class="sidebar-toggle-btn collapsed"
      @click="sidebarCollapsed = false"
      aria-label="显示参数"
      style="position: fixed; left: 0; top: 880px; height: 36px; width: 40px; z-index: 1000;"
    >
      <span class="arrow">▶</span>
    </div>
    
    <div class="result-area" :class="{ 'expand': sidebarCollapsed }">
      <h2>漏斗分析与会话聚合结果</h2>
      
      <!-- 漏斗分析结果 -->
      <div class="result-block">
        <h3>漏斗分析</h3>
        <div id="table1" class="table-container"></div>
        <div id="funnel-chart" class="chart-container"></div>
        <div id="box-chart" class="chart-container"></div>
          </div>
      
      <!-- 会话聚合结果 -->
      <div class="result-block">
        <h3>会话聚合</h3>
        <div id="table2" class="table-container"></div>
        <div id="sankey-chart" class="chart-container"></div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.container {
  display: flex;
  height: 100vh;
  width: 100vw;
  position: relative;
}

.sidebar {
  width: 400px;
  background: #f7f7f7;
  padding: 16px;
  box-sizing: border-box;
  border-right: 1px solid #eee;
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  position: relative;
  overflow-y: auto;
}

.sidebar-toggle-btn {
  position: fixed;
  top: 880px;
  height: 36px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: #409eff;
  color: #fff;
  border: none;
  border-radius: 0 4px 4px 0;
  font-size: 16px;
  cursor: pointer;
  box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  z-index: 10;
  transition: width 0.2s, left 0.2s;
  width: 40px;
  padding: 0 8px;
}

.sidebar-toggle-btn .arrow {
  font-size: 18px;
  margin: 0;
}

.param-row {
    display: flex;
  flex-direction: column;
  gap: 16px;
}

.param-block {
  flex: 1;
  min-width: 0;
}

.result-area {
  padding: 16px;
  position: relative;
  z-index: 1;
  flex: 1;
  transition: width 0.2s;
  overflow-y: auto;
}

.result-area.expand {
  width: 100vw;
}

.param-form {
  background: #fff;
  border: 1px solid #ddd;
  border-radius: 4px;
  padding: 12px;
  margin-bottom: 8px;
}

.query-btn {
  width: 100%;
  padding: 12px;
  background: #409eff;
  color: #fff;
  border: none;
  border-radius: 4px;
  font-size: 14px;
  cursor: pointer;
  margin-top: 16px;
}

.query-btn:hover {
  background: #337ecc;
}

.result-block {
  background: #fff;
  border: 1px solid #ddd;
  border-radius: 4px;
  padding: 16px;
  margin-bottom: 16px;
}

.form-row {
    display: flex;
  align-items: center;
  margin-bottom: 8px;
}

.form-row label {
  width: 120px;
  font-size: 12px;
  color: #333;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.form-row input,
.form-row select {
  flex: 1;
  min-width: 0;
  padding: 4px 8px;
  font-size: 12px;
  height: 28px;
  border: 1px solid #ccc;
  border-radius: 4px;
}

#table1, #table2 {
  width: 100%;
  min-height: 200px;
  margin-bottom: 16px;
}

.table-container {
  width: 100%;
  min-height: 200px;
  margin-bottom: 16px;
}

.chart-row {
  display: flex;
  gap: 16px;
  margin-bottom: 16px;
}

.chart-container {
  flex: 1;
  min-height: 400px;
  border: 1px solid #eee;
  border-radius: 4px;
  margin-bottom: 16px;
}

#funnel-chart,
#box-chart,
#sankey-chart {
  width: 100%;
  height: 400px;
}

h2 {
  margin: 0 0 16px 0;
  color: #333;
}

h3 {
  margin: 0 0 12px 0;
  color: #666;
}
</style>
