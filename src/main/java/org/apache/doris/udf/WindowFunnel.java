package org.apache.doris.udf;

import org.apache.doris.udf.UDF;
import java.util.*;
import java.text.SimpleDateFormat;

public class WindowFunnel extends UDF {
    private static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    // 分析模式常量
    private static final String MODE_DEFAULT = "default";
    private static final String MODE_BACKTRACK = "backtrack";
    private static final String MODE_BACKTRACK_LONG = "backtrack_long";
    private static final String MODE_INCREASE = "increase";
    
    /**
     * 通用漏斗分析UDF，按步骤顺序查找每步的第一个1，返回时间差和标签
     * @param windowSeconds 分析窗口时长（秒）
     * @param stepIntervalSeconds 步骤间隔时长（秒）
     * @param mode 分析模式：default/backtrack/backtrack_long/increase
     * @param eventString 事件字符串，格式：时间戳#0@1@0#group,...
     * @return 分析结果
     */
    public String evaluate(Integer windowSeconds, Integer stepIntervalSeconds, String mode, String eventString) {
        if (eventString == null || eventString.isEmpty()) {
            return null;
        }
        try {
            List<EventRow> events = parseEventString(eventString);
            if (events.isEmpty()) return null;
            
            // 按时间排序
            events.sort(Comparator.comparingLong(e -> e.timestamp));
            // 步骤数由第一个事件的steps长度决定
            int stepCount = events.get(0).steps.length;
            
            // 查找所有符合条件的路径
            List<FunnelPath> allPaths = new ArrayList<>();
            
            // 遍历每个可能的起始点
            for (int startIndex = 0; startIndex < events.size(); startIndex++) {
                EventRow startEvent = events.get(startIndex);
                
                // 检查是否可以作为第一个步骤
                if (startEvent.steps[0] != 1) continue;
                
                // 尝试从这个起始点构建路径
                FunnelPath path = buildPath(events, startIndex, windowSeconds, stepIntervalSeconds, stepCount, mode);
                if (path != null && path.stepCount > 0) {
                    allPaths.add(path);
                }
            }
            
            // 如果没有找到任何路径
            if (allPaths.isEmpty()) return "[]";
            
            // 构建结果数组
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < allPaths.size(); i++) {
                FunnelPath path = allPaths.get(i);
                
                sb.append("[");
                sb.append(path.firstTimestamp);
                
                // 添加时间差值
                for (int j = 0; j < path.timeDiffs.size(); j++) {
                    sb.append(",").append(path.timeDiffs.get(j));
                }
                
                // 添加标签数组
                sb.append(",[");
                for (int k = 0; k < path.tags.size(); k++) {
                    sb.append("\"").append(path.tags.get(k)).append("\"");
                    if (k < path.tags.size() - 1) sb.append(",");
                }
                sb.append("]");
                
                sb.append("]");
                if (i < allPaths.size() - 1) sb.append(",");
            }
            sb.append("]");
            return sb.toString();
            
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * 从指定起始点构建漏斗路径
     */
    private FunnelPath buildPath(List<EventRow> events, int startIndex, int windowSeconds, int stepIntervalSeconds, int stepCount, String mode) {
        FunnelPath path = new FunnelPath();
        Long[] stepTimestamps = new Long[stepCount];
        String[] stepTags = new String[stepCount];
        
        // 设置第一个步骤
        EventRow startEvent = events.get(startIndex);
        stepTimestamps[0] = startEvent.timestamp;
        stepTags[0] = startEvent.group;
        
        // 查找后续步骤
        for (int eventIndex = startIndex + 1; eventIndex < events.size(); eventIndex++) {
            EventRow event = events.get(eventIndex);
            
            // 检查是否在时间窗口内
            if (event.timestamp - stepTimestamps[0] > windowSeconds * 1000L) {
                break;
            }
            
            // 检查每个步骤
            for (int stepIndex = 1; stepIndex < stepCount; stepIndex++) {
                if (stepTimestamps[stepIndex] == null && event.steps[stepIndex] == 1) {
                    // 检查前一个步骤是否已找到
                    if (stepTimestamps[stepIndex - 1] != null) {
                        // 根据模式检查步骤间隔
                        if (isValidStepInterval(stepTimestamps[stepIndex - 1], event.timestamp, stepIntervalSeconds, mode)) {
                            stepTimestamps[stepIndex] = event.timestamp;
                            stepTags[stepIndex] = event.group;
                        }
                    }
                }
            }
        }
        
        // 构建路径结果
        path.firstTimestamp = stepTimestamps[0];
        path.stepCount = 0;
        
        // 计算时间差和收集标签
        for (int i = 0; i < stepCount; i++) {
            if (stepTimestamps[i] != null) {
                path.stepCount++;
                // 只有当存在前一个步骤时，才计算时间差
                if (i > 0 && stepTimestamps[i-1] != null) {
                    path.timeDiffs.add(stepTimestamps[i] - stepTimestamps[i-1]);
                }
                path.tags.add(stepTags[i]);
            }
        }
        
        return path.stepCount > 0 ? path : null;
    }
    
    /**
     * 根据分析模式验证步骤间隔是否有效
     */
    private boolean isValidStepInterval(long prevTimestamp, long currentTimestamp, int stepIntervalSeconds, String mode) {
        long timeDiff = currentTimestamp - prevTimestamp;
        long intervalMs = stepIntervalSeconds * 1000L;
        
        switch (mode) {
            case MODE_DEFAULT:
                // 默认模式：允许时间相等，间隔必须小于等于设定值
                return timeDiff <= intervalMs;
                
            case MODE_BACKTRACK:
                // 回溯模式：允许倒序，时间间隔取绝对值，允许5分钟回溯
                long absTimeDiff = Math.abs(timeDiff);
                return absTimeDiff <= intervalMs && timeDiff >= -300000L; // 5分钟 = 300000ms
                
            case MODE_BACKTRACK_LONG:
                // 长回溯模式：允许倒序，时间间隔取绝对值，允许30分钟回溯
                long absTimeDiffLong = Math.abs(timeDiff);
                return absTimeDiffLong <= intervalMs && timeDiff >= -1800000L; // 30分钟 = 1800000ms
                
            case MODE_INCREASE:
                // 递增模式：时间间隔必须严格递增，不允许相等
                return timeDiff > 0 && timeDiff <= intervalMs;
                
            default:
                // 默认使用default模式
                return timeDiff <= intervalMs;
        }
    }

    // 解析事件字符串
    private List<EventRow> parseEventString(String eventString) {
        List<EventRow> list = new ArrayList<>();
        String[] parts = eventString.split(",");
        for (String part : parts) {
            String[] fields = part.split("#");
            if (fields.length >= 3) {
                long ts = parseTimestamp(fields[0]);
                String[] stepStrs = fields[1].split("@");
                int[] steps = new int[stepStrs.length];
                for (int i = 0; i < stepStrs.length; i++) {
                    steps[i] = "1".equals(stepStrs[i]) ? 1 : 0;
                }
                String group = fields[2]; // 提取标签
                list.add(new EventRow(ts, steps, group));
            }
        }
        return list;
    }

    // 解析时间戳 - 支持多种格式
    private long parseTimestamp(String timestamp) {
        try {
            // 先尝试毫秒格式
            return sdf1.parse(timestamp).getTime();
        } catch (Exception e1) {
            try {
                // 再尝试秒格式
                return sdf2.parse(timestamp).getTime();
            } catch (Exception e2) {
                return 0;
            }
        }
    }

    // 事件行
    private static class EventRow {
        long timestamp;
        int[] steps;
        String group;
        EventRow(long ts, int[] steps, String group) {
            this.timestamp = ts;
            this.steps = steps;
            this.group = group;
        }
    }
    
    // 漏斗路径
    private static class FunnelPath {
        long firstTimestamp;
        List<Long> timeDiffs = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        int stepCount;
    }
} 