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
        
        // 对于backtrack模式，需要特殊处理
        if (MODE_BACKTRACK.equals(mode) || MODE_BACKTRACK_LONG.equals(mode)) {
            return buildBacktrackPath(events, startIndex, windowSeconds, stepIntervalSeconds, stepCount, mode, stepTimestamps, stepTags);
        }
        
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
     * 构建回溯模式的路径
     */
    private FunnelPath buildBacktrackPath(List<EventRow> events, int startIndex, int windowSeconds, int stepIntervalSeconds, int stepCount, String mode, Long[] stepTimestamps, String[] stepTags) {
        // 在时间窗口内找到所有事件
        List<EventRow> windowEvents = new ArrayList<>();
        long startTime = stepTimestamps[0];
        
        for (EventRow event : events) {
            if (Math.abs(event.timestamp - startTime) <= windowSeconds * 1000L) {
                windowEvents.add(event);
            }
        }
        
        // 为每个步骤找到最佳匹配的事件
        for (int stepIndex = 1; stepIndex < stepCount; stepIndex++) {
            Long bestTimestamp = null;
            String bestTag = null;
            long minInterval = Long.MAX_VALUE;
            
            for (EventRow event : windowEvents) {
                if (event.steps[stepIndex] == 1) {
                    long interval = Math.abs(event.timestamp - stepTimestamps[stepIndex - 1]);
                    if (interval <= stepIntervalSeconds * 1000L && interval < minInterval) {
                        minInterval = interval;
                        bestTimestamp = event.timestamp;
                        bestTag = event.group;
                    }
                }
            }
            
            if (bestTimestamp != null) {
                stepTimestamps[stepIndex] = bestTimestamp;
                stepTags[stepIndex] = bestTag;
            }
        }
        
        // 构建路径结果
        FunnelPath path = new FunnelPath();
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
        
        // 使用更智能的分割方法，处理标签中包含逗号的情况
        int startIndex = 0;
        while (startIndex < eventString.length()) {
            // 找到第一个#的位置（时间戳结束）
            int firstHashIndex = eventString.indexOf('#', startIndex);
            if (firstHashIndex == -1) break;
            
            // 找到第二个#的位置（步骤标志结束）
            int secondHashIndex = eventString.indexOf('#', firstHashIndex + 1);
            if (secondHashIndex == -1) break;
            
            // 找到下一个事件的开始位置（下一个时间戳的开始）
            int nextEventStart = findNextEventStart(eventString, secondHashIndex + 1);
            
            // 提取当前事件（不包含下一个事件的时间戳）
            String part;
            if (nextEventStart < eventString.length()) {
                // 如果有下一个事件，找到当前事件的结束位置（逗号位置）
                int commaIndex = eventString.lastIndexOf(',', nextEventStart - 1);
                if (commaIndex > startIndex) {
                    part = eventString.substring(startIndex, commaIndex);
                } else {
                    part = eventString.substring(startIndex, nextEventStart);
                }
            } else {
                // 如果是最后一个事件，直接取到字符串末尾
                part = eventString.substring(startIndex);
            }
            
            // 解析事件
            EventRow event = parseSingleEvent(part);
            if (event != null) {
                list.add(event);
            }
            
            // 移动到下一个事件
            startIndex = nextEventStart;
        }
        
        return list;
    }
    
    // 找到下一个事件的开始位置
    private int findNextEventStart(String eventString, int startIndex) {
        // 查找下一个时间戳模式：YYYY-MM-DD HH:mm:ss
        for (int i = startIndex; i < eventString.length() - 18; i++) {
            if (eventString.charAt(i) == '2' && 
                eventString.charAt(i + 4) == '-' && 
                eventString.charAt(i + 7) == '-' && 
                eventString.charAt(i + 10) == ' ' && 
                eventString.charAt(i + 13) == ':' && 
                eventString.charAt(i + 16) == ':') {
                return i;
            }
        }
        return eventString.length();
    }
    
    // 解析单个事件
    private EventRow parseSingleEvent(String part) {
        // 找到第一个#的位置（时间戳结束）
        int firstHashIndex = part.indexOf('#');
        if (firstHashIndex == -1) return null;
        
        // 找到第二个#的位置（步骤标志结束）
        int secondHashIndex = part.indexOf('#', firstHashIndex + 1);
        if (secondHashIndex == -1) return null;
        
        // 提取各个部分
        String timestamp = part.substring(0, firstHashIndex);
        String stepStr = part.substring(firstHashIndex + 1, secondHashIndex);
        String group = part.substring(secondHashIndex + 1); // 标签是剩余的所有内容
        
        // 解析时间戳
        long ts = parseTimestamp(timestamp);
        
        // 解析步骤标志
        String[] stepStrs = stepStr.split("@");
        int[] steps = new int[stepStrs.length];
        for (int i = 0; i < stepStrs.length; i++) {
            steps[i] = "1".equals(stepStrs[i]) ? 1 : 0;
        }
        
        return new EventRow(ts, steps, group);
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