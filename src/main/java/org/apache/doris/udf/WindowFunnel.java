package org.apache.doris.udf;

import org.apache.doris.udf.UDF;
import java.util.*;

public class WindowFunnel extends UDF {
    /**
     * 通用漏斗分析UDF，按步骤顺序查找每步的第一个1，返回时间差和标签
     * @param windowSeconds 分析窗口时长（秒）
     * @param stepIntervalSeconds 步骤间隔时长（秒）
     * @param mode 分析模式（可忽略）
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
                FunnelPath path = buildPath(events, startIndex, windowSeconds, stepIntervalSeconds, stepCount);
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
    private FunnelPath buildPath(List<EventRow> events, int startIndex, int windowSeconds, int stepIntervalSeconds, int stepCount) {
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
                        // 检查步骤间隔
                        if (event.timestamp - stepTimestamps[stepIndex - 1] <= stepIntervalSeconds * 1000L) {
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
                if (i > 0 && stepTimestamps[i-1] != null) {
                    path.timeDiffs.add(stepTimestamps[i] - stepTimestamps[i-1]);
                } else if (i > 0) {
                    path.timeDiffs.add(0L);
                }
                path.tags.add(stepTags[i]);
            } else if (i > 0) {
                path.timeDiffs.add(0L);
            }
        }
        
        return path.stepCount > 0 ? path : null;
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

    // 解析时间戳
    private long parseTimestamp(String timestamp) {
        try {
            String[] parts = timestamp.split(" ");
            String datePart = parts[0];
            String timePart = parts[1];
            String[] dateFields = datePart.split("-");
            String[] timeFields = timePart.split(":");
            String[] secondParts = timeFields[2].split("\\.");
            int year = Integer.parseInt(dateFields[0]);
            int month = Integer.parseInt(dateFields[1]);
            int day = Integer.parseInt(dateFields[2]);
            int hour = Integer.parseInt(timeFields[0]);
            int minute = Integer.parseInt(timeFields[1]);
            int second = Integer.parseInt(secondParts[0]);
            int millisecond = secondParts.length > 1 ? Integer.parseInt(secondParts[1]) : 0;
            Calendar cal = Calendar.getInstance();
            cal.set(year, month - 1, day, hour, minute, second);
            cal.set(Calendar.MILLISECOND, millisecond);
            return cal.getTimeInMillis();
        } catch (Exception e) {
            return 0;
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