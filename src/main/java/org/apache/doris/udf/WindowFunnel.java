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
     * @param tagCol 标签列（可选）
     * @return 分析结果
     */
    public String evaluate(Integer windowSeconds, Integer stepIntervalSeconds, String mode, String eventString, String tagCol) {
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
            // 查找每个步骤的第一个1
            Long[] stepTimestamps = new Long[stepCount];
            int foundStep = 0;
            for (EventRow event : events) {
                for (int i = 0; i < stepCount; i++) {
                    if (stepTimestamps[i] == null && event.steps[i] == 1) {
                        // 步骤必须按顺序推进
                        if (i == 0 || stepTimestamps[i-1] != null) {
                            // 检查窗口和间隔
                            if (i == 0 || (event.timestamp - stepTimestamps[i-1] <= stepIntervalSeconds * 1000L)) {
                                if (i == 0 || (event.timestamp - stepTimestamps[0] <= windowSeconds * 1000L)) {
                                    stepTimestamps[i] = event.timestamp;
                                }
                            }
                        }
                    }
                }
            }
            // 只要第一个步骤发生就输出
            if (stepTimestamps[0] == null) return "[]";
            StringBuilder sb = new StringBuilder("[");
            sb.append(stepTimestamps[0]);
            for (int i = 1; i < stepCount; i++) {
                sb.append(",");
                if (stepTimestamps[i] != null && stepTimestamps[i-1] != null) {
                    sb.append(stepTimestamps[i] - stepTimestamps[i-1]);
                } else {
                    sb.append(0);
                }
            }
            // 标签
            sb.append(",[");
            if (tagCol != null && !tagCol.isEmpty()) {
                String[] tags = tagCol.split(",");
                for (int i = 0; i < tags.length; i++) {
                    sb.append("\"").append(tags[i]).append("\"");
                    if (i < tags.length - 1) sb.append(",");
                }
            }
            sb.append("]]");
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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
                list.add(new EventRow(ts, steps));
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
        EventRow(long ts, int[] steps) {
            this.timestamp = ts;
            this.steps = steps;
        }
    }
} 