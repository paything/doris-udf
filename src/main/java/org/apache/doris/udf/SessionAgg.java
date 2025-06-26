package org.apache.doris.udf;

import org.apache.doris.udf.UDF;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.regex.*;

public class SessionAgg extends UDF {
    private static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    // 分析模式常量
    private static final String MODE_DEFAULT = "default";
    private static final String MODE_CONS_UNIQ = "cons_uniq";
    private static final String MODE_SESSION_UNIQ = "session_uniq";
    private static final String MODE_CONS_UNIQ_WITH_GROUP = "cons_uniq_with_group";
    private static final String MODE_SESSION_UNIQ_WITH_GROUP = "session_uniq_with_group";
    
    /**
     * 用户会话聚合UDF
     * @param markEvent 标记事件的名称
     * @param markType 标记事件是start还是end
     * @param eventString 事件字符串，格式：["时间戳","事件名","group0","group1"],...
     * @param sessionIntervalSeconds 会话间隔时长（秒）
     * @param maxSteps 会话最大步数
     * @param mode 分析模式
     * @return 会话聚合结果
     */
    public String evaluate(String markEvent, String markType, String eventString, 
                          Integer sessionIntervalSeconds, Integer maxSteps, String mode) {
        if (eventString == null || eventString.isEmpty()) {
            return null;
        }
        
        try {
            List<EventData> events = parseEventString(eventString);
            if (events.isEmpty()) return null;
            
            // 按时间排序
            events.sort(Comparator.comparingLong(e -> e.timestamp));
            
            // 根据标记类型处理事件
            List<Session> sessions = buildSessions(events, markEvent, markType, 
                                                 sessionIntervalSeconds, maxSteps, mode);
            
            // 构建返回结果
            return buildResult(sessions);
            
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * 构建会话列表
     */
    private List<Session> buildSessions(List<EventData> events, String markEvent, String markType,
                                      int sessionIntervalSeconds, int maxSteps, String mode) {
        List<Session> sessions = new ArrayList<>();
        Session currentSession = null;
        
        for (EventData event : events) {
            // 检查是否需要开始新会话
            boolean shouldStartNewSession = false;
            
            if ("start".equals(markType)) {
                // 以标记事件开始新会话
                shouldStartNewSession = markEvent.equals(event.eventName);
            }
            
            // 开始新会话
            if (shouldStartNewSession) {
                if (currentSession != null) {
                    sessions.add(currentSession);
                }
                currentSession = new Session();
                currentSession.events.add(event);
                continue;
            }
            
            // 如果没有当前会话，跳过事件
            if (currentSession == null) {
                continue;
            }
            
            // 检查会话间隔
            if (!currentSession.events.isEmpty()) {
                EventData lastEvent = currentSession.events.get(currentSession.events.size() - 1);
                long timeDiff = event.timestamp - lastEvent.timestamp;
                
                if (timeDiff > sessionIntervalSeconds * 1000L) {
                    // 时间间隔超过限制，结束当前会话
                    sessions.add(currentSession);
                    currentSession = null;
                    continue;
                }
            }
            
            // 添加事件到当前会话
            if (currentSession != null) {
                // 根据模式处理事件去重
                if (shouldAddEvent(currentSession, event, mode)) {
                    currentSession.events.add(event);
                    
                    // 检查最大步数限制
                    if (currentSession.events.size() >= maxSteps) {
                        sessions.add(currentSession);
                        currentSession = null;
                    }
                }
            }
            
            // 处理end模式
            if ("end".equals(markType) && markEvent.equals(event.eventName)) {
                if (currentSession != null) {
                    sessions.add(currentSession);
                    currentSession = null;
                }
            }
        }
        
        // 添加最后一个会话
        if (currentSession != null && !currentSession.events.isEmpty()) {
            sessions.add(currentSession);
        }
        
        return sessions;
    }
    
    /**
     * 根据模式判断是否应该添加事件
     */
    private boolean shouldAddEvent(Session session, EventData event, String mode) {
        if (session.events.isEmpty()) {
            return true;
        }
        
        EventData lastEvent = session.events.get(session.events.size() - 1);
        
        switch (mode) {
            case MODE_DEFAULT:
                return true;
                
            case MODE_CONS_UNIQ:
                // 连续事件去重
                return !event.eventName.equals(lastEvent.eventName);
                
            case MODE_SESSION_UNIQ:
                // 事件严格去重
                for (EventData existingEvent : session.events) {
                    if (event.eventName.equals(existingEvent.eventName)) {
                        return false;
                    }
                }
                return true;
                
            case MODE_CONS_UNIQ_WITH_GROUP:
                // 连续事件去重（带分组）
                return !(event.eventName.equals(lastEvent.eventName) && 
                        event.group0.equals(lastEvent.group0) && 
                        event.group1.equals(lastEvent.group1));
                
            case MODE_SESSION_UNIQ_WITH_GROUP:
                // 事件严格去重（带分组）
                for (EventData existingEvent : session.events) {
                    if (event.eventName.equals(existingEvent.eventName) && 
                        event.group0.equals(existingEvent.group0) && 
                        event.group1.equals(existingEvent.group1)) {
                        return false;
                    }
                }
                return true;
                
            default:
                return true;
        }
    }
    
    /**
     * 构建返回结果
     */
    private String buildResult(List<Session> sessions) {
        if (sessions.isEmpty()) {
            return "[]";
        }
        
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < sessions.size(); i++) {
            Session session = sessions.get(i);
            
            sb.append("[");
            for (int j = 0; j < session.events.size(); j++) {
                EventData event = session.events.get(j);
                
                sb.append("[");
                sb.append(j + 1).append(","); // 步骤序号
                sb.append("\"").append(event.eventName).append("\",");
                sb.append("\"").append(event.group0).append("\",");
                sb.append("\"").append(event.group1).append("\"");
                sb.append("]");
                
                if (j < session.events.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
            
            if (i < sessions.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        
        return sb.toString();
    }
    
    /**
     * 解析事件字符串
     */
    private List<EventData> parseEventString(String eventString) {
        List<EventData> events = new ArrayList<>();
        
        // 匹配每个事件数组，允许空字符串
        Pattern pattern = Pattern.compile("\\[\"([^\"]*)\",\"([^\"]*)\",\"([^\"]*)\",\"([^\"]*)\"\\]");
        Matcher matcher = pattern.matcher(eventString);
        
        while (matcher.find()) {
            String timestamp = matcher.group(1);
            String eventName = matcher.group(2);
            String group0 = matcher.group(3);
            String group1 = matcher.group(4);
            
            long ts = parseTimestamp(timestamp);
            if (ts > 0) {
                events.add(new EventData(ts, eventName, group0, group1));
            }
        }
        
        return events;
    }
    
    /**
     * 解析时间戳 - 支持多种格式
     */
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
    
    /**
     * 事件数据类
     */
    private static class EventData {
        long timestamp;
        String eventName;
        String group0;
        String group1;
        
        EventData(long timestamp, String eventName, String group0, String group1) {
            this.timestamp = timestamp;
            this.eventName = eventName;
            this.group0 = group0;
            this.group1 = group1;
        }
    }
    
    /**
     * 会话类
     */
    private static class Session {
        List<EventData> events = new ArrayList<>();
    }
} 