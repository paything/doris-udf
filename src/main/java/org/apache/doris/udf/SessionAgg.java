package org.apache.doris.udf;

import org.apache.doris.udf.UDF;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.regex.*;

public class SessionAgg extends UDF {
    private static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    // 分析模式常量
    //  - `default`: 默认模式，不做特殊处理，正常对事件排序后按条件返回结果
    //  - `cons_uniq`: 连续事件去重，例如以iap事件做起始，后续连续发生的iap都不计入会话链条中，如果中间有插入其他事件，后续的iap又会被计入链条中
    //  - `session_uniq`: 事件严格去重，对比cons_uniq的差异是，任何事件都只会计入一次会话链条中，无论中间是否有插入其他事件
    //  - `cons_uniq_with_group`: 连续事件去重，但是加入了分组值的判断，例如group为商品id，如果连续购买同一个商品id，则只计1次，如果购买不同的商品id，则会被计入会话链条中
    //  - `session_uniq_with_group`: 事件严格去重，但是加入了分组值的判断，例如group为商品id，不管是否连续，只要在同一个会话中出现过相同的事件名+分组组合，就不会再计入会话链条中
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
     * @param maxGroupCount 分组值最大数量，超过此数量的分组值会被合并为replaceValue
     * @param replaceValue 分组值超过限制时的替换值
     * @param mode 分析模式
     * @return 会话聚合结果
     */
    public String evaluate(String markEvent, String markType, String eventString, 
                          Integer sessionIntervalSeconds, Integer maxSteps, Integer maxGroupCount, 
                          String replaceValue, String mode) {
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
                                                 sessionIntervalSeconds, maxSteps, maxGroupCount, replaceValue, mode);
            
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
                                      int sessionIntervalSeconds, int maxSteps, int maxGroupCount, String replaceValue, String mode) {
        List<Session> sessions = new ArrayList<>();
        
        // 处理分组值数量控制
        if (maxGroupCount > 0) {
            events = processGroupCountLimit(events, maxGroupCount, replaceValue);
        }
        
        if ("start".equals(markType)) {
            // start模式：从标记事件开始正向构建会话
            return buildStartSessions(events, markEvent, sessionIntervalSeconds, maxSteps, mode);
        } else if ("end".equals(markType)) {
            // end模式：从标记事件开始逆序回数路径节点
            return buildEndSessions(events, markEvent, sessionIntervalSeconds, maxSteps, mode);
        }
        
        return sessions;
    }
    
    /**
     * 构建start模式的会话列表
     */
    private List<Session> buildStartSessions(List<EventData> events, String markEvent,
                                           int sessionIntervalSeconds, int maxSteps, String mode) {
        List<Session> sessions = new ArrayList<>();
        Session currentSession = null;
        
        for (EventData event : events) {
            // 检查是否需要开始新会话
            boolean shouldStartNewSession = false;
            
            if (currentSession == null) {
                shouldStartNewSession = markEvent.equals(event.eventName);
            } else {
                // 检查时间间隔
                EventData lastEvent = currentSession.events.get(currentSession.events.size() - 1);
                long timeDiff = event.timestamp - lastEvent.timestamp;
                
                if (timeDiff > sessionIntervalSeconds * 1000L) {
                    // 时间间隔超过限制，结束当前会话，开始新会话
                    sessions.add(currentSession);
                    currentSession = null;
                    shouldStartNewSession = markEvent.equals(event.eventName);
                }
            }
            
            // 开始新会话
            if (shouldStartNewSession) {
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
        }
        
        // 添加最后一个会话
        if (currentSession != null && !currentSession.events.isEmpty()) {
            sessions.add(currentSession);
        }
        
        return sessions;
    }
    
    /**
     * 构建end模式的会话列表
     */
    private List<Session> buildEndSessions(List<EventData> events, String markEvent,
                                         int sessionIntervalSeconds, int maxSteps, String mode) {
        List<Session> sessions = new ArrayList<>();
        
        // 找到所有标记事件的位置
        List<Integer> markEventIndices = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            if (markEvent.equals(events.get(i).eventName)) {
                markEventIndices.add(i);
            }
        }
        
        // 为每个标记事件构建会话
        for (int markIndex : markEventIndices) {
            Session session = new Session();
            EventData markEventData = events.get(markIndex);
            
            // 从标记事件开始，逆序向前查找符合条件的事件
            List<EventData> sessionEvents = new ArrayList<>();
            sessionEvents.add(markEventData); // 先添加标记事件
            
            // 逆序向前查找
            for (int i = markIndex - 1; i >= 0; i--) {
                EventData currentEvent = events.get(i);
                
                // 检查时间间隔（与标记事件的时间差）
                long timeDiff = markEventData.timestamp - currentEvent.timestamp;
                if (timeDiff > sessionIntervalSeconds * 1000L) {
                    break; // 时间间隔超过限制，停止查找
                }
                
                // 检查最大步数限制
                if (sessionEvents.size() >= maxSteps) {
                    break;
                }
                
                // 根据模式判断是否应该添加事件
                if (shouldAddEventForEndMode(sessionEvents, currentEvent, mode)) {
                    sessionEvents.add(0, currentEvent); // 在开头插入，保持时间顺序
                }
            }
            
            session.events = sessionEvents;
            sessions.add(session);
        }
        
        return sessions;
    }
    
    /**
     * 为end模式判断是否应该添加事件
     */
    private boolean shouldAddEventForEndMode(List<EventData> sessionEvents, EventData event, String mode) {
        if (sessionEvents.isEmpty()) {
            return true;
        }
        
        switch (mode) {
            case MODE_DEFAULT:
                return true;
                
            case MODE_CONS_UNIQ:
                // 连续事件去重（在end模式中，检查与已添加的最后一个事件是否相同）
                EventData lastAddedEvent = sessionEvents.get(sessionEvents.size() - 1);
                return !event.eventName.equals(lastAddedEvent.eventName);
                
            case MODE_SESSION_UNIQ:
                // 事件严格去重
                for (EventData existingEvent : sessionEvents) {
                    if (event.eventName.equals(existingEvent.eventName)) {
                        return false;
                    }
                }
                return true;
                
            case MODE_CONS_UNIQ_WITH_GROUP:
                // 连续事件去重（带分组）
                EventData lastAddedEventWithGroup = sessionEvents.get(sessionEvents.size() - 1);
                return !(event.eventName.equals(lastAddedEventWithGroup.eventName) && 
                        event.groups.equals(lastAddedEventWithGroup.groups));
                
            case MODE_SESSION_UNIQ_WITH_GROUP:
                // 事件严格去重（带分组）
                for (EventData existingEvent : sessionEvents) {
                    if (event.eventName.equals(existingEvent.eventName) && 
                        event.groups.equals(existingEvent.groups)) {
                        return false;
                    }
                }
                return true;
                
            default:
                return true;
        }
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
                        event.groups.equals(lastEvent.groups));
                
            case MODE_SESSION_UNIQ_WITH_GROUP:
                // 事件严格去重（带分组）
                for (EventData existingEvent : session.events) {
                    if (event.eventName.equals(existingEvent.eventName) && 
                        event.groups.equals(existingEvent.groups)) {
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
                sb.append("\"").append(event.eventName).append("\"");
                
                // 动态输出所有分组字段
                for (String group : event.groups) {
                    sb.append(",\"").append(group).append("\"");
                }
                
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
        
        // 匹配每个事件数组，支持任意数量的分组字段
        // 格式：["时间戳","事件名","分组1","分组2",...]
        Pattern pattern = Pattern.compile("\\[\"([^\"]*)\",\"([^\"]*)\"(,\"[^\"]*\")*\\]");
        Matcher matcher = pattern.matcher(eventString);
        
        while (matcher.find()) {
            String fullMatch = matcher.group(0);
            // 提取时间戳和事件名
            String timestamp = matcher.group(1);
            String eventName = matcher.group(2);
            
            // 提取所有分组字段
            List<String> groups = new ArrayList<>();
            Pattern groupPattern = Pattern.compile("\"([^\"]*)\"");
            Matcher groupMatcher = groupPattern.matcher(fullMatch);
            
            // 跳过前两个字段（时间戳和事件名）
            groupMatcher.find(); // 时间戳
            groupMatcher.find(); // 事件名
            
            // 提取剩余的分组字段
            while (groupMatcher.find()) {
                groups.add(groupMatcher.group(1));
            }
            
            long ts = parseTimestamp(timestamp);
            if (ts > 0) {
                events.add(new EventData(ts, eventName, groups));
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
        List<String> groups; // 动态分组字段列表
        
        EventData(long timestamp, String eventName, List<String> groups) {
            this.timestamp = timestamp;
            this.eventName = eventName;
            this.groups = groups;
        }
    }
    
    /**
     * 会话类
     */
    private static class Session {
        List<EventData> events = new ArrayList<>();
    }
    
    /**
     * 处理分组值数量限制，超过maxGroupCount的分组值会被合并为replaceValue
     */
    private List<EventData> processGroupCountLimit(List<EventData> events, int maxGroupCount, String replaceValue) {
        // 统计每个分组值的出现次数
        Map<String, Integer> groupCountMap = new HashMap<>();
        for (EventData event : events) {
            String groupKey = String.join("|", event.groups);
            groupCountMap.put(groupKey, groupCountMap.getOrDefault(groupKey, 0) + 1);
        }
        
        // 按出现次数排序，取前maxGroupCount个
        List<Map.Entry<String, Integer>> sortedGroups = new ArrayList<>(groupCountMap.entrySet());
        sortedGroups.sort((a, b) -> b.getValue().compareTo(a.getValue()));
        
        // 获取保留的分组值
        Set<String> keepGroups = new HashSet<>();
        for (int i = 0; i < Math.min(maxGroupCount, sortedGroups.size()); i++) {
            keepGroups.add(sortedGroups.get(i).getKey());
        }
        
        // 处理事件，将不在保留列表中的分组值替换为replaceValue
        List<EventData> processedEvents = new ArrayList<>();
        for (EventData event : events) {
            String groupKey = String.join("|", event.groups);
            if (keepGroups.contains(groupKey)) {
                processedEvents.add(event);
            } else {
                // 创建新的事件，将分组值替换为replaceValue
                List<String> otherGroups = new ArrayList<>();
                for (int i = 0; i < event.groups.size(); i++) {
                    otherGroups.add(replaceValue);
                }
                processedEvents.add(new EventData(event.timestamp, event.eventName, otherGroups));
            }
        }
        
        return processedEvents;
    }
} 