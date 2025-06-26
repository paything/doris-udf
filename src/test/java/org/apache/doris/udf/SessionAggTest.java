package org.apache.doris.udf;

public class SessionAggTest {
    public static void main(String[] args) {
        SessionAgg sessionAgg = new SessionAgg();
        
        System.out.println("=== 测试1：default模式 - 以iap为起始事件 ===");
        String input1 = "[\"2024-01-01 00:00:00.123\",\"reg\",\"tiktok#1\",\"cn\"],[\"2024-01-01 00:00:01.345\",\"iap\",\"tiktok#1\",\"cn\"],[\"2024-01-01 00:00:03.111\",\"chat\",\"tiktok#1\",\"cn\"],[\"2024-02-01 00:00:00.012\",\"reg\",\"fb@,#2\",\"cn\"],[\"2024-02-01 00:00:01.001\",\"iap\",\"fb@,#2\",\"cn\"],[\"2024-02-01 00:00:02.434\",\"chat\",\"fb\",\"cn\"]";
        System.out.println("输入数据: " + input1);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分析模式: default");
        String result1 = sessionAgg.evaluate("iap", "start", input1, 1800, 10, "default");
        System.out.println("输出结果: " + result1);
        System.out.println();
        
        System.out.println("=== 测试2：cons_uniq模式 - 连续事件去重 ===");
        String input2 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"iap\",\"product2\",\"cn\"]";
        System.out.println("输入数据: " + input2);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分析模式: cons_uniq");
        String result2 = sessionAgg.evaluate("iap", "start", input2, 1800, 10, "cons_uniq");
        System.out.println("输出结果: " + result2);
        System.out.println();
        
        System.out.println("=== 测试3：session_uniq模式 - 事件严格去重 ===");
        String input3 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"logout\",\"product2\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"login\",\"product2\",\"cn\"]";
        System.out.println("输入数据: " + input3);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分析模式: session_uniq");
        String result3 = sessionAgg.evaluate("iap", "start", input3, 1800, 10, "session_uniq");
        System.out.println("输出结果: " + result3);
        System.out.println();
        
        System.out.println("=== 测试4：cons_uniq_with_group模式 - 连续事件去重（带分组） ===");
        String input4 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"iap\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input4);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分析模式: cons_uniq_with_group");
        String result4 = sessionAgg.evaluate("iap", "start", input4, 1800, 10, "cons_uniq_with_group");
        System.out.println("输出结果: " + result4);
        System.out.println();
        
        System.out.println("=== 测试5：session_uniq_with_group模式 - 事件严格去重（带分组） ===");
        String input5 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"iap\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input5);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分析模式: session_uniq_with_group");
        String result5 = sessionAgg.evaluate("iap", "start", input5, 1800, 10, "session_uniq_with_group");
        System.out.println("输出结果: " + result5);
        System.out.println();
        
        System.out.println("=== 测试6：以end事件结束会话 ===");
        String input6 = "[\"2024-01-01 00:00:00.000\",\"login\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"logout\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input6);
        System.out.println("标记事件: logout");
        System.out.println("标记类型: end");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分析模式: default");
        String result6 = sessionAgg.evaluate("logout", "end", input6, 1800, 10, "default");
        System.out.println("输出结果: " + result6);
        System.out.println();
        
        System.out.println("=== 测试7：会话间隔超时 ===");
        String input7 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:31:00.000\",\"iap\",\"product2\",\"cn\"]";
        System.out.println("输入数据: " + input7);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分析模式: default");
        String result7 = sessionAgg.evaluate("iap", "start", input7, 1800, 10, "default");
        System.out.println("输出结果: " + result7);
        System.out.println();
        
        System.out.println("=== 测试8：最大步数限制 ===");
        String input8 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"event1\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"event2\",\"product1\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"event3\",\"product1\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"event4\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input8);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 3");
        System.out.println("分析模式: default");
        String result8 = sessionAgg.evaluate("iap", "start", input8, 1800, 3, "default");
        System.out.println("输出结果: " + result8);
        System.out.println();
        
        System.out.println("=== 测试9：混合时间格式 ===");
        String input9 = "[\"2024-01-01 00:00:00.123\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.456\",\"logout\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input9);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分析模式: default");
        String result9 = sessionAgg.evaluate("iap", "start", input9, 1800, 10, "default");
        System.out.println("输出结果: " + result9);
        System.out.println();
        
        System.out.println("=== 测试10：空数据测试 ===");
        String input10 = "";
        System.out.println("输入数据: " + input10);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分析模式: default");
        String result10 = sessionAgg.evaluate("iap", "start", input10, 1800, 10, "default");
        System.out.println("输出结果: " + result10);
        System.out.println();
    }
} 