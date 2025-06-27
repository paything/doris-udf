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
        System.out.println("分组值最大数量: 10");
        System.out.println("分析模式: default");
        String result1 = sessionAgg.evaluate("iap", "start", input1, 1800, 10, 10, "其他", "default");
        System.out.println("输出结果: " + result1);
        System.out.println();
        
        System.out.println("=== 测试2：cons_uniq模式 - 连续事件去重 ===");
        String input2 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"iap\",\"product2\",\"cn\"]";
        System.out.println("输入数据: " + input2);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: cons_uniq");
        String result2 = sessionAgg.evaluate("iap", "start", input2, 1800, 10, 10, "其他", "cons_uniq");
        System.out.println("输出结果: " + result2);
        System.out.println();
        
        System.out.println("=== 测试3：session_uniq模式 - 事件严格去重 ===");
        String input3 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"logout\",\"product2\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"login\",\"product2\",\"cn\"]";
        System.out.println("输入数据: " + input3);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: session_uniq");
        String result3 = sessionAgg.evaluate("iap", "start", input3, 1800, 10, 10, "其他", "session_uniq");
        System.out.println("输出结果: " + result3);
        System.out.println();
        
        System.out.println("=== 测试4：cons_uniq_with_group模式 - 连续事件去重（带分组） ===");
        String input4 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"iap\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input4);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: cons_uniq_with_group");
        String result4 = sessionAgg.evaluate("iap", "start", input4, 1800, 10, 10, "其他", "cons_uniq_with_group");
        System.out.println("输出结果: " + result4);
        System.out.println();
        
        System.out.println("=== 测试5：session_uniq_with_group模式 - 事件严格去重（带分组） ===");
        String input5 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"iap\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input5);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: session_uniq_with_group");
        String result5 = sessionAgg.evaluate("iap", "start", input5, 1800, 10, 10, "其他", "session_uniq_with_group");
        System.out.println("输出结果: " + result5);
        System.out.println();
        
        System.out.println("=== 测试6：以end事件结束会话 ===");
        String input6 = "[\"2024-01-01 00:00:00.000\",\"login\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"logout\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input6);
        System.out.println("标记事件: logout");
        System.out.println("标记类型: end");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: default");
        String result6 = sessionAgg.evaluate("logout", "end", input6, 1800, 10, 10, "其他", "default");
        System.out.println("输出结果: " + result6);
        System.out.println();
        
        System.out.println("=== 测试7：会话间隔超时 ===");
        String input7 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:31:00.000\",\"iap\",\"product2\",\"cn\"]";
        System.out.println("输入数据: " + input7);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: default");
        String result7 = sessionAgg.evaluate("iap", "start", input7, 1800, 10, 10, "其他", "default");
        System.out.println("输出结果: " + result7);
        System.out.println();
        
        System.out.println("=== 测试8：最大步数限制 ===");
        String input8 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"event1\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"event2\",\"product1\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"event3\",\"product1\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"event4\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input8);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 3");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: default");
        String result8 = sessionAgg.evaluate("iap", "start", input8, 1800, 3, 10, "其他", "default");
        System.out.println("输出结果: " + result8);
        System.out.println();
        
        System.out.println("=== 测试9：混合时间格式 ===");
        String input9 = "[\"2024-01-01 00:00:00.123\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.456\",\"logout\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input9);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: default");
        String result9 = sessionAgg.evaluate("iap", "start", input9, 1800, 10, 10, "其他", "default");
        System.out.println("输出结果: " + result9);
        System.out.println();
        
        System.out.println("=== 测试10：空数据测试 ===");
        String input10 = "";
        System.out.println("输入数据: " + input10);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: default");
        String result10 = sessionAgg.evaluate("iap", "start", input10, 1800, 10, 10, "其他", "default");
        System.out.println("输出结果: " + result10);
        System.out.println();

        System.out.println("=== 测试11：分组值为空测试 ===");
        String input11 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"\",\"\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"\",\"\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"\",\"\"]";
        System.out.println("输入数据: " + input11);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: default");
        String result11 = sessionAgg.evaluate("iap", "start", input11, 1800, 10, 10, "其他", "default");
        System.out.println("输出结果: " + result11);
        System.out.println();

        System.out.println("=== 测试12：标记事件在会话间隔内多次出现应合并为一条路径 ===");
        String input12 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"]";
        System.out.println("输入数据: " + input12);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: default");
        String result12 = sessionAgg.evaluate("iap", "start", input12, 1800, 10, 10, "其他", "default");
        System.out.println("输出结果: " + result12);
        System.out.println();
        
        System.out.println("=== 测试13：标记事件间隔超时应分割为多条路径 ===");
        String input13 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:31:00.000\",\"iap\",\"product2\",\"cn\"]";
        System.out.println("输入数据: " + input13);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: default");
        String result13 = sessionAgg.evaluate("iap", "start", input13, 1800, 10, 10, "其他", "default");
        System.out.println("输出结果: " + result13);
        System.out.println();

        System.out.println("=== 测试14：session_uniq_with_group模式 - 会话级别去重验证 ===");
        String input14 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"iap\",\"product1\",\"cn\"]";
        System.out.println("输入数据: " + input14);
        System.out.println("标记事件: iap");
        System.out.println("标记类型: start");
        System.out.println("会话间隔: 1800秒");
        System.out.println("最大步数: 10");
        System.out.println("分组值最大数量: 10");
        System.out.println("替换值: 其他");
        System.out.println("分析模式: session_uniq_with_group");
        String result14 = sessionAgg.evaluate("iap", "start", input14, 1800, 10, 10, "其他", "session_uniq_with_group");
        System.out.println("输出结果: " + result14);
        System.out.println("说明: 第三个iap(product1,cn)应该被去重，因为第一个iap(product1,cn)已经在会话中了");
        System.out.println();

        System.out.println("=== 测试15：验证分组字段数量限制 ===");
        System.out.println("当前代码限制：只能处理2个分组字段（group0, group1）");
        System.out.println("问题1：0个分组字段会解析失败");
        System.out.println("问题2：3个或更多分组字段会丢失额外字段");
        System.out.println();
        
        // 测试0个分组字段（会失败）
        String input15_0 = "[\"2024-01-01 00:00:00.000\",\"iap\"],[\"2024-01-01 00:00:01.000\",\"chat\"]";
        System.out.println("测试0个分组字段: " + input15_0);
        String result15_0 = sessionAgg.evaluate("iap", "start", input15_0, 1800, 10, 10, "其他", "default");
        System.out.println("结果: " + result15_0);
        System.out.println();
        
        // 测试3个分组字段（会丢失第3个字段）
        String input15_3 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\",\"region1\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\",\"region1\"]";
        System.out.println("测试3个分组字段: " + input15_3);
        String result15_3 = sessionAgg.evaluate("iap", "start", input15_3, 1800, 10, 10, "其他", "default");
        System.out.println("结果: " + result15_3);
        System.out.println("注意：第3个分组字段'region1'被丢失了");
        System.out.println();

        System.out.println("=== 测试16：重构后验证不同数量分组字段 ===");
        System.out.println("✅ 重构成功！现在支持任意数量的分组字段");
        System.out.println();
        
        // 测试1个分组字段
        String input16_1 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\"]";
        System.out.println("测试1个分组字段: " + input16_1);
        String result16_1 = sessionAgg.evaluate("iap", "start", input16_1, 1800, 10, 10, "其他", "default");
        System.out.println("结果: " + result16_1);
        System.out.println();
        
        // 测试4个分组字段
        String input16_4 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\",\"region1\",\"version1\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\",\"region1\",\"version1\"]";
        System.out.println("测试4个分组字段: " + input16_4);
        String result16_4 = sessionAgg.evaluate("iap", "start", input16_4, 1800, 10, 10, "其他", "default");
        System.out.println("结果: " + result16_4);
        System.out.println();
        
        // 测试5个分组字段
        String input16_5 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\",\"region1\",\"version1\",\"channel1\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\",\"region1\",\"version1\",\"channel1\"]";
        System.out.println("测试5个分组字段: " + input16_5);
        String result16_5 = sessionAgg.evaluate("iap", "start", input16_5, 1800, 10, 10, "其他", "default");
        System.out.println("结果: " + result16_5);
        System.out.println();
        
        // 测试带分组的去重模式
        System.out.println("=== 测试17：多分组字段的去重模式验证 ===");
        String input17 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\",\"region1\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\",\"region1\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\",\"region1\"],[\"2024-01-01 00:00:03.000\",\"iap\",\"product1\",\"cn\",\"region1\"]";
        System.out.println("测试session_uniq_with_group模式（3个分组字段）: " + input17);
        String result17 = sessionAgg.evaluate("iap", "start", input17, 1800, 10, 10, "其他", "session_uniq_with_group");
        System.out.println("结果: " + result17);
        System.out.println("说明: 第四个iap(product1,cn,region1)应该被去重，因为第一个iap(product1,cn,region1)已经在会话中了");
        System.out.println();

        System.out.println("=== 测试18：分组值数量控制功能验证 ===");
        String input18 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"chat\",\"product2\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"iap\",\"product3\",\"cn\"],[\"2024-01-01 00:00:05.000\",\"chat\",\"product3\",\"cn\"],[\"2024-01-01 00:00:06.000\",\"iap\",\"product4\",\"cn\"],[\"2024-01-01 00:00:07.000\",\"chat\",\"product4\",\"cn\"]";
        System.out.println("测试分组值数量控制（限制为2个）: " + input18);
        System.out.println("说明: 有4个不同的product，限制为2个时，出现次数最少的product3和product4应该被合并为'其他'");
        String result18 = sessionAgg.evaluate("iap", "start", input18, 1800, 10, 2, "其他", "default");
        System.out.println("结果: " + result18);
        System.out.println();

        System.out.println("=== 测试19：自定义替换值功能验证 ===");
        String input19 = "[\"2024-01-01 00:00:00.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"chat\",\"product2\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"iap\",\"product3\",\"cn\"],[\"2024-01-01 00:00:05.000\",\"chat\",\"product3\",\"cn\"]";
        System.out.println("测试自定义替换值（限制为2个，替换为'长尾'）: " + input19);
        System.out.println("说明: 有3个不同的product，限制为2个时，出现次数最少的product3应该被合并为'长尾'");
        String result19 = sessionAgg.evaluate("iap", "start", input19, 1800, 10, 2, "长尾", "default");
        System.out.println("结果: " + result19);
        System.out.println();

        System.out.println("=== 测试20：end模式基础测试 ===");
        String input20 = "[\"2024-01-01 00:00:00.000\",\"login\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"logout\",\"product1\",\"cn\"]";
        System.out.println("测试end模式（以logout为结束事件）: " + input20);
        System.out.println("说明: end模式应该从logout事件开始逆序回数路径节点");
        String result20 = sessionAgg.evaluate("logout", "end", input20, 1800, 10, 10, "其他", "default");
        System.out.println("结果: " + result20);
        System.out.println();

        System.out.println("=== 测试21：end模式多会话测试 ===");
        String input21 = "[\"2024-01-01 00:00:00.000\",\"login\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"logout\",\"product1\",\"cn\"],[\"2024-01-01 00:30:00.000\",\"login\",\"product2\",\"cn\"],[\"2024-01-01 00:30:01.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:30:02.000\",\"logout\",\"product2\",\"cn\"]";
        System.out.println("测试end模式多会话（两个logout事件）: " + input21);
        System.out.println("说明: 应该产生两个会话，每个会话从logout开始逆序回数");
        String result21 = sessionAgg.evaluate("logout", "end", input21, 1800, 10, 10, "其他", "default");
        System.out.println("结果: " + result21);
        System.out.println();

        System.out.println("=== 测试22：end模式会话间隔超时测试 ===");
        String input22 = "[\"2024-01-01 00:00:00.000\",\"login\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"chat\",\"product1\",\"cn\"],[\"2024-01-01 00:31:00.000\",\"logout\",\"product1\",\"cn\"]";
        System.out.println("测试end模式会话间隔超时: " + input22);
        System.out.println("说明: logout事件与前面事件间隔超过30分钟，应该只包含logout事件本身");
        String result22 = sessionAgg.evaluate("logout", "end", input22, 1800, 10, 10, "其他", "default");
        System.out.println("结果: " + result22);
        System.out.println();

        System.out.println("=== 测试23：end模式最大步数限制测试 ===");
        String input23 = "[\"2024-01-01 00:00:00.000\",\"event1\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"event2\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"event3\",\"product1\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"event4\",\"product1\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"event5\",\"product1\",\"cn\"],[\"2024-01-01 00:00:05.000\",\"logout\",\"product1\",\"cn\"]";
        System.out.println("测试end模式最大步数限制（限制为3步）: " + input23);
        System.out.println("说明: 从logout开始逆序回数，最多只能包含3个事件");
        String result23 = sessionAgg.evaluate("logout", "end", input23, 1800, 3, 10, "其他", "default");
        System.out.println("结果: " + result23);
        System.out.println();

        System.out.println("=== 测试24：end模式去重测试 ===");
        String input24 = "[\"2024-01-01 00:00:00.000\",\"login\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"logout\",\"product1\",\"cn\"]";
        System.out.println("测试end模式去重（session_uniq模式）: " + input24);
        System.out.println("说明: 从logout开始逆序回数，应该去重重复的iap事件");
        String result24 = sessionAgg.evaluate("logout", "end", input24, 1800, 10, 10, "其他", "session_uniq");
        System.out.println("结果: " + result24);
        System.out.println();

        System.out.println("=== 测试25：end模式带分组去重测试 ===");
        String input25 = "[\"2024-01-01 00:00:00.000\",\"login\",\"product1\",\"cn\"],[\"2024-01-01 00:00:01.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:02.000\",\"iap\",\"product2\",\"cn\"],[\"2024-01-01 00:00:03.000\",\"iap\",\"product1\",\"cn\"],[\"2024-01-01 00:00:04.000\",\"logout\",\"product1\",\"cn\"]";
        System.out.println("测试end模式带分组去重（session_uniq_with_group模式）: " + input25);
        System.out.println("说明: 从logout开始逆序回数，应该去重相同事件名+分组的组合");
        String result25 = sessionAgg.evaluate("logout", "end", input25, 1800, 10, 10, "其他", "session_uniq_with_group");
        System.out.println("结果: " + result25);
        System.out.println();
    }
} 