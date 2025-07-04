package org.apache.doris.udf;

public class WindowFunnelTest {
    public static void main(String[] args) {
        WindowFunnel funnel = new WindowFunnel();
        
        System.out.println("=== 测试1：3步骤多路径 ===");
        String input1 = "2024-01-01 00:00:00.123#1@0@0#tiktok,2024-01-01 00:00:01.345#0@1@0#fb,2024-01-01 00:00:03.111#0@0@1#wechat,2024-02-01 00:00:00.123#1@0@0#tiktok,2024-02-01 00:00:01.345#0@1@0#fb,2024-02-01 00:00:03.111#0@0@1#wechat";
        System.out.println("输入数据: " + input1);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: strict");
        String result1 = funnel.evaluate(10, 5, "strict", input1);
        System.out.println("输出结果: " + result1);
        System.out.println();
        
        System.out.println("=== 测试2：20步骤单路径 ===");
        String input2 = "2024-01-01 00:00:00.000#1@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0#step1,2024-01-01 00:00:00.100#0@1@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0#step2,2024-01-01 00:00:00.200#0@0@1@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0#step3,2024-01-01 00:00:00.300#0@0@0@1@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0#step4,2024-01-01 00:00:00.400#0@0@0@0@1@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0#step5,2024-01-01 00:00:00.500#0@0@0@0@0@1@0@0@0@0@0@0@0@0@0@0@0@0@0@0#step6,2024-01-01 00:00:00.600#0@0@0@0@0@0@1@0@0@0@0@0@0@0@0@0@0@0@0@0#step7,2024-01-01 00:00:00.700#0@0@0@0@0@0@0@1@0@0@0@0@0@0@0@0@0@0@0@0#step8,2024-01-01 00:00:00.800#0@0@0@0@0@0@0@0@1@0@0@0@0@0@0@0@0@0@0@0#step9,2024-01-01 00:00:00.900#0@0@0@0@0@0@0@0@0@1@0@0@0@0@0@0@0@0@0@0#step10,2024-01-01 00:00:01.000#0@0@0@0@0@0@0@0@0@0@1@0@0@0@0@0@0@0@0@0#step11,2024-01-01 00:00:01.100#0@0@0@0@0@0@0@0@0@0@0@1@0@0@0@0@0@0@0@0#step12,2024-01-01 00:00:01.200#0@0@0@0@0@0@0@0@0@0@0@0@1@0@0@0@0@0@0@0#step13,2024-01-01 00:00:01.300#0@0@0@0@0@0@0@0@0@0@0@0@0@1@0@0@0@0@0@0#step14,2024-01-01 00:00:01.400#0@0@0@0@0@0@0@0@0@0@0@0@0@0@1@0@0@0@0@0#step15,2024-01-01 00:00:01.500#0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@1@0@0@0@0#step16,2024-01-01 00:00:01.600#0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@1@0@0@0#step17,2024-01-01 00:00:01.700#0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@1@0@0#step18,2024-01-01 00:00:01.800#0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@1@0#step19,2024-01-01 00:00:01.900#0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@0@1#step20";
        System.out.println("输入数据: " + input2);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        String result2 = funnel.evaluate(10, 5, "strict", input2);
        System.out.println("输出结果: " + result2);
        System.out.println();
        
        System.out.println("=== 测试3：混合时间格式（毫秒级和秒级） ===");
        String input3 = "2024-01-01 00:00:00.123#1@0@0#step1,2024-01-01 00:00:01#0@1@0#step2,2024-01-01 00:00:02.456#0@0@1#step3";
        System.out.println("输入数据: " + input3);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        String result3 = funnel.evaluate(10, 5, "strict", input3);
        System.out.println("输出结果: " + result3);
        System.out.println();
        
        System.out.println("=== 测试4：更多混合格式 ===");
        String input4 = "2024-01-01 12:00:00#1@0@0@0#start,2024-01-01 12:00:01.500#0@1@0@0#middle1,2024-01-01 12:00:02#0@0@1@0#middle2,2024-01-01 12:00:03.750#0@0@0@1#end";
        System.out.println("输入数据: " + input4);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        String result4 = funnel.evaluate(10, 5, "strict", input4);
        System.out.println("输出结果: " + result4);
        System.out.println();
        
        System.out.println("=== 测试5：路径长度为1（只有第一步） ===");
        String input5 = "2024-01-01 00:00:00.123#1@0@0#step1,2024-01-01 00:00:01.345#0@0@0#step2,2024-01-01 00:00:02.111#0@0@0#step3";
        System.out.println("输入数据: " + input5);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        String result5 = funnel.evaluate(10, 5, "strict", input5);
        System.out.println("输出结果: " + result5);
        System.out.println();
        
        System.out.println("=== 测试6：路径长度为2（只有前两步） ===");
        String input6 = "2024-01-01 00:00:00.123#1@0@0#step1,2024-01-01 00:00:01.345#0@1@0#step2,2024-01-01 00:00:02.111#0@0@0#step3";
        System.out.println("输入数据: " + input6);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        String result6 = funnel.evaluate(10, 5, "strict", input6);
        System.out.println("输出结果: " + result6);
        System.out.println();
        
        System.out.println("=== 测试7：default模式（允许时间相等） ===");
        String input7 = "2024-01-01 00:00:00.000#1@0@0#step1,2024-01-01 00:00:00.000#0@1@0#step2,2024-01-01 00:00:00.000#0@0@1#step3";
        System.out.println("输入数据: " + input7);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: default");
        String result7 = funnel.evaluate(10, 5, "default", input7);
        System.out.println("输出结果: " + result7);
        System.out.println();

        System.out.println("=== 测试7.1：default模式，事件名相同（允许时间相等） ===");
        String input7_1 = "2024-01-01 00:00:00.000#1@1@1#step1,2024-01-01 00:00:00.000#1@1@1#step2,2024-01-01 00:00:00.000#1@1@1#step3";
        System.out.println("输入数据: " + input7_1);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: default");
        String result7_1 = funnel.evaluate(10, 5, "default", input7_1);
        System.out.println("输出结果: " + result7_1);
        System.out.println();        
        
        System.out.println("=== 测试8：backtrack模式（允许倒序） ===");
        String input8 = "2024-01-01 00:00:00.000#1@0@0#step1,2024-01-01 00:00:00.100#0@0@1#step3,2024-01-01 00:00:00.200#0@1@0#step2";
        System.out.println("输入数据: " + input8);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: backtrack");
        String result8 = funnel.evaluate(10, 5, "backtrack", input8);
        System.out.println("输出结果: " + result8);
        System.out.println();
        
        System.out.println("=== 测试9：increase模式（严格递增） ===");
        String input9 = "2024-01-01 00:00:00.000#1@0@0#step1,2024-01-01 00:00:01.000#0@1@0#step2,2024-01-01 00:00:01.100#0@0@1#step3";
        System.out.println("输入数据: " + input9);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: increase");
        String result9 = funnel.evaluate(10, 5, "increase", input9);
        System.out.println("输出结果: " + result9);
        System.out.println();
        
        System.out.println("=== 测试10：backtrack_long模式（30分钟回溯） ===");
        String input10 = "2024-01-01 00:31:00.000#1@0@0#step1,2024-01-01 00:31:15.000#0@1@0#step2,2024-01-01 00:31:00.000#0@0@1#step3";
        System.out.println("输入数据: " + input10);
        System.out.println("窗口时长: 60秒");
        System.out.println("步骤间隔: 20秒");
        System.out.println("分析模式: backtrack_long");
        String result10 = funnel.evaluate(60, 20, "backtrack_long", input10);
        System.out.println("输出结果: " + result10);
        System.out.println();

        System.out.println("=== 测试11：标签包含特殊字符（#@,） ===");
        String input11 = "2024-01-01 00:00:00.000#1@0@0#tag#with#hash,2024-01-01 00:00:01.000#0@1@0#tag@with@at,2024-01-01 00:00:02.000#0@0@1#tag,with,comma";
        System.out.println("输入数据: " + input11);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: default");
        String result11 = funnel.evaluate(10, 5, "default", input11);
        System.out.println("输出结果: " + result11);
        System.out.println();
        
        System.out.println("=== 测试12：相同事件名称的漏斗链路判定 ===");
        System.out.println("--- 子测试12.1：只触发一次iap事件的用户（所有步骤都是1@1@1） ---");
        String input12_1 = "2024-01-01 00:00:00.000#1@1@1#iap,2024-01-01 00:00:01.000#1@1@1#iap,2024-01-01 00:00:02.000#1@1@1#iap";
        System.out.println("输入数据: " + input12_1);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: default");
        String result12_1 = funnel.evaluate(10, 5, "default", input12_1);
        System.out.println("输出结果: " + result12_1);
        System.out.println();
        
        System.out.println("--- 子测试12.2：触发3次iap事件的用户（完整漏斗，每次都是1@1@1） ---");
        String input12_2 = "2024-01-01 00:00:00.000#1@1@1#iap,2024-01-01 00:00:01.000#1@1@1#iap,2024-01-01 00:00:02.000#1@1@1#iap";
        System.out.println("输入数据: " + input12_2);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: default");
        String result12_2 = funnel.evaluate(10, 5, "default", input12_2);
        System.out.println("输出结果: " + result12_2);
        System.out.println();
        
        System.out.println("--- 子测试12.3：触发2次iap事件的用户（部分漏斗，前两次1@1@1，第三次0@0@0） ---");
        String input12_3 = "2024-01-01 00:00:00.000#1@1@1#iap,2024-01-01 00:00:01.000#1@1@1#iap,2024-01-01 00:00:03.000#1@1@1#iap";
        System.out.println("输入数据: " + input12_3);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: default");
        String result12_3 = funnel.evaluate(10, 5, "default", input12_3);
        System.out.println("输出结果: " + result12_3);
        System.out.println();
        
        System.out.println("--- 子测试12.4：相同事件名称但不同标签（每次都是1@1@1） ---");
        String input12_4 = "2024-01-01 00:00:00.000#1@1@1#iap#product1,2024-01-01 00:00:01.000#1@1@1#iap#product2,2024-01-01 00:00:02.000#1@1@1#iap#product3";
        System.out.println("输入数据: " + input12_4);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: default");
        String result12_4 = funnel.evaluate(10, 5, "default", input12_4);
        System.out.println("输出结果: " + result12_4);
        System.out.println();
        
        System.out.println("--- 子测试12.5：相同事件名称，时间间隔超时（每次都是1@1@1） ---");
        String input12_5 = "2024-01-01 00:00:00.000#1@1@1#iap,2024-01-01 00:00:11.000#1@1@1#iap,2024-01-01 00:00:22.000#1@1@1#iap";
        System.out.println("输入数据: " + input12_5);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: default");
        String result12_5 = funnel.evaluate(10, 5, "default", input12_5);
        System.out.println("输出结果: " + result12_5);
        System.out.println();
        
        System.out.println("--- 子测试12.6：混合事件名称（部分相同，iap事件标记为1@1@1） ---");
        String input12_6 = "2024-01-01 00:00:00.000#1@0@0#reg,2024-01-01 00:00:01.000#0@1@1#iap,2024-01-01 00:00:02.000#0@0@1#chat";
        System.out.println("输入数据: " + input12_6);
        System.out.println("窗口时长: 10秒");
        System.out.println("步骤间隔: 5秒");
        System.out.println("分析模式: default");
        String result12_6 = funnel.evaluate(10, 5, "default", input12_6);
        System.out.println("输出结果: " + result12_6);
        System.out.println();
    }
}