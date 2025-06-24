package org.apache.doris.udf;

public class WindowFunnelTest {
    public static void main(String[] args) {
        WindowFunnel windowFunnel = new WindowFunnel();
        
        // 测试数据：2024-01-01 00:00:00.123#1@0@0#tiktok,2024-01-01 00:00:01.345#0@1@0#tiktok,2024-01-01 00:00:03.111#0@0@1#tiktok
        String testEventString = "2024-01-01 00:00:00.123#1@0@0#tiktok,2024-01-01 00:00:01.345#0@1@0#tiktok,2024-01-01 00:00:03.111#0@0@1#tiktok";
        
        // 测试参数
        Integer windowSeconds = 10; // 10秒窗口
        Integer stepIntervalSeconds = 1; // 1秒间隔
        String mode = "strict"; // 严格模式
        String tagCol = "hk,cn,de"; // 标签列
        
        // 执行测试
        String result = windowFunnel.evaluate(windowSeconds, stepIntervalSeconds, mode, testEventString, tagCol);
        
        System.out.println("测试结果:");
        System.out.println("输入数据: " + testEventString);
        System.out.println("窗口时长: " + windowSeconds + "秒");
        System.out.println("步骤间隔: " + stepIntervalSeconds + "秒");
        System.out.println("分析模式: " + mode);
        System.out.println("标签列: " + tagCol);
        System.out.println("输出结果: " + result);
    }
} 