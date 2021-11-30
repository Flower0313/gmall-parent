package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName gmall-parent-test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日19:39 - 周二
 * @Describe
 */
public class test {
    public static void main(String[] args) {
        JSONObject zhangsan = new JSONObject();
        //添加
        zhangsan.put("name", "张三");
        zhangsan.put("age", 18.4);
        zhangsan.put("birthday", "1900-20-03");
        zhangsan.put("majar", new String[]{"哈哈", "嘿嘿"});
        zhangsan.put("null", null);
        zhangsan.put("house", false);
        System.out.println(zhangsan.toString());

    }
}
