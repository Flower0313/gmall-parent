package com.atguigu.gmall.dw.publisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.dw.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName gmall-parent-Controller
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月01日10:29 - 周三
 * @Describe 常见注解
 * @Controller 定义在类上面，作用是将这个类标识为controller层
 * @ResponsBody 定义在方法上，作用是返回普通对象而不是页面
 * @RestController 定义在类上，作用=Controller+ResponsBody
 * @RequestMapping("xxx") 定义在方法上，作用标识请求所调用的方法，根据括号中映射名来确定调取哪个方法
 * @RequestParam("xxx") 定义在每个形参前，作用当发送过来的请求携带参数时可以标识参数所对应的方法中的参数
 * @Autowired 定义在类中，自动找到接口的实现
 * @Service 定义为Service层
 */

@RestController
public class Controller {

    @Autowired
    private PublisherService publisherService;

    /**
     * 访问方式:
     *
     * @param date api的参数，要对应
     * @return
     */
    @RequestMapping("realtime-total")
    public String realtimeToal(@RequestParam("date") String date) {
        //从service层获取数据
        Integer dauTotal = publisherService.getDauTotal(date);

        //创建list集合放最终数据
        ArrayList<JSONObject> result = new ArrayList<>();

        //创建存放新增日活的map集合
        JSONObject dauMap = new JSONObject();
        //创建存放新增设备的map集合
        JSONObject devMap = new JSONObject();

        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);//写死

        result.add(dauMap);
        result.add(devMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String DauTotalHours(@RequestParam("id") String id, @RequestParam("date") String date) {

        //获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //获取今天日活数据
        Map todayHourMap = publisherService.getDauTotalHours(date);

        //获取昨天数据
        Map yesterdayHourMap = publisherService.getDauTotalHours(yesterday);

        //创建map集合用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();

        result.put("yesterday", yesterdayHourMap);
        result.put("today", todayHourMap);

        return JSONObject.toJSONString(result);

    }

    public String realtimeTotal(@RequestParam("date") String date) {
        return "";
    }
}
