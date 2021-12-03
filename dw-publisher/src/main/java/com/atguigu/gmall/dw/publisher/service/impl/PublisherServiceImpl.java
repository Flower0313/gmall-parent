package com.atguigu.gmall.dw.publisher.service.impl;

import com.atguigu.gmall.dw.publisher.mapper.DauMapper;
import com.atguigu.gmall.dw.publisher.mapper.OrderMapper;
import com.atguigu.gmall.dw.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-parent-PublisherServiceImpl
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月01日11:00 - 周三
 * @Describe
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper duaMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public int getDauTotal(String date) {
        return duaMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHours(String date) {
        /*
         *这是获取到的数据格式
         * | LH  | CT  |
         * +-----+-----+
         * | 15  | 8   |
         * */

        //原始map集合
        List<Map> list = duaMapper.selectDauTotalHourMap(date);
        //新的map集合
        HashMap<String, Long> result = new HashMap<>();

        for (Map map : list) {
            result.put(map.get("LH").toString(), (Long) map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHourMap(String date) {
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        Map<String, Double> result = new HashMap<>();
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

}
