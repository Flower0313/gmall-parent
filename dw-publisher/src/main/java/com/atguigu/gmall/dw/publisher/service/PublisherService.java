package com.atguigu.gmall.dw.publisher.service;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName gmall-parent-PublisherService
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月01日10:33 - 周三
 * @Describe
 */
public interface PublisherService {
    //获取日活总数数据
    public int getDauTotal(String date);

    //获取日活分时数据
    public Map getDauTotalHours(String date);

    //获取全部订单的总数
    public Double getGmvTotal(String date);

    //交易额分时数据
    public Map<String, Double> getOrderAmountHourMap(String date);

    //性别年龄比例
    public Map getSaleDetail(String date, int startPage, int size, String keyword) throws IOException;
}
