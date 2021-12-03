package com.atguigu.gmall.dw.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-parent-OrderMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月03日8:47 - 周五
 * @Describe
 */
public interface OrderMapper {
    public Double selectOrderAmountTotal(String date);

    public List<Map> selectOrderAmountHourMap(String date);
}
