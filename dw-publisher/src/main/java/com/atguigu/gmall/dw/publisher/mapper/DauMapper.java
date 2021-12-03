package com.atguigu.gmall.dw.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-parent-DauMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月01日11:01 - 周三
 * @Describe
 */
public interface DauMapper {
    //实现类写在xml文件中了
    public Integer selectDauTotal(String date);

    public List<Map> selectDauTotalHourMap(String date);



}
