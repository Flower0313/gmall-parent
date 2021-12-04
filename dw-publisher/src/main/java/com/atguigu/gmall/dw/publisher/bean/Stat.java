package com.atguigu.gmall.dw.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @ClassName gmall-parent-Stat
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月04日10:22 - 周六
 * @Describe
 */
@Data
@AllArgsConstructor
public class Stat {
    List<Option> options;
    String title;
}
