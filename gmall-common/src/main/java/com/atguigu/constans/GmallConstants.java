package com.atguigu.constans;

/**
 * @ClassName gmall-parent-GmallConstants
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月29日15:40 - 周一
 * @Describe
 */
public class GmallConstants {
    //启动数据主题
    public static final String KAFKA_TOPIC_STARTUP = "GMALL_STARTUP"; //脚本生成

    //订单主题
    public static final String KAFKA_TOPIC_ORDER = "GMALL_ORDER";//canal传输

    //事件数据主题
    public static final String KAFKA_TOPIC_EVENT = "GMALL_EVENT";//脚本生成

    //预警需求索引名
    public static final String ES_ALERT_INDEX = "gmall_coupon_alert"; //脚本生成

    //订单详情主题
    public static final String KAFKA_TOPIC_ORDER_DETAIL = "TOPIC_ORDER_DETAIL";//canal传输

    //用户主题
    public static final String KAFKA_TOPIC_USER = "TOPIC_USER_INFO";//canal传输

    //灵活分析索引名
    public static final String ES_DETAIL_INDEXNAME = "gmall2021_sale_detail";

    //灵活分析索引别名
    public static final String ES_QUERY_INDEXNAME = "gmall2021_sale_detail-query";

    //连接Redis的密码
    public static final String PASSWORD = "w654646";

}
