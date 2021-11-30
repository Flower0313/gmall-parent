package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constans.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;


/**
 * @ClassName gmall-parent-CanalClient
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日17:41 - 周二
 * @Describe 将数据库表中变动的数据加入到对应的kafka主题
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", "canal", "canal");

        while (true) {
            //2.获取连接
            canalConnector.connect();

            //3.监控所有数据库
            canalConnector.subscribe("gmall.order_info_test");
            //回滚到未进行ack的地方，下次fetch的时候，可以从最后一个没有ack的地方开始拿
            canalConnector.rollback();
            //4.获取Message
            Message message = canalConnector.get(100);

            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() <= 0) {
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    //TODO 获取表名
                    String tableName = entry.getHeader().getTableName();
                    //Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //判断entryType是否为ROWDATA
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        //数据反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //TODO 获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //TODO 根据条件获取数据
                        handler(tableName, eventType, rowDatasList);
                    }
                }
            }

        }
    }


    /**
     * @param tableName 表名
     * @param eventType DDL类型(Insert、Update、Delete)
     * @param rowDatasList 改变的值，以集合形式输出[{..}{..}{..}]，有几个字段就有几个{}
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //System.out.println(tableName + "|" + eventType + "|" + rowDatasList);
        //若表示student，并且操作是INSERT
        if ("order_info_test".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                //rowData是关于列的json数据
                //获取存放列的集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                //获取每个列
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    //column.getName获得列名,column.getValue获得列值
                    jsonObject.put(column.getName(), column.getValue());
                }
                System.out.println(jsonObject.toString());
                //发送过去的是json，类似{"name":"xxl","id":"12","age":"11"}
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER, jsonObject.toString());
            }
        }
    }


}
