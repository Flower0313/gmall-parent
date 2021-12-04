package com.atguigu.gmall.dw.publisher.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.constans.GmallConstants;
import com.atguigu.gmall.dw.publisher.bean.Option;
import com.atguigu.gmall.dw.publisher.bean.Stat;
import com.atguigu.gmall.dw.publisher.mapper.DauMapper;
import com.atguigu.gmall.dw.publisher.mapper.OrderMapper;
import com.atguigu.gmall.dw.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.io.IOException;
import java.util.ArrayList;
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
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHourMap(String date) {
        //使用List里面装Map,因为可能有多行数据,每行都是Map格式
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        Map<String, Double> result = new HashMap<>();
        for (Map map : list) {
            //字段别名要用大写去接
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }

    @Override
    public Map getSaleDetail(String date, int startPage, int size, String keyword) throws IOException {
        JestClientFactory clientFactory = new JestClientFactory();

        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        clientFactory.setHttpClientConfig(clientConfig);

        JestClient jestClient = clientFactory.getObject();


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //----query
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        //----must
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword)
                .operator(MatchQueryBuilder.Operator.AND));

        searchSourceBuilder.query(boolQueryBuilder);

        //----aggs
        TermsBuilder genderAggs = AggregationBuilders.terms("groupBy_user_gender")
                .field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        TermsBuilder ageAggs = AggregationBuilders.terms("groupBy_user_age")
                .field("user_age").size(size);
        searchSourceBuilder.aggregation(ageAggs);

        searchSourceBuilder.from(startPage);
        searchSourceBuilder.size(size);

        //TODO 执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_QUERY_INDEXNAME)
                .addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        //获得查询总数
        Long total = searchResult.getTotal();

        ArrayList<Map> detail = new ArrayList<>();


        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);

        //取出的是一条条json
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add(hit.source);
        }

        //存放年龄的Option对象
        ArrayList<Option> ageOptions = new ArrayList<>();
        //存放性别的Option对象
        ArrayList<Option> genderOptions = new ArrayList<>();
        //获取年龄占比数据
        MetricAggregation aggregations = searchResult.getAggregations();//获得查询结果中所有聚合组

        //TODO 年龄比例聚合组
        TermsAggregation groupByAge = aggregations.getTermsAggregation("groupBy_user_age");//获取聚合组中指定聚合

        List<TermsAggregation.Entry> buckets = groupByAge.getBuckets();
        Long low20Count = 0L;
        Long up30Count = 0L;
        //TODO 循环groupBy_user_age组的分组数据,getKey是获取buckets中key,getCount则是获取key的分组统计数
        for (TermsAggregation.Entry bucket : buckets) {
            if (Integer.parseInt(bucket.getKey()) < 20) {
                low20Count += bucket.getCount();
            } else if (Integer.parseInt(bucket.getKey()) > 30) {
                up30Count += bucket.getCount();
            }
        }

        //计算三者的比例
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;
        double up20low30Ratio = Math.round((100D - low20Ratio - up30Ratio));

        Option low20Opt = new Option("20岁以下", low20Ratio);
        Option up30Opt = new Option("20岁到30岁", up20low30Ratio);
        Option up20AndLow30Opt = new Option("30岁以上", up30Ratio);
        ageOptions.add(low20Opt);
        ageOptions.add(up30Opt);
        ageOptions.add(up20AndLow30Opt);


        //TODO 性别比例聚合组
        TermsAggregation groupByGender = aggregations.getTermsAggregation("groupBy_user_gender");//获取聚合组中指定聚合
        List<TermsAggregation.Entry> buckets2 = groupByGender.getBuckets();
        Long maleCount = 0L;
        Long femaleCount = 0L;
        for (TermsAggregation.Entry bucket : buckets2) {
            if ("M".equals(bucket.getKey())) {//男性的数量
                maleCount += bucket.getCount();
            } else if ("F".equals(bucket.getKey())) {
                femaleCount += bucket.getCount();
            }
        }

        //计算三者的比例
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;

        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);

        Stat ageStat = new Stat(ageOptions, "用户年龄占比");
        Stat genderStat = new Stat(genderOptions, "用户性别占比");

        ArrayList<Object> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        HashMap<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);


        jestClient.shutdownClient();
        return result;
    }

}
