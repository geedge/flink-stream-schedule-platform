package com.zdjizhi.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.pre.handler.CommonMetric;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricLinkProcessFunc extends ProcessWindowFunction<CnMetricLog, String, Tuple12<String, String, String, String, String, String, String, String, Long, Long ,String, String>, TimeWindow> {

    @Override
    public void process(Tuple12<String, String, String, String, String, String, String, String, Long, Long ,String, String> key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setClient_country_region(next.getClient_country_region());
        metricResultLog.setClient_super_admin_area(next.getClient_super_admin_area());
        metricResultLog.setClient_admin_area(next.getClient_admin_area());
        metricResultLog.setClient_zone(next.getClient_zone());
        metricResultLog.setServer_country_region(next.getServer_country_region());
        metricResultLog.setServer_super_admin_area(next.getServer_super_admin_area());
        metricResultLog.setServer_admin_area(next.getServer_admin_area());
        metricResultLog.setServer_zone(next.getServer_zone());
        metricResultLog.setCommon_out_link_id(next.getCommon_out_link_id());
        metricResultLog.setCommon_in_link_id(next.getCommon_in_link_id());
        metricResultLog.setOut_link_direction(next.getOut_link_direction());
        metricResultLog.setIn_link_direction(next.getIn_link_direction());

        metricResultLog.setStat_time(context.window().getStart()/1000);

        CommonMetric.setCommonMetric(next, metricResultLog);
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }
}
