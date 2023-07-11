package com.zdjizhi.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.pre.handler.CommonMetric;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricLinkProcessFunc extends ProcessWindowFunction<CnMetricLog, String, Tuple2<Long, Long>, TimeWindow> {

    @Override
    public void process(Tuple2<Long, Long> key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setClient_country(next.getClient_country());
        metricResultLog.setClient_province(next.getClient_province());
        metricResultLog.setClient_region(next.getClient_region());
        metricResultLog.setClient_zone(next.getClient_zone());
        metricResultLog.setServer_country(next.getServer_country());
        metricResultLog.setServer_province(next.getServer_province());
        metricResultLog.setServer_region(next.getServer_region());
        metricResultLog.setServer_zone(next.getServer_zone());
        metricResultLog.setCommon_egress_link_id(next.getCommon_egress_link_id());
        metricResultLog.setCommon_ingress_link_id(next.getCommon_ingress_link_id());
        metricResultLog.setEgress_link_direction(next.getEgress_link_direction());
        metricResultLog.setIngress_link_direction(next.getIngress_link_direction());

        metricResultLog.setStat_time(context.window().getStart()/1000);

        CommonMetric.setCommonMetric(next, metricResultLog);
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }
}
