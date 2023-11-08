package com.zdjizhi.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.pre.handler.CommonMetric;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricDomainProcessFunc extends AbstractMetricProcessFunc<String> {
    @Override
    public void process(String key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setDomain(next.getDomain());
        metricResultLog.setDomain_sld(next.getDomain_sld());
        metricResultLog.setDomain_category_group(next.getDomain_category_group());
        metricResultLog.setDomain_category_name(next.getDomain_category_name());
        metricResultLog.setStat_time(context.window().getStart()/1000);

        CommonMetric.setCommonMetric(next, metricResultLog, preMetricsRoundScale);
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }
}
