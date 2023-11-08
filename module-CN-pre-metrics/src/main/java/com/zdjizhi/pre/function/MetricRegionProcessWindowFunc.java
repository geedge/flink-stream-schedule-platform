package com.zdjizhi.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.pre.handler.CommonMetric;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricRegionProcessWindowFunc extends AbstractMetricProcessFunc<Tuple3<String, String, String>> {
    private String side;

    public MetricRegionProcessWindowFunc(String side) {
        this.side = side;
    }

    @Override
    public void process(Tuple3<String, String, String> key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();

        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setCountry_region(key.f0);
        metricResultLog.setSuper_admin_area(key.f1);
        metricResultLog.setAdmin_area(key.f2);
        metricResultLog.setSide(side);
        metricResultLog.setStat_time(context.window().getStart() / 1000);

        CommonMetric.setCommonMetric(next, metricResultLog, preMetricsRoundScale);
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }
}
