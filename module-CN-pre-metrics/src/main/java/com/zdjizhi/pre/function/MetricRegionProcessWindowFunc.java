package com.zdjizhi.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.handler.CommonMetric;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;

public class MetricRegionProcessWindowFunc extends ProcessWindowFunction<CnMetricLog, String, Tuple3<String, String, String>, TimeWindow> {
    private String side;

    public MetricRegionProcessWindowFunc(String side) {
        this.side = side;
    }

    @Override
    public void process(Tuple3<String, String, String> key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();

        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setCountry(key.f0);
        metricResultLog.setProvince(key.f1);
        metricResultLog.setCity(key.f2);
        metricResultLog.setSide(side);
        metricResultLog.setStat_time(context.window().getEnd() / 1000);

        CommonMetric.setCommonMetric(next, metricResultLog);
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }
}
