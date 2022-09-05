package com.zdjizhi.top.function;

import com.zdjizhi.top.common.CnMetricLog;
import com.zdjizhi.top.handler.CommonMetric;
import com.zdjizhi.top.common.MetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;

public class MetricRegionProcessWindowFunc extends ProcessWindowFunction<CnMetricLog, String, Tuple3<String, String, String>, TimeWindow> {
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    @Override
    public void process(Tuple3<String, String, String> key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        if (key.f0.equals(next.getClient_country()) && key.f1.equals(next.getClient_province()) && key.f2.equals(next.getClient_region())) {
            MetricResultLog metricResultLog = new MetricResultLog();
            metricResultLog.setCountry(next.getClient_country());
            metricResultLog.setProvince(next.getClient_province());
            metricResultLog.setCity(next.getClient_region());
            metricResultLog.setSide("client");
            metricResultLog.setStat_time(context.window().getEnd() / 1000);

            CommonMetric.setCommonMetric(next, metricResultLog);
            String metricResultJsonStr = jsonMapperInstance.toJson(metricResultLog);
            out.collect(metricResultJsonStr);
        }else if (key.f0.equals(next.getServer_country()) && key.f1.equals(next.getServer_province()) && key.f2.equals(next.getServer_region())){
            MetricResultLog metricResultLog = new MetricResultLog();
            metricResultLog.setCountry(next.getServer_country());

                metricResultLog.setProvince(next.getServer_province());
                metricResultLog.setCity(next.getServer_region());

            metricResultLog.setSide("server");
            metricResultLog.setStat_time(context.window().getEnd() / 1000);

            CommonMetric.setCommonMetric(next, metricResultLog);
            String metricResultJsonStr = jsonMapperInstance.toJson(metricResultLog);
            out.collect(metricResultJsonStr);
        }
    }
}
