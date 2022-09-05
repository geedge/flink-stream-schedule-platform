package com.zdjizhi.pre.function;

import org.apache.flink.api.java.tuple.Tuple2;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.handler.CommonMetric;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricAsnProcessFunc extends ProcessWindowFunction<CnMetricLog, String, Tuple2<String, String>, TimeWindow> {
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    @Override
    public void process(Tuple2<String, String> key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        if (key.f0.equals(next.getClient_asn()) && key.f1.equals(next.getClient_isp())) {
            MetricResultLog metricResultLog = new MetricResultLog();
            metricResultLog.setAsn(next.getClient_asn());
            metricResultLog.setIsp(next.getClient_isp());
            metricResultLog.setSide("client");
            metricResultLog.setStat_time(context.window().getEnd() / 1000);

            CommonMetric.setCommonMetric(next, metricResultLog);
            String metricResultJsonStr = jsonMapperInstance.toJson(metricResultLog);
            out.collect(metricResultJsonStr);
        }else if (key.f0.equals(next.getServer_asn()) && key.f1.equals(next.getServer_isp())) {
            MetricResultLog metricResultLog = new MetricResultLog();
            metricResultLog.setAsn(next.getServer_asn());
            metricResultLog.setIsp(next.getServer_isp());
            metricResultLog.setSide("server");
            metricResultLog.setStat_time(context.window().getEnd() / 1000);

            CommonMetric.setCommonMetric(next, metricResultLog);
            String metricResultJsonStr = jsonMapperInstance.toJson(metricResultLog);
            out.collect(metricResultJsonStr);
        }
    }
}
