package com.zdjizhi.top.function;

import com.zdjizhi.top.common.CnMetricLog;
import com.zdjizhi.top.handler.CommonMetric;
import com.zdjizhi.top.common.MetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricIdcProcessFunc extends ProcessWindowFunction<CnMetricLog, String, String, TimeWindow> {
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    @Override
    public void process(String key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        if (key.equals(next.getClient_idc_renter())) {
            MetricResultLog metricResultLog = new MetricResultLog();
            metricResultLog.setIdc_renter(next.getClient_idc_renter());
            metricResultLog.setSide("client");
            metricResultLog.setStat_time(context.window().getEnd() / 1000);

            CommonMetric.setCommonMetric(next, metricResultLog);
            String metricResultJsonStr = jsonMapperInstance.toJson(metricResultLog);
            out.collect(metricResultJsonStr);
        }else if(key.equals(next.getServer_idc_renter())){
            MetricResultLog metricResultLog = new MetricResultLog();
            metricResultLog.setIdc_renter(next.getClient_idc_renter());
            metricResultLog.setSide("server");
            metricResultLog.setStat_time(context.window().getEnd() / 1000);

            CommonMetric.setCommonMetric(next, metricResultLog);
            String metricResultJsonStr = jsonMapperInstance.toJson(metricResultLog);
            out.collect(metricResultJsonStr);
        }
    }
}
