package com.zdjizhi.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.handler.CommonMetric;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricSslProcessFunc extends ProcessWindowFunction<CnMetricLog, String, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setSsl_sni(next.getSsl_sni());
        metricResultLog.setStat_time(context.window().getStart()/1000);

        CommonMetric.setCommonMetric(next, metricResultLog);
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }
}
