package com.zdjizhi.pre.function;

import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.handler.CommonMetric;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricIpProcessWindowFunc extends ProcessWindowFunction<CnMetricLog, String, String, TimeWindow> {
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private String side;

    public MetricIpProcessWindowFunc(String side) {
        this.side = side;
    }

    @Override
    public void process(String ip, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setIp(ip);
        metricResultLog.setSide(side);
        metricResultLog.setZone(next.getZone());
        metricResultLog.setStat_time(context.window().getEnd()/1000);

        CommonMetric.setCommonMetric(next,metricResultLog);
        String metricResultJsonStr = jsonMapperInstance.toJson(metricResultLog);
        out.collect(metricResultJsonStr);
    }



}
