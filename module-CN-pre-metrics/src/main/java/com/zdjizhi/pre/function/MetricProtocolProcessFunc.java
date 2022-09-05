package com.zdjizhi.pre.function;

import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.handler.CommonMetric;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricProtocolProcessFunc extends ProcessWindowFunction<CnMetricLog, String, Tuple2<String, Integer>, TimeWindow> {
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    @Override
    public void process(Tuple2<String, Integer> stringIntegerTuple2, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setCommon_l7_protocol(next.getCommon_l7_protocol());
        metricResultLog.setCommon_server_port(next.getCommon_server_port());
        metricResultLog.setStat_time(context.window().getEnd()/1000);

        CommonMetric.setCommonMetric(next, metricResultLog);
        String metricResultJsonStr = jsonMapperInstance.toJson(metricResultLog);
        out.collect(metricResultJsonStr);
    }
}
