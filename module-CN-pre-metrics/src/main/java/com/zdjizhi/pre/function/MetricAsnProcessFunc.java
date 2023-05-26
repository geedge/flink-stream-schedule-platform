package com.zdjizhi.pre.function;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.java.tuple.Tuple2;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.handler.CommonMetric;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricAsnProcessFunc extends ProcessWindowFunction<CnMetricLog, String, Tuple2<String, String>, TimeWindow> {
    private String side;

    public MetricAsnProcessFunc(String side) {
        this.side = side;
    }

    @Override
    public void process(Tuple2<String, String> key, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();

        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setAsn(key.f0);
        metricResultLog.setIsp(key.f1);
        metricResultLog.setSide(side);
        metricResultLog.setStat_time(context.window().getEnd() / 1000);

        CommonMetric.setCommonMetric(next, metricResultLog);
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }
}
