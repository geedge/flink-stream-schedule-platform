package com.zdjizhi.pre.operator;

import com.zdjizhi.pre.common.CnMetricLog;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class FirstAggregationProcessFunc<KEY> extends ProcessWindowFunction<List<CnMetricLog>, CnMetricLog, KEY, TimeWindow> {


    @Override
    public void process(KEY s, ProcessWindowFunction<List<CnMetricLog>, CnMetricLog, KEY, TimeWindow>.Context context, Iterable<List<CnMetricLog>> elements, Collector<CnMetricLog> out) throws Exception {
        List<CnMetricLog> next = elements.iterator().next();
        for (CnMetricLog cnMetricLog : next) {
            out.collect(cnMetricLog);
        }
    }
}
