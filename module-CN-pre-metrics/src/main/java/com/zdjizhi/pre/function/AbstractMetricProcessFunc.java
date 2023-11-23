package com.zdjizhi.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.CommonConfig;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.pre.handler.CommonMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public abstract class AbstractMetricProcessFunc<KEY>  extends ProcessWindowFunction<CnMetricLog, String, KEY, TimeWindow> {

    public int preMetricsRoundScale = 4;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final Configuration configuration = (Configuration) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        preMetricsRoundScale = configuration.get(CommonConfig.PRE_METRICS_ROUND_SCALE);
    }

    @Override
    public void process(KEY key, ProcessWindowFunction<CnMetricLog, String, KEY, TimeWindow>.Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        MetricResultLog metricResultLog = new MetricResultLog();
        setCommonFields(key, next, metricResultLog);
        metricResultLog.setStat_time(context.window().getStart() / 1000);
        CommonMetric.setCommonMetric(next, metricResultLog, preMetricsRoundScale);
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }

    public abstract void setCommonFields(KEY key, CnMetricLog cnMetricLog, MetricResultLog metricResultLog);
}