package com.zdjizhi.pre.function;

import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.CommonConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public abstract class AbstractMetricProcessFunc<KEY>  extends ProcessWindowFunction<CnMetricLog, String, KEY, TimeWindow> {

    public int preMetricsRoundScale = 4;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final Configuration configuration = (Configuration) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        preMetricsRoundScale = configuration.get(CommonConfig.PRE_METRICS_ROUND_SCALE);
    }
}