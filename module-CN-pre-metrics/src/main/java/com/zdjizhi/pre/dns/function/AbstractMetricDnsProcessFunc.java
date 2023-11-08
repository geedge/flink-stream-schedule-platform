package com.zdjizhi.pre.dns.function;

import com.zdjizhi.pre.dns.common.CommonConfig;
import com.zdjizhi.pre.dns.common.DnsMetricLog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public abstract class AbstractMetricDnsProcessFunc<KEY> extends ProcessWindowFunction<DnsMetricLog,String,KEY, TimeWindow> {

    public int preDnsMetricsRoundScale = 4;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        final Configuration configuration = (Configuration) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        preDnsMetricsRoundScale = configuration.get(CommonConfig.DNS_PRE_METRICS_ROUND_SCALE);
    }
}
