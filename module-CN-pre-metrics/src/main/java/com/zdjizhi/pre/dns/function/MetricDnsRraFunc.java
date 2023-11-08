package com.zdjizhi.pre.dns.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.dns.common.CommonDnsMetric;
import com.zdjizhi.pre.dns.common.DnsMetricLog;
import com.zdjizhi.pre.dns.common.DnsMetricResultLog;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricDnsRraFunc extends AbstractMetricDnsProcessFunc<String> {

    @Override
    public void process(String s, Context context, Iterable<DnsMetricLog> elements, Collector<String> out) throws Exception {
        DnsMetricLog next = elements.iterator().next();
        DnsMetricResultLog dnsMetricResultLog = new DnsMetricResultLog();
        dnsMetricResultLog.setRr_a(next.getDns_rra());
        dnsMetricResultLog.setStat_time(context.window().getStart()/1000);

        CommonDnsMetric.setCommonMetric(next,dnsMetricResultLog, preDnsMetricsRoundScale);
        String resultJsonStr = JSON.toJSONString(dnsMetricResultLog);
        out.collect(resultJsonStr);
    }
}
