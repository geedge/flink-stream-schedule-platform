package com.zdjizhi.dns.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.dns.pre.common.CommonDnsMetric;
import com.zdjizhi.dns.pre.common.DnsMetricLog;
import com.zdjizhi.dns.pre.common.DnsMetricResultLog;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricDnsRraFunc extends ProcessWindowFunction<DnsMetricLog,String,String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<DnsMetricLog> elements, Collector<String> out) throws Exception {
        DnsMetricLog next = elements.iterator().next();
        DnsMetricResultLog dnsMetricResultLog = new DnsMetricResultLog();
        dnsMetricResultLog.setRr_a(next.getDns_rra());
        dnsMetricResultLog.setStat_time(context.window().getStart()/1000);

        CommonDnsMetric.setCommonMetric(next,dnsMetricResultLog);
        String resultJsonStr = JSON.toJSONString(dnsMetricResultLog);
        out.collect(resultJsonStr);
    }
}
