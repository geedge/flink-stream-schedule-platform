package com.zdjizhi.pre.dns.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.dns.common.CommonDnsMetric;
import com.zdjizhi.pre.dns.common.DnsMetricLog;
import com.zdjizhi.pre.dns.common.DnsMetricResultLog;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricDnsQnameFunc extends ProcessWindowFunction<DnsMetricLog,String,String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<DnsMetricLog> elements, Collector<String> out) throws Exception {
        DnsMetricLog next = elements.iterator().next();
        String dns_qname = next.getDns_qname();
        DnsMetricResultLog dnsMetricResultLog = new DnsMetricResultLog();
        dnsMetricResultLog.setQname(dns_qname);
        dnsMetricResultLog.setQname_sld(next.getQname_sld());
        dnsMetricResultLog.setQname_tld(next.getQname_tld());
        dnsMetricResultLog.setStat_time(context.window().getStart()/1000);

        CommonDnsMetric.setCommonMetric(next,dnsMetricResultLog);
        String resultJsonStr = JSON.toJSONString(dnsMetricResultLog);
        out.collect(resultJsonStr);
    }
}
