package com.zdjizhi.dns.pre.function;

import com.zdjizhi.dns.pre.common.CommonDnsMetric;
import com.zdjizhi.dns.pre.common.DnsMetricLog;
import com.zdjizhi.dns.pre.common.DnsMetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricDnsRrcnameFunc extends ProcessWindowFunction<DnsMetricLog,String,String, TimeWindow> {
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    @Override
    public void process(String s, Context context, Iterable<DnsMetricLog> elements, Collector<String> out) throws Exception {
        DnsMetricLog next = elements.iterator().next();
//        String dns_rr = next.getDns_rr();
//        String rrJson = dns_rr.replace("'\'", "");

        DnsMetricResultLog dnsMetricResultLog = new DnsMetricResultLog();
        dnsMetricResultLog.setRr_cname(next.getDns_rrcname());
        dnsMetricResultLog.setStat_time(context.window().getEnd()/1000);

        CommonDnsMetric.setCommonMetric(next,dnsMetricResultLog);
        String resultJsonStr = jsonMapperInstance.toJson(dnsMetricResultLog);
        out.collect(resultJsonStr);
    }
}
