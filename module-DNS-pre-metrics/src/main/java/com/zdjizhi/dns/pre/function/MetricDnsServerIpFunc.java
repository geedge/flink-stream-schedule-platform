package com.zdjizhi.dns.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.dns.pre.common.CommonDnsMetric;
import com.zdjizhi.dns.pre.common.DnsMetricLog;
import com.zdjizhi.dns.pre.common.DnsMetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricDnsServerIpFunc extends ProcessWindowFunction<DnsMetricLog, String, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<DnsMetricLog> elements, Collector<String> out) throws Exception {
        DnsMetricLog next = elements.iterator().next();

        DnsMetricResultLog dnsMetricResultLog = new DnsMetricResultLog();
        dnsMetricResultLog.setServer_ip(next.getCommon_server_ip());
        dnsMetricResultLog.setServer_country(next.getServer_country());
        dnsMetricResultLog.setServer_city(next.getServer_city());
        dnsMetricResultLog.setServer_isp(next.getServer_isp());
        dnsMetricResultLog.setServer_org(next.getDns_server_org());
        dnsMetricResultLog.setServer_role(next.getDns_server_role());
        dnsMetricResultLog.setStat_time(context.window().getStart() / 1000);

        CommonDnsMetric.setCommonMetric(next, dnsMetricResultLog);
        String resultJsonStr = JSON.toJSONString(dnsMetricResultLog);
        out.collect(resultJsonStr);
    }
}
