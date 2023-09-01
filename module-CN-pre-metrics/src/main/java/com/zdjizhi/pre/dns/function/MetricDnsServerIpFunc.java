package com.zdjizhi.pre.dns.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.dns.common.CommonDnsMetric;
import com.zdjizhi.pre.dns.common.DnsMetricLog;
import com.zdjizhi.pre.dns.common.DnsMetricResultLog;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricDnsServerIpFunc extends ProcessWindowFunction<DnsMetricLog, String, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<DnsMetricLog> elements, Collector<String> out) throws Exception {
        DnsMetricLog next = elements.iterator().next();

        DnsMetricResultLog dnsMetricResultLog = new DnsMetricResultLog();
        dnsMetricResultLog.setServer_ip(next.getCommon_server_ip());
        dnsMetricResultLog.setServer_country_region(next.getServer_country_region());
        dnsMetricResultLog.setServer_super_admin_area(next.getServer_super_admin_area());
        dnsMetricResultLog.setServer_admin_area(next.getServer_admin_area());
        dnsMetricResultLog.setServer_isp(next.getServer_isp());
        dnsMetricResultLog.setServer_role(next.getServer_role());
        dnsMetricResultLog.setStat_time(context.window().getStart() / 1000);

        CommonDnsMetric.setCommonMetric(next, dnsMetricResultLog);
        String resultJsonStr = JSON.toJSONString(dnsMetricResultLog);
        out.collect(resultJsonStr);
    }
}
