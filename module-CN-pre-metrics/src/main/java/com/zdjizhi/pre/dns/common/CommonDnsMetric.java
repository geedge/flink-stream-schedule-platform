package com.zdjizhi.pre.dns.common;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class CommonDnsMetric {
    public static void setCommonMetric(DnsMetricLog dnsMetricLog, DnsMetricResultLog dnsMetricResultLog, int preDnsMetricsRoundScale) {
        dnsMetricResultLog.setQuery_num(dnsMetricLog.getCommon_sessions());
        dnsMetricResultLog.setInternal_query_num(dnsMetricLog.getInternal_query_num());
        dnsMetricResultLog.setExternal_query_num(dnsMetricLog.getExternal_query_num());
        dnsMetricResultLog.setTraffic_inbound_byte(dnsMetricLog.getTraffic_inbound_byte());
        dnsMetricResultLog.setTraffic_outbound_byte(dnsMetricLog.getTraffic_outbound_byte());

        float responseLatencyAvg = (float) dnsMetricLog.getDns_response_latency_ms() / dnsMetricLog.getDns_count();
        if (Float.isFinite(responseLatencyAvg)){
            dnsMetricResultLog.setAvg_response_latency_ms(BigDecimal.valueOf(responseLatencyAvg).setScale(preDnsMetricsRoundScale, RoundingMode.HALF_UP).floatValue());
        }
    }
}
