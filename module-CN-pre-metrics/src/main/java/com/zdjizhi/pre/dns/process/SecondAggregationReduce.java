package com.zdjizhi.pre.dns.process;


import com.zdjizhi.pre.dns.common.DnsMetricLog;
import org.apache.flink.api.common.functions.ReduceFunction;

public class SecondAggregationReduce implements ReduceFunction<DnsMetricLog> {

    @Override
    public DnsMetricLog reduce(DnsMetricLog value1, DnsMetricLog value2) throws Exception {
        value1.setCommon_sessions(value1.getCommon_sessions() + value2.getCommon_sessions());
        value1.setDns_count(value1.getDns_count()+value2.getDns_count());
        value1.setTraffic_outbound_byte(value1.getTraffic_outbound_byte() + value2.getTraffic_outbound_byte());
        value1.setTraffic_inbound_byte(value1.getTraffic_inbound_byte() + value2.getTraffic_inbound_byte());
        value1.setInternal_query_num(value1.getInternal_query_num()+value2.getInternal_query_num());
        value1.setExternal_query_num(value1.getExternal_query_num()+value2.getExternal_query_num());
        value1.setDns_response_latency_ms(value1.getDns_response_latency_ms() + value2.getDns_response_latency_ms());
        return value1;
    }
}
