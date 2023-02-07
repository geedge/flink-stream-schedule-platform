package com.zdjizhi.pre.operator;

import com.zdjizhi.pre.common.CnMetricLog;
import org.apache.flink.api.common.functions.ReduceFunction;

public class SecondAggregationReduce implements ReduceFunction<CnMetricLog> {

    @Override
    public CnMetricLog reduce(CnMetricLog value1, CnMetricLog value2) throws Exception {
        value1.setCommon_c2s_pkt_num(value1.getCommon_c2s_pkt_num() + value2.getCommon_c2s_pkt_num());
        value1.setCommon_s2c_pkt_num(value1.getCommon_s2c_pkt_num() + value2.getCommon_s2c_pkt_num());
        value1.setCommon_c2s_byte_num(value1.getCommon_c2s_byte_num() + value2.getCommon_c2s_byte_num());
        value1.setCommon_s2c_byte_num(value1.getCommon_s2c_byte_num() + value2.getCommon_s2c_byte_num());
        value1.setCommon_sessions(value1.getCommon_sessions() + value2.getCommon_sessions());
        value1.setCommon_c2s_tcp_lostlen(value1.getCommon_c2s_tcp_lostlen() + value2.getCommon_c2s_tcp_lostlen());
        value1.setCommon_s2c_tcp_lostlen(value1.getCommon_s2c_tcp_lostlen() + value2.getCommon_s2c_tcp_lostlen());
        value1.setCommon_c2s_tcp_unorder_num(value1.getCommon_c2s_tcp_unorder_num() + value2.getCommon_c2s_tcp_unorder_num());
        value1.setCommon_s2c_tcp_unorder_num(value1.getCommon_s2c_tcp_unorder_num() + value2.getCommon_s2c_tcp_unorder_num());
        value1.setCommon_c2s_byte_retrans(value1.getCommon_c2s_byte_retrans() + value2.getCommon_c2s_byte_retrans());
        value1.setCommon_s2c_byte_retrans(value1.getCommon_s2c_byte_retrans() + value2.getCommon_s2c_byte_retrans());
        value1.setCommon_c2s_pkt_retrans(value1.getCommon_c2s_pkt_retrans() + value2.getCommon_c2s_pkt_retrans());
        value1.setCommon_s2c_pkt_retrans(value1.getCommon_s2c_pkt_retrans()+value2.getCommon_s2c_pkt_retrans());
        value1.setCommon_establish_latency_ms(value1.getCommon_establish_latency_ms() + value2.getCommon_establish_latency_ms());
        value1.setHttp_response_latency_ms(value1.getHttp_response_latency_ms() + value2.getHttp_response_latency_ms());
        value1.setSsl_con_latency_ms(value1.getSsl_con_latency_ms() + value2.getSsl_con_latency_ms());
//        value1.setDns_response_latency_ms(value1.getDns_response_latency_ms() + value2.getDns_response_latency_ms());
        value1.setC2s_tcp_pkt_num(value1.getC2s_tcp_pkt_num() + value2.getC2s_tcp_pkt_num());
        value1.setS2c_tcp_pkt_num(value1.getS2c_tcp_pkt_num() + value2.getS2c_tcp_pkt_num());
        value1.setC2s_tcp_byte_num(value1.getC2s_tcp_byte_num() + value2.getC2s_tcp_byte_num());
        value1.setS2c_tcp_byte_num(value1.getS2c_tcp_byte_num() + value2.getS2c_tcp_byte_num());
        value1.setTcp_count(value1.getTcp_count() + value2.getTcp_count());
        value1.setHttp_count(value1.getHttp_count() + value2.getHttp_count());
        value1.setSsl_count(value1.getSsl_count() + value2.getSsl_count());
        value1.setTraffic_inbound_byte(value1.getTraffic_inbound_byte() + value2.getTraffic_inbound_byte());
        value1.setTraffic_inbound_pkt(value1.getTraffic_inbound_pkt() + value2.getTraffic_inbound_pkt());
        value1.setTraffic_outbound_byte(value1.getTraffic_outbound_byte() + value2.getTraffic_outbound_byte());
        value1.setTraffic_outbound_pkt(value1.getTraffic_outbound_pkt() + value2.getTraffic_outbound_pkt());
        value1.setTraffic_internal_byte(value1.getTraffic_internal_byte() + value2.getTraffic_internal_byte());
        value1.setTraffic_internal_pkt(value1.getTraffic_internal_pkt() + value2.getTraffic_internal_pkt());
        value1.setTraffic_through_byte(value1.getTraffic_through_byte() + value2.getTraffic_through_byte());
        value1.setTraffic_through_pkt(value1.getTraffic_through_pkt() + value2.getTraffic_through_pkt());
        return value1;
    }

}

