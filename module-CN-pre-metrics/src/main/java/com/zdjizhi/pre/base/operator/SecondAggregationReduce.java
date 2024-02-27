package com.zdjizhi.pre.base.operator;

import com.zdjizhi.pre.base.common.CnMetricLog;
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
        value1.setAsymmetric_sessions(value1.getAsymmetric_sessions() + value2.getAsymmetric_sessions());
        value1.setBulky_sessions(value1.getBulky_sessions() + value2.getBulky_sessions());
        value1.setCbr_streaming_sessions(value1.getCbr_streaming_sessions() + value2.getCbr_streaming_sessions());
        value1.setDownload_sessions(value1.getDownload_sessions() + value2.getDownload_sessions());
        value1.setInteractive_sessions(value1.getInteractive_sessions() + value2.getInteractive_sessions());
        value1.setPseudo_unidirectional_sessions(value1.getPseudo_unidirectional_sessions() + value2.getPseudo_unidirectional_sessions());
        value1.setStreaming_sessions(value1.getStreaming_sessions() + value2.getStreaming_sessions());
        value1.setUnidirectional_sessions(value1.getUnidirectional_sessions() + value2.getUnidirectional_sessions());
        value1.setRandom_looking_sessions(value1.getRandom_looking_sessions() + value2.getRandom_looking_sessions());
        value1.setBidirectional_sessions(value1.getBidirectional_sessions() + value2.getBidirectional_sessions());
        return value1;
    }

}

