package com.zdjizhi.pre.handler;

import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.CommonConfig;
import com.zdjizhi.pre.common.MetricResultLog;


import java.math.BigDecimal;

public class CommonMetric {
    public static void setCommonMetric(CnMetricLog next, MetricResultLog metricResultLog) {
        metricResultLog.setCommon_c2s_pkt_num(next.getCommon_c2s_pkt_num());
        metricResultLog.setCommon_s2c_pkt_num(next.getCommon_s2c_pkt_num());
        metricResultLog.setCommon_c2s_byte_num(next.getCommon_c2s_byte_num());
        metricResultLog.setCommon_s2c_byte_num(next.getCommon_s2c_byte_num());
        metricResultLog.setCommon_sessions(next.getCommon_sessions());
        metricResultLog.setTraffic_inbound_byte(next.getTraffic_inbound_byte());
        metricResultLog.setTraffic_inbound_pkt(next.getTraffic_inbound_pkt());
        metricResultLog.setTraffic_outbound_byte(next.getTraffic_outbound_byte());
        metricResultLog.setTraffic_outbound_pkt(next.getTraffic_outbound_pkt());
        metricResultLog.setTraffic_internal_byte(next.getTraffic_internal_byte());
        metricResultLog.setTraffic_internal_pkt(next.getTraffic_internal_pkt());
        metricResultLog.setTraffic_through_byte(next.getTraffic_through_byte());
        metricResultLog.setTraffic_through_pkt(next.getTraffic_through_pkt());

        float c2sLostlenRatio = (float) next.getCommon_c2s_tcp_lostlen() / (next.getC2s_tcp_byte_num()+next.getCommon_c2s_tcp_lostlen());
        if (Float.isFinite(c2sLostlenRatio)){
            metricResultLog.setC2s_tcp_lostlen_ratio(BigDecimal.valueOf(c2sLostlenRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float s2cLostlenRatio = (float) next.getCommon_s2c_tcp_lostlen() / (next.getS2c_tcp_byte_num()+next.getCommon_s2c_tcp_lostlen());
        if (Float.isFinite(s2cLostlenRatio)){
            metricResultLog.setS2c_tcp_lostlen_ratio(BigDecimal.valueOf(s2cLostlenRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float lostlenRatio = (float) (next.getCommon_c2s_tcp_lostlen() + next.getCommon_s2c_tcp_lostlen()) / (next.getC2s_tcp_byte_num() + next.getS2c_tcp_byte_num()+next.getCommon_c2s_tcp_lostlen() + next.getCommon_s2c_tcp_lostlen());
        if (Float.isFinite(lostlenRatio)){
            metricResultLog.setTcp_lostlen_ratio(BigDecimal.valueOf(lostlenRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float c2sUnorderRatio = (float) next.getCommon_c2s_tcp_unorder_num() / next.getC2s_tcp_byte_num();
        if (Float.isFinite(c2sUnorderRatio)){
            metricResultLog.setC2s_tcp_unorder_num_ratio(BigDecimal.valueOf(c2sUnorderRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float s2cUnorderRatio = (float) next.getCommon_s2c_tcp_unorder_num() / next.getS2c_tcp_byte_num();
        if (Float.isFinite(s2cUnorderRatio)){
            metricResultLog.setS2c_tcp_unorder_num_ratio(BigDecimal.valueOf(s2cUnorderRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float unorderRatio = (float) (next.getCommon_c2s_tcp_unorder_num() + next.getCommon_s2c_tcp_unorder_num()) / (next.getC2s_tcp_byte_num() + next.getS2c_tcp_byte_num());
        if (Float.isFinite(unorderRatio)){
            metricResultLog.setTcp_unorder_num_ratio(BigDecimal.valueOf(unorderRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float c2sByteRetrandRatio = (float) next.getCommon_c2s_byte_retrans() / next.getC2s_tcp_byte_num();
        if (Float.isFinite(c2sByteRetrandRatio)){
            metricResultLog.setC2s_byte_retrans_ratio(BigDecimal.valueOf(c2sByteRetrandRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float s2cByteRetransRatio = (float) next.getCommon_s2c_byte_retrans() / next.getS2c_tcp_byte_num();
        if (Float.isFinite(s2cByteRetransRatio)){
            metricResultLog.setS2c_byte_retrans_ratio(BigDecimal.valueOf(s2cByteRetransRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float byteRetransRatio = (float) (next.getCommon_c2s_byte_retrans() + next.getCommon_s2c_byte_retrans()) / (next.getC2s_tcp_byte_num() + next.getS2c_tcp_byte_num());
        if (Float.isFinite(byteRetransRatio)){
            metricResultLog.setByte_retrans_ratio(BigDecimal.valueOf(byteRetransRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float c2sPktRetransRatio = (float) next.getCommon_c2s_pkt_retrans() / next.getC2s_tcp_pkt_num();
        if (Float.isFinite(c2sPktRetransRatio)){
            metricResultLog.setC2s_pkt_retrans_ratio(BigDecimal.valueOf(c2sPktRetransRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float s2cPktRetransRatio = (float) next.getCommon_s2c_pkt_retrans() / next.getS2c_tcp_pkt_num();
        if (Float.isFinite(s2cPktRetransRatio)){
            metricResultLog.setS2c_pkt_retrans_ratio(BigDecimal.valueOf(s2cPktRetransRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float pktRetransRatio = (float) (next.getCommon_c2s_pkt_retrans() + next.getCommon_s2c_pkt_retrans()) / (next.getC2s_tcp_pkt_num() + next.getS2c_tcp_pkt_num());
        if (Float.isFinite(pktRetransRatio)){
            metricResultLog.setPkt_retrans_ratio(BigDecimal.valueOf(pktRetransRatio).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float establishMsAvg = (float) next.getCommon_establish_latency_ms() / next.getTcp_count();
        if (Float.isFinite(establishMsAvg)){
            metricResultLog.setAvg_establish_latency_ms(BigDecimal.valueOf(establishMsAvg).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float responseLatencyAvg = (float) next.getHttp_response_latency_ms() / next.getHttp_count();
        if (Float.isFinite(responseLatencyAvg)){
            metricResultLog.setAvg_http_response_latency_ms(BigDecimal.valueOf(responseLatencyAvg).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }

        float sslLatencyAvg = (float) next.getSsl_con_latency_ms() / next.getSsl_count();
        if (Float.isFinite(sslLatencyAvg)){
            metricResultLog.setAvg_ssl_con_latency_ms(BigDecimal.valueOf(sslLatencyAvg).setScale(CommonConfig.PRE_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).floatValue());
        }
    }
}
