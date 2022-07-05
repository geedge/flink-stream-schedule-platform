package com.zdjizhi.top.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.top.common.CommonConfig;
import com.zdjizhi.top.common.TopBaseLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;
import java.text.ParseException;

public class TopMetricAggregate implements AggregateFunction<CnRecordLog, Tuple2<TopBaseLog,Long>,TopBaseLog> {

    private String groupBy;
    private boolean countryCityFlag;

    public TopMetricAggregate(String groupBy, boolean countryCityFlag) {
        this.groupBy = groupBy;
        this.countryCityFlag = countryCityFlag;
    }

    @Override
    public Tuple2<TopBaseLog, Long> createAccumulator() {

        return new Tuple2<>(new TopBaseLog(), 0L);
    }

    @Override
    public Tuple2<TopBaseLog, Long> add(CnRecordLog value, Tuple2<TopBaseLog, Long> accumulator) {
        TopBaseLog baseLog = accumulator.f0;

        baseLog.set(groupBy,value);

//        if (countryCityFlag){
//            baseLog.setServer_country(value.getServer_country());
//            baseLog.setClient_country(value.getClient_country());
//            baseLog.setServer_province(value.getServer_province());
//            baseLog.setClient_province(value.getClient_province());
//            baseLog.setServer_region(value.getServer_region());
//            baseLog.setCommon_schema_type(value.getCommon_schema_type());
//            baseLog.setCommon_l4_protocol(value.getCommon_l4_protocol());
//            baseLog.setCommon_recv_time(value.getCommon_recv_time() > baseLog.getCommon_recv_time() ? value.getCommon_recv_time() : baseLog.getCommon_recv_time());
//        }


        baseLog.setTime_granularity(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME);

        long commonC2sPktNum = value.getCommon_c2s_pkt_num() + baseLog.getCommon_c2s_pkt_num();
        long commonC2sByteNum = value.getCommon_c2s_byte_num() + baseLog.getCommon_c2s_byte_num();
        long commonS2cPktNum = value.getCommon_s2c_pkt_num() + baseLog.getCommon_s2c_pkt_num();
        long commonS2cByteNum = value.getCommon_s2c_byte_num() + baseLog.getCommon_s2c_byte_num();
        long commonC2sTcpLostlen = value.getCommon_c2s_tcp_lostlen() + baseLog.getCommon_c2s_tcp_lostlen();
        long commonS2cTcpLostlen = value.getCommon_s2c_tcp_lostlen() + baseLog.getCommon_s2c_tcp_lostlen();
        long commonC2sPktRetrans = value.getCommon_c2s_pkt_retrans() + baseLog.getCommon_c2s_pkt_retrans();
        long commonS2cPktRetrans = value.getCommon_s2c_pkt_retrans() + baseLog.getCommon_s2c_pkt_retrans();
        baseLog.setCommon_c2s_pkt_num(commonC2sPktNum);
        baseLog.setCommon_c2s_byte_num(commonC2sByteNum);
        baseLog.setCommon_s2c_pkt_num(commonS2cPktNum);
        baseLog.setCommon_s2c_byte_num(commonS2cByteNum);
        baseLog.setCommon_sessions(value.getCommon_sessions() + baseLog.getCommon_sessions());
        baseLog.setCommon_c2s_tcp_lostlen(commonC2sTcpLostlen);
        baseLog.setCommon_s2c_tcp_lostlen(commonS2cTcpLostlen);
        baseLog.setCommon_c2s_tcp_unorder_num(value.getCommon_c2s_tcp_unorder_num() + baseLog.getCommon_c2s_tcp_unorder_num());
        baseLog.setCommon_s2c_tcp_unorder_num(value.getCommon_s2c_tcp_unorder_num() + baseLog.getCommon_s2c_tcp_unorder_num());
        baseLog.setCommon_c2s_byte_retrans(value.getCommon_c2s_byte_retrans() + baseLog.getCommon_c2s_byte_retrans());
        baseLog.setCommon_s2c_byte_retrans(value.getCommon_s2c_byte_retrans() + baseLog.getCommon_s2c_byte_retrans());
        baseLog.setCommon_c2s_pkt_retrans(commonC2sPktRetrans);
        baseLog.setCommon_s2c_pkt_retrans(commonS2cPktRetrans);
        baseLog.setCommon_establish_latency_ms(value.getCommon_establish_latency_ms()+ baseLog.getCommon_establish_latency_ms());
        baseLog.setHttp_response_latency_ms(value.getHttp_response_latency_ms()+ baseLog.getHttp_response_latency_ms());
        baseLog.setSsl_con_latency_ms(value.getSsl_con_latency_ms()+ baseLog.getSsl_con_latency_ms());

        double packetLoss = (double) (commonC2sTcpLostlen + commonS2cTcpLostlen) / (double) (commonC2sTcpLostlen + commonS2cTcpLostlen + commonC2sByteNum + commonS2cByteNum);
        double packetRetrans = (double) (commonC2sPktRetrans + commonS2cPktRetrans) / (double) (commonC2sPktNum + commonS2cPktNum);

        baseLog.setPacket_loss(packetLoss);
        baseLog.setPacket_retrans(packetRetrans);

        accumulator.f1 += 1;

        return accumulator;
    }

    public static void main(String[] args) throws ParseException {
        double f = 111231.5585;
        BigDecimal bg = new BigDecimal(f);
        double f1 = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        System.out.println(f1);
    }

    @Override
    public TopBaseLog getResult(Tuple2<TopBaseLog, Long> accumulator) {
        TopBaseLog baseLog = accumulator.f0;
        Long cnt = accumulator.f1;
        baseLog.setCommon_establish_latency_ms(baseLog.getCommon_establish_latency_ms()/cnt);
        baseLog.setHttp_response_latency_ms(baseLog.getHttp_response_latency_ms()/cnt);
        baseLog.setSsl_con_latency_ms(baseLog.getSsl_con_latency_ms()/cnt);
        double packetLoss = baseLog.getPacket_loss() / cnt;
        baseLog.setPacket_loss(new BigDecimal(packetLoss).setScale(CommonConfig.TOP_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).doubleValue());
        double packetRetrans = baseLog.getPacket_retrans() / cnt;
        baseLog.setPacket_retrans(new BigDecimal(packetRetrans).setScale(CommonConfig.TOP_METRICS_ROUND_SCALE, BigDecimal.ROUND_HALF_UP).doubleValue());
        return baseLog;
    }

    @Override
    public Tuple2<TopBaseLog, Long> merge(Tuple2<TopBaseLog, Long> a, Tuple2<TopBaseLog, Long> b) {
        TopBaseLog baseLogA = a.f0;
        TopBaseLog baseLogB = b.f0;
//        baseLogA.setCommon_recv_time(baseLogA.getCommon_recv_time() > baseLogB.getCommon_recv_time() ? baseLogA.getCommon_recv_time() : baseLogB.getCommon_recv_time());

        baseLogA.setCommon_c2s_pkt_num(baseLogA.getCommon_c2s_pkt_num() + baseLogB.getCommon_c2s_pkt_num());
        baseLogA.setCommon_c2s_byte_num(baseLogA.getCommon_c2s_byte_num() + baseLogB.getCommon_c2s_byte_num());
        baseLogA.setCommon_s2c_pkt_num(baseLogA.getCommon_s2c_pkt_num() + baseLogB.getCommon_s2c_pkt_num());
        baseLogA.setCommon_s2c_byte_num(baseLogA.getCommon_s2c_byte_num() + baseLogB.getCommon_s2c_byte_num());
        baseLogA.setCommon_sessions(baseLogA.getCommon_sessions() + baseLogB.getCommon_sessions());
        baseLogA.setCommon_c2s_tcp_lostlen(baseLogA.getCommon_c2s_tcp_lostlen() + baseLogB.getCommon_c2s_tcp_lostlen());
        baseLogA.setCommon_s2c_tcp_lostlen(baseLogA.getCommon_s2c_tcp_lostlen() + baseLogB.getCommon_s2c_tcp_lostlen());
        baseLogA.setCommon_c2s_tcp_unorder_num(baseLogA.getCommon_c2s_tcp_unorder_num() + baseLogB.getCommon_c2s_tcp_unorder_num());
        baseLogA.setCommon_s2c_tcp_unorder_num(baseLogA.getCommon_s2c_tcp_unorder_num() + baseLogB.getCommon_s2c_tcp_unorder_num());
        baseLogA.setCommon_c2s_byte_retrans(baseLogA.getCommon_c2s_byte_retrans() + baseLogB.getCommon_c2s_byte_retrans());
        baseLogA.setCommon_s2c_byte_retrans(baseLogA.getCommon_s2c_byte_retrans() + baseLogB.getCommon_s2c_byte_retrans());
        baseLogA.setCommon_c2s_pkt_retrans(baseLogA.getCommon_c2s_pkt_retrans() + baseLogB.getCommon_c2s_pkt_retrans());
        baseLogA.setCommon_s2c_pkt_retrans(baseLogA.getCommon_s2c_pkt_retrans() + baseLogB.getCommon_s2c_pkt_retrans());
        baseLogA.setCommon_establish_latency_ms(baseLogA.getCommon_establish_latency_ms()+ baseLogB.getCommon_establish_latency_ms());
        baseLogA.setHttp_response_latency_ms(baseLogA.getHttp_response_latency_ms()+ baseLogB.getHttp_response_latency_ms());
        baseLogA.setSsl_con_latency_ms(baseLogA.getSsl_con_latency_ms()+ baseLogB.getSsl_con_latency_ms());

        baseLogA.setPacket_loss(baseLogA.getPacket_loss()+baseLogB.getPacket_loss());
        baseLogA.setPacket_retrans(baseLogA.getPacket_retrans()+baseLogB.getPacket_retrans());

        a.f1 = a.f1+b.f1;

        return a;
    }

}
