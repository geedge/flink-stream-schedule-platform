package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.utils.SnowflakeId;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * @author wlh
 */
public class DnsErrorDetectionProcess extends KeyedBroadcastProcessFunction<String, CnRecordLog, Integer, PerformanceEventLog> {

    private ValueState<Long> currentTimer;
    private MapStateDescriptor<Void, Integer> broadcastTotalCntState;
    private MapState<String, PerformanceEventLog> previousResultDesc;
    private MapState<String, Tuple2<PerformanceEventLog, Double>> resultDesc;

    private String entityType;

    public DnsErrorDetectionProcess(String entityType) {
        this.entityType = entityType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));
        broadcastTotalCntState = new MapStateDescriptor<>("cnt", Types.VOID, Types.INT);
        previousResultDesc = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("previous_result",
                        Types.STRING,
                        Types.POJO(PerformanceEventLog.class)));
        resultDesc = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("result",
                        Types.STRING,
                        Types.TUPLE(Types.POJO(PerformanceEventLog.class), Types.DOUBLE)));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PerformanceEventLog> out) throws Exception {
        String currentKey = ctx.getCurrentKey();
        PerformanceEventLog eventLog = detectionEventLog(currentKey, ctx);
        if (eventLog != null) {
            if (previousResultDesc.contains(currentKey)) {
                PerformanceEventLog previousEventLog = previousResultDesc.get(currentKey);
                eventLog.setEvent_id(previousEventLog.getEvent_id());
                eventLog.setStart_time(Math.min(eventLog.getStart_time(),previousEventLog.getStart_time()));
                eventLog.setDuration_ms((eventLog.getEnd_time() - eventLog.getStart_time()) * 1000);
            }
            out.collect(eventLog);
            previousResultDesc.put(currentKey, eventLog);
        } else {
            previousResultDesc.remove(currentKey);
        }
        resultDesc.remove(currentKey);
        currentTimer.clear();
    }

    @Override
    public void processElement(CnRecordLog value, ReadOnlyContext ctx, Collector<PerformanceEventLog> out) throws Exception {
        Long curTimeStamp = currentTimer.value();
        String currentKey = ctx.getCurrentKey();
        if (curTimeStamp == null || curTimeStamp == 0) {
            long onTimer = ctx.timestamp() + CommonConfig.FLINK_WINDOW_MAX_TIME;
            ctx.timerService().registerEventTimeTimer(onTimer);
            currentTimer.update(onTimer);
        }

        if (value.getDns_rcode() != 0) {
            if (resultDesc.contains(currentKey)) {
                Tuple2<PerformanceEventLog, Double> eventLogIntegerTuple = resultDesc.get(currentKey);
                PerformanceEventLog eventLog = eventLogIntegerTuple.f0;
                updateEventLog(eventLog, value);
                resultDesc.put(currentKey, Tuple2.of(eventLog, eventLogIntegerTuple.f1++));
            } else {
                PerformanceEventLog eventLog = new PerformanceEventLog();
                initEventLog(eventLog, value);
                resultDesc.put(currentKey, Tuple2.of(eventLog, 1D));
            }
        }
    }

    @Override
    public void processBroadcastElement(Integer value, Context ctx, Collector<PerformanceEventLog> out) throws Exception {
        BroadcastState<Void, Integer> broadcastState = ctx.getBroadcastState(broadcastTotalCntState);
        broadcastState.put(null, value);
    }

    private void initEventLog(PerformanceEventLog eventLog, CnRecordLog value) {
        eventLog.setEvent_id(SnowflakeId.generateId());
        eventLog.setStart_time(value.getCommon_recv_time());
        eventLog.setEnd_time(value.getCommon_recv_time());
        eventLog.setDuration_ms(0);
        eventLog.setEvent_severity("info");
        eventLog.setEvent_type("high dns response latency");
        eventLog.setEntity_type(entityType);

        switch (entityType) {
            case "ip":
                initIpEventLog(eventLog, value);
                break;
            case "domain":
                initDomainEventLog(eventLog, value);
                break;
            case "app":
                initAppEventLog(eventLog,value);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private void initIpEventLog(PerformanceEventLog eventLog, CnRecordLog value) {
        eventLog.setEvent_key(value.getCommon_server_ip());
        eventLog.setClient_ip(value.getCommon_client_ip());
        eventLog.setClient_location_country(value.getClient_country());
        eventLog.setClient_location_province(value.getClient_province());
        eventLog.setClient_location_region(value.getClient_region());
        eventLog.setClient_asn(value.getClient_country());
        eventLog.setServer_ip(value.getCommon_server_ip());
        eventLog.setServer_location_country(value.getServer_country());
        eventLog.setServer_location_province(value.getServer_province());
        eventLog.setServer_location_region(value.getServer_region());
        eventLog.setServer_asn(value.getServer_country());
    }

    private void initDomainEventLog(PerformanceEventLog eventLog, CnRecordLog value) {
        eventLog.setEvent_key(value.getDomain());
        eventLog.setDomain(value.getDomain());
        eventLog.setDomain_category_name(value.getDomain_category_name());
        eventLog.setDomain_category_group(value.getDomain_category_group());
        eventLog.setDomain_reputation_score(String.valueOf(value.getDomain_reputation_score()));
        eventLog.setDomain_reputation_level(value.getDomain_reputation_level());
    }

    private void initAppEventLog(PerformanceEventLog eventLog, CnRecordLog value){
        eventLog.setEvent_key(value.getCommon_app_label());
        eventLog.setApp_name(value.getCommon_app_label());
        eventLog.setApp_category(value.getApp_category());
        eventLog.setApp_subcategory(value.getApp_subcategory());
        eventLog.setApp_risk(String.valueOf(value.getApp_risk()));
    }

    private void updateEventLog(PerformanceEventLog eventLog, CnRecordLog value) {
        long commonRecvTime = value.getCommon_recv_time();
        long startTime = eventLog.getStart_time();
        long endTime = eventLog.getEnd_time();
        long maxTime = Math.max(commonRecvTime, Math.max(startTime, endTime));
        long minTime = Math.min(commonRecvTime, Math.min(startTime, endTime));
        eventLog.setStart_time(minTime);
        eventLog.setEnd_time(maxTime);
        eventLog.setDuration_ms((maxTime - minTime) * 1000);
    }

    private PerformanceEventLog detectionEventLog(String key, OnTimerContext ctx) throws Exception {
        try {
            if (resultDesc.contains(key)) {
                Integer totalNum = ctx.getBroadcastState(broadcastTotalCntState).get(null);
                Tuple2<PerformanceEventLog, Double> eventLogIntegerTuple = resultDesc.get(key);
                PerformanceEventLog eventLog = eventLogIntegerTuple.f0;
                Double num = eventLogIntegerTuple.f1;
                double rate = BigDecimal.valueOf(num / totalNum).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                if (rate >= CommonConfig.DNS_DETECTION_INFO_THRESHOLD && rate < CommonConfig.DNS_DETECTION_LOW_THRESHOLD) {
                    eventLog.setEvent_severity("info");
                } else if (rate >= CommonConfig.DNS_DETECTION_LOW_THRESHOLD && rate < CommonConfig.DNS_DETECTION_MEDIUM_THRESHOLD) {
                    eventLog.setEvent_severity("low");
                } else if (rate >= CommonConfig.DNS_DETECTION_MEDIUM_THRESHOLD && rate < CommonConfig.DNS_DETECTION_HIGH_THRESHOLD) {
                    eventLog.setEvent_severity("medium");
                } else if (rate >= CommonConfig.DNS_DETECTION_HIGH_THRESHOLD && rate < CommonConfig.DNS_DETECTION_CRITICAL_THRESHOLD) {
                    eventLog.setEvent_severity("high");
                } else if (rate >= CommonConfig.DNS_DETECTION_CRITICAL_THRESHOLD) {
                    eventLog.setEvent_severity("critical");
                }
                return eventLog;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
