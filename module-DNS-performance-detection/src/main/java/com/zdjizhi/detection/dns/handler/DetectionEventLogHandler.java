package com.zdjizhi.detection.dns.handler;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.utils.SnowflakeId;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.etl.utils.IPUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.util.Collector;

public class DetectionEventLogHandler {

    public static void initDetectionEventLog(PerformanceEventLog eventLog, CnRecordLog value,String eventType,String entityType){
        initEventLog(eventLog,value,eventType,entityType);
    }

    private static void initEventLog(PerformanceEventLog eventLog, CnRecordLog value,String eventType,String entityType) {
        eventLog.setEvent_id(SnowflakeId.generateId());
        eventLog.setStart_time(value.getCommon_recv_time());
        eventLog.setEnd_time(value.getCommon_recv_time());
        eventLog.setDuration_ms(0);
        eventLog.setEvent_severity("info");
        eventLog.setEvent_type(eventType);
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


    private static void initIpEventLog(PerformanceEventLog eventLog, CnRecordLog value) {
        eventLog.setEvent_key(value.getCommon_server_ip());
        eventLog.setClient_ip(value.getCommon_client_ip());
        eventLog.setClient_location_country(value.getClient_country());
        eventLog.setClient_location_province(value.getClient_province());
        eventLog.setClient_location_region(value.getClient_region());
        eventLog.setClient_asn(IPUtils.ipLookup.asnLookup(value.getCommon_client_ip()));
        eventLog.setServer_ip(value.getCommon_server_ip());
        eventLog.setServer_location_country(value.getServer_country());
        eventLog.setServer_location_province(value.getServer_province());
        eventLog.setServer_location_region(value.getServer_region());
        eventLog.setServer_asn(IPUtils.ipLookup.asnLookup(value.getCommon_server_ip()));
    }

    private static void initDomainEventLog(PerformanceEventLog eventLog, CnRecordLog value) {
        eventLog.setEvent_key(value.getDomain());
        eventLog.setDomain(value.getDomain());
        eventLog.setDomain_category_name(value.getDomain_category_name());
        eventLog.setDomain_category_group(value.getDomain_category_group());
        eventLog.setDomain_reputation_score(String.valueOf(value.getDomain_reputation_score()));
        eventLog.setDomain_reputation_level(value.getDomain_reputation_level());
    }

    private static void initAppEventLog(PerformanceEventLog eventLog, CnRecordLog value){
        eventLog.setEvent_key(value.getCommon_app_label());
        eventLog.setApp_name(value.getCommon_app_label());
        eventLog.setApp_category(value.getApp_category());
        eventLog.setApp_subcategory(value.getApp_subcategory());
        eventLog.setApp_risk(String.valueOf(value.getApp_risk()));
    }

    public static void updateEventLog(PerformanceEventLog eventLog, CnRecordLog value) {
        long commonRecvTime = value.getCommon_recv_time();
        long startTime = eventLog.getStart_time();
        long endTime = eventLog.getEnd_time();
        long maxTime = Math.max(commonRecvTime, Math.max(startTime, endTime));
        long minTime = Math.min(commonRecvTime, Math.min(startTime, endTime));
        eventLog.setStart_time(minTime);
        eventLog.setEnd_time(maxTime);
        eventLog.setDuration_ms((maxTime - minTime) * 1000);
    }

    public static void outputResult(String currentKey, PerformanceEventLog eventLog, MapState<String, PerformanceEventLog> previousResultDesc, Collector<PerformanceEventLog> out) throws Exception {
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
    }

}
