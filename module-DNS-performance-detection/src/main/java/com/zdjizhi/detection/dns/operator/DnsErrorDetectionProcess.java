package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.handler.DetectionEventLogHandler;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * @author wlh
 */
public class DnsErrorDetectionProcess extends KeyedProcessFunction<String, CnRecordLog, PerformanceEventLog> {

    private static Logger LOG = LoggerFactory.getLogger(DnsErrorDetectionProcess.class);

    private ValueState<Long> currentTimer;
    private MapState<String, PerformanceEventLog> previousResultDesc;
    private MapState<String, Tuple3<PerformanceEventLog, Long, Double>> resultDesc;

    private String entityType;

    public DnsErrorDetectionProcess(String entityType) {
        this.entityType = entityType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("dns_error_detection_timer", Types.LONG));
        previousResultDesc = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("dns_error_detection_previous_result",
                        Types.STRING,
                        Types.POJO(PerformanceEventLog.class)));
        resultDesc = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("dns_error_detection_result",
                        Types.STRING,
                        Types.TUPLE(Types.POJO(PerformanceEventLog.class), Types.LONG, Types.DOUBLE)));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PerformanceEventLog> out) {
        try {
            String currentKey = ctx.getCurrentKey();
            PerformanceEventLog eventLog = dnsErrorDetectionEventLog(currentKey, ctx);
            DetectionEventLogHandler.outputResult(currentKey,eventLog,previousResultDesc,out);
            resultDesc.remove(currentKey);
            currentTimer.clear();
        }catch (Exception e){
            LOG.error("dns error detection 定时器执行失败\n",e);
        }
    }

    @Override
    public void processElement(CnRecordLog value, Context ctx, Collector<PerformanceEventLog> out) throws Exception {
        try {
            Long curTimeStamp = currentTimer.value();
            String currentKey = ctx.getCurrentKey();
            if (curTimeStamp == null || curTimeStamp == 0) {
                long onTimer = ctx.timestamp() + CommonConfig.FLINK_WINDOW_MAX_TIME * 1000;
                ctx.timerService().registerEventTimeTimer(onTimer);
                currentTimer.update(onTimer);
            }
            if (resultDesc.contains(currentKey)) {
                Tuple3<PerformanceEventLog,Long, Double> eventLogIntegerTuple = resultDesc.get(currentKey);
                PerformanceEventLog eventLog = eventLogIntegerTuple.f0;
                eventLogIntegerTuple.f1 += 1;
                DetectionEventLogHandler.updateEventLog(eventLog, value);
                resultDesc.put(currentKey, eventLogIntegerTuple);
            } else {
                PerformanceEventLog eventLog = new PerformanceEventLog();
                DetectionEventLogHandler.initDetectionEventLog(eventLog, value, "dns error", entityType);
                resultDesc.put(currentKey, Tuple3.of(eventLog, 1L, 0D));
            }

            if ("DNS".equals(value.getCommon_schema_type()) && value.getDns_rcode() != 0) {
                Tuple3<PerformanceEventLog,Long, Double> eventLogIntegerTuple = resultDesc.get(currentKey);
                eventLogIntegerTuple.f2 += 1;
                resultDesc.put(currentKey, eventLogIntegerTuple);
            }
        }catch (Exception e){
            LOG.error("dns error detection 数据解析错误,value:\n{}\n",value,e);
        }
    }

    private PerformanceEventLog dnsErrorDetectionEventLog(String key, OnTimerContext ctx) {
        try {
            if (StringUtil.isNotEmpty(key) && resultDesc.contains(key)) {
                Tuple3<PerformanceEventLog, Long, Double> eventLogIntegerTuple = resultDesc.get(key);
                PerformanceEventLog eventLog = eventLogIntegerTuple.f0;
                Double num = eventLogIntegerTuple.f2;
                Long totalNum = eventLogIntegerTuple.f1;
                if (totalNum != null && totalNum > 0) {
                    double rate = BigDecimal.valueOf(num / totalNum).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                    LOG.info("dns error detection key:{} num:{},totalNum:{},rate:{}",key,num,totalNum,rate);
                    if (rate >= CommonConfig.DNS_ERROR_DETECTION_INFO_THRESHOLD && rate < CommonConfig.DNS_ERROR_DETECTION_LOW_THRESHOLD) {
                        eventLog.setEvent_severity("info");
                    } else if (rate >= CommonConfig.DNS_ERROR_DETECTION_LOW_THRESHOLD && rate < CommonConfig.DNS_ERROR_DETECTION_MEDIUM_THRESHOLD) {
                        eventLog.setEvent_severity("low");
                    } else if (rate >= CommonConfig.DNS_ERROR_DETECTION_MEDIUM_THRESHOLD && rate < CommonConfig.DNS_ERROR_DETECTION_HIGH_THRESHOLD) {
                        eventLog.setEvent_severity("medium");
                    } else if (rate >= CommonConfig.DNS_ERROR_DETECTION_HIGH_THRESHOLD && rate < CommonConfig.DNS_ERROR_DETECTION_CRITICAL_THRESHOLD) {
                        eventLog.setEvent_severity("high");
                    } else if (rate >= CommonConfig.DNS_ERROR_DETECTION_CRITICAL_THRESHOLD) {
                        eventLog.setEvent_severity("critical");
                    }else {
                        return null;
                    }
                    return eventLog;
                }
            }
        } catch (Exception e) {
            LOG.error("dns error检测异常,key:{}",e,key);
        }
        return null;
    }
}
