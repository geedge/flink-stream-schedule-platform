package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.handler.DetectionEventLogHandler;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class HttpErrorDetectionProcess extends KeyedBroadcastProcessFunction<String, CnRecordLog, Tuple2<Long,Long>, PerformanceEventLog> {

    private static Logger LOG = LoggerFactory.getLogger(HttpErrorDetectionProcess.class);

    private ValueState<Long> currentTimer;
    private MapStateDescriptor<Void, Tuple2<Long,Long>> broadcastTotalCntState;
    private MapState<String, PerformanceEventLog> previousResultDesc;
    private MapState<String, Tuple2<PerformanceEventLog, Double>> resultDesc;

    private String entityType;

    public HttpErrorDetectionProcess(String entityType) {
        this.entityType = entityType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("http_error_detection_timer", Types.LONG));
        broadcastTotalCntState = new MapStateDescriptor<>("broadcast", Types.VOID, Types.TUPLE(Types.LONG,Types.LONG));
        previousResultDesc = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("http_error_detection_previous_result",
                        Types.STRING,
                        Types.POJO(PerformanceEventLog.class)));
        resultDesc = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("http_error_detection_result",
                        Types.STRING,
                        Types.TUPLE(Types.POJO(PerformanceEventLog.class), Types.DOUBLE)));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PerformanceEventLog> out) {
        try {
            String currentKey = ctx.getCurrentKey();
            PerformanceEventLog eventLog = httpErrorDetectionEventLog(currentKey, ctx);
            DetectionEventLogHandler.outputResult(currentKey,eventLog,previousResultDesc,out);
            resultDesc.remove(currentKey);
            currentTimer.clear();
        }catch (Exception e){
            LOG.error("http error detection 定时器执行失败\n",e);
        }
    }


    @Override
    public void processElement(CnRecordLog value, ReadOnlyContext ctx, Collector<PerformanceEventLog> out) throws Exception {
        try {
            Long curTimeStamp = currentTimer.value();
            String currentKey = ctx.getCurrentKey();
            if (curTimeStamp == null || curTimeStamp == 0) {
                long onTimer = ctx.timestamp() + CommonConfig.FLINK_WINDOW_MAX_TIME * 1000;
                ctx.timerService().registerEventTimeTimer(onTimer);
                currentTimer.update(onTimer);
            }

            if ("HTTP".equals(value.getCommon_schema_type()) && value.getHttp_status_code() != 200 && value.getHttp_status_code() != 0) {
                if (resultDesc.contains(currentKey)) {
                    Tuple2<PerformanceEventLog, Double> httpEventLogIntegerTuple = resultDesc.get(currentKey);
                    PerformanceEventLog httpEventLog = httpEventLogIntegerTuple.f0;
                    httpEventLogIntegerTuple.f1 += 1;
                    DetectionEventLogHandler.updateEventLog(httpEventLog, value);
                    resultDesc.put(currentKey, httpEventLogIntegerTuple);
                } else {
                    PerformanceEventLog eventLog = new PerformanceEventLog();
                    DetectionEventLogHandler.initDetectionEventLog(eventLog, value, "http error", entityType);
                    resultDesc.put(currentKey, Tuple2.of(eventLog, 1D));
                }
            }
        }catch (Exception e){
            LOG.error("dns error detection 数据解析错误,value:\n{}\n",value,e);
        }
    }

    @Override
    public void processBroadcastElement(Tuple2<Long, Long> value, Context ctx, Collector<PerformanceEventLog> out) throws Exception {
        BroadcastState<Void, Tuple2<Long,Long>> broadcastState = ctx.getBroadcastState(broadcastTotalCntState);
        broadcastState.put(null, value);
    }

    private PerformanceEventLog httpErrorDetectionEventLog(String currentKey, OnTimerContext ctx) {
        try {
            if (StringUtil.isNotEmpty(currentKey) && resultDesc.contains(currentKey)) {
                Tuple2<PerformanceEventLog, Double> eventLogIntegerTuple = resultDesc.get(currentKey);
                PerformanceEventLog eventLog = eventLogIntegerTuple.f0;
                Double num = eventLogIntegerTuple.f1;
                Long totalNum = ctx.getBroadcastState(broadcastTotalCntState).get(null).f1;
                if (totalNum != null && totalNum > 0) {
                    double rate = BigDecimal.valueOf(num / totalNum).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                    LOG.info("http error detection key:{} num:{},totalNum:{},rate:{}",currentKey,num,totalNum,rate);
                    if (rate >= CommonConfig.HTTP_ERROR_DETECTION_INFO_THRESHOLD && rate < CommonConfig.HTTP_ERROR_DETECTION_LOW_THRESHOLD) {
                        eventLog.setEvent_severity("info");
                    } else if (rate >= CommonConfig.HTTP_ERROR_DETECTION_LOW_THRESHOLD && rate < CommonConfig.HTTP_ERROR_DETECTION_MEDIUM_THRESHOLD) {
                        eventLog.setEvent_severity("low");
                    } else if (rate >= CommonConfig.HTTP_ERROR_DETECTION_MEDIUM_THRESHOLD && rate < CommonConfig.HTTP_ERROR_DETECTION_HIGH_THRESHOLD) {
                        eventLog.setEvent_severity("medium");
                    } else if (rate >= CommonConfig.HTTP_ERROR_DETECTION_HIGH_THRESHOLD && rate < CommonConfig.HTTP_ERROR_DETECTION_CRITICAL_THRESHOLD) {
                        eventLog.setEvent_severity("high");
                    } else if (rate >= CommonConfig.HTTP_ERROR_DETECTION_CRITICAL_THRESHOLD) {
                        eventLog.setEvent_severity("critical");
                    }else {
                        return null;
                    }
                    return eventLog;
                }
            }
        } catch (Exception e) {
            LOG.error("http dns error检测异常,key:{}",e,currentKey);
        }
        return null;
    }
}
