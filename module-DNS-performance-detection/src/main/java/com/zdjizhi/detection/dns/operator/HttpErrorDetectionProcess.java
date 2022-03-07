package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.handler.DetectionEventLogHandler;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpErrorDetectionProcess extends KeyedBroadcastProcessFunction<String, CnRecordLog, Tuple3<Integer,Integer, Double>, PerformanceEventLog> {

    private static Logger LOG = LoggerFactory.getLogger(HttpErrorDetectionProcess.class);

    private ValueState<Long> currentTimer;
    private MapStateDescriptor<Void, Tuple3<Integer,Integer, Double>> broadcastTotalCntState;
    private MapState<String, PerformanceEventLog> previousResultDesc;
    private MapState<String, Tuple2<PerformanceEventLog, Double>> resultDesc;

    private String entityType;

    public HttpErrorDetectionProcess(String entityType) {
        this.entityType = entityType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("http_error_detection_timer", Types.LONG));
        broadcastTotalCntState = new MapStateDescriptor<>("broadcast", Types.VOID, Types.TUPLE(Types.INT,Types.INT, Types.DOUBLE));
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

    }

    @Override
    public void processBroadcastElement(Tuple3<Integer, Integer, Double> value, Context ctx, Collector<PerformanceEventLog> out) throws Exception {

    }

    private PerformanceEventLog httpErrorDetectionEventLog(String currentKey, OnTimerContext ctx) {
        return null;
    }
}
