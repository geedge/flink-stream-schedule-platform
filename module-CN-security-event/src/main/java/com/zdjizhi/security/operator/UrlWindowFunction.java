package com.zdjizhi.security.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.security.common.IocJsonLog;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.security.handler.SecurityHandler;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/4
 * @ Description
 */
public class UrlWindowFunction extends KeyedCoProcessFunction<String,CnRecordLog, Tuple2<String, IocJsonLog>, SecurityEventLog> {
    private static Logger LOG = LoggerFactory.getLogger(ServerIpWindowFunction.class);

    //上一窗口的状态
    private MapState<String, SecurityEventLog> previousMapState;
    private MapState<String,IocJsonLog> iocMapState;
    private ValueState<Long> currentTimer;
    private ValueState<SecurityEventLog> currentResult;

    @Override
    public void open(Configuration parameters) throws Exception {
        previousMapState = getRuntimeContext()
                .getMapState(new MapStateDescriptor<String, SecurityEventLog>("UrlResult"
                        , Types.STRING
                        , Types.POJO(SecurityEventLog.class)));
        iocMapState = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("urlState",
                        Types.STRING,
                        Types.POJO(IocJsonLog.class)));
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("urlTimer", Types.LONG));
        currentResult = getRuntimeContext().getState(new ValueStateDescriptor<>("urlResult", Types.POJO(SecurityEventLog.class)));

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SecurityEventLog> out) throws Exception {
        SecurityEventLog result = currentResult.value();
        if (result != null){
            out.collect(result);
            previousMapState.put(ctx.getCurrentKey(), result);
            currentResult.clear();
        }else {
            previousMapState.clear();
        }
        currentTimer.clear();
    }

    @Override
    public void processElement1(CnRecordLog value, Context ctx, Collector<SecurityEventLog> out) throws Exception {
        try {
            Long curTimeStamp = currentTimer.value();
            if (curTimeStamp == null || curTimeStamp == 0) {
                long onTimer = ctx.timestamp() + com.zdjizhi.security.common.CommonConfig.FLINK_WINDOW_TIME * 60 * 1000;
                ctx.timerService().registerProcessingTimeTimer(onTimer);
                currentTimer.update(onTimer);
            }

            if (currentResult.value() == null){
                String key = ctx.getCurrentKey();
                if (iocMapState.contains(key)) {
                    if (previousMapState.contains(key)) {
                        SecurityEventLog securityEventLog = previousMapState.get(key);
                        SecurityEventLog eventLog = SecurityHandler.updateEventLog(securityEventLog, value);
                        currentResult.update(eventLog);
                    } else {
                        SecurityEventLog securityEventLog = new SecurityEventLog();
                        SecurityEventLog eventLog = initEventLog(securityEventLog, value);
                        currentResult.update(eventLog);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("处理Url:{} security失败\n",ctx.getCurrentKey(),e);
        }
    }

    @Override
    public void processElement2(Tuple2<String, IocJsonLog> value, Context ctx, Collector<SecurityEventLog> out) throws Exception {
        iocMapState.put(value.f0,value.f1);
    }

    private SecurityEventLog initEventLog(SecurityEventLog securityEventLog, CnRecordLog cnRecordLog) {
        try {
            String key = cnRecordLog.getHttp_url();
            if (iocMapState.contains(key)) {
                IocJsonLog iocJsonLog = iocMapState.get(key);
                if ("url".equals(iocJsonLog.getIoc_type()) && iocJsonLog.getIoc_value().equals(key)) {
                    SecurityHandler.setIocField(securityEventLog, key, iocJsonLog);
                    SecurityHandler.initMessage(securityEventLog, cnRecordLog);
                }
            }
            return securityEventLog;
        } catch (Exception e) {
            LOG.error(e.toString());
            return null;
        }

    }
}


