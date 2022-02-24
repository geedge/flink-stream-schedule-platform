package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.detection.dns.common.CommonConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastTotalNumProcess extends ProcessFunction<CnRecordLog, Integer> {

    private ValueState<Long> currentTimer;
    private ValueState<Integer> cntState;

    @Override
    public void open(Configuration parameters) throws Exception {
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));
        cntState = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Types.INT));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
        out.collect(cntState.value());
        currentTimer.clear();
        cntState.clear();
    }

    @Override
    public void processElement(CnRecordLog value, Context ctx, Collector<Integer> out) throws Exception {
        Long curTimestamp = currentTimer.value();
        if (curTimestamp == 0) {
            long onTimer = ctx.timestamp() + CommonConfig.FLINK_WINDOW_MAX_TIME;
            ctx.timerService().registerEventTimeTimer(onTimer);
            currentTimer.update(onTimer);
            cntState.update(1);
        } else {
            cntState.update(cntState.value() + 1);
        }
    }

}
