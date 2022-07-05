package com.zdjizhi.top.operator;

import com.zdjizhi.top.common.TopBaseLog;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.TreeSet;

public class TopMetricProcessV2 extends KeyedProcessFunction<Long, TopBaseLog, TopBaseLog> {

    private String groupBy;
    private String orderBy;

    private ValueState<Long> currentTimer;
    private ListState<TopBaseLog> itemState;

    public TopMetricProcessV2(String groupBy, String orderBy) {
        this.groupBy = groupBy;
        this.orderBy = orderBy;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>(groupBy+"_"+orderBy+"_timer", Types.LONG));
        ListStateDescriptor<TopBaseLog> itemViewStateDesc = new ListStateDescriptor<TopBaseLog>(
                groupBy+"_"+orderBy+"_state", TopBaseLog.class
        );
        itemState = getRuntimeContext().getListState(itemViewStateDesc);
    }

    @Override
    public void processElement(TopBaseLog value, Context ctx, Collector<TopBaseLog> out) throws Exception {
        Long curTimeStamp = currentTimer.value();
        if (curTimeStamp == null || curTimeStamp == 0) {
            long onTimer = ctx.timestamp() + com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME * 1000;
            ctx.timerService().registerEventTimeTimer(onTimer);
            currentTimer.update(onTimer);
        }
        value.setGroup_by(groupBy);
        value.setOrder_by(orderBy);
        itemState.add(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TopBaseLog> out) throws Exception {
        TreeSet<TopBaseLog> baseLogs = new TreeSet<>(new Comparator<TopBaseLog>() {
            @Override
            public int compare(TopBaseLog o1, TopBaseLog o2) {
                return (Double.compare(o1.get(orderBy).doubleValue(), o2.get(orderBy).doubleValue()));
            }
        });

        for (TopBaseLog topBaseLog:itemState.get()){
            topBaseLog.setGroup_by(groupBy);
            topBaseLog.setOrder_by(orderBy);
            if (baseLogs.size() > com.zdjizhi.top.common.CommonConfig.TOP_METRICS_ROWNUM_LIMIT){
                TopBaseLog last = baseLogs.last();
                if (Double.compare(topBaseLog.get(orderBy).doubleValue(), last.get(orderBy).doubleValue()) > 0){
                    baseLogs.pollLast();
                    baseLogs.add(topBaseLog);
                }
            }else {
                baseLogs.add(topBaseLog);
            }
        }

        currentTimer.clear();
        itemState.clear();

        for (TopBaseLog topBaseLog:baseLogs){
            out.collect(topBaseLog);
        }

    }

}
