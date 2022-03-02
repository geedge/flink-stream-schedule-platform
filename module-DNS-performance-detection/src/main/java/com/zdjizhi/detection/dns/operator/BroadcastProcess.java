package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.detection.dns.common.CommonConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class BroadcastProcess extends ProcessFunction<CnRecordLog, Tuple2<Integer, Double>> implements CheckpointedFunction {

    private ValueState<Long> currentTimer;
    private AggregatingState<CnRecordLog, Tuple2<Integer, Double>> aggregatingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("broadcast_timer", Types.LONG));
        aggregatingState = getRuntimeContext().getAggregatingState(
                new AggregatingStateDescriptor<CnRecordLog, Tuple2<Integer, List<Long>>, Tuple2<Integer, Double>>("broadcast_aggregating_state",
                        new AggregateFunction<CnRecordLog, Tuple2<Integer, List<Long>>,
                                Tuple2<Integer, Double>>() {
                            @Override
                            public Tuple2<Integer, List<Long>> createAccumulator() {
                                return Tuple2.of(0, new ArrayList<>());
                            }

                            @Override
                            public Tuple2<Integer, List<Long>> add(CnRecordLog value, Tuple2<Integer, List<Long>> accumulator) {
                                accumulator.f0++;
                                if ("DNS".equals(value.getCommon_schema_type())) {
                                    accumulator.f1.add(value.getDns_response_latency_ms());
                                }
                                return accumulator;
                            }

                            @Override
                            public Tuple2<Integer, Double> getResult(Tuple2<Integer, List<Long>> accumulator) {
                                List<Long> cnRecordLogs = accumulator.f1;
                                double median = 0d;
                                cnRecordLogs.sort(Comparator.comparingInt(Long::intValue));
                                int size = cnRecordLogs.size();
                                if (size > 0) {
                                    if (size % 2 == 0) {
                                        median = (cnRecordLogs.get(size / 2 - 1) + cnRecordLogs.get(size / 2) + 0.0) / 2;
                                    } else {
                                        median = cnRecordLogs.get((size - 1) / 2).doubleValue();
                                    }
                                }
                                return Tuple2.of(accumulator.f0, median);
                            }

                            @Override
                            public Tuple2<Integer, List<Long>> merge(Tuple2<Integer, List<Long>> a, Tuple2<Integer, List<Long>> b) {
                                a.f0 += b.f0;
                                a.f1.addAll(b.f1);
                                return a;
                            }
                        }, Types.TUPLE(Types.INT, Types.LIST(Types.LONG))));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, Double>> out) throws Exception {
        out.collect(aggregatingState.get());
        aggregatingState.clear();
    }

    @Override
    public void processElement(CnRecordLog value, Context ctx, Collector<Tuple2<Integer, Double>> out) throws Exception {
        Long curTimeStamp = currentTimer.value();
        if (curTimeStamp == null || curTimeStamp == 0) {
            long onTimer = ctx.timestamp() + CommonConfig.FLINK_WINDOW_MAX_TIME * 1000;
            ctx.timerService().registerEventTimeTimer(onTimer);
            currentTimer.update(onTimer);
        }
        aggregatingState.add(value);
    }

    public void process(Context context, Iterable<CnRecordLog> elements, Collector<Tuple2<Integer, Double>> out) throws Exception {
        int cnt = 0;
        double median = 0d;
        ArrayList<Long> dnsResponseList = new ArrayList<>();
        for (CnRecordLog ignored : elements) {
            cnt++;
            if ("DNS".equals(ignored.getCommon_schema_type())) {
                long dnsResponseLatencyMs = ignored.getDns_response_latency_ms();
                dnsResponseList.add(dnsResponseLatencyMs);
            }
        }
        dnsResponseList.sort(Comparator.comparingInt(Long::intValue));
        int size = dnsResponseList.size();
        if (size > 0) {
            if (size % 2 == 0) {
                median = (dnsResponseList.get(size / 2 - 1) + dnsResponseList.get(size / 2) + 0.0) / 2;
            } else {
                median = dnsResponseList.get((size - 1) / 2).doubleValue();
            }
        }
        out.collect(Tuple2.of(cnt, median));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
