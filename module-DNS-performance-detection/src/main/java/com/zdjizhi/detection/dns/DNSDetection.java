package com.zdjizhi.detection.dns;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.etl.CnRecordEtl;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

public class DNSDetection implements Schedule {

    private static Logger LOG = LoggerFactory.getLogger(DNSDetection.class);

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.getCnRecordSource()
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(10),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );
        /*
        SingleOutputStreamOperator<Integer> process = cnRecordSource
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new ProcessAllWindowFunction<CnRecordLog, Integer, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<CnRecordLog> elements, Collector<Integer> out) throws Exception {
                        int cnt = 0;
                        for (CnRecordLog ignored : elements) {
                            cnt++;
                        }
                        out.collect(cnt);
                    }
                });
                */

        SingleOutputStreamOperator<Integer> process = cnRecordSource.process(new ProcessFunction<CnRecordLog, Integer>() {
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
                    long onTimer = ctx.timestamp() + 60;
                    ctx.timerService().registerEventTimeTimer(onTimer);
                    currentTimer.update(onTimer);
                    cntState.update(1);
                } else {
                    cntState.update(cntState.value() + 1);
                }
            }
        });

        MapStateDescriptor<Void, Integer> bcStateDescriptor =
                new MapStateDescriptor<>("patterns", Types.VOID, Types.INT);

        BroadcastStream<Integer> broadcast = process.broadcast(bcStateDescriptor);
        List<Integer> integers = process.executeAndCollect(1);
        Integer cnt = integers.get(0);
        System.out.println(cnt);
        System.out.println(integers.size());

        SingleOutputStreamOperator<Tuple2<PerformanceEventLog, Boolean>> result = cnRecordSource
                .keyBy(CnRecordLog::getCommon_server_ip)
                .connect(broadcast)
//                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new KeyedBroadcastProcessFunction<String, CnRecordLog, Integer, Tuple2<PerformanceEventLog, Boolean>>() {
                    private ValueState<Long> currentTimer;
                    private MapStateDescriptor<Void, Integer> broadcastTotalCntState;
                    private MapState<String, PerformanceEventLog> previousResultDesc;
                    private MapState<String, Tuple2<PerformanceEventLog, Double>> resultDesc;

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
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<PerformanceEventLog, Boolean>> out) throws Exception {
                        String currentKey = ctx.getCurrentKey();
                        PerformanceEventLog eventLog = detectionEventLog(currentKey, ctx);
                        if (eventLog != null) {
                            if (previousResultDesc.contains(currentKey)) {
                                PerformanceEventLog previousEventLog = previousResultDesc.get(currentKey);
                                eventLog.setStart_time(previousEventLog.getStart_time());
                                eventLog.setDuration_ms((eventLog.getEnd_time() - eventLog.getStart_time()) * 1000);
                                out.collect(Tuple2.of(eventLog, true));
                            } else {
                                out.collect(Tuple2.of(eventLog, false));
                            }
                            previousResultDesc.put(currentKey, eventLog);
                        } else {
                            previousResultDesc.remove(currentKey);
                        }
                        resultDesc.remove(currentKey);
                        currentTimer.clear();
                    }

                    @Override
                    public void processElement(CnRecordLog value, ReadOnlyContext ctx, Collector<Tuple2<PerformanceEventLog, Boolean>> out) throws Exception {
                        Long curTimeStamp = currentTimer.value();
                        String currentKey = ctx.getCurrentKey();
                        if (curTimeStamp == 0) {
                            long onTimer = ctx.timestamp() + 60;
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
                    public void processBroadcastElement(Integer value, Context ctx, Collector<Tuple2<PerformanceEventLog, Boolean>> out) throws Exception {
                        BroadcastState<Void, Integer> broadcastState = ctx.getBroadcastState(broadcastTotalCntState);
                        broadcastState.put(null, value);
                    }

                    private void initEventLog(PerformanceEventLog eventLog, CnRecordLog value) {
                        eventLog.setEvent_id(eventLog.getServer_ip().hashCode());
                        eventLog.setEvent_key(eventLog.getServer_ip());
                        eventLog.setStart_time(value.getCommon_recv_time());
                        eventLog.setEnd_time(value.getCommon_recv_time());
                        eventLog.setDuration_ms(0);
                        eventLog.setEvent_severity("info");
                        eventLog.setEvent_type("high dns response latency");
                        eventLog.setEntity_type("ip");
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

                    private void updateEventLog(PerformanceEventLog eventLog, CnRecordLog value) {
                        eventLog.setStart_time(value.getCommon_recv_time() > eventLog.getStart_time() ? eventLog.getStart_time() : value.getCommon_recv_time());
                        eventLog.setEnd_time(value.getCommon_recv_time() > eventLog.getEnd_time() ? value.getCommon_recv_time() : eventLog.getEnd_time());
                        eventLog.setDuration_ms((eventLog.getEnd_time() - eventLog.getStart_time()) * 1000);
                    }

                    private PerformanceEventLog detectionEventLog(String key, OnTimerContext ctx) throws Exception {
                        if (resultDesc.contains(key)) {
                            Integer totalNum = ctx.getBroadcastState(broadcastTotalCntState).get(null);
                            Tuple2<PerformanceEventLog, Double> eventLogIntegerTuple = resultDesc.get(key);
                            PerformanceEventLog eventLog = eventLogIntegerTuple.f0;
                            Double num = eventLogIntegerTuple.f1;
                            double rate = BigDecimal.valueOf(num / totalNum).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
                            if (rate >= 0.1 && rate < 0.2) {
                                eventLog.setEvent_severity("info");
                            } else if (rate >= 0.2 && rate < 0.3) {
                                eventLog.setEvent_severity("low");
                            } else if (rate >= 0.3 && rate < 0.4) {
                                eventLog.setEvent_severity("medium");
                            } else if (rate >= 0.4 && rate < 0.5) {
                                eventLog.setEvent_severity("high");
                            } else if (rate >= 0.5) {
                                eventLog.setEvent_severity("critical");
                            }
                            return eventLog;
                        } else {
                            return null;
                        }
                    }
                });

        String host = "192.168.40.151:9500,192.168.40.152:9500,192.168.40.153:9500";
        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(host);
        ESSinkUtils.addSink(esAddresses, 1000, result, new ElasticsearchSinkFunction<Tuple2<PerformanceEventLog, Boolean>>() {
            @Override
            public void process(Tuple2<PerformanceEventLog, Boolean> element, RuntimeContext ctx, RequestIndexer indexer) {
                if (element.f1){

                }else {

                }
            }
        });

        FlinkEnvironmentUtils.streamExeEnv.execute();
    }

    public static void main(String[] args) throws Exception {
        new DNSDetection().schedule();
    }

    private static class DnsDetectionProcess extends ProcessWindowFunction<CnRecordLog, Tuple2<CnRecordLog, Double>, String, TimeWindow> {

        private int cnt;

        public DnsDetectionProcess(int cnt) {
            this.cnt = cnt;
        }

        @Override
        public void process(String s, Context context, Iterable<CnRecordLog> elements, Collector<Tuple2<CnRecordLog, Double>> out) throws Exception {
            double errorNum = 0;
            CnRecordLog log = null;
            for (CnRecordLog cnRecordLog : elements) {
                int dnsRcode = cnRecordLog.getDns_rcode();
                if (dnsRcode == 0) {
                    log = cnRecordLog;
                    errorNum++;
                }
            }
            if (errorNum > 0) {
                out.collect(Tuple2.of(log, errorNum / cnt));
                LOG.info("key:{},errorNum:{},cnt:{},ratio:{}", s, errorNum, cnt, errorNum / cnt);
            }
        }
    }

}
