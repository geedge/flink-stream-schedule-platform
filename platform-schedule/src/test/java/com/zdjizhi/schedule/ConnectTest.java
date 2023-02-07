package com.zdjizhi.schedule;

import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class ConnectTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExeEnv = FlinkEnvironmentUtils.streamExeEnv;
        Properties property = KafkaUtils.getDefaultKafkaSourceProperty();
        property.setProperty("group.id", "abcd");
        SingleOutputStreamOperator<MetricResultLog> source = streamExeEnv.addSource(new FlinkKafkaConsumer<String>(
                "METRIC-IP",
                new SimpleStringSchema(),
                property)).map(new MyMapFunction())/*.assignTimestampsAndWatermarks(
                FlinkEnvironmentUtils.createWatermarkStrategy(
                        Duration.ofSeconds(1),
                        (event, timestamp) -> event.getStat_time() * 1000
                )
        )*/.setParallelism(5);

        DataStreamSource<Tuple2<String, String>> tupleSource = streamExeEnv.addSource(new SourceFunction<Tuple2<String, String>>() {
            private boolean isRunning = true;

            @Override
            public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                String[] strings = new String[]{"116.178.66.251", "116.178.71.4", "116.178.71.6", "116.178.66.250", "116.178.76.16", "116.178.76.24", "116.178.76.52", "116.178.73.197"};
                /*while (isRunning) {
                    String string = strings[new Random().nextInt(strings.length - 1)];
                    Tuple2<String, String> tuple2 = Tuple2.of(string, string + "--123");
                    ctx.collect(tuple2);
                    Thread.sleep(10000);
                }*/

                for (String s : strings) {
//                    String string = strings[new Random().nextInt(strings.length - 1)];
                    Tuple2<String, String> tuple2 = Tuple2.of(s, s + "--123");
                    ctx.collect(tuple2);
//                    Thread.sleep(10000);
                }

            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        source.keyBy(MetricResultLog::getIp)
                .connect(tupleSource.keyBy((KeySelector<Tuple2<String, String>, String>) value -> value.f0))
                .process(new KeyedCoProcessFunction<String, MetricResultLog, Tuple2<String, String>, Tuple2<String, MetricResultLog>>() {

                    private MapState<String, String> mapState;
                    private ValueState<Long> currentTimer;
                    private ValueState<Tuple2<String, MetricResultLog>> ressult;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));
                        ressult = getRuntimeContext().getState(new ValueStateDescriptor<>("result", Types.TUPLE(Types.STRING, Types.POJO(MetricResultLog.class))));
                        mapState = getRuntimeContext()
                                .getMapState(new MapStateDescriptor<>("state",
                                        Types.STRING,
                                        Types.STRING));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, MetricResultLog>> out) throws Exception {

                        Tuple2<String, MetricResultLog> value = ressult.value();
                        if (value != null) {
                            out.collect(value);
                            ressult.clear();
                        }
                        currentTimer.clear();
                    }

                    @Override
                    public void processElement1(MetricResultLog value, Context ctx, Collector<Tuple2<String, MetricResultLog>> out) throws Exception {

                        Long curTimeStamp = currentTimer.value();
                        if (curTimeStamp == null || curTimeStamp == 0) {
                            long onTimer = ctx.timestamp() + 60 * 1000;
                            ctx.timerService().registerProcessingTimeTimer(onTimer);
                            currentTimer.update(onTimer);
                        }

                        String ip = value.getIp();

                        if (ressult.value() == null && mapState.contains(ip)) {
                            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                            System.out.println("processElement1 " + value.getIp() + " " + indexOfThisSubtask);
                            String s = mapState.get(ip);
                            ressult.update(Tuple2.of(s, value));
//                    out.collect(Tuple2.of(s,value));
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, MetricResultLog>> out) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println("processElement2 " + value.f0 + " " + indexOfThisSubtask);
                        mapState.put(value.f0, value.f1);
                    }
                }).setParallelism(4).print();


        /*
        source.keyBy(MetricResultLog::getIp)
//                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .connect(tupleSource)
                .process(new KeyedCoProcessFunction<String, MetricResultLog, Tuple2<String, String>, Tuple2<String,MetricResultLog>>() {
                    private MapState<String,String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext()
                                .getMapState(new MapStateDescriptor<>("state",
                                        Types.STRING,
                                        Types.STRING));

                    }

                    @Override
                    public void processElement1(MetricResultLog value, Context ctx, Collector<Tuple2<String, MetricResultLog>> out) throws Exception {
                        String ip = value.getIp();

                        if (mapState.contains(ip)){
                            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                            System.out.println("processElement1 "+value.getIp()+" "+indexOfThisSubtask);
                            String s = mapState.get(ip);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, MetricResultLog>> out) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println("processElement2 "+value.f0+" "+indexOfThisSubtask);
                        mapState.put(value.f0,value.f1);
                    }
                }).setParallelism(4).print();
                */

        streamExeEnv.execute();

    }


}

class MyMapFunction implements MapFunction<String, MetricResultLog> {
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    @Override
    public MetricResultLog map(String value) throws Exception {
        return jsonMapperInstance.fromJson(value, MetricResultLog.class);
    }
}
