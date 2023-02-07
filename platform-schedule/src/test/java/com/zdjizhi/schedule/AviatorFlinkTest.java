package com.zdjizhi.schedule;

import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.pre.common.MetricResultLog;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class AviatorFlinkTest {

    public static void main(String[] args) {
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

//        StreamExecutionEnvironment.createRemoteEnvironment();

        streamExeEnv.addSource(new SourceFunction<Tuple2<String, String>>() {
            private boolean isRunning = true;

            @Override
            public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {


            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


    }

}
