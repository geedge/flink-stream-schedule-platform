package com.zdjizhi.base.source;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class Source {
    private static StreamExecutionEnvironment streamExeEnv = FlinkEnvironmentUtils.streamExeEnv;

    public static DataStreamSource<String> createSource(String topic){

        return createSource(topic,CommonConfig.KAFKA_INPUT_PARALLELISM);
    }

    public static DataStreamSource<String> createSource(String topic,int parallelism){

        return streamExeEnv.addSource(KafkaUtils.createSource(topic))
                .setParallelism(parallelism);
    }

    public static DataStreamSource<String> createSource(String topic, int parallelism, Properties properties){

        return streamExeEnv.addSource(KafkaUtils.createSource(topic,properties))
                .setParallelism(parallelism);
    }

}
