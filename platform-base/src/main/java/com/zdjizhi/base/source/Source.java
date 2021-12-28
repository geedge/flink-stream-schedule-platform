package com.zdjizhi.base.source;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Source {
    private static StreamExecutionEnvironment streamExeEnv = FlinkEnvironmentUtils.streamExeEnv;

    public static DataStreamSource<String> createSource(){

        return streamExeEnv.addSource(KafkaUtils.createSource(CommonConfig.KAFKA_INPUT_TOPIC_NAME))
                .setParallelism(CommonConfig.KAFKA_INPUT_PARALLELISM);
    }

}
