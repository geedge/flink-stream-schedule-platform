package com.zdjizhi.base.source;

import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Source {
    private static StreamExecutionEnvironment streamExeEnv = FlinkEnvironmentUtils.streamExeEnv;
    private static Map<String, DataStreamSource<String>> sourceHashMap = new ConcurrentHashMap<>();

    public static DataStreamSource<String> getSingleSource(SourceConfig sourceConfig) {
        return createSingleSource(sourceConfig);
    }

    private synchronized static DataStreamSource<String> createSingleSource(SourceConfig sourceConfig) {
        if (checkConfigNotNull(sourceConfig)) {
            if (sourceHashMap.containsKey(sourceConfig.getTopic())) {
                return sourceHashMap.get(sourceConfig.getTopic());
            }else {
                DataStreamSource<String> streamSource = createSource(sourceConfig);
                sourceHashMap.put(sourceConfig.getTopic(),streamSource);
                return streamSource;
            }
        } else {
            throw new IllegalArgumentException("SourceConfig is empty");
        }
    }

    private static DataStreamSource<String> createSource(SourceConfig sourceConfig) {
        return streamExeEnv.addSource(KafkaUtils.createSource(sourceConfig.getTopic(), sourceConfig.getProperties()))
                .setParallelism(sourceConfig.getParallelism());
    }

    private static boolean checkConfigNotNull(SourceConfig sourceConfig) {
        return sourceConfig != null && sourceConfig.getTopic() != null && !sourceConfig.getProperties().isEmpty();
    }
}
