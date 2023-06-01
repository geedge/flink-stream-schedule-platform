package com.zdjizhi.base.source;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.connectors.http.HttpSource;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Source {
    private static StreamExecutionEnvironment streamExeEnv = FlinkEnvironmentUtils.streamExeEnv;
    private static Map<String, DataStreamSource<String>> sourceHashMap = new ConcurrentHashMap<>();

    public static DataStreamSource<String> getSingleSource(SourceConfig sourceConfig) {
        return createSingleSource(sourceConfig);
    }

    private static DataStreamSource<String> createSource(SourceConfig sourceConfig) {
        return streamExeEnv.addSource(KafkaUtils.createSource(sourceConfig.getTopic(), sourceConfig.getProperties()))
                .setParallelism(sourceConfig.getParallelism());
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

    private static boolean checkConfigNotNull(SourceConfig sourceConfig) {
        return sourceConfig != null && sourceConfig.getTopic() != null && !sourceConfig.getProperties().isEmpty();
    }

    public static DataStreamSource<Map<String, String>> getKnowledgeSource(String knowledgeExpression) {
        return streamExeEnv.addSource(new HttpSource(knowledgeExpression));
    }

    public static BroadcastStream<Map<String, String>> getKnowledgeBroadcastStream(String knowledgeExpression) {
        DataStreamSource<Map<String, String>> knowledgeSource = getKnowledgeSource(knowledgeExpression);
        MapStateDescriptor<String,Map> descriptor =
                new MapStateDescriptor<>("descriptorTest", Types.STRING, TypeInformation.of(Map.class));
        BroadcastStream<Map<String, String>> broadcastStream = knowledgeSource.broadcast(descriptor);
        return broadcastStream;
    }



}
