package com.zdjizhi.base.source;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CustomFile;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.connectors.http.HttpSource;
import com.zdjizhi.base.connectors.http.SingleHttpSource;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
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

    public static DataStreamSource<List<CustomFile>> broadcastSource(Properties nacosProperties,String STORE_PATH){
        return streamExeEnv.addSource(new HttpSource(nacosProperties, CommonConfig.NACOS_DATA_ID, CommonConfig.NACOS_GROUP, CommonConfig.NACOS_READ_TIMEOUT,STORE_PATH));
    }

    public static DataStreamSource<List<CustomFile>> singleBroadcastSource(Properties nacosProperties){
        return streamExeEnv.addSource(new SingleHttpSource(nacosProperties, CommonConfig.NACOS_DATA_ID, CommonConfig.NACOS_GROUP, CommonConfig.NACOS_READ_TIMEOUT));
    }

}
