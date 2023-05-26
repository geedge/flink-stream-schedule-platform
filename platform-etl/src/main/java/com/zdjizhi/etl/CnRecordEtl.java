package com.zdjizhi.etl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.broadcast.BroadcastProcessFunc;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class CnRecordEtl implements Schedule {
    private static final Logger LOG = LoggerFactory.getLogger(CnRecordEtl.class);
    public static SingleOutputStreamOperator<CnRecordLog> singleOutputStreamOperator;

    static {
        if ("SESSION-RECORD".equals(CommonConfig.SESSION_RECORD_COMPLETED_TOPIC)){
                singleOutputStreamOperator = getCnRecordSource();
        }else {
            singleOutputStreamOperator = getCnRecordSourceWithoutEtl();
        }
    }

    //todo 集群模式
    private static SingleOutputStreamOperator<CnRecordLog> getCnRecordSource() {
        return createCnRecordSource().assignTimestampsAndWatermarks(
                FlinkEnvironmentUtils.createWatermarkStrategy(
                        Duration.ofSeconds(1),
                        (event, timestamp) -> event.getCommon_recv_time() * 1000
                )
        );
    }

    private static SingleOutputStreamOperator<CnRecordLog> getCnRecordSourceWithoutEtl() {
        Properties property = KafkaUtils.getDefaultKafkaSourceProperty();
        property.setProperty("group.id", CommonConfig.SESSION_RECORD_COMPLETED_GROUP_ID);
        SourceConfig sourceConfig = new SourceConfig(
                CommonConfig.SESSION_RECORD_COMPLETED_TOPIC,
                CommonConfig.SESSION_RECORD_COMPLETED_PARALLELISM,
                property);
        return Source.getSingleSource(sourceConfig).map(source -> JSON.parseObject(source, CnRecordLog.class))
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(1),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );
    }

    @Override
    public void schedule() throws Exception {
        singleOutputStreamOperator
                .filter(Objects::nonNull)
                .map(JSON::toJSONString)
                .addSink(KafkaUtils.getKafkaSink(CommonConfig.CN_RECORD_TOPIC))
                .setParallelism(CommonConfig.CN_RECORD_PARALLELISM);
    }

    private static SingleOutputStreamOperator<CnRecordLog> createCnRecordSource() {
        DataStreamSource<Map<String, byte[]>> broadcastSource=null;
        Properties property = KafkaUtils.getDefaultKafkaSourceProperty();
        property.setProperty("group.id", CommonConfig.SESSION_RECORD_COMPLETED_GROUP_ID);
        SourceConfig sourceConfig = new SourceConfig(
                CommonConfig.SESSION_RECORD_COMPLETED_TOPIC,
                CommonConfig.SESSION_RECORD_COMPLETED_PARALLELISM,
                property);

        Properties nacosProperties = new Properties();
        nacosProperties.put(PropertyKeyConst.SERVER_ADDR,CommonConfig.NACOS_SERVER_ADDR);
        nacosProperties.setProperty(PropertyKeyConst.USERNAME, CommonConfig.NACOS_USERNAME);
        nacosProperties.setProperty(PropertyKeyConst.PASSWORD, CommonConfig.NACOS_PASSWORD);
        //nacosProperties.setProperty(PropertyKeyConst.NAMESPACE, CommonConfig.NACOS_NAMESPACE);

        if ("CLUSTER".equals(CommonConfig.CLUSTER_OR_SINGLE)){
             broadcastSource = Source.broadcastSource(nacosProperties,CommonConfig.HDFS_PATH);
        }else {
             broadcastSource= Source.singleBroadcastSource(nacosProperties);
        }

        MapStateDescriptor<String,Map> descriptor =
                new MapStateDescriptor<>("descriptorTest", Types.STRING, TypeInformation.of(Map.class));

        BroadcastStream<Map<String, byte[]>> broadcast = broadcastSource.broadcast(descriptor);
        BroadcastConnectedStream<String, Map<String, byte[]>> connect = Source.getSingleSource(sourceConfig).connect(broadcast);

        return connect.process(new BroadcastProcessFunc());

    }

}

