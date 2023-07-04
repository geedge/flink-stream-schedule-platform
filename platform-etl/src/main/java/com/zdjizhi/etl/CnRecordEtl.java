package com.zdjizhi.etl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.broadcast.BroadcastProcessFunc;
import com.zdjizhi.utils.StringUtil;
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

    private static final String KNOWLEDGE_EXPR;

    static {
        StringBuilder sb = new StringBuilder("");
        sb.append("$.[?(@.version=='latest' && @.type in ['");
        sb.append(CommonInternalConfig.IPV4_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.IPV6_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.ASNV4_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.ASNV6_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.WEBSKT_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.DNS_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.ICP_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.LINK_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.IDC_RENTER_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.INTERNAL_IP_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.APPSKT_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.WHOIS_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.IP_TAG_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.DOMAIN_TAG_TYPE);
        sb.append("','");
        sb.append(CommonInternalConfig.APP_TAG_TYPE);
        sb.append("'])].['name','sha256','format','path','type','isValid']");
        KNOWLEDGE_EXPR = sb.toString();

        if ("SESSION-RECORD".equals(CommonConfig.SESSION_RECORD_COMPLETED_TOPIC)){
                singleOutputStreamOperator = getCnRecordSource();
        }else {
            singleOutputStreamOperator = getCnRecordSourceWithoutEtl();
        }
    }

    private static SingleOutputStreamOperator<CnRecordLog> getCnRecordSource() {
        return createCnRecordSource().assignTimestampsAndWatermarks(
                FlinkEnvironmentUtils.createWatermarkStrategy(
                        Duration.ofSeconds(CommonConfig.WATERMARK_SECONDS),
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
                                Duration.ofSeconds(CommonConfig.WATERMARK_SECONDS),
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
        Properties kafkaProperty = KafkaUtils.getDefaultKafkaSourceProperty();
        kafkaProperty.setProperty("group.id", CommonConfig.SESSION_RECORD_COMPLETED_GROUP_ID);
        SourceConfig kafkaSourceConfig = new SourceConfig(
                CommonConfig.SESSION_RECORD_COMPLETED_TOPIC,
                CommonConfig.SESSION_RECORD_COMPLETED_PARALLELISM,
                kafkaProperty);

        BroadcastStream<Map<String, String>> broadcastStream = Source.getKnowledgeBroadcastStream(KNOWLEDGE_EXPR);

        BroadcastConnectedStream<String, Map<String, String>> connect = Source.getSingleSource(kafkaSourceConfig).connect(broadcastStream);

        return connect.process(new BroadcastProcessFunc());

    }

}

