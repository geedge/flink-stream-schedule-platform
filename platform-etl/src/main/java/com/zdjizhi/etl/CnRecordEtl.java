package com.zdjizhi.etl;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.operator.EtlProcessFunc;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

public class CnRecordEtl implements Schedule {

    private static final Logger LOG = LoggerFactory.getLogger(CnRecordEtl.class);
    public static SingleOutputStreamOperator<CnRecordLog> singleOutputStreamOperator;

    static {
        if (CommonInternalConfig.ETL_TOPIC.equals(CommonConfig.SESSION_RECORD_COMPLETED_TOPIC)){
                singleOutputStreamOperator = getCnRecordSource();
        }else {
            singleOutputStreamOperator = getCnRecordSourceWithoutEtl();
        }
    }

    private static SingleOutputStreamOperator<CnRecordLog> getCnRecordSource() {
        return getKafkaSource().process(new EtlProcessFunc()).name("etlProcess")
                .assignTimestampsAndWatermarks(
                FlinkEnvironmentUtils.createWatermarkStrategy(
                        Duration.ofSeconds(CommonConfig.WATERMARK_SECONDS),
                        (event, timestamp) -> event.getCommon_recv_time() * 1000
                )
        );
    }

    private static SingleOutputStreamOperator<CnRecordLog> getCnRecordSourceWithoutEtl() {
        return getKafkaSource().map(source -> JSON.parseObject(source, CnRecordLog.class))
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(CommonConfig.WATERMARK_SECONDS),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );
    }

    private static DataStreamSource<String> getKafkaSource() {
        Properties property = KafkaUtils.getDefaultKafkaSourceProperty();
        property.setProperty("group.id", CommonConfig.SESSION_RECORD_COMPLETED_GROUP_ID);
        SourceConfig sourceConfig = new SourceConfig(
                CommonConfig.SESSION_RECORD_COMPLETED_TOPIC,
                CommonConfig.SESSION_RECORD_COMPLETED_PARALLELISM,
                property);
        return Source.getSingleSource(sourceConfig);
    }

    @Override
    public void schedule() throws Exception {
        singleOutputStreamOperator
                .filter(Objects::nonNull)
                .map(JSON::toJSONString)
                .addSink(KafkaUtils.getKafkaSink(CommonConfig.CN_RECORD_TOPIC))
                .setParallelism(CommonConfig.CN_RECORD_PARALLELISM);
    }
}

