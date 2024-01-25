package com.zdjizhi.etl;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.config.Configs;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.operator.EtlProcessFunc;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;
import java.util.Properties;

public class CnRecordEtl {

    public static SingleOutputStreamOperator<CnRecordLog> singleOutputStreamOperator;

    public CnRecordEtl() {
        if (Configs.get(CommonConfig.ENABLE_ETL)) {
            singleOutputStreamOperator = getCnRecordSource();
        } else {
            singleOutputStreamOperator = getCnRecordSourceWithoutEtl();
        }
    }

    private SingleOutputStreamOperator<CnRecordLog> getCnRecordSource() {
        return getKafkaSource().process(new EtlProcessFunc()).name("etlProcess")
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(Configs.get(CommonConfig.WATERMARK_SECONDS)),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );
    }

    private SingleOutputStreamOperator<CnRecordLog> getCnRecordSourceWithoutEtl() {
        return getKafkaSource().map(message -> JSON.parseObject(message, CnRecordLog.class))
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(Configs.get(CommonConfig.WATERMARK_SECONDS)),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );
    }

    private DataStreamSource<String> getKafkaSource() {
        Properties property = KafkaUtils.getDefaultKafkaSourceProperty();
        property.setProperty("group.id", Configs.get(CommonConfig.SESSION_RECORD_COMPLETED_GROUP_ID));
        SourceConfig sourceConfig = new SourceConfig(
                Configs.get(CommonConfig.SESSION_RECORD_COMPLETED_TOPIC),
                Configs.get(CommonConfig.SESSION_RECORD_COMPLETED_PARALLELISM),
                property);
        return Source.getSingleSource(sourceConfig);
    }
}

