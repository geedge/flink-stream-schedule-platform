package com.zdjizhi.etl;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.utils.*;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

public class CnRecordEtl implements Schedule {
    private static final Logger LOG = LoggerFactory.getLogger(CnRecordEtl.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    public static SingleOutputStreamOperator<CnRecordLog> singleOutputStreamOperatorWithEtl;
    public static SingleOutputStreamOperator<CnRecordLog> singleOutputStreamOperatorWithoutEtl;

    static {
        if ("SESSION-RECORD".equals(CommonConfig.SESSION_RECORD_COMPLETED_TOPIC)){
            singleOutputStreamOperatorWithEtl = getCnRecordSource();
        }else {
            singleOutputStreamOperatorWithoutEtl = getCnRecordSourceWithoutEtl();
        }
    }

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
        return Source.getSingleSource(sourceConfig).map(source -> jsonMapperInstance.fromJson(source, CnRecordLog.class))
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(1),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );
    }

    @Override
    public void schedule() throws Exception {
        singleOutputStreamOperatorWithEtl
                .filter(Objects::nonNull)
                .map(JsonMapper::toJsonString)
                .addSink(KafkaUtils.getKafkaSink(CommonConfig.CN_RECORD_TOPIC))
                .setParallelism(CommonConfig.CN_RECORD_PARALLELISM);
    }

    private static SingleOutputStreamOperator<CnRecordLog> createCnRecordSource() {
        Properties property = KafkaUtils.getDefaultKafkaSourceProperty();
        property.setProperty("group.id", CommonConfig.SESSION_RECORD_COMPLETED_GROUP_ID);
        SourceConfig sourceConfig = new SourceConfig(
                CommonConfig.SESSION_RECORD_COMPLETED_TOPIC,
                CommonConfig.SESSION_RECORD_COMPLETED_PARALLELISM,
                property);

        return Source.getSingleSource(sourceConfig).map((MapFunction<String, CnRecordLog>) source -> {
            CnRecordLog recordLog = new CnRecordLog();
            try {
                recordLog = jsonMapperInstance.fromJson(source, CnRecordLog.class);
                CompletedUtils.setCompletedMessage(recordLog);
                IPUtils.setMmdbMessage(recordLog);
                IdcRenterUtils.setIdcRenter(recordLog);
                LinkUtils.setLinkMessage(recordLog);
                IspUtils.setIspMessage(recordLog);
                FcUtils.setWebSketchMessage(recordLog);
                FcUtils.setIcpMessage(recordLog);
                WhoisUtils.setWhoisSketchMessage(recordLog);
                DnsServerUtils.setWebSktMessage(recordLog);
                AppUtils.setAppSketchMessage(recordLog);
                IPUtils.setInternalIpFlag(recordLog);
            } catch (Exception e) {
                LOG.error("ETL解析错误\nsource:{} \nrecordLog:{} \n", source, recordLog, e);
            }
            return recordLog;
        });

    }


}
