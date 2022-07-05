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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

public class CnRecordEtl implements Schedule {
    private static final Logger LOG = LoggerFactory.getLogger(CnRecordEtl.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    public static SingleOutputStreamOperator<CnRecordLog> singleOutputStreamOperator;

    static {
        singleOutputStreamOperator = getCnRecordSource();
    }

    private static SingleOutputStreamOperator<CnRecordLog> getCnRecordSource() {
        return createCnRecordSource().assignTimestampsAndWatermarks(
                FlinkEnvironmentUtils.createWatermarkStrategy(
                        Duration.ofSeconds(1),
                        (event, timestamp) -> event.getCommon_recv_time() * 1000
                )
        ).setParallelism(CommonConfig.SESSION_RECORD_COMPLETED_PARALLELISM);
    }

    @Override
    public void schedule() throws Exception {
        singleOutputStreamOperator
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

        DataStreamSource<String> source = Source.getSingleSource(sourceConfig);

        return source.map((MapFunction<String, CnRecordLog>) entity -> {
            CnRecordLog recordLog = new CnRecordLog();
            try {
                recordLog = jsonMapperInstance.fromJson(entity, CnRecordLog.class);
                CompletedUtils.getCompletedMessage(recordLog);
                IPUtils.getMmdbMessage(recordLog);
                IdcRenterUtils.setIdcRenter(recordLog);
                LinkUtils.getLinkMessage(recordLog);
                IspUtils.getIspMessage(recordLog);
                FcUtils.getWebSketchMessage(recordLog);
                FcUtils.getIcpMessage(recordLog);
                WhoisUtils.getWhoisSketchMessage(recordLog);
                DnsServerUtils.getWebSktMessage(recordLog);
                AppUtils.getAppSketchMessage(recordLog);
            }catch (Exception e){
                LOG.error("{}\n ETL解析错误\n",entity,e);
            }
            return recordLog;
        });

    }


}
