package com.zdjizhi.etl;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.Entity;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.utils.*;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;

public class CnRecordEtl implements Schedule {
    private static final Logger LOG = LoggerFactory.getLogger(CnRecordEtl.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();


    public static SingleOutputStreamOperator<CnRecordLog> getCnRecordSource() {
        return createCnRecordSource();
    }

    @Override
    public void schedule() throws Exception {
        getCnRecordSource()
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

        SingleOutputStreamOperator<Entity> input = source.map((MapFunction<String, Entity>) message -> {
            Entity entity = new Entity();
            try {
                entity = jsonMapperInstance.fromJson(message, Entity.class);
                String commonAppLabel = entity.getCommon_app_label();
                if (StringUtil.isNotEmpty(commonAppLabel)) {
                    if (commonAppLabel.startsWith("05")) {
                        entity.setCommon_app_label("");
                    }
                }
                if (entity.getCommon_ingress_link_id() > 2050 || entity.getCommon_egress_link_id() > 2050) {
                    entity.setCommon_ingress_link_id(0);
                    entity.setCommon_egress_link_id(0);
                }
            } catch (Exception e) {
                LOG.error("Entity Parsing ERROR");
                entity.setIfError(1);
            }
            return entity;
        }).filter((FilterFunction<Entity>) entity -> entity.getIfError() != 1);

        return input.map((MapFunction<Entity, CnRecordLog>) entity -> {
            CnRecordLog recordLog = new CnRecordLog();
            try {
                CompletedUtils.getCompletedMessage(entity,recordLog);
                IPUtils.getMmdbMessage(recordLog);
                FcUtils.getWebSketchMessage(recordLog);
                FcUtils.getIcpMessage(recordLog);
                WhoisUtils.getWhoisSketchMessage(recordLog);
                DnsServerUtils.getWebSktMessage(entity,recordLog);
                AppUtils.getAppSketchMessage(recordLog);
            }catch (Exception e){
                LOG.error("{}\n ETL解析错误\n",entity.toString(),e);
            }
            return recordLog;
        });
    }


}
