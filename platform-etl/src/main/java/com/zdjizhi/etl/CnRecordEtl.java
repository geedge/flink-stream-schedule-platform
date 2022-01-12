package com.zdjizhi.etl;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.Entity;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.utils.IPUtils;
import com.zdjizhi.etl.utils.appUtils;
import com.zdjizhi.etl.utils.fcUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                .addSink(KafkaUtils.getKafkaSink(CommonConfig.KAFKA_OUTPUT_TOPIC_NAME))
                .setParallelism(CommonConfig.KAFKA_OUTPUT_PARALLELISM);
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

//        input.print();

        SingleOutputStreamOperator<CnRecordLog> map = input.map((MapFunction<Entity, CnRecordLog>) entity -> {
            CnRecordLog richEntity = new CnRecordLog();
            richEntity.setCommon_recv_time(entity.getCommon_recv_time());
            richEntity.setCommon_schema_type(entity.getCommon_schema_type());
            richEntity.setCommon_client_ip(entity.getCommon_client_ip());
            richEntity.setCommon_server_ip(entity.getCommon_server_ip());
            richEntity.setCommon_app_label(entity.getCommon_app_label());
            richEntity.setCommon_l4_protocol(entity.getCommon_l4_protocol());
            richEntity.setHttp_host(entity.getHttp_host());
            richEntity.setSsl_sni(entity.getSsl_sni());
            richEntity.setCommon_egress_link_id(entity.getCommon_egress_link_id());
            richEntity.setCommon_ingress_link_id(entity.getCommon_ingress_link_id());
            richEntity.setDomain(entity.getHttp_domain());
            richEntity.setCommon_c2s_pkt_num(entity.getCommon_c2s_pkt_num());
            richEntity.setCommon_c2s_byte_num(entity.getCommon_c2s_byte_num());
            richEntity.setCommon_s2c_pkt_num(entity.getCommon_s2c_pkt_num());
            richEntity.setCommon_s2c_byte_num(entity.getCommon_s2c_byte_num());
            richEntity.setCommon_sessions(entity.getCommon_sessions());
            richEntity.setCommon_c2s_tcp_lostlen(entity.getCommon_c2s_tcp_lostlen());
            richEntity.setCommon_s2c_tcp_lostlen(entity.getCommon_s2c_tcp_lostlen());
            richEntity.setCommon_c2s_tcp_unorder_num(entity.getCommon_c2s_tcp_unorder_num());
            richEntity.setCommon_s2c_tcp_unorder_num(entity.getCommon_s2c_tcp_unorder_num());
            richEntity.setCommon_c2s_byte_retrans(entity.getCommon_c2s_byte_retrans());
            richEntity.setCommon_s2c_byte_retrans(entity.getCommon_s2c_byte_retrans());
            richEntity.setCommon_c2s_pkt_retrans(entity.getCommon_c2s_pkt_retrans());
            richEntity.setCommon_s2c_pkt_retrans(entity.getCommon_s2c_pkt_retrans());
            richEntity.setCommon_establish_latency_ms(entity.getCommon_establish_latency_ms());
            richEntity.setHttp_response_latency_ms(entity.getHttp_response_latency_ms());
            richEntity.setSsl_con_latency_ms(entity.getSsl_con_latency_ms());

            IPUtils.getMmdbMessage(richEntity);
            fcUtils.getWebSketchMessage(richEntity);
            appUtils.getAppSketchMessage(richEntity);
            return richEntity;
        });

        return map;
    }

}
