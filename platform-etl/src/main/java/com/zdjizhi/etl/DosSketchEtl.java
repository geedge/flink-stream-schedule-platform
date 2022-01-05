package com.zdjizhi.etl;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.DosSketchLog;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class DosSketchEtl implements Schedule {

    public static SingleOutputStreamOperator<DosSketchLog> getSketchSource(){
        return flatSketchSource();
    }

    private static SingleOutputStreamOperator<DosSketchLog> flatSketchSource(){
        SourceConfig sourceConfig = new SourceConfig(
                CommonConfig.KAFKA_INPUT_TOPIC_NAME,
                CommonConfig.KAFKA_INPUT_PARALLELISM,
                getKafkaSourceProperty());
        return Source.getSingleSource(sourceConfig).flatMap(new FlatSketchLog());
    }

    @Override
    public void schedule() throws Exception {
        getSketchSource().print();
    }

    private static Properties getKafkaSourceProperty() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", CommonConfig.KAFKA_INPUT_BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", CommonConfig.KAFKA_GROUP_ID);
        if (CommonConfig.SASL_JAAS_CONFIG_FLAG == 1){
            KafkaUtils.updateSasl(properties);
        }
        return properties;
    }

    static class FlatSketchLog implements FlatMapFunction<String, DosSketchLog> {
        private static Logger logger = LoggerFactory.getLogger(DosSketchEtl.class);
        private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
        private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);
        private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);

        @Override
        public void flatMap(String s, Collector<DosSketchLog> collector) {
            try {
                if (StringUtil.isNotBlank(s)){
                    HashMap<String, Object> sketchSource = jsonMapperInstance.fromJson(s, hashmapJsonType);
                    long sketchStartTime = Long.parseLong(sketchSource.get("sketch_start_time").toString());
                    long sketchDuration = Long.parseLong(sketchSource.get("sketch_duration").toString());
                    String attackType = sketchSource.get("attack_type").toString();
                    ArrayList<HashMap<String, Object>> reportIpList = jsonMapperInstance.fromJson(jsonMapperInstance.toJson(sketchSource.get("report_ip_list")), listType);
                    for (HashMap<String, Object> obj : reportIpList) {
                        DosSketchLog dosSketchLog = new DosSketchLog();
                        dosSketchLog.setSketch_start_time(sketchStartTime);
                        dosSketchLog.setSketch_duration(sketchDuration);
                        dosSketchLog.setAttack_type(attackType);
                        String sourceIp = obj.get("source_ip").toString();
                        String destinationIp = obj.get("destination_ip").toString();
                        long sketchSessions = Long.parseLong(obj.get("sketch_sessions").toString());
                        long sketchPackets = Long.parseLong(obj.get("sketch_packets").toString());
                        long sketchBytes = Long.parseLong(obj.get("sketch_bytes").toString());
                        dosSketchLog.setSource_ip(sourceIp);
                        dosSketchLog.setDestination_ip(destinationIp);
                        dosSketchLog.setSketch_sessions(sketchSessions);
                        dosSketchLog.setSketch_packets(sketchPackets);
                        dosSketchLog.setSketch_bytes(sketchBytes);
                        collector.collect(dosSketchLog);
                        logger.debug("数据解析成功：{}",dosSketchLog.toString());
                    }
                }
            } catch (Exception e) {
                logger.error("数据解析错误：{} \n{}",s,e);
            }
        }
    }

}
