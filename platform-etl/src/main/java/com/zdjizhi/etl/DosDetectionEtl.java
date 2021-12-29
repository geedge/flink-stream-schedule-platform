package com.zdjizhi.etl;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.DosSketchLog;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Properties;

public class DosDetectionEtl implements Schedule {

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


}
