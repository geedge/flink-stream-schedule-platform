package com.zdjizhi.etl;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.config.Configs;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.sink.clickhouse.ClickHouseTableFactory;

import java.util.Objects;

public class CnRecordPersistence implements Schedule {

    @Override
    public void schedule() throws Exception {
        CnRecordEtl.singleOutputStreamOperator
                .filter(Objects::nonNull)
                .map(JSON::toJSONString)
                .addSink(ClickHouseTableFactory.getSinkFunction(Configs.get(CommonConfig.CN_RECORD_TABLE)))
                //.addSink(KafkaUtils.getKafkaSink(Configs.get(CommonConfig.CN_RECORD_TOPIC)))
                .setParallelism(Configs.get(CommonConfig.CN_RECORD_PARALLELISM));
    }
}
