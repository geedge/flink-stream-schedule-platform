package com.zdjizhi.pre.operator;

import com.zdjizhi.base.common.CnRecordLog;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

public class AddRandomKey implements MapFunction<CnRecordLog, CnRecordLog> {
    private final Random random = new Random();

    private final int parallelism = com.zdjizhi.base.common.CommonConfig.STREAM_EXECUTION_ENVIRONMENT_PARALLELISM;

    @Override
    public CnRecordLog map(CnRecordLog value) throws Exception {
        value.setRandom_num(random.nextInt(parallelism));
        return value;
    }
}
