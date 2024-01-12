package com.zdjizhi.base.sink.clickhouse;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.config.Configs;
import com.zdjizhi.base.sink.clickhouse.sink.EventBatchIntervalClickHouseSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Properties;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/8 17:04
 */
public class ClickHouseTableFactory {

    public static RichSinkFunction<String> getSinkFunction(String table) {
        String address = Configs.get(CommonConfig.CLICKHOUSE_ADDRESS);
        int batchSize = Configs.get(CommonConfig.CLICKHOUSE_BATCH_SIZE);
        long batchIntervalMs = Configs.get(CommonConfig.CLICKHOUSE_BATCH_INTERVAL_MS).toMillis();
        Properties connInfo = new Properties();
        connInfo.put("user", Configs.get(CommonConfig.CLICKHOUSE_USERNAME));
        connInfo.put("password", Configs.get(CommonConfig.CLICKHOUSE_PASSWORD));
        return new EventBatchIntervalClickHouseSink(
                batchSize,
                batchIntervalMs,
                address,
                Configs.get(CommonConfig.CN_RECORD_DATABASE) + "." + table,
                connInfo);
    }
}
