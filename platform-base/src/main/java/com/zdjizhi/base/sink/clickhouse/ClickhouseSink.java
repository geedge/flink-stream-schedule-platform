package com.zdjizhi.base.sink.clickhouse;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.connectors.clickhouse.sink.EventBatchIntervalClickHouseSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/2/2 9:45
 */
public class ClickhouseSink extends RichSinkFunction<String> {

    private EventBatchIntervalClickHouseSink eventBatchIntervalClickHouseSink;

    public ClickhouseSink(int batchSize, long batchIntervalMs, String host, String table, Properties properties) {
        this.eventBatchIntervalClickHouseSink = new EventBatchIntervalClickHouseSink(null, batchSize, batchIntervalMs, host, table, properties);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.eventBatchIntervalClickHouseSink.setRuntimeContext(this.getRuntimeContext());
        this.eventBatchIntervalClickHouseSink.open(parameters);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        Event event = convertValue(value);
        this.eventBatchIntervalClickHouseSink.invoke(event, context);
    }

    private Event convertValue(String value) {
        Event event = new Event();
        Map<String, Object> map = objectToMap(value);
        event.setExtractedFields(map);
        return event;
    }

    private Map<String, Object> objectToMap(String json) {
        return JSON.parseObject(json, TypeReference.mapType(HashMap.class, String.class, Object.class));
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.eventBatchIntervalClickHouseSink.close();
    }
}
