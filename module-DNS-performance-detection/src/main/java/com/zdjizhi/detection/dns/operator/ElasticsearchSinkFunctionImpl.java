package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * @author wlh
 */
public class ElasticsearchSinkFunctionImpl implements ElasticsearchSinkFunction<Tuple2<PerformanceEventLog, Boolean>> {
    @Override
    public void process(Tuple2<PerformanceEventLog, Boolean> element, RuntimeContext ctx, RequestIndexer indexer) {
        UpdateRequest performanceEvent = new UpdateRequest("performance_event", String.valueOf(element.f0.getEvent_id()))
                .doc(JsonMapper.toJsonString(element.f0), XContentType.JSON)
                .docAsUpsert(true);
        indexer.add(performanceEvent);
    }
}
