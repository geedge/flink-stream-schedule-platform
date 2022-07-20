package com.zdjizhi.security.operator;

import com.zdjizhi.security.common.CommonConfig;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
/**
 * @ Author pengfeixu
 * @ Date 2022/2/25
 * @ Description
 */
public class EsSinkFunctionImpl implements ElasticsearchSinkFunction<SecurityEventLog> {
    @Override
    public void process(SecurityEventLog element, RuntimeContext ctx, RequestIndexer indexer) {
        UpdateRequest securityEvent = new UpdateRequest(CommonConfig.ES_SECURITY_TABLE_NAME, String.valueOf(element.getEvent_id()));
        UpdateRequest doc = securityEvent
                .doc(JsonMapper.toJsonString(element), XContentType.JSON)
                .docAsUpsert(true);
        indexer.add(doc);

    }
}
