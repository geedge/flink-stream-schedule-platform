package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlh
 */
public class ElasticsearchSinkFunctionImpl implements ElasticsearchSinkFunction<PerformanceEventLog> {

    private static Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkFunctionImpl.class);

    @Override
    public void process(PerformanceEventLog element, RuntimeContext ctx, RequestIndexer indexer) {
        try {
            UpdateRequest performanceEvent = new UpdateRequest(CommonConfig.ES_PERFORMANCE_TABLE_NAME , String.valueOf(element.getEvent_id()))
                    .doc(JsonMapper.toJsonString(element), XContentType.JSON)
                    .docAsUpsert(true)
                    .retryOnConflict(CommonConfig.ES_RETRY_CONFLICT_NUM);
            indexer.add(performanceEvent);
        }catch (Exception e){
            LOG.error("文档{}更新失败 \n{}",element,e);
        }
    }
}
