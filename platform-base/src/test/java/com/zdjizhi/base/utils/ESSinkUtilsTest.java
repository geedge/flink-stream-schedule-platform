package com.zdjizhi.base.utils;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.SourceConfig;
import com.zdjizhi.base.source.Source;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class ESSinkUtilsTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExeEnv = FlinkEnvironmentUtils.streamExeEnv;

        String host = "192.168.40.151:9500,192.168.40.152:9500,192.168.40.153:9500";
        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(host);

        Properties property = KafkaUtils.getDefaultKafkaSourceProperty();
        property.setProperty("group.id", CommonConfig.SESSION_RECORD_COMPLETED_GROUP_ID);
        SourceConfig sourceConfig = new SourceConfig(
                CommonConfig.SESSION_RECORD_COMPLETED_TOPIC,
                CommonConfig.SESSION_RECORD_COMPLETED_PARALLELISM,
                property);
        DataStreamSource<String> source = Source.getSingleSource(sourceConfig);

        /*
        ESSinkUtils.addSink(esAddresses,10000,source,
                (String metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index("test")
                            .source(metric,XContentType.JSON));
                });
        */

        ESSinkUtils.addSink(esAddresses, 10000, source,
                (String metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {

                    try {
                        UpdateRequest updateRequest = new UpdateRequest("test", "1")
                                .doc(XContentFactory.jsonBuilder().startObject()
                                        .field("common_app_behavior", "3")
//                                        .field("http_cookie","abc")
                                        .endObject())
                                .docAsUpsert(true);
                        requestIndexer.add(updateRequest);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

//                    requestIndexer.add(Requests.indexRequest()
//                            .index("test")
//                            .source(metric, XContentType.JSON));
                }
        );

        esAddresses.forEach(System.out::println);

        streamExeEnv.execute();
    }

}
