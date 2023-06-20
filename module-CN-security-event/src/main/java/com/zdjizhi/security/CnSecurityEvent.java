package com.zdjizhi.security;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.source.Source;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.security.common.CommonConfig;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.security.operator.DomainWindowFunction;
import com.zdjizhi.security.operator.EsSinkFunctionImpl;
import com.zdjizhi.security.operator.ServerIpWindowFunction;
import com.zdjizhi.security.operator.UrlWindowFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;

import java.util.List;
import java.util.Map;

/**
 * @ Author pengfeixu
 * @ Description
 */
public class CnSecurityEvent implements Schedule {

    private static final String KNOWLEDGE_EXPR;

    static {
        StringBuilder sb = new StringBuilder("");
        sb.append("$.[?(@.version=='latest' && @.type in ['");
        sb.append(CommonInternalConfig.IOC_TYPE);
        sb.append("'])].['name','sha256','format','path','type','isValid']");
        KNOWLEDGE_EXPR = sb.toString();
    }

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.singleOutputStreamOperator;

        DataStreamSource<Map<String, String>> knowledgeSource = Source.getKnowledgeSource(KNOWLEDGE_EXPR);

        SingleOutputStreamOperator<byte[]> knowledgeBytes = knowledgeSource.process(new ProcessFunction<Map<String, String>, byte[]>() {
            @Override
            public void processElement(Map<String, String> value, ProcessFunction<Map<String, String>, byte[]>.Context ctx, Collector<byte[]> out) throws Exception {
                String iocFileName = CommonInternalConfig.IOC_TYPE + ".csv";
                if (value.containsKey(iocFileName)) {
                    byte[] iocBytes = new byte[0];
                    iocBytes = FileUtils.downloadFile(iocFileName, value.get(iocFileName));
                    out.collect(iocBytes);
                }
            }
        });

        MapStateDescriptor<String, Byte[]> descriptor =
                new MapStateDescriptor<>("descriptorTest", Types.STRING, TypeInformation.of(Byte[].class));
        BroadcastStream<byte[]> broadcastStream = knowledgeBytes.broadcast(descriptor);

        SingleOutputStreamOperator<SecurityEventLog> ipProcess = cnRecordSource
                .filter((log) -> log.getCommon_server_ip() != null)
                .keyBy(CnRecordLog::getCommon_server_ip)
                .connect(broadcastStream)
                .process(new ServerIpWindowFunction())
                .setParallelism(CommonConfig.SECURITY_SERVER_PARALLELISM);

        SingleOutputStreamOperator<SecurityEventLog> domainProcess = cnRecordSource
                .filter((log) -> log.getDomain() != null)
                .keyBy(CnRecordLog::getDomain)
                .connect(broadcastStream)
                .process(new DomainWindowFunction())
                .setParallelism(CommonConfig.SECURITY_DOMAIN_PARALLELISM);

        SingleOutputStreamOperator<SecurityEventLog> urlProcess = cnRecordSource
                .filter((log) -> log.getHttp_url() != null)
                .keyBy(CnRecordLog::getHttp_url)
                .connect(broadcastStream)
                .process(new UrlWindowFunction())
                .setParallelism(CommonConfig.SECURITY_URL_PARALLELISM);

        DataStream<SecurityEventLog> resultDataStream = ipProcess.union(domainProcess).union(urlProcess);
        resultDataStream.print();
        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses, CommonConfig.ES_MAX_FLUSH, resultDataStream, new EsSinkFunctionImpl());

    }


    public static void main(String[] args) throws Exception {
        new CnSecurityEvent().schedule();
    }
}
