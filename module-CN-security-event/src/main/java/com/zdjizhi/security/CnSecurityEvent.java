package com.zdjizhi.security;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.security.common.CommonConfig;
import com.zdjizhi.security.operator.DomainWindowFunction;
import com.zdjizhi.security.operator.EsSinkFunctionImpl;
import com.zdjizhi.security.operator.ServerIpWindowFunction;
import com.zdjizhi.security.operator.UrlWindowFunction;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
/**
 * @ Author pengfeixu
 * @ Description
 */
public class CnSecurityEvent implements Schedule {
    private static Logger LOG = LoggerFactory.getLogger(CnSecurityEvent.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.singleOutputStreamOperator
                /*.assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(CommonConfig.FLINK_WATERMARK_MAX_ORDERNESS),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                )
                .setParallelism(CommonConfig.SECURITY_SOURCE_PARALLELISM)*/;

        SingleOutputStreamOperator ipProcess = cnRecordSource
//                .startNewChain()
                .filter((log) -> log.getCommon_server_ip() != null)
                .keyBy(CnRecordLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.FLINK_WINDOW_TIME)))
                .process(new ServerIpWindowFunction())
                .slotSharingGroup(CommonConfig.SECURITY_SERVER_SLOT_NAME);
        ipProcess.setParallelism(CommonConfig.SECURITY_SERVER_PARALLELISM);
        SingleOutputStreamOperator domainProcess = cnRecordSource
//                .startNewChain()
                .filter((log) -> log.getDomain() != null)
                .keyBy(CnRecordLog::getDomain)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.FLINK_WINDOW_TIME)))
                .process(new DomainWindowFunction())
//                .startNewChain()
                .slotSharingGroup(CommonConfig.SECURITY_DOMAIN_SLOT_NAME);
        domainProcess.setParallelism(CommonConfig.SECURITY_DOMAIN_PARALLELISM);
        SingleOutputStreamOperator urlProcess = cnRecordSource
//                .startNewChain()
                .filter((log) -> log.getHttp_url() != null)
                .keyBy(CnRecordLog::getHttp_url)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.FLINK_WINDOW_TIME)))
                .process(new UrlWindowFunction())
                .slotSharingGroup(CommonConfig.SECURITY_URL_SLOT_NAME);
        urlProcess.setParallelism(CommonConfig.SECURITY_URL_PARALLELISM);
        DataStream resultDataStream = ipProcess.union(domainProcess).union(urlProcess);
        resultDataStream.print();
        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses,CommonConfig.ES_MAX_FlUSH, resultDataStream, new EsSinkFunctionImpl());

    }



    public static void main(String[] args) throws Exception {
        new CnSecurityEvent().schedule();
    }
}
