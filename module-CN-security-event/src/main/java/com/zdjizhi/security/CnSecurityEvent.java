package com.zdjizhi.security;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.security.common.CommonConfig;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.security.operator.DomainWindowFunction;
import com.zdjizhi.security.operator.EsSinkFunctionImpl;
import com.zdjizhi.security.operator.ServerIpWindowFunction;
import com.zdjizhi.security.operator.UrlWindowFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.http.HttpHost;

import java.util.List;

/**
 * @ Author pengfeixu
 * @ Description
 */
public class CnSecurityEvent implements Schedule {

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.singleOutputStreamOperator
                .setParallelism(CommonConfig.SECURITY_SOURCE_PARALLELISM);

        SingleOutputStreamOperator<SecurityEventLog> ipProcess = cnRecordSource
                .filter((log) -> log.getCommon_server_ip() != null)
                .keyBy(CnRecordLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.FLINK_WINDOW_TIME)))
                .process(new ServerIpWindowFunction())
//                .slotSharingGroup(CommonConfig.SECURITY_SERVER_SLOT_NAME)
                .setParallelism(CommonConfig.SECURITY_SERVER_PARALLELISM);
        SingleOutputStreamOperator<SecurityEventLog> domainProcess = cnRecordSource
                .filter((log) -> log.getDomain() != null)
                .keyBy(CnRecordLog::getDomain)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.FLINK_WINDOW_TIME)))
                .process(new DomainWindowFunction())
//                .slotSharingGroup(CommonConfig.SECURITY_DOMAIN_SLOT_NAME)
                .setParallelism(CommonConfig.SECURITY_DOMAIN_PARALLELISM);
        SingleOutputStreamOperator<SecurityEventLog> urlProcess = cnRecordSource
                .filter((log) -> log.getHttp_url() != null)
                .keyBy(CnRecordLog::getHttp_url)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.FLINK_WINDOW_TIME)))
                .process(new UrlWindowFunction())
//                .slotSharingGroup(CommonConfig.SECURITY_URL_SLOT_NAME)
                .setParallelism(CommonConfig.SECURITY_URL_PARALLELISM);
        DataStream<SecurityEventLog> resultDataStream = ipProcess.union(domainProcess).union(urlProcess);
        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses, CommonConfig.ES_MAX_FLUSH, resultDataStream, new EsSinkFunctionImpl());

    }


    public static void main(String[] args) throws Exception {
        new CnSecurityEvent().schedule();
    }
}
