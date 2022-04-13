package com.zdjizhi.detection.dns;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.handler.BroadcastHandler;
import com.zdjizhi.detection.dns.operator.DnsErrorDetectionProcess;
import com.zdjizhi.detection.dns.operator.ElasticsearchSinkFunctionImpl;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * @author wlh
 */
public class DnsErrorDetection implements Schedule {

    private static Logger LOG = LoggerFactory.getLogger(DnsErrorDetection.class);

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.getCnRecordSource()
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(CommonConfig.FLINK_WATERMARK_MAX_ORDERNESS),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );

        BroadcastStream<Tuple3<Integer,Integer, Double>> broadcast = BroadcastHandler.getBroadcast(cnRecordSource);

        SingleOutputStreamOperator<PerformanceEventLog> ipResult = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_server_ip()))
                .keyBy(CnRecordLog::getCommon_server_ip)
                .connect(broadcast)
                .process(new DnsErrorDetectionProcess("ip"))
                .setParallelism(CommonConfig.PERFORMANCE_DETECTION_PARALLELISM);

        SingleOutputStreamOperator<PerformanceEventLog> domainResult = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getDns_qname()))
                .keyBy(CnRecordLog::getDns_qname)
                .connect(broadcast)
                .process(new DnsErrorDetectionProcess("domain"))
                .setParallelism(CommonConfig.PERFORMANCE_DETECTION_PARALLELISM);

        SingleOutputStreamOperator<PerformanceEventLog> appResult = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_app_label()))
                .keyBy(CnRecordLog::getCommon_app_label)
                .connect(broadcast)
                .process(new DnsErrorDetectionProcess("app"))
                .setParallelism(CommonConfig.PERFORMANCE_DETECTION_PARALLELISM);

        DataStream<PerformanceEventLog> result = ipResult.union(domainResult).union(appResult);

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses, CommonConfig.BULK_FLUSH_MAX_ACTION, result, new ElasticsearchSinkFunctionImpl());

    }

    public static void main(String[] args) throws Exception {
        new DnsErrorDetection().schedule();
        FlinkEnvironmentUtils.streamExeEnv.execute();
    }

}
