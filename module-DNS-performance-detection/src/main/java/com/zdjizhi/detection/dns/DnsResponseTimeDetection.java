package com.zdjizhi.detection.dns;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.handler.BroadcastHandler;
import com.zdjizhi.detection.dns.operator.DnsResponseTimeDetectionProcess;
import com.zdjizhi.detection.dns.operator.ElasticsearchSinkFunctionImpl;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.http.HttpHost;

import java.time.Duration;
import java.util.List;

public class DnsResponseTimeDetection implements Schedule {
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

        SingleOutputStreamOperator<PerformanceEventLog> dnsResponseResultByIp = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_server_ip()))
                .keyBy(CnRecordLog::getCommon_server_ip)
                .connect(broadcast)
                .process(new DnsResponseTimeDetectionProcess("ip"));

        SingleOutputStreamOperator<PerformanceEventLog> dnsResponseResultByDomain = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getDns_qname()))
                .keyBy(CnRecordLog::getDns_qname)
                .connect(broadcast)
                .process(new DnsResponseTimeDetectionProcess("domain"));

        SingleOutputStreamOperator<PerformanceEventLog> dnsResponseResultByApp = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_app_label()))
                .keyBy(CnRecordLog::getCommon_app_label)
                .connect(broadcast)
                .process(new DnsResponseTimeDetectionProcess("app"));

        DataStream<PerformanceEventLog> result = dnsResponseResultByIp.union(dnsResponseResultByDomain).union(dnsResponseResultByApp);

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses, CommonConfig.BULK_FLUSH_MAX_ACTION, result, new ElasticsearchSinkFunctionImpl());

        dnsResponseResultByIp.print();

    }

    public static void main(String[] args) throws Exception {
        new DnsResponseTimeDetection().schedule();
        FlinkEnvironmentUtils.streamExeEnv.execute();
    }

}
