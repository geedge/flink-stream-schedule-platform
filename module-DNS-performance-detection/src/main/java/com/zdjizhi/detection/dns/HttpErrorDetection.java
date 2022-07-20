package com.zdjizhi.detection.dns;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.operator.ElasticsearchSinkFunctionImpl;
import com.zdjizhi.detection.dns.operator.HttpErrorDetectionProcess;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.http.HttpHost;

import java.util.List;

public class HttpErrorDetection implements Schedule {

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.singleOutputStreamOperatorWithoutEtl
                .filter((log) -> "HTTP".equals(log.getCommon_schema_type()));

        SingleOutputStreamOperator<PerformanceEventLog> ipResult = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_server_ip()))
                .keyBy(CnRecordLog::getCommon_server_ip)
                .process(new HttpErrorDetectionProcess("ip"))
                .setParallelism(CommonConfig.PERFORMANCE_DETECTION_PARALLELISM);

        SingleOutputStreamOperator<PerformanceEventLog> domainResult = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getDns_qname()))
                .keyBy(CnRecordLog::getDns_qname)
                .process(new HttpErrorDetectionProcess("domain"))
                .setParallelism(CommonConfig.PERFORMANCE_DETECTION_PARALLELISM);

        SingleOutputStreamOperator<PerformanceEventLog> appResult = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_app_label()))
                .keyBy(CnRecordLog::getCommon_app_label)
                .process(new HttpErrorDetectionProcess("app"))
                .setParallelism(CommonConfig.PERFORMANCE_DETECTION_PARALLELISM);

        DataStream<PerformanceEventLog> result = ipResult.union(domainResult).union(appResult);

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses, CommonConfig.BULK_FLUSH_MAX_ACTION, result, new ElasticsearchSinkFunctionImpl());

    }

    public static void main(String[] args) throws Exception {
        new HttpErrorDetection().schedule();
        FlinkEnvironmentUtils.streamExeEnv.execute();
    }

}
