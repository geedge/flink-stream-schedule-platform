package com.zdjizhi.detection.dns;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.operator.DnsResponseTimeDetectionProcess;
import com.zdjizhi.detection.dns.operator.ElasticsearchSinkFunctionImpl;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.http.HttpHost;

import java.time.Duration;
import java.util.List;

public class DnsResponseTimeDetection implements Schedule {
    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.singleOutputStreamOperator
                /*.assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(CommonConfig.FLINK_WATERMARK_MAX_ORDERNESS),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                )*/.filter((log) -> "DNS".equals(log.getCommon_schema_type()));

        SingleOutputStreamOperator<PerformanceEventLog> result = cnRecordSource
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_server_ip()))
                .keyBy(CnRecordLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .process(new DnsResponseTimeDetectionProcess())
                .setParallelism(CommonConfig.PERFORMANCE_DETECTION_PARALLELISM);

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses, CommonConfig.BULK_FLUSH_MAX_ACTION, result, new ElasticsearchSinkFunctionImpl());

    }

    public static void main(String[] args) throws Exception {
        new DnsResponseTimeDetection().schedule();
        FlinkEnvironmentUtils.streamExeEnv.execute();
    }

}
