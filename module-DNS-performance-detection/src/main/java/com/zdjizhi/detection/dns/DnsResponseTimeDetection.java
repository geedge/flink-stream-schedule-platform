package com.zdjizhi.detection.dns;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.handler.BroadcastHandler;
import com.zdjizhi.detection.dns.operator.DnsResponseTimeDetectionProcess;
import com.zdjizhi.etl.CnRecordEtl;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

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

        BroadcastStream<Tuple2<Integer, Double>> broadcast = BroadcastHandler.getBroadcast(cnRecordSource);

        SingleOutputStreamOperator<PerformanceEventLog> dnsResponseResultByIp = cnRecordSource
                .filter((log) -> log.getCommon_server_ip() != null)
                .keyBy(CnRecordLog::getCommon_server_ip)
                .connect(broadcast)
                .process(new DnsResponseTimeDetectionProcess("ip"));

        dnsResponseResultByIp.print();

        FlinkEnvironmentUtils.streamExeEnv.execute();
    }

    public static void main(String[] args) throws Exception {
        new DnsResponseTimeDetection().schedule();
    }

}
