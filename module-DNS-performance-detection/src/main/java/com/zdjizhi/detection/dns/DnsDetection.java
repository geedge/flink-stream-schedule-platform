package com.zdjizhi.detection.dns;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.operator.BroadcastTotalNumProcess;
import com.zdjizhi.detection.dns.operator.DnsDetectionProcess;
import com.zdjizhi.detection.dns.operator.ElasticsearchSinkFunctionImpl;
import com.zdjizhi.etl.CnRecordEtl;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * @author wlh
 */
public class DnsDetection implements Schedule {

    private static Logger LOG = LoggerFactory.getLogger(DnsDetection.class);

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.getCnRecordSource()
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(CommonConfig.FLINK_WATERMARK_MAX_ORDERNESS),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );

//        SingleOutputStreamOperator<Integer> broadcastTotalNum = cnRecordSource.process(new BroadcastTotalNumProcess());

        SingleOutputStreamOperator<Integer> broadcastTotalNum = cnRecordSource
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .process(new ProcessAllWindowFunction<CnRecordLog, Integer, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<CnRecordLog> elements, Collector<Integer> out) throws Exception {
                        int cnt = 0;
                        for (CnRecordLog ignored : elements) {
                            cnt++;
                        }
                        out.collect(cnt);
                    }
                });


        MapStateDescriptor<Void, Integer> bcStateDescriptor =
                new MapStateDescriptor<>("cnt", Types.VOID, Types.INT);

        BroadcastStream<Integer> broadcast = broadcastTotalNum.broadcast(bcStateDescriptor);

        SingleOutputStreamOperator<Tuple2<PerformanceEventLog, Boolean>> ipResult = cnRecordSource
                .filter((log) -> log.getCommon_server_ip() != null)
                .keyBy(CnRecordLog::getCommon_server_ip)
                .connect(broadcast)
                .process(new DnsDetectionProcess("ip"));

        SingleOutputStreamOperator<Tuple2<PerformanceEventLog, Boolean>> domainResult = cnRecordSource
                .filter((log) -> log.getDomain() != null)
                .keyBy(CnRecordLog::getDomain)
                .connect(broadcast)
                .process(new DnsDetectionProcess("domain"));

        SingleOutputStreamOperator<Tuple2<PerformanceEventLog, Boolean>> appResult = cnRecordSource
                .filter((log) -> log.getCommon_app_label() != null)
                .keyBy(CnRecordLog::getCommon_app_label)
                .connect(broadcast)
                .process(new DnsDetectionProcess("app"));

        DataStream<Tuple2<PerformanceEventLog, Boolean>> result = ipResult.union(domainResult).union(appResult);

        result.print();

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses, 1, result, new ElasticsearchSinkFunctionImpl());

        FlinkEnvironmentUtils.streamExeEnv.execute();
    }

    public static void main(String[] args) throws Exception {
        new DnsDetection().schedule();
    }

}
