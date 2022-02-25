package com.zdjizhi.detection.dns;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.etl.CnRecordEtl;
import javafx.collections.transformation.SortedList;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

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

        SingleOutputStreamOperator<Integer> broadcastTotalNum = cnRecordSource
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .process(new ProcessAllWindowFunction<CnRecordLog, Integer, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<CnRecordLog> elements, Collector<Integer> out) throws Exception {
                        ArrayList<Long> dnsResponseList = new ArrayList<>();
                        for (CnRecordLog recordLog:elements){
                            long dnsResponseLatencyMs = recordLog.getDns_response_latency_ms();
                            dnsResponseList.add(dnsResponseLatencyMs);
                        }
                        dnsResponseList.sort(Comparator.comparingInt(Long::intValue));

                    }
                });

    }
}
