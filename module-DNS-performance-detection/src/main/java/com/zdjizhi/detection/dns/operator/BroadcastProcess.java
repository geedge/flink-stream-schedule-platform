package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class BroadcastProcess extends ProcessAllWindowFunction<CnRecordLog, Tuple2<Integer, Double>, TimeWindow> {
    @Override
    public void process(Context context, Iterable<CnRecordLog> elements, Collector<Tuple2<Integer, Double>> out) throws Exception {
        int cnt = 0;
        double median = 0d;
        ArrayList<Long> dnsResponseList = new ArrayList<>();
        for (CnRecordLog ignored : elements) {
            cnt++;
            if ("DNS".equals(ignored.getCommon_schema_type())) {
                long dnsResponseLatencyMs = ignored.getDns_response_latency_ms();
                dnsResponseList.add(dnsResponseLatencyMs);
            }
        }
        dnsResponseList.sort(Comparator.comparingInt(Long::intValue));
        int size = dnsResponseList.size();
        if (size > 0) {
            if (size % 2 == 0) {
                median = (dnsResponseList.get(size / 2 - 1) + dnsResponseList.get(size / 2) + 0.0) / 2;
            } else {
                median = dnsResponseList.get((size - 1) / 2).doubleValue();
            }
        }
        out.collect(Tuple2.of(cnt, median));
    }

}
