package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author wlh
 */
public class BroadcastProcess extends ProcessAllWindowFunction<CnRecordLog, Tuple3<Integer,Integer, Double>,TimeWindow> {

    private static Logger LOG = LoggerFactory.getLogger(BroadcastProcess.class);

    @Override
    public void process(Context context, Iterable<CnRecordLog> elements, Collector<Tuple3<Integer,Integer, Double>> out) {
        try {
            int dnsCnt = 0;
            int httpCnt = 0;
            double median = 0d;
            ArrayList<Long> dnsResponseList = new ArrayList<>();
            for (CnRecordLog ignored : elements) {
                if ("DNS".equals(ignored.getCommon_schema_type())) {
                    dnsCnt++;
                    long dnsResponseLatencyMs = ignored.getDns_response_latency_ms();
                    dnsResponseList.add(dnsResponseLatencyMs);
                }
                if ("HTTP".equals(ignored.getCommon_schema_type())){
                    httpCnt++;
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
            out.collect(Tuple3.of(dnsCnt,httpCnt, median));
        }catch (Exception e){
            LOG.error("计算 broadcast 失败",e);
        }
    }

}
