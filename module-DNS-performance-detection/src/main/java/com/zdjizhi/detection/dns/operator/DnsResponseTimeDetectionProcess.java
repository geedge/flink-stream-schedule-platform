package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.common.PerformanceEventLog;
import com.zdjizhi.detection.dns.handler.DetectionEventLogHandler;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;

public class DnsResponseTimeDetectionProcess extends ProcessWindowFunction<CnRecordLog, PerformanceEventLog, String, TimeWindow> {

    private static Logger LOG = LoggerFactory.getLogger(DnsResponseTimeDetectionProcess.class);

    private MapState<String, PerformanceEventLog> previousResultDesc;
    private MapState<String, Tuple2<Double, Long>> middleState;

    @Override
    public void open(Configuration parameters) {
        previousResultDesc = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("dns_response_detection_previous_result",
                        Types.STRING, Types.POJO(PerformanceEventLog.class)));
        middleState = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("dns_response_detection_result",
                        Types.STRING, Types.TUPLE(Types.DOUBLE, Types.LONG)));
    }

    @Override
    public void process(String key, Context context, Iterable<CnRecordLog> elements, Collector<PerformanceEventLog> out) {
        try {
            ArrayList<Long> dnsResponseList = new ArrayList<>();
            for (CnRecordLog cnRecordLog : elements) {
                dnsResponseList.add(cnRecordLog.getDns_response_latency_ms());
            }
            int size = dnsResponseList.size();

            if (size > CommonConfig.DNS_RESPONSE_DETECTION_MEDIUM_THRESHOLD){
                double median = 0d;
                dnsResponseList.sort(Long::compareTo);
                if (size > 0) {
                    median = dnsResponseList.get(size / 2).doubleValue();
                }
                double dis = 1e-6;

                PerformanceEventLog eventLog = null;
                if (Math.abs(median - 0d) > dis) {
                    if (middleState.contains(key)) {
                        Tuple2<Double, Long> state = middleState.get(key);
                        Long cnt = state.f1;
                        Double threshold = state.f0;
                        if (cnt < CommonConfig.DNS_RESPONSE_DETECTION_TIME_COUNT) {
                            threshold = (threshold * cnt + median) / (cnt + 1);
                            middleState.put(key, Tuple2.of(threshold, ++cnt));
                        } else {
                            double rate = BigDecimal.valueOf(median / threshold).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            if (rate >= CommonConfig.DNS_RESPONSE_DETECTION_INFO_THRESHOLD) {
                                eventLog = new PerformanceEventLog();
                                CnRecordLog value = elements.iterator().next();
                                DetectionEventLogHandler.initDetectionEventLog(eventLog, value, "high dns response time", "ip");
                                LOG.info("high dns response time detection key:{} median:{},threshold:{},cnt:{},rate:{}", key, median, threshold, cnt, rate);
                                dnsResponseDetectionEventLog(eventLog, rate);
                            } else {
                                threshold = (threshold * cnt + median) / (cnt + 1);
                                middleState.put(key, Tuple2.of(threshold, ++cnt));
                            }
                        }
                    } else {
                        middleState.put(key, Tuple2.of(median, 1L));
                    }
                }
                DetectionEventLogHandler.outputResult(key, eventLog, previousResultDesc, out);
            }
        } catch (Exception e) {
            LOG.error("high dns response time 检测异常,key: {} \n{}", key, e);
        }
    }

    private void dnsResponseDetectionEventLog(PerformanceEventLog eventLog, double rate) {
        if (rate >= CommonConfig.DNS_RESPONSE_DETECTION_INFO_THRESHOLD && rate < CommonConfig.DNS_RESPONSE_DETECTION_LOW_THRESHOLD) {
            eventLog.setEvent_severity("info");
        } else if (rate >= CommonConfig.DNS_RESPONSE_DETECTION_LOW_THRESHOLD && rate < CommonConfig.DNS_RESPONSE_DETECTION_MEDIUM_THRESHOLD) {
            eventLog.setEvent_severity("low");
        } else if (rate >= CommonConfig.DNS_RESPONSE_DETECTION_MEDIUM_THRESHOLD && rate < CommonConfig.DNS_RESPONSE_DETECTION_HIGH_THRESHOLD) {
            eventLog.setEvent_severity("medium");
        } else if (rate >= CommonConfig.DNS_RESPONSE_DETECTION_HIGH_THRESHOLD && rate < CommonConfig.DNS_RESPONSE_DETECTION_CRITICAL_THRESHOLD) {
            eventLog.setEvent_severity("high");
        } else if (rate >= CommonConfig.DNS_RESPONSE_DETECTION_CRITICAL_THRESHOLD) {
            eventLog.setEvent_severity("critical");
        }
    }

}
