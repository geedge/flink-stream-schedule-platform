package com.zdjizhi.pre.function;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.pre.handler.CommonMetric;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricIpProcessWindowFunc extends AbstractMetricProcessFunc<String> {
    private String side;

    public MetricIpProcessWindowFunc(String side) {
        this.side = side;
    }

    @Override
    public void process(String ip, Context context, Iterable<CnMetricLog> elements, Collector<String> out) throws Exception {
        CnMetricLog next = elements.iterator().next();
        MetricResultLog metricResultLog = new MetricResultLog();
        metricResultLog.setIp(ip);
        metricResultLog.setSide(side);
        metricResultLog.setZone(next.getZone());
        metricResultLog.setStat_time(context.window().getStart()/1000);

        CommonMetric.setCommonMetric(next,metricResultLog, preMetricsRoundScale);
        metricResultLog.setAsymmetric_sessions(next.getAsymmetric_sessions());
        metricResultLog.setBulky_sessions(next.getBulky_sessions());
        metricResultLog.setCbr_streaming_sessions(next.getCbr_streaming_sessions());
        metricResultLog.setDownload_sessions(next.getDownload_sessions());
        metricResultLog.setInteractive_sessions(next.getInteractive_sessions());
        metricResultLog.setPseudo_unidirectional_sessions(next.getPseudo_unidirectional_sessions());
        metricResultLog.setStreaming_sessions(next.getStreaming_sessions());
        metricResultLog.setUnidirectional_sessions(next.getUnidirectional_sessions());
        metricResultLog.setRandom_looking_sessions(next.getRandom_looking_sessions());
        metricResultLog.setBidirectional_sessions(next.getBidirectional_sessions());
        String metricResultJsonStr = JSON.toJSONString(metricResultLog);
        out.collect(metricResultJsonStr);
    }



}
