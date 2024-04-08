package com.zdjizhi.pre.base.function;

import com.zdjizhi.pre.base.common.CnMetricLog;
import com.zdjizhi.pre.base.common.MetricResultLog;

public class MetricIpProcessWindowFunc extends AbstractMetricProcessFunc<String> {
    private String side;

    public MetricIpProcessWindowFunc(String side) {
        this.side = side;
    }

    @Override
    public void setCommonFields(String key, CnMetricLog next, MetricResultLog metricResultLog) {
        metricResultLog.setIp(key);
        metricResultLog.setSide(side);
        metricResultLog.setZone(next.getZone());
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
    }


}
