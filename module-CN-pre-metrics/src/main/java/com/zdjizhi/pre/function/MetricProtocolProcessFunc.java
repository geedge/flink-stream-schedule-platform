package com.zdjizhi.pre.function;

import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;
import org.apache.flink.api.java.tuple.Tuple2;

public class MetricProtocolProcessFunc extends AbstractMetricProcessFunc<Tuple2<String, Integer>> {

    @Override
    public void setCommonFields(Tuple2<String, Integer> stringIntegerTuple2, CnMetricLog next, MetricResultLog metricResultLog) {
        metricResultLog.setCommon_l7_protocol(next.getCommon_l7_protocol());
        metricResultLog.setCommon_server_port(next.getCommon_server_port());
    }
}
