package com.zdjizhi.pre.function;

import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;
import org.apache.flink.api.java.tuple.Tuple2;

public class MetricAsnProcessFunc extends AbstractMetricProcessFunc<Tuple2<String, String>> {
    private String side;

    public MetricAsnProcessFunc(String side) {
        this.side = side;
    }

    @Override
    public void setCommonFields(Tuple2<String, String> key, CnMetricLog cnMetricLog, MetricResultLog metricResultLog) {
        metricResultLog.setAsn(key.f0);
        metricResultLog.setIsp(key.f1);
        metricResultLog.setSide(side);
    }
}
