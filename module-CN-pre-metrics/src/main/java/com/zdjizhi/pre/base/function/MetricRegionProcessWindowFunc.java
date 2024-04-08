package com.zdjizhi.pre.base.function;

import com.zdjizhi.pre.base.common.CnMetricLog;
import com.zdjizhi.pre.base.common.MetricResultLog;
import org.apache.flink.api.java.tuple.Tuple3;

public class MetricRegionProcessWindowFunc extends AbstractMetricProcessFunc<Tuple3<String, String, String>> {
    private String side;

    public MetricRegionProcessWindowFunc(String side) {
        this.side = side;
    }

    @Override
    public void setCommonFields(Tuple3<String, String, String> key, CnMetricLog cnMetricLog, MetricResultLog metricResultLog) {
        metricResultLog.setCountry_region(key.f0);
        metricResultLog.setSuper_admin_area(key.f1);
        metricResultLog.setAdmin_area(key.f2);
        metricResultLog.setSide(side);
    }
}
