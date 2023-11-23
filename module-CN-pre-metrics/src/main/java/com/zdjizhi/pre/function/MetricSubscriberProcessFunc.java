package com.zdjizhi.pre.function;

import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;

public class MetricSubscriberProcessFunc extends AbstractMetricProcessFunc<String> {

    @Override
    public void setCommonFields(String key, CnMetricLog cnMetricLog, MetricResultLog metricResultLog) {
        metricResultLog.setSubscriber_id(key);
        metricResultLog.setImei(cnMetricLog.getImei());
        metricResultLog.setImsi(cnMetricLog.getImsi());
        metricResultLog.setPhone_number(cnMetricLog.getPhone_number());
        metricResultLog.setApn(cnMetricLog.getApn());
    }
}
