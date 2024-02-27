package com.zdjizhi.pre.base.function;

import com.zdjizhi.pre.base.common.CnMetricLog;
import com.zdjizhi.pre.base.common.MetricResultLog;
import org.apache.flink.api.java.tuple.Tuple2;


public class MetricSubscriberAppProcessFunc extends AbstractMetricProcessFunc<Tuple2<String, String>> {

    @Override
    public void setCommonFields(Tuple2<String, String> key, CnMetricLog cnMetricLog, MetricResultLog metricResultLog) {
        metricResultLog.setSubscriber_id(key.f0);
        metricResultLog.setCommon_app_label(key.f1);
        metricResultLog.setImei(cnMetricLog.getImei());
        metricResultLog.setImsi(cnMetricLog.getImsi());
        metricResultLog.setPhone_number(cnMetricLog.getPhone_number());
        metricResultLog.setApn(cnMetricLog.getApn());
    }
}
