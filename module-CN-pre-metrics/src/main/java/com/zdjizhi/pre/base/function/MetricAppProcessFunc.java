package com.zdjizhi.pre.base.function;

import com.zdjizhi.pre.base.common.CnMetricLog;
import com.zdjizhi.pre.base.common.MetricResultLog;

public class MetricAppProcessFunc extends AbstractMetricProcessFunc<String> {

    @Override
    public void setCommonFields(String key, CnMetricLog next, MetricResultLog metricResultLog) {
        metricResultLog.setCommon_app_label(next.getCommon_app_label());
        metricResultLog.setApp_category(next.getApp_category());
        metricResultLog.setApp_company(next.getApp_company());
        metricResultLog.setApp_subcategory(next.getApp_subcategory());
    }
}
