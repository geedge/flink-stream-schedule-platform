package com.zdjizhi.pre.base.function;

import com.zdjizhi.pre.base.common.CnMetricLog;
import com.zdjizhi.pre.base.common.MetricResultLog;

public class MetricDomainProcessFunc extends AbstractMetricProcessFunc<String> {

    @Override
    public void setCommonFields(String s, CnMetricLog next, MetricResultLog metricResultLog) {
        metricResultLog.setDomain(next.getDomain());
        metricResultLog.setDomain_sld(next.getDomain_sld());
        metricResultLog.setDomain_category_group(next.getDomain_category_group());
        metricResultLog.setDomain_category_name(next.getDomain_category_name());
    }
}
