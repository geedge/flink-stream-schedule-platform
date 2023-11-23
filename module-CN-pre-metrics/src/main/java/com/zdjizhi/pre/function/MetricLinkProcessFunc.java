package com.zdjizhi.pre.function;

import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.MetricResultLog;
import org.apache.flink.api.java.tuple.Tuple12;

public class MetricLinkProcessFunc extends AbstractMetricProcessFunc<Tuple12<String, String, String, String, String, String, String, String, Long, Long ,String, String>> {

    @Override
    public void setCommonFields(Tuple12<String, String, String, String, String, String, String, String, Long, Long, String, String> key, CnMetricLog next, MetricResultLog metricResultLog) {
        metricResultLog.setClient_country_region(next.getClient_country_region());
        metricResultLog.setClient_super_admin_area(next.getClient_super_admin_area());
        metricResultLog.setClient_admin_area(next.getClient_admin_area());
        metricResultLog.setClient_zone(next.getClient_zone());
        metricResultLog.setServer_country_region(next.getServer_country_region());
        metricResultLog.setServer_super_admin_area(next.getServer_super_admin_area());
        metricResultLog.setServer_admin_area(next.getServer_admin_area());
        metricResultLog.setServer_zone(next.getServer_zone());
        metricResultLog.setCommon_out_link_id(next.getCommon_out_link_id());
        metricResultLog.setCommon_in_link_id(next.getCommon_in_link_id());
        metricResultLog.setOut_link_direction(next.getOut_link_direction());
        metricResultLog.setIn_link_direction(next.getIn_link_direction());
    }
}
