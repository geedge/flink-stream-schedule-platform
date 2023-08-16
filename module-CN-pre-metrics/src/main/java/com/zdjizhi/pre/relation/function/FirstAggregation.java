package com.zdjizhi.pre.relation.function;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.operator.AbstractFirstAggregation;
import com.zdjizhi.pre.relation.common.RelationMetricLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FirstAggregation extends AbstractFirstAggregation<RelationMetricLog> {

    private final String relationMetricKey = "relationMetric";
    private final String attributeMetricKey = "relationMetric";

    @Override
    public Map<String, Map<String, RelationMetricLog>> createAccumulator() {
        Map<String, Map<String, RelationMetricLog>> accumulator = new HashMap<>();
        accumulator.put(relationMetricKey, new HashMap<>());
        accumulator.put(attributeMetricKey, new HashMap<>());
        return accumulator;
    }

    @Override
    public void add(CnRecordLog value, Map<String, Map<String, RelationMetricLog>> accumulator) {
        String appName = value.getCommon_app_label();
        String domain = value.getDomain();
        String serverIp = value.getCommon_server_ip();
        String l7Protocol = value.getCommon_l7_protocol();
        int serverPort = value.getCommon_server_port();


        String relationKey = appName + "-" + domain + "-" + serverIp;
        Map<String, RelationMetricLog> relationMetricMap = accumulator.get(relationMetricKey);
        RelationMetricLog relationMetricLog = relationMetricMap.get(relationKey);
        if (relationMetricLog == null) {
            relationMetricMap.put(relationKey, initRelationMetric(value));
        }

        String attributeKey = serverIp + "-" + l7Protocol + "-" + serverPort;
        Map<String, RelationMetricLog> attributeMetricMap = accumulator.get(attributeMetricKey);
        RelationMetricLog attributeMetricLog = attributeMetricMap.get(attributeKey);
        if (attributeMetricLog == null) {
            attributeMetricMap.put(attributeKey, initAttribute(value));
        }

    }

    private RelationMetricLog initRelationMetric(CnRecordLog cnRecordLog) {
        RelationMetricLog relationMetricLog = new RelationMetricLog(1);
        relationMetricLog.setApp_name(cnRecordLog.getCommon_app_label());
        relationMetricLog.setDomain(cnRecordLog.getDomain());
        relationMetricLog.setIp(cnRecordLog.getCommon_server_ip());
        relationMetricLog.setIp_country_region(cnRecordLog.getServer_country_region());
        relationMetricLog.setIp_super_admin_area(cnRecordLog.getServer_super_admin_area());
        relationMetricLog.setIp_admin_area(cnRecordLog.getServer_admin_area());
        relationMetricLog.setIp_asn(cnRecordLog.getServer_asn());
        relationMetricLog.setIp_isp(cnRecordLog.getServer_isp());
        relationMetricLog.setDomain_category_name(cnRecordLog.getDomain_category_name());
        relationMetricLog.setDomain_category_group(cnRecordLog.getDomain_category_group());
        relationMetricLog.setApp_category(cnRecordLog.getApp_category());
        relationMetricLog.setApp_subcategory(cnRecordLog.getApp_subcategory());
        List<String> tags = new ArrayList<>();
        tags.addAll(cnRecordLog.getServer_ip_tags());
        tags.addAll(cnRecordLog.getDomain_tags());
        tags.addAll(cnRecordLog.getApp_tags());
        relationMetricLog.setEntity_tags(tags);
        return relationMetricLog;
    }

    private RelationMetricLog initAttribute(CnRecordLog cnRecordLog) {
        RelationMetricLog relationMetricLog = new RelationMetricLog(2);
        relationMetricLog.setIp(cnRecordLog.getCommon_server_ip());
        relationMetricLog.setL7_protocol(cnRecordLog.getCommon_l7_protocol());
        relationMetricLog.setPort(cnRecordLog.getCommon_server_port());
        return relationMetricLog;
    }
}
