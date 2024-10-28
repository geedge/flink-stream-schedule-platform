package com.zdjizhi.pre.relation.function;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.pre.base.operator.AbstractFirstAggregation;
import com.zdjizhi.pre.relation.common.RelationMetricLog;

import java.util.*;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/4/8 17:58
 */
public class SubscriberAppFirstAggregation extends AbstractFirstAggregation<RelationMetricLog> {

    private final String subscriberAppRelationMetricKey = "subscriberAppRelationMetric";

    @Override
    public Map<String, Map<String, RelationMetricLog>> createAccumulator() {
        Map<String, Map<String, RelationMetricLog>> accumulator = new HashMap<>();
        accumulator.put(subscriberAppRelationMetricKey, new HashMap<>());
        return accumulator;
    }

    @Override
    public void add(CnRecordLog value, Map<String, Map<String, RelationMetricLog>> accumulator) {
        String appName = value.getCommon_app_label();
        String subscriberId = value.getSubscriber_id();

        String subscriberAppRelationKey = subscriberId + "-" + appName;
        Map<String, RelationMetricLog> subscriberAppRelationMetricMap = accumulator.get(subscriberAppRelationMetricKey);
        RelationMetricLog subscriberAppRelationMetricLog = subscriberAppRelationMetricMap.get(subscriberAppRelationKey);
        if (subscriberAppRelationMetricLog == null) {
            subscriberAppRelationMetricMap.put(subscriberAppRelationKey, initSubscriberAppRelationMetric(value));
        }
    }

    private RelationMetricLog initSubscriberAppRelationMetric(CnRecordLog cnRecordLog) {
        RelationMetricLog relationMetricLog = new RelationMetricLog(3);
        relationMetricLog.setApp_name(cnRecordLog.getCommon_app_label());
        relationMetricLog.setApp_category(cnRecordLog.getApp_category());
        relationMetricLog.setApp_subcategory(cnRecordLog.getApp_subcategory());
        relationMetricLog.setSubscriber_id(cnRecordLog.getSubscriber_id());
        relationMetricLog.setImei(cnRecordLog.getImei());
        relationMetricLog.setImsi(cnRecordLog.getImsi());
        relationMetricLog.setPhone_number(cnRecordLog.getPhone_number());
        relationMetricLog.setApn(cnRecordLog.getApn());
        List<String> tags = new ArrayList<>();
        Optional.ofNullable(cnRecordLog.getApp_tags()).ifPresent(appTags -> appTags.stream().distinct().map(tag -> "app." + tag).forEach(tags::add));
        Optional.ofNullable(cnRecordLog.getSubscriber_tags()).ifPresent(subscriberTags -> subscriberTags.stream().distinct().map(tag -> "subscriber." + tag).forEach(tags::add));
        relationMetricLog.setEntity_tags(tags);
        return relationMetricLog;
    }
}
