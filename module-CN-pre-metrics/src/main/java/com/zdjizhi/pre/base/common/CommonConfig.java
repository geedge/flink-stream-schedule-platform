package com.zdjizhi.pre.base.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author wlh
 * @date 2021/1/6
 */
public class CommonConfig {

    public static final ConfigOption<Integer> PRE_METRICS_WINDOW_TIME = ConfigOptions.key("pre.metrics.window.time")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> PRE_METRICS_ROUND_SCALE = ConfigOptions.key("pre.metrics.round.scale")
            .intType()
            .defaultValue(4);

    public static final ConfigOption<String> METRIC_IP_TOPIC = ConfigOptions.key("metric.ip.topic")
            .stringType()
            .defaultValue("METRIC-IP");

    public static final ConfigOption<String> METRIC_REGION_TOPIC = ConfigOptions.key("metric.region.topic")
            .stringType()
            .defaultValue("METRIC-REGION");

    public static final ConfigOption<String> METRIC_ASN_TOPIC = ConfigOptions.key("metric.asn.topic")
            .stringType()
            .defaultValue("METRIC-ASN");

    public static final ConfigOption<String> METRIC_APPLICATION_TOPIC = ConfigOptions.key("metric.application.topic")
            .stringType()
            .defaultValue("METRIC-APPLICATION");

    public static final ConfigOption<String> METRIC_DOMAIN_TOPIC = ConfigOptions.key("metric.domain.topic")
            .stringType()
            .defaultValue("METRIC-DOMAIN");

    public static final ConfigOption<String> METRIC_PROTOCOL_TOPIC = ConfigOptions.key("metric.protocol.topic")
            .stringType()
            .defaultValue("METRIC-PROTOCOL");

    public static final ConfigOption<String> METRIC_LINK_TOPIC = ConfigOptions.key("metric.link.topic")
            .stringType()
            .defaultValue("METRIC-LINK");

    public static final ConfigOption<String> METRIC_SUBSCRIBER_TOPIC = ConfigOptions.key("metric.subscriber.topic")
            .stringType()
            .defaultValue("METRIC-SUBSCRIBER");

    public static final ConfigOption<String> METRIC_SUBSCRIBER_APP_TOPIC = ConfigOptions.key("metric.subscriber_app.topic")
            .stringType()
            .defaultValue("METRIC-SUBSCRIBER-APP");

    public static final ConfigOption<String> METRIC_RELATION_TOPIC = ConfigOptions.key("metric.relation.topic")
            .stringType()
            .defaultValue("METRIC-RELATION");

    public static final ConfigOption<String> METRIC_ATTRIBUTE_TOPIC = ConfigOptions.key("metric.attribute.topic")
            .stringType()
            .defaultValue("METRIC-ATTRIBUTE");

    public static final ConfigOption<String> METRIC_SUBSCRIBER_APP_RELATION_TOPIC = ConfigOptions.key("metric.subscriber_app_relation.topic")
            .stringType()
            .defaultValue("METRIC-RELATION-SUBSCRIBER-APP");

    public static final ConfigOption<String> METRIC_IP_TABLE = ConfigOptions.key("metric.ip.table")
            .stringType()
            .defaultValue("metric_ip_local");

    public static final ConfigOption<String> METRIC_REGION_TABLE = ConfigOptions.key("metric.region.table")
            .stringType()
            .defaultValue("metric_region_local");

    public static final ConfigOption<String> METRIC_ASN_TABLE = ConfigOptions.key("metric.asn.table")
            .stringType()
            .defaultValue("metric_asn_local");

    public static final ConfigOption<String> METRIC_APPLICATION_TABLE = ConfigOptions.key("metric.application.table")
            .stringType()
            .defaultValue("metric_application_local");

    public static final ConfigOption<String> METRIC_DOMAIN_TABLE = ConfigOptions.key("metric.domain.table")
            .stringType()
            .defaultValue("metric_domain_local");

    public static final ConfigOption<String> METRIC_PROTOCOL_TABLE = ConfigOptions.key("metric.protocol.table")
            .stringType()
            .defaultValue("metric_protocol_local");

    public static final ConfigOption<String> METRIC_LINK_TABLE = ConfigOptions.key("metric.link.table")
            .stringType()
            .defaultValue("metric_link_local");

    public static final ConfigOption<String> METRIC_SUBSCRIBER_APP_TABLE = ConfigOptions.key("metric.subscriber_app.table")
            .stringType()
            .defaultValue("metric_subscriber_app_local");

    public static final ConfigOption<String> METRIC_RELATION_TABLE = ConfigOptions.key("metric.relation.table")
            .stringType()
            .defaultValue("metric_relation_local");

    public static final ConfigOption<String> METRIC_ATTRIBUTE_TABLE = ConfigOptions.key("metric.attribute.table")
            .stringType()
            .defaultValue("metric_ip_dynamic_attribute_local");

    public static final ConfigOption<String> METRIC_SUBSCRIBER_APP_RELATION_TABLE = ConfigOptions.key("metric.subscriber_app_relation.table")
            .stringType()
            .defaultValue("metric_relation_subscriber_app_local");

    public static final ConfigOption<Integer> METRIC_OUTPUT_PARALLELISM = ConfigOptions.key("metric.output.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> ENTITY_RELATION_OUTPUT_PARALLELISM = ConfigOptions.key("metric.entity.relation.output.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> DYNAMIC_ATTRIBUTE_OUTPUT_PARALLELISM = ConfigOptions.key("metric.dynamic.attribute.output.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> SUBSCRIBER_APP_RELATION_OUTPUT_PARALLELISM = ConfigOptions.key("metric.subscriber.app.relation.output.parallelism")
            .intType()
            .defaultValue(1);
}
