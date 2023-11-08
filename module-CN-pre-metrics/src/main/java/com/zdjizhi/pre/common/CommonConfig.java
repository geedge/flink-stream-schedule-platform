package com.zdjizhi.pre.common;

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

    public static final ConfigOption<String> METRIC_RELATION_TOPIC = ConfigOptions.key("metric.relation.topic")
            .stringType()
            .defaultValue("METRIC-RELATION");

    public static final ConfigOption<String> METRIC_ATTRIBUTE_TOPIC = ConfigOptions.key("metric.attribute.topic")
            .stringType()
            .defaultValue("METRIC-ATTRIBUTE");

    public static final ConfigOption<Integer> METRIC_OUTPUT_PARALLELISM = ConfigOptions.key("metric.output.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> ENTITY_RELATION_OUTPUT_PARALLELISM = ConfigOptions.key("metric.entity.relation.output.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> DYNAMIC_ATTRIBUTE_OUTPUT_PARALLELISM = ConfigOptions.key("metric.dynamic.attribute.output.parallelism")
            .intType()
            .defaultValue(1);
}
