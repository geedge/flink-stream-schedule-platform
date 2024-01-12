package com.zdjizhi.schedule.indicator.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * The Configs class provides utilities for managing configuration options.
 *
 * @author chaoc
 * @since 23.10
 */
public final class CommonConfig {

    /**
     * Prefix for Kafka sink properties in configuration.
     */
    public static final String SINK_KAFKA_PROPERTIES_PREFIX = "sink.kafka.props.";

    /**
     * Configuration option for defining the Kafka sink topic name.
     */
    public static final ConfigOption<String> SINK_TOPIC_NAME = ConfigOptions.key("sink.kafka.topic")
            .stringType()
            .defaultValue("SECURITY-EVENT-CN")
            .withDescription("The name of the Kafka sink topic.");

    public static final ConfigOption<String> SINK_TABLE_NAME = ConfigOptions.key("sink.clickhouse.table")
            .stringType()
            .defaultValue("cn_security_event_local")
            .withDescription("The name of the Kafka sink topic.");

    /**
     * Configuration option for the full URL of a rule.
     */
    public static final ConfigOption<String> RULE_FULL_URL = ConfigOptions.key("rule.full.url")
            .stringType()
            .noDefaultValue()
            .withDescription("The full URL of a rule.");

    /**
     * Configuration option for the incremental URL of a rule.
     */
    public static final ConfigOption<String> RULE_INC_URL = ConfigOptions.key("rule.inc.url")
            .stringType()
            .noDefaultValue()
            .withDescription("The incremental URL of a rule.");

    /**
     * Configuration option for the authentication token used in the Rule API.
     */
    public static final ConfigOption<String> RULE_API_TOKEN = ConfigOptions.key("rule.authorization.token")
            .stringType()
            .defaultValue("1a653ea0-d39b-4246-94b0-1ba95db4b6a7")
            .withDescription("Authentication token for the Rule API.");

    /**
     * Configuration option for the asynchronous refresh interval of the rule repository.
     * Specifies how often, in milliseconds, the rule repository is queried for updates.
     */
    public static final ConfigOption<Long> RULE_ASYNC_FLUSH_INTERVAL_MS =
            ConfigOptions.key("rule.async.flush.interval.ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("The asynchronous refresh interval of the rule repository, in milliseconds.");
}
