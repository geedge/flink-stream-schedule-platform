package com.zdjizhi.pre.location.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author wlh
 * @date 2021/1/6
 */
public class CommonConfig {

    public static final ConfigOption<Integer> LOCATION_METRICS_WINDOW_TIME = ConfigOptions.key("location.metrics.window.time")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> LOCATION_METRIC_OUTPUT_PARALLELISM = ConfigOptions.key("location.metric.output.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<String> LOCATION_SUBSCRIBER_TOPIC = ConfigOptions.key("location.subscriber.topic")
            .stringType()
            .defaultValue("LOCATION-SUBSCRIBER");

    public static final ConfigOption<String> LOCATION_SUBSCRIBER_TABLE = ConfigOptions.key("location.subscriber.table")
            .stringType()
            .defaultValue("location_subscriber_local");

    public static final ConfigOption<Integer> H3_FIRST_RES = ConfigOptions.key("h3.first.res")
            .intType()
            .defaultValue(9);

    public static final ConfigOption<Integer> H3_SECOND_RES = ConfigOptions.key("h3.second.res")
            .intType()
            .defaultValue(8);

    public static final ConfigOption<Integer> H3_THIRD_RES = ConfigOptions.key("h3.third.res")
            .intType()
            .defaultValue(7);
}
