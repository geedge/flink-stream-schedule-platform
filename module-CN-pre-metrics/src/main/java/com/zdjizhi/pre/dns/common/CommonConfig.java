package com.zdjizhi.pre.dns.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class CommonConfig {

    public static final ConfigOption<Integer> DNS_PRE_METRICS_WINDOW_TIME = ConfigOptions.key("dns.pre.metrics.window.time")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> DNS_PRE_METRICS_ROUND_SCALE = ConfigOptions.key("dns.pre.metrics.round.scale")
            .intType()
            .defaultValue(4);

    public static final ConfigOption<String> METRIC_DNS_SERVER_IP_TOPIC = ConfigOptions.key("dns.metric.server.ip.topic")
            .stringType()
            .defaultValue("METRIC-DNS-SERVER-IP");

    public static final ConfigOption<String> METRIC_DNS_QNAME_TOPIC = ConfigOptions.key("dns.metric.qname.topic")
            .stringType()
            .defaultValue("METRIC-DNS-QNAME");

    public static final ConfigOption<String> METRIC_DNS_QTYPE_TOPIC = ConfigOptions.key("dns.metric.qtype.topic")
            .stringType()
            .defaultValue("METRIC-DNS-QTYPE");

    public static final ConfigOption<String> METRIC_DNS_RCODE_TOPIC = ConfigOptions.key("dns.metric.rcode.topic")
            .stringType()
            .defaultValue("METRIC-DNS-RCODE");

    public static final ConfigOption<String> METRIC_DNS_RRA_TOPIC = ConfigOptions.key("dns.metric.rra.topic")
            .stringType()
            .defaultValue("METRIC-DNS-RR-A");

    public static final ConfigOption<String> METRIC_DNS_RRAAAA_TOPIC = ConfigOptions.key("dns.metric.rraaaa.topic")
            .stringType()
            .defaultValue("METRIC-DNS-RR-AAAA");

    public static final ConfigOption<String> METRIC_DNS_RRCNAME_TOPIC = ConfigOptions.key("dns.metric.rrcname.topic")
            .stringType()
            .defaultValue("METRIC-DNS-RR-CNAME");

    public static final ConfigOption<Integer> DNS_METRIC_OUTPUT_PARALLELISM = ConfigOptions.key("dns.metric.output.parallelism")
            .intType()
            .defaultValue(1);
}
