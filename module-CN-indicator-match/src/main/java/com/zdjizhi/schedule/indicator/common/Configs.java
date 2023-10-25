package com.zdjizhi.schedule.indicator.common;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The Configs class provides utilities for managing configuration options.
 *
 * @author chaoc
 * @since 23.10
 */
public final class Configs {

    /**
     * Prefix for Kafka sink properties in configuration.
     */
    public static final String SINK_KAFKA_PROPERTIES_PREFIX = "sink.kafka.props.";

    /**
     * Configuration option for defining the Kafka sink topic name.
     */
    public static final ConfigOption<String> SINK_TOPIC_NAME = ConfigOptions.key("sink.kafka.topic")
            .stringType()
            .noDefaultValue()
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


    /**
     * Creates a configuration based on command-line arguments and properties file.
     *
     * @param args The command-line arguments.
     * @return A Configuration object.
     */
    public static Configuration create(final String[] args) {
        final Configuration config = create();
        if (args != null && args.length > 0) {
            try {
                final ParameterTool tool =
                        ParameterTool.fromPropertiesFile(args[0]);
                config.addAll(tool.getConfiguration());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return config;
    }

    /**
     * Creates a default configuration based on properties.
     *
     * @return A Configuration object with default properties.
     */
    public static Configuration create() {
        final Configuration config = new Configuration();
        final Properties properties = DefaultConfigLazyLoader.PROPS;
        for (final String name : properties.stringPropertyNames()) {
            config.setString(name, properties.getProperty(name));
        }
        return config;
    }

    /**
     * Retrieves properties from a Configuration object with a given prefix.
     *
     * @param config The Configuration object to extract properties from.
     * @param prefix The prefix for property keys to retrieve (can be null for all properties).
     * @return A Properties object containing the extracted properties.
     */
    public static Properties subProperties(final Configuration config, final String prefix) {
        if (prefix == null) {
            final Properties props = new Properties();
            props.putAll(config.toMap());
            return props;
        }
        return config.toMap()
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .collect(Properties::new, (props, e) ->
                                props.setProperty(e.getKey().substring(prefix.length()), e.getValue()),
                        Properties::putAll);
    }

    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    private static final class DefaultConfigLazyLoader {

        private static final String DEFAULT_PROPERTIES_NAME = "indicator-match.properties";

        public static final Properties PROPS = new Properties();

        static {
            try (final InputStream resource =
                         Configs.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTIES_NAME)) {
                PROPS.load(resource);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Configs() {
    }
}
