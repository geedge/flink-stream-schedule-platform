package com.zdjizhi.base.config;

import cn.hutool.core.io.FileUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * The Configs class provides utilities for managing configuration options.
 *
 * @author chaoc
 * @since 23.10
 */
public final class Configs {

    private static Configuration config = new Configuration();

    private static Configuration pluginConfig = new Configuration();

    public static Configuration create(final String[] args) throws IOException {
        String configPath = args[0];
        if (!FileUtil.exist(configPath)) {
            throw new RuntimeException("can not find the config path: " + configPath);
        }
        List<String> list = FileUtil.listFileNames(configPath);
        for (String fileName : list) {
            if (fileName.endsWith(".properties")) {
                Configuration configuration = ParameterTool.fromPropertiesFile(configPath + File.separator + fileName).getConfiguration();
                config.addAll(configuration);
                if ("business.properties".equals(fileName)) {
                    pluginConfig = configuration;
                }
            }
        }
        return config;
    }

    public static Configuration getPluginConfig() {
        return pluginConfig;
    }

    public static <T> T get(ConfigOption<T> option) {
        return config.get(option);
    }

    public static Properties subProperties(String prefix) {
        Properties properties = new Properties();
        if (prefix != null && prefix.length() > 0) {
            properties = config.toMap()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().startsWith(prefix))
                    .collect(Properties::new, (props, e) ->
                                    props.setProperty(e.getKey().substring(prefix.length()), e.getValue()),
                            Properties::putAll);
        }
        return properties;
    }

    private Configs() {
    }
}
