package com.zdjizhi.base.common;

import java.util.Properties;

public class SourceConfig {

    private String topic;
    private int parallelism;
    private Properties properties;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    private SourceConfig(){}

    public SourceConfig(String topic, int parallelism, Properties properties) {
        this.topic = topic;
        this.parallelism = parallelism;
        this.properties = properties;
    }
}
