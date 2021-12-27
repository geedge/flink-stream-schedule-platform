package com.zdjizhi.base.utils;

import java.util.Properties;

public final class CommonConfigurations {

    private Properties propService;

    public CommonConfigurations(Properties propService){
        this.propService = propService;
    }


    public String getStringProperty(String key) {
            return propService.getProperty(key);
    }

    public Integer getIntProperty(String key) {

            return Integer.parseInt(propService.getProperty(key));

    }

    public Double getDoubleProperty(String key) {

        return Double.parseDouble(propService.getProperty(key));

    }

    public Long getLongProperty(String key) {
            return Long.parseLong(propService.getProperty(key));

    }

    public Boolean getBooleanProperty(Integer type, String key) {
            return "true".equals(propService.getProperty(key).toLowerCase().trim());
    }
}
