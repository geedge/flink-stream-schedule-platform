package com.zdjizhi.etl.common;

import com.zdjizhi.base.utils.CommonConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author wlh
 * @date 2021/1/6
 */
public class CommonConfig {

    private static CommonConfigurations configurations;
    private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);

    static {
        Properties propService;
        try {
            propService = new Properties();
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("etl.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载etl.properties配置文件失败");
            System.exit(1);
        }
    }
    public static final int IP_INTERNAL_OR_EXTERNAL_PATTERN = configurations.getIntProperty("ip.internal.or.external.pattern");
    public static final String IP_GEO_PATTERN_FIELD = configurations.getStringProperty("ip.geo.pattern.field");
    public static final String IP_GEO_PATTERN_VALUE = configurations.getStringProperty("ip.geo.pattern.value");

}
