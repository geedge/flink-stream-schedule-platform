package com.zdjizhi.top.common;

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
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("top-metrics.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载top-metrics.properties配置文件失败");
            System.exit(1);
        }
    }

    public static final int TOP_METRICS_MAX_ORDERNESS = configurations.getIntProperty("top.metrics.max.orderness");
    public static final int TOP_METRICS_WINDOW_TIME = configurations.getIntProperty("top.metrics.window.time");
    public static final int TOP_METRICS_ROUND_SCALE = configurations.getIntProperty("top.metrics.round.scale");
    public static final int TOP_METRICS_ROWNUM_LIMIT = configurations.getIntProperty("top.metrics.rownum.limit");

}
