package com.zdjizhi.base.common;

import com.zdjizhi.base.utils.CommonConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CommonInternalConfig {

    private static CommonConfigurations configurations;
    private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);

    public static final String LOCATION_SEPARATOR = ".";

    static {
        Properties propService;
        try {
            propService = new Properties();
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("common.internal.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载common.internal.properties配置文件失败", e);
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }


    public static final String IPV4_TYPE = CommonConfigurations.getStringProperty("ipv4.type");
    public static final String IPV6_TYPE = CommonConfigurations.getStringProperty("ipv6.type");
    public static final String ASNV4_TYPE = CommonConfigurations.getStringProperty("asnv4.type");
    public static final String ASNV6_TYPE = CommonConfigurations.getStringProperty("asnv6.type");
    public static final String WEBSKT_TYPE = CommonConfigurations.getStringProperty("webskt.type");
    public static final String DNS_TYPE = CommonConfigurations.getStringProperty("dns.type");
    public static final String ICP_TYPE = CommonConfigurations.getStringProperty("icp.type");
    public static final String LINK_TYPE = CommonConfigurations.getStringProperty("link.type");
    public static final String IDC_RENTER_TYPE = CommonConfigurations.getStringProperty("idc.renter.type");
    public static final String INTERNAL_IP_TYPE = CommonConfigurations.getStringProperty("internal.ip.type");
    public static final String APPSKT_TYPE = CommonConfigurations.getStringProperty("appskt.type");
    public static final String WHOIS_TYPE = CommonConfigurations.getStringProperty("whois.type");
    public static final String IOC_TYPE = CommonConfigurations.getStringProperty("ioc.type");
    public static final String IP_TAG_TYPE = CommonConfigurations.getStringProperty("ip.tag.type");
    public static final String DOMAIN_TAG_TYPE = CommonConfigurations.getStringProperty("domain.tag.type");
    public static final String APP_TAG_TYPE = CommonConfigurations.getStringProperty("app.tag.type");
    public static final String PSIPHON_TYPE = CommonConfigurations.getStringProperty("psiphon.type");
    public static final String IOC_DARKWEB_TYPE = CommonConfigurations.getStringProperty("ioc.darkweb.type");
    public static final String IOC_MALWARE_TYPE = CommonConfigurations.getStringProperty("ioc.malware.type");
    public static final String NOT_EXIST_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    public static final String ETL_TOPIC = CommonConfigurations.getStringProperty("etl.topic");
    public static final int TEST_RUNNING = CommonConfigurations.getIntProperty("test.running", 0);
}
