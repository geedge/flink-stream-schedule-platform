package com.zdjizhi.base.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class CommonInternalConfig {

    //WebSketch
    public static final ConfigOption<String> IOC_DARKWEB_TYPE = ConfigOptions.key("ioc.darkweb.type")
            .stringType()
            .defaultValue("cn_ioc_darkweb");
    public static final ConfigOption<String> IOC_MALWARE_TYPE = ConfigOptions.key("ioc.malware.type")
            .stringType()
            .defaultValue("cn_ioc_malware");
    public static final ConfigOption<String> APPSKT_TYPE = ConfigOptions.key("appskt.type")
            .stringType()
            .defaultValue("cn_app_category_built_in");
    public static final ConfigOption<String> FQDN_ICP_TYPE = ConfigOptions.key("fqdn.icp.type")
            .stringType()
            .defaultValue("cn_fqdn_icp_built_in");
    public static final ConfigOption<String> FQDN_WHOIS_TYPE = ConfigOptions.key("fqdn.whois.type")
            .stringType()
            .defaultValue("cn_fqdn_who_is_built_in");
    public static final ConfigOption<String> FQDN_CATEGORY_TYPE = ConfigOptions.key("fqdn.category.type")
            .stringType()
            .defaultValue("cn_fqdn_category_built_in");
    public static final ConfigOption<String> IP_TYPE = ConfigOptions.key("ip.type")
            .stringType()
            .defaultValue("cn_ip_location_built_in");
    public static final ConfigOption<String> ASN_TYPE = ConfigOptions.key("asn.type")
            .stringType()
            .defaultValue("cn_ip_asn_built_in");
    public static final ConfigOption<String> IDC_RENTER_TYPE = ConfigOptions.key("idc.renter.type")
            .stringType()
            .defaultValue("cn_idc_renter_built_in");
    public static final ConfigOption<String> INTERNAL_IP_TYPE = ConfigOptions.key("internal.ip.type")
            .stringType()
            .defaultValue("cn_internal_ip_built_in");
    public static final ConfigOption<String> LINK_TYPE = ConfigOptions.key("link.type")
            .stringType()
            .defaultValue("cn_link_direction_built_in");
    public static final ConfigOption<String> DNS_TYPE = ConfigOptions.key("dns.type")
            .stringType()
            .defaultValue("cn_dns_server_info_built_in");

    //User Defined
    public static final ConfigOption<String> IP_TAG_TYPE = ConfigOptions.key("ip.tag.type")
            .stringType()
            .defaultValue("cn_ip_tag_user_defined");
    public static final ConfigOption<String> DOMAIN_TAG_TYPE = ConfigOptions.key("domain.tag.type")
            .stringType()
            .defaultValue("cn_domain_tag_user_defined");
    public static final ConfigOption<String> APP_TAG_TYPE = ConfigOptions.key("app.tag.type")
            .stringType()
            .defaultValue("cn_app_tag_user_defined");

    //AI Tagging
    public static final ConfigOption<String> PSIPHON_TYPE = ConfigOptions.key("psiphon.type")
            .stringType()
            .defaultValue("cn_psiphon3_ip");

    public static final ConfigOption<String> ETL_TOPIC = ConfigOptions.key("etl.topic")
            .stringType()
            .defaultValue("SESSION-RECORD-TEMP");

    public static final ConfigOption<Integer> INIT_CHECK = ConfigOptions.key("init.check")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> TEST_RUNNING = ConfigOptions.key("test.running")
            .intType()
            .defaultValue(0);
}
