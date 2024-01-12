package com.zdjizhi.etl.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author wlh
 * @date 2021/1/6
 */
public class CommonConfig {

    public static final ConfigOption<Integer> IP_INTERNAL_OR_EXTERNAL_PATTERN = ConfigOptions.key("ip.internal.or.external.pattern")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<String> IP_GEO_PATTERN_FIELD = ConfigOptions.key("ip.geo.pattern.field")
            .stringType()
            .defaultValue("province");

    public static final ConfigOption<String> IP_GEO_PATTERN_VALUE = ConfigOptions.key("ip.geo.pattern.value")
            .stringType()
            .defaultValue("beijing,shanghai,tianjin");

    public static final ConfigOption<String> L7_PROTOCOL = ConfigOptions.key("l7.protocol")
            .stringType()
            .defaultValue("DHCP,DNS,FTP,GRE,GTP,HTTP,HTTPS,ICMP,IMAP,IMAPS,IPSEC,ISAKMP,XMPP,L2TP,LDAP,MMS,NETBIOS,NETFLOW,NTP,POP3,POP3S,RDP,PPTP,RADIUS,RTCP,RTP,RTSP,SIP,SMB,SMTP,SMTPS,SNMP,SSDP,SSH,SSL,STUN,TELNET,TFTP,OPENVPN,RTMP,TEREDO,FTPS,DTLS,SPDY,BJNP,QUIC,MDNS,Unknown TCP,Unknown UDP,Unknown Other,IKE,MAIL,SOCKS,DoH,SLP,SSL with ESNI,ISATAP,Stratum,SSL with ECH");

    public static final ConfigOption<String> GATEWAY_HOST = ConfigOptions.key("gateway.host")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> GATEWAY_PORT = ConfigOptions.key("gateway.port")
            .stringType()
            .defaultValue("9999");
    public static final ConfigOption<String> KNOWLEDGE_PATH = ConfigOptions.key("knowledge.path")
            .stringType()
            .defaultValue("/v1/knowledge_base");
    public static final ConfigOption<Integer> KNOWLEDGE_URL_INTERVAL_MS = ConfigOptions.key("knowledge.url.interval.ms")
            .intType()
            .defaultValue(30000);
}
