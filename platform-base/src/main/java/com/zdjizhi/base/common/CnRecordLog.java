package com.zdjizhi.base.common;

import com.alibaba.fastjson2.annotation.JSONField;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CnRecordLog implements Serializable {
    //Common Attributes
    @JSONField(name = "recv_time")
    private long common_recv_time;
    @JSONField(name = "log_id")
    private long common_log_id;
    @JSONField(name = "flags")
    private long common_flags;
    @JSONField(name = "start_timestamp_ms")
    private long common_start_time;
    @JSONField(name = "end_timestamp_ms")
    private long common_end_time;
    @JSONField(name = "duration_ms")
    private long common_con_duration_ms;
    @JSONField(name = "decoded_as")
    private String common_schema_type;
    @JSONField(name = "client_ip")
    private String common_client_ip;
    @JSONField(name = "server_ip")
    private String common_server_ip;
    @JSONField(name = "client_port")
    private int common_client_port;
    @JSONField(name = "server_port")
    private int common_server_port;
    @JSONField(name = "app")
    private String common_app_label;
    @JSONField(name = "app_transition")
    private String common_app_full_path;
    @JSONField(name = "decoded_path")
    private String protocol_path;
    @JSONField(serialize = false)
    private String common_l4_protocol;
    @JSONField(name = "l7_protocol")
    private String common_l7_protocol;
    @JSONField(name = "out_link_id")
    private Long common_out_link_id;
    @JSONField(name = "in_link_id")
    private Long common_in_link_id;

    //Source
    private String subscriber_id;
    private String imei;
    private String imsi;
    private String phone_number;
    private String apn;

    //HTTP Attributes
    private String http_host;
    private String http_url;
    private String http_response_line;
    private int http_status_code;

    //SSL Attributes
    private String ssl_sni;
    private String ssl_san;
    private String ssl_ja3_hash;
    private String ssl_ja3s_hash;
    private String ssl_cert_issuer;
    private String ssl_cert_subject;

    //DNS Attributes
    private int dns_qr;
    private int dns_opcode;
    private int dns_aa;
    private int dns_rcode;
    private String dns_qname;
    private int dns_qtype;
    private int dns_qclass;
    private int dns_sub;
    private String dns_rr;

    //Tags
    private String out_link_direction;
    private String in_link_direction;

    private String domain;
    private String domain_sld;
    private String domain_category_name;
    private String domain_category_group;
    private String domain_reputation_level;
    private String domain_icp_company_name;
    private String domain_whois_org;
    private List<String> domain_tags = new ArrayList<>();

    private String client_zone;
    private String client_country_region;
    private String client_super_admin_area;
    private String client_admin_area;
    private Double client_longitude;
    private Double client_latitude;
    private String client_isp;
    private String client_asn;
    private List<String> client_ip_tags = new ArrayList<>();

    private String server_zone;
    private String server_country_region;
    private String server_super_admin_area;
    private String server_admin_area;
    private Double server_longitude;
    private Double server_latitude;
    private String server_isp;
    private String server_asn;
    private List<String> server_ip_tags = new ArrayList<>();

    private Double subscriber_longitude;
    private Double subscriber_latitude;
    private String mail_account;
    private String wx_account;

    private String app_category;
    private String app_subcategory;
    private String app_company;
    private String app_company_category;
    private List<String> app_tags = new ArrayList<>();

    //Metrics
    @JSONField(name = "sent_pkts")
    private long common_c2s_pkt_num;
    @JSONField(name = "sent_bytes")
    private long common_c2s_byte_num;
    @JSONField(name = "received_pkts")
    private long common_s2c_pkt_num;
    @JSONField(name = "received_bytes")
    private long common_s2c_byte_num;
    @JSONField(name = "sessions")
    private long common_sessions = 1;
    @JSONField(name = "tcp_c2s_lost_bytes")
    private long common_c2s_tcp_lostlen;
    @JSONField(name = "tcp_s2c_lost_bytes")
    private long common_s2c_tcp_lostlen;
    @JSONField(name = "tcp_c2s_o3_pkts")
    private long common_c2s_tcp_unorder_num;
    @JSONField(name = "tcp_s2c_o3_pkts")
    private long common_s2c_tcp_unorder_num;
    @JSONField(name = "tcp_c2s_rtx_bytes")
    private long common_c2s_byte_retrans;
    @JSONField(name = "tcp_s2c_rtx_bytes")
    private long common_s2c_byte_retrans;
    @JSONField(name = "tcp_c2s_rtx_pkts")
    private long common_c2s_pkt_retrans;
    @JSONField(name = "tcp_s2c_rtx_pkts")
    private long common_s2c_pkt_retrans;
    @JSONField(name = "tcp_rtt_ms")
    private long common_establish_latency_ms;
    @JSONField(name = "http_response_latency_ms")
    private long http_response_latency_ms;
    @JSONField(name = "ssl_handshake_latency_ms")
    private long ssl_con_latency_ms;
    @JSONField(name = "dns_response_latency_ms")
    private long dns_response_latency_ms;

    //逻辑过程需要增加的字段,不属于业务字段
    @JSONField(serialize = false)
    private long traffic_inbound_byte;
    @JSONField(serialize = false)
    private long traffic_inbound_pkt;
    @JSONField(serialize = false)
    private long traffic_outbound_byte;
    @JSONField(serialize = false)
    private long traffic_outbound_pkt;
    @JSONField(serialize = false)
    private long traffic_internal_byte;
    @JSONField(serialize = false)
    private long traffic_internal_pkt;
    @JSONField(serialize = false)
    private long traffic_through_byte;
    @JSONField(serialize = false)
    private long traffic_through_pkt;

    @JSONField(serialize = false)
    private long internal_query_num;
    @JSONField(serialize = false)
    private long external_query_num;

    private List<Long> rule_id_list = new ArrayList<>();

    private List<String> ioc_type_list = new ArrayList<>();

    public List<String> getClient_ip_tags() {
        return Collections.unmodifiableList(client_ip_tags);
    }

    public void putClient_ip_tag(String client_ip_tag) {
        client_ip_tags.add("ip." + client_ip_tag);
    }

    public void putClient_ip_tags(List<String> client_ip_tags) {
        for (String client_ip_tag : client_ip_tags) {
            putClient_ip_tag(client_ip_tag);
        }
    }

    public List<String> getServer_ip_tags() {
        return Collections.unmodifiableList(server_ip_tags);
    }

    public void putServer_ip_tag(String server_ip_tag) {
        server_ip_tags.add("ip." + server_ip_tag);
    }

    public void putServer_ip_tags(List<String> server_ip_tags) {
        for (String server_ip_tag : server_ip_tags) {
            putServer_ip_tag(server_ip_tag);
        }
    }

    public List<String> getDomain_tags() {
        return Collections.unmodifiableList(domain_tags);
    }

    public void putDomain_tag(String domain_tag) {
        domain_tags.add("domain." + domain_tag);
    }

    public void putDomain_tags(List<String> domain_tags) {
        for (String domain_tag : domain_tags) {
            putDomain_tag(domain_tag);
        }
    }

    public List<String> getApp_tags() {
        return Collections.unmodifiableList(app_tags);
    }

    public void putApp_tag(String app_tag) {
        app_tags.add("app." + app_tag);
    }

    public void putApp_tags(List<String> app_tags) {
        for (String app_tag : app_tags) {
            putApp_tag(app_tag);
        }
    }

    public long getInternal_query_num() {
        return internal_query_num;
    }

    public void setInternal_query_num(long internal_query_num) {
        this.internal_query_num = internal_query_num;
    }

    public long getExternal_query_num() {
        return external_query_num;
    }

    public void setExternal_query_num(long external_query_num) {
        this.external_query_num = external_query_num;
    }

    public long getTraffic_inbound_byte() {
        return traffic_inbound_byte;
    }

    public void setTraffic_inbound_byte(long traffic_inbound_byte) {
        this.traffic_inbound_byte = traffic_inbound_byte;
    }

    public long getTraffic_inbound_pkt() {
        return traffic_inbound_pkt;
    }

    public void setTraffic_inbound_pkt(long traffic_inbound_pkt) {
        this.traffic_inbound_pkt = traffic_inbound_pkt;
    }

    public long getTraffic_outbound_byte() {
        return traffic_outbound_byte;
    }

    public void setTraffic_outbound_byte(long traffic_outbound_byte) {
        this.traffic_outbound_byte = traffic_outbound_byte;
    }

    public long getTraffic_outbound_pkt() {
        return traffic_outbound_pkt;
    }

    public void setTraffic_outbound_pkt(long traffic_outbound_pkt) {
        this.traffic_outbound_pkt = traffic_outbound_pkt;
    }

    public long getTraffic_internal_byte() {
        return traffic_internal_byte;
    }

    public void setTraffic_internal_byte(long traffic_internal_byte) {
        this.traffic_internal_byte = traffic_internal_byte;
    }

    public long getTraffic_internal_pkt() {
        return traffic_internal_pkt;
    }

    public void setTraffic_internal_pkt(long traffic_internal_pkt) {
        this.traffic_internal_pkt = traffic_internal_pkt;
    }

    public long getTraffic_through_byte() {
        return traffic_through_byte;
    }

    public void setTraffic_through_byte(long traffic_through_byte) {
        this.traffic_through_byte = traffic_through_byte;
    }

    public long getTraffic_through_pkt() {
        return traffic_through_pkt;
    }

    public void setTraffic_through_pkt(long traffic_through_pkt) {
        this.traffic_through_pkt = traffic_through_pkt;
    }

    public long getCommon_con_duration_ms() {
        return common_con_duration_ms;
    }

    public void setCommon_con_duration_ms(long common_con_duration_ms) {
        this.common_con_duration_ms = common_con_duration_ms;
    }

    public long getCommon_start_time() {
        return common_start_time;
    }

    public void setCommon_start_time(long common_start_time) {
        this.common_start_time = common_start_time;
    }

    public String getClient_asn() {
        return client_asn;
    }

    public void setClient_asn(String client_asn) {
        this.client_asn = client_asn;
    }

    public String getServer_asn() {
        return server_asn;
    }

    public void setServer_asn(String server_asn) {
        this.server_asn = server_asn;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public long getCommon_end_time() {
        return common_end_time;
    }

    public void setCommon_end_time(long common_end_time) {
        this.common_end_time = common_end_time;
    }

    public String getCommon_schema_type() {
        return common_schema_type;
    }

    public void setCommon_schema_type(String common_schema_type) {
        this.common_schema_type = common_schema_type;
    }

    public String getCommon_client_ip() {
        return common_client_ip;
    }

    public void setCommon_client_ip(String common_client_ip) {
        this.common_client_ip = common_client_ip;
    }

    public String getCommon_server_ip() {
        return common_server_ip;
    }

    public void setCommon_server_ip(String common_server_ip) {
        this.common_server_ip = common_server_ip;
    }

    public int getCommon_client_port() {
        return common_client_port;
    }

    public void setCommon_client_port(int common_client_port) {
        this.common_client_port = common_client_port;
    }

    public int getCommon_server_port() {
        return common_server_port;
    }

    public void setCommon_server_port(int common_server_port) {
        this.common_server_port = common_server_port;
    }

    public String getCommon_app_label() {
        return common_app_label;
    }

    public void setCommon_app_label(String common_app_label) {
        this.common_app_label = common_app_label;
    }

    public String getProtocol_path() {
        return protocol_path;
    }

    public void setProtocol_path(String protocol_path) {
        this.protocol_path = protocol_path;
    }

    public String getCommon_l4_protocol() {
        return common_l4_protocol;
    }

    public void setCommon_l4_protocol(String common_l4_protocol) {
        this.common_l4_protocol = common_l4_protocol;
    }

    public String getCommon_l7_protocol() {
        return common_l7_protocol;
    }

    public void setCommon_l7_protocol(String common_l7_protocol) {
        this.common_l7_protocol = common_l7_protocol;
    }

    public String getHttp_host() {
        return http_host;
    }

    public void setHttp_host(String http_host) {
        this.http_host = http_host;
    }

    public String getSsl_sni() {
        return ssl_sni;
    }

    public void setSsl_sni(String ssl_sni) {
        this.ssl_sni = ssl_sni;
    }

    public String getHttp_url() {
        return http_url;
    }

    public void setHttp_url(String http_url) {
        this.http_url = http_url;
    }

    public String getHttp_response_line() {
        return http_response_line;
    }

    public void setHttp_response_line(String http_response_line) {
        this.http_response_line = http_response_line;
    }

    public int getHttp_status_code() {
        return http_status_code;
    }

    public void setHttp_status_code(int http_status_code) {
        this.http_status_code = http_status_code;
    }

    public Long getCommon_out_link_id() {
        return common_out_link_id;
    }

    public void setCommon_out_link_id(Long common_out_link_id) {
        this.common_out_link_id = common_out_link_id;
    }

    public Long getCommon_in_link_id() {
        return common_in_link_id;
    }

    public void setCommon_in_link_id(Long common_in_link_id) {
        this.common_in_link_id = common_in_link_id;
    }

    public String getSubscriber_id() {
        return subscriber_id;
    }

    public void setSubscriber_id(String subscriber_id) {
        this.subscriber_id = subscriber_id;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getPhone_number() {
        return phone_number;
    }

    public void setPhone_number(String phone_number) {
        this.phone_number = phone_number;
    }

    public String getApn() {
        return apn;
    }

    public void setApn(String apn) {
        this.apn = apn;
    }

    public String getSsl_san() {
        return ssl_san;
    }

    public void setSsl_san(String ssl_san) {
        this.ssl_san = ssl_san;
    }

    public String getSsl_ja3_hash() {
        return ssl_ja3_hash;
    }

    public void setSsl_ja3_hash(String ssl_ja3_hash) {
        this.ssl_ja3_hash = ssl_ja3_hash;
    }

    public String getSsl_cert_issuer() {
        return ssl_cert_issuer;
    }

    public void setSsl_cert_issuer(String ssl_cert_issuer) {
        this.ssl_cert_issuer = ssl_cert_issuer;
    }

    public String getSsl_cert_subject() {
        return ssl_cert_subject;
    }

    public void setSsl_cert_subject(String ssl_cert_subject) {
        this.ssl_cert_subject = ssl_cert_subject;
    }

    public int getDns_qr() {
        return dns_qr;
    }

    public void setDns_qr(int dns_qr) {
        this.dns_qr = dns_qr;
    }

    public int getDns_opcode() {
        return dns_opcode;
    }

    public void setDns_opcode(int dns_opcode) {
        this.dns_opcode = dns_opcode;
    }

    public int getDns_aa() {
        return dns_aa;
    }

    public void setDns_aa(int dns_aa) {
        this.dns_aa = dns_aa;
    }

    public int getDns_rcode() {
        return dns_rcode;
    }

    public void setDns_rcode(int dns_rcode) {
        this.dns_rcode = dns_rcode;
    }

    public String getDns_qname() {
        return dns_qname;
    }

    public void setDns_qname(String dns_qname) {
        this.dns_qname = dns_qname;
    }

    public int getDns_qtype() {
        return dns_qtype;
    }

    public void setDns_qtype(int dns_qtype) {
        this.dns_qtype = dns_qtype;
    }

    public int getDns_qclass() {
        return dns_qclass;
    }

    public void setDns_qclass(int dns_qclass) {
        this.dns_qclass = dns_qclass;
    }

    public int getDns_sub() {
        return dns_sub;
    }

    public void setDns_sub(int dns_sub) {
        this.dns_sub = dns_sub;
    }

    public String getDns_rr() {
        return dns_rr;
    }

    public void setDns_rr(String dns_rr) {
        this.dns_rr = dns_rr;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getDomain_category_name() {
        return domain_category_name;
    }

    public void setDomain_category_name(String domain_category_name) {
        this.domain_category_name = domain_category_name;
    }

    public String getDomain_category_group() {
        return domain_category_group;
    }

    public void setDomain_category_group(String domain_category_group) {
        this.domain_category_group = domain_category_group;
    }

    public String getDomain_reputation_level() {
        return domain_reputation_level;
    }

    public void setDomain_reputation_level(String domain_reputation_level) {
        this.domain_reputation_level = domain_reputation_level;
    }

    public String getDomain_icp_company_name() {
        return domain_icp_company_name;
    }

    public void setDomain_icp_company_name(String domain_icp_company_name) {
        this.domain_icp_company_name = domain_icp_company_name;
    }

    public String getDomain_whois_org() {
        return domain_whois_org;
    }

    public void setDomain_whois_org(String domain_whois_org) {
        this.domain_whois_org = domain_whois_org;
    }

    public String getClient_zone() {
        return client_zone;
    }

    public void setClient_zone(String client_zone) {
        this.client_zone = client_zone;
    }

    public String getServer_zone() {
        return server_zone;
    }

    public void setServer_zone(String server_zone) {
        this.server_zone = server_zone;
    }

    public Double getClient_longitude() {
        return client_longitude;
    }

    public void setClient_longitude(Double client_longitude) {
        this.client_longitude = client_longitude;
    }

    public Double getClient_latitude() {
        return client_latitude;
    }

    public void setClient_latitude(Double client_latitude) {
        this.client_latitude = client_latitude;
    }

    public Double getServer_longitude() {
        return server_longitude;
    }

    public void setServer_longitude(Double server_longitude) {
        this.server_longitude = server_longitude;
    }

    public Double getServer_latitude() {
        return server_latitude;
    }

    public void setServer_latitude(Double server_latitude) {
        this.server_latitude = server_latitude;
    }

    public String getApp_category() {
        return app_category;
    }

    public void setApp_category(String app_category) {
        this.app_category = app_category;
    }

    public String getApp_subcategory() {
        return app_subcategory;
    }

    public void setApp_subcategory(String app_subcategory) {
        this.app_subcategory = app_subcategory;
    }

    public long getCommon_c2s_pkt_num() {
        return common_c2s_pkt_num;
    }

    public void setCommon_c2s_pkt_num(long common_c2s_pkt_num) {
        this.common_c2s_pkt_num = common_c2s_pkt_num;
    }

    public long getCommon_c2s_byte_num() {
        return common_c2s_byte_num;
    }

    public void setCommon_c2s_byte_num(long common_c2s_byte_num) {
        this.common_c2s_byte_num = common_c2s_byte_num;
    }

    public long getCommon_s2c_pkt_num() {
        return common_s2c_pkt_num;
    }

    public void setCommon_s2c_pkt_num(long common_s2c_pkt_num) {
        this.common_s2c_pkt_num = common_s2c_pkt_num;
    }

    public long getCommon_s2c_byte_num() {
        return common_s2c_byte_num;
    }

    public void setCommon_s2c_byte_num(long common_s2c_byte_num) {
        this.common_s2c_byte_num = common_s2c_byte_num;
    }

    public long getCommon_sessions() {
        return common_sessions;
    }

    public void setCommon_sessions(long common_sessions) {
        this.common_sessions = common_sessions;
    }

    public long getCommon_c2s_tcp_lostlen() {
        return common_c2s_tcp_lostlen;
    }

    public void setCommon_c2s_tcp_lostlen(long common_c2s_tcp_lostlen) {
        this.common_c2s_tcp_lostlen = common_c2s_tcp_lostlen;
    }

    public long getCommon_s2c_tcp_lostlen() {
        return common_s2c_tcp_lostlen;
    }

    public void setCommon_s2c_tcp_lostlen(long common_s2c_tcp_lostlen) {
        this.common_s2c_tcp_lostlen = common_s2c_tcp_lostlen;
    }

    public long getCommon_c2s_tcp_unorder_num() {
        return common_c2s_tcp_unorder_num;
    }

    public void setCommon_c2s_tcp_unorder_num(long common_c2s_tcp_unorder_num) {
        this.common_c2s_tcp_unorder_num = common_c2s_tcp_unorder_num;
    }

    public long getCommon_s2c_tcp_unorder_num() {
        return common_s2c_tcp_unorder_num;
    }

    public void setCommon_s2c_tcp_unorder_num(long common_s2c_tcp_unorder_num) {
        this.common_s2c_tcp_unorder_num = common_s2c_tcp_unorder_num;
    }

    public long getCommon_c2s_byte_retrans() {
        return common_c2s_byte_retrans;
    }

    public void setCommon_c2s_byte_retrans(long common_c2s_byte_retrans) {
        this.common_c2s_byte_retrans = common_c2s_byte_retrans;
    }

    public long getCommon_s2c_byte_retrans() {
        return common_s2c_byte_retrans;
    }

    public void setCommon_s2c_byte_retrans(long common_s2c_byte_retrans) {
        this.common_s2c_byte_retrans = common_s2c_byte_retrans;
    }

    public long getCommon_c2s_pkt_retrans() {
        return common_c2s_pkt_retrans;
    }

    public void setCommon_c2s_pkt_retrans(long common_c2s_pkt_retrans) {
        this.common_c2s_pkt_retrans = common_c2s_pkt_retrans;
    }

    public long getCommon_s2c_pkt_retrans() {
        return common_s2c_pkt_retrans;
    }

    public void setCommon_s2c_pkt_retrans(long common_s2c_pkt_retrans) {
        this.common_s2c_pkt_retrans = common_s2c_pkt_retrans;
    }

    public long getCommon_establish_latency_ms() {
        return common_establish_latency_ms;
    }

    public void setCommon_establish_latency_ms(long common_establish_latency_ms) {
        this.common_establish_latency_ms = common_establish_latency_ms;
    }

    public long getHttp_response_latency_ms() {
        return http_response_latency_ms;
    }

    public void setHttp_response_latency_ms(long http_response_latency_ms) {
        this.http_response_latency_ms = http_response_latency_ms;
    }

    public long getSsl_con_latency_ms() {
        return ssl_con_latency_ms;
    }

    public void setSsl_con_latency_ms(long ssl_con_latency_ms) {
        this.ssl_con_latency_ms = ssl_con_latency_ms;
    }

    public long getDns_response_latency_ms() {
        return dns_response_latency_ms;
    }

    public void setDns_response_latency_ms(long dns_response_latency_ms) {
        this.dns_response_latency_ms = dns_response_latency_ms;
    }

    public String getClient_isp() {
        return client_isp;
    }

    public void setClient_isp(String client_isp) {
        this.client_isp = client_isp;
    }

    public String getServer_isp() {
        return server_isp;
    }

    public void setServer_isp(String server_isp) {
        this.server_isp = server_isp;
    }

    public String getApp_company() {
        return app_company;
    }

    public void setApp_company(String app_company) {
        this.app_company = app_company;
    }

    public String getApp_company_category() {
        return app_company_category;
    }

    public void setApp_company_category(String app_company_category) {
        this.app_company_category = app_company_category;
    }

    public long getCommon_log_id() {
        return common_log_id;
    }

    public void setCommon_log_id(long common_log_id) {
        this.common_log_id = common_log_id;
    }

    public long getCommon_flags() {
        return common_flags;
    }

    public void setCommon_flags(long common_flags) {
        this.common_flags = common_flags;
    }

    public String getCommon_app_full_path() {
        return common_app_full_path;
    }

    public void setCommon_app_full_path(String common_app_full_path) {
        this.common_app_full_path = common_app_full_path;
    }

    public String getSsl_ja3s_hash() {
        return ssl_ja3s_hash;
    }

    public void setSsl_ja3s_hash(String ssl_ja3s_hash) {
        this.ssl_ja3s_hash = ssl_ja3s_hash;
    }

    public String getOut_link_direction() {
        return out_link_direction;
    }

    public void setOut_link_direction(String out_link_direction) {
        this.out_link_direction = out_link_direction;
    }

    public String getIn_link_direction() {
        return in_link_direction;
    }

    public void setIn_link_direction(String in_link_direction) {
        this.in_link_direction = in_link_direction;
    }

    public String getDomain_sld() {
        return domain_sld;
    }

    public void setDomain_sld(String domain_sld) {
        this.domain_sld = domain_sld;
    }

    public String getClient_country_region() {
        return client_country_region;
    }

    public void setClient_country_region(String client_country_region) {
        this.client_country_region = client_country_region;
    }

    public String getClient_super_admin_area() {
        return client_super_admin_area;
    }

    public void setClient_super_admin_area(String client_super_admin_area) {
        this.client_super_admin_area = client_super_admin_area;
    }

    public String getClient_admin_area() {
        return client_admin_area;
    }

    public void setClient_admin_area(String client_admin_area) {
        this.client_admin_area = client_admin_area;
    }

    public String getServer_country_region() {
        return server_country_region;
    }

    public void setServer_country_region(String server_country_region) {
        this.server_country_region = server_country_region;
    }

    public String getServer_super_admin_area() {
        return server_super_admin_area;
    }

    public void setServer_super_admin_area(String server_super_admin_area) {
        this.server_super_admin_area = server_super_admin_area;
    }

    public String getServer_admin_area() {
        return server_admin_area;
    }

    public void setServer_admin_area(String server_admin_area) {
        this.server_admin_area = server_admin_area;
    }

    public List<Long> getRule_id_list() {
        return rule_id_list;
    }

    public void setRule_id_list(List<Long> rule_id_list) {
        this.rule_id_list = rule_id_list;
    }

    public List<String> getIoc_type_list() {
        return ioc_type_list;
    }

    public void setIoc_type_list(List<String> ioc_type_list) {
        this.ioc_type_list = ioc_type_list;
    }

    public void putRuleIdAndIocType(Long ruleId, String iocType) {
        this.rule_id_list.add(ruleId);
        this.ioc_type_list.add(iocType);
    }

    public String getGeo(String geoField, String type) {
        switch (geoField) {
            case "country":
                if ("client".equals(type)) {
                    return client_country_region;
                } else {
                    return server_country_region;
                }
            case "province":
                if ("client".equals(type)) {
                    return client_super_admin_area;
                } else {
                    return server_super_admin_area;
                }
            case "region":
                if ("client".equals(type)) {
                    return client_admin_area;
                } else {
                    return server_admin_area;
                }
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public String toString() {
        return "CnRecordLog{" +
                "common_recv_time=" + common_recv_time +
                ", common_log_id=" + common_log_id +
                ", common_flags=" + common_flags +
                ", common_start_time=" + common_start_time +
                ", common_end_time=" + common_end_time +
                ", common_con_duration_ms=" + common_con_duration_ms +
                ", common_schema_type='" + common_schema_type + '\'' +
                ", common_client_ip='" + common_client_ip + '\'' +
                ", common_server_ip='" + common_server_ip + '\'' +
                ", common_client_port=" + common_client_port +
                ", common_server_port=" + common_server_port +
                ", common_app_label='" + common_app_label + '\'' +
                ", common_app_full_path='" + common_app_full_path + '\'' +
                ", common_l4_protocol='" + common_l4_protocol + '\'' +
                ", common_l7_protocol='" + common_l7_protocol + '\'' +
                ", common_out_link_id=" + common_out_link_id +
                ", common_in_link_id=" + common_in_link_id +
                ", http_host='" + http_host + '\'' +
                ", http_url='" + http_url + '\'' +
                ", http_response_line='" + http_response_line + '\'' +
                ", http_status_code=" + http_status_code +
                ", ssl_sni='" + ssl_sni + '\'' +
                ", ssl_san='" + ssl_san + '\'' +
                ", ssl_ja3_hash='" + ssl_ja3_hash + '\'' +
                ", ssl_ja3s_hash='" + ssl_ja3s_hash + '\'' +
                ", ssl_cert_issuer='" + ssl_cert_issuer + '\'' +
                ", ssl_cert_subject='" + ssl_cert_subject + '\'' +
                ", dns_qr=" + dns_qr +
                ", dns_opcode=" + dns_opcode +
                ", dns_aa=" + dns_aa +
                ", dns_rcode=" + dns_rcode +
                ", dns_qname='" + dns_qname + '\'' +
                ", dns_qtype=" + dns_qtype +
                ", dns_qclass=" + dns_qclass +
                ", dns_sub=" + dns_sub +
                ", dns_rr='" + dns_rr + '\'' +
                ", out_link_direction='" + out_link_direction + '\'' +
                ", in_link_direction='" + in_link_direction + '\'' +
                ", domain='" + domain + '\'' +
                ", domain_sld='" + domain_sld + '\'' +
                ", domain_category_name='" + domain_category_name + '\'' +
                ", domain_category_group='" + domain_category_group + '\'' +
                ", domain_reputation_level='" + domain_reputation_level + '\'' +
                ", domain_icp_company_name='" + domain_icp_company_name + '\'' +
                ", domain_whois_org='" + domain_whois_org + '\'' +
                ", domain_tags=" + domain_tags +
                ", client_zone='" + client_zone + '\'' +
                ", client_country_region='" + client_country_region + '\'' +
                ", client_super_admin_area='" + client_super_admin_area + '\'' +
                ", client_admin_area='" + client_admin_area + '\'' +
                ", client_longitude=" + client_longitude +
                ", client_latitude=" + client_latitude +
                ", client_isp='" + client_isp + '\'' +
                ", client_asn='" + client_asn + '\'' +
                ", client_ip_tags=" + client_ip_tags +
                ", server_zone='" + server_zone + '\'' +
                ", server_country_region='" + server_country_region + '\'' +
                ", server_super_admin_area='" + server_super_admin_area + '\'' +
                ", server_admin_area='" + server_admin_area + '\'' +
                ", server_longitude=" + server_longitude +
                ", server_latitude=" + server_latitude +
                ", server_isp='" + server_isp + '\'' +
                ", server_asn='" + server_asn + '\'' +
                ", server_ip_tags=" + server_ip_tags +
                ", app_category='" + app_category + '\'' +
                ", app_subcategory='" + app_subcategory + '\'' +
                ", app_company='" + app_company + '\'' +
                ", app_company_category='" + app_company_category + '\'' +
                ", app_tags=" + app_tags +
                ", common_c2s_pkt_num=" + common_c2s_pkt_num +
                ", common_c2s_byte_num=" + common_c2s_byte_num +
                ", common_s2c_pkt_num=" + common_s2c_pkt_num +
                ", common_s2c_byte_num=" + common_s2c_byte_num +
                ", common_sessions=" + common_sessions +
                ", common_c2s_tcp_lostlen=" + common_c2s_tcp_lostlen +
                ", common_s2c_tcp_lostlen=" + common_s2c_tcp_lostlen +
                ", common_c2s_tcp_unorder_num=" + common_c2s_tcp_unorder_num +
                ", common_s2c_tcp_unorder_num=" + common_s2c_tcp_unorder_num +
                ", common_c2s_byte_retrans=" + common_c2s_byte_retrans +
                ", common_s2c_byte_retrans=" + common_s2c_byte_retrans +
                ", common_c2s_pkt_retrans=" + common_c2s_pkt_retrans +
                ", common_s2c_pkt_retrans=" + common_s2c_pkt_retrans +
                ", common_establish_latency_ms=" + common_establish_latency_ms +
                ", http_response_latency_ms=" + http_response_latency_ms +
                ", ssl_con_latency_ms=" + ssl_con_latency_ms +
                ", dns_response_latency_ms=" + dns_response_latency_ms +
                ", traffic_inbound_byte=" + traffic_inbound_byte +
                ", traffic_inbound_pkt=" + traffic_inbound_pkt +
                ", traffic_outbound_byte=" + traffic_outbound_byte +
                ", traffic_outbound_pkt=" + traffic_outbound_pkt +
                ", traffic_internal_byte=" + traffic_internal_byte +
                ", traffic_internal_pkt=" + traffic_internal_pkt +
                ", traffic_through_byte=" + traffic_through_byte +
                ", traffic_through_pkt=" + traffic_through_pkt +
                ", internal_query_num=" + internal_query_num +
                ", external_query_num=" + external_query_num +
                ", rule_id_list=" + rule_id_list +
                ", ioc_type_list=" + ioc_type_list +
                '}';
    }
}
