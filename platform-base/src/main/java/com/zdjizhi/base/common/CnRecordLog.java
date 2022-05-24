package com.zdjizhi.base.common;

import java.io.Serializable;
import java.util.ArrayList;

public class CnRecordLog implements Serializable {
    //Common Attributes
    private long common_recv_time;
    private long common_end_time ;
    private int common_direction;
    private int common_stream_dir;
    private String common_schema_type;
    private String common_client_ip;
    private String common_server_ip;
    private int common_client_port;
    private int common_server_port;
    private String common_app_label;
    private String common_l4_protocol;
    private String common_l7_protocol;
    private String common_isp;
    private long common_egress_link_id;
    private long common_ingress_link_id;

    //HTTP Attributes
    private String http_host;
    private String http_url;
    private String http_cookie;
    private String http_referer;
    private String http_user_agent;
    private String http_request_line;
    private String http_response_line;
    private int http_status_code;

    //SSL Attributes
    private String ssl_sni ;
    private String ssl_san;
    private String ssl_ja3_fingerprint;
    private String ssl_ja3_hash;
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

    //SSH Attributes
    private String ssh_version;
    private String ssh_auth_success;
    private String ssh_client_version;
    private String ssh_server_version;
    private String ssh_cipher_alg;
    private String ssh_mac_alg;
    private String ssh_compression_alg;
    private String ssh_kex_alg;
    private String ssh_host_key_alg;
    private String ssh_host_key;
    private String ssh_hassh;

    //Tags
    private String egress_link_direction;
    private String ingress_link_direction;

    private String domain;
    private String domain_category_name;
    private String domain_category_group;
    private long domain_reputation_score;
    private String domain_reputation_level;
    private String domain_icp_company_name;
    private String domain_whois_org;

    private String client_country;
    private String client_province;
    private String client_region;
    private Double client_longitude;
    private Double client_latitude;
    private String client_isp;
    private String client_whois_owner;
    private String client_idc_renter;
    private String server_country;
    private String server_province;
    private String server_region;
    private Double server_longitude;
    private Double server_latitude;
    private String server_isp;
    private String server_whois_owner;
    private String server_idc_renter;

    private String app_category;
    private String app_subcategory;
    private Integer app_is_protocol;
    private String app_company;
    private String app_company_category;
    private long app_risk;

    private ArrayList<String> dns_server_role;
    private String dns_server_org;
    private String dns_server_os;
    private String dns_server_software;
    private String dns_protocol;

    //Metrics
    private long common_c2s_pkt_num;
    private long common_c2s_byte_num;
    private long common_s2c_pkt_num;
    private long common_s2c_byte_num;
    private long common_sessions;
    private long common_c2s_tcp_lostlen;
    private long common_s2c_tcp_lostlen;
    private long common_c2s_tcp_unorder_num;
    private long common_s2c_tcp_unorder_num;
    private long common_c2s_byte_retrans;
    private long common_s2c_byte_retrans;
    private long common_c2s_pkt_retrans;
    private long common_s2c_pkt_retrans;
    private long common_establish_latency_ms;
    private long http_response_latency_ms;
    private long ssl_con_latency_ms;
    private long dns_response_latency_ms;

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

    public int getCommon_direction() {
        return common_direction;
    }

    public void setCommon_direction(int common_direction) {
        this.common_direction = common_direction;
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

    public String getCommon_isp() {
        return common_isp;
    }

    public void setCommon_isp(String common_isp) {
        this.common_isp = common_isp;
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

    public String getHttp_cookie() {
        return http_cookie;
    }

    public void setHttp_cookie(String http_cookie) {
        this.http_cookie = http_cookie;
    }

    public String getHttp_referer() {
        return http_referer;
    }

    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }

    public String getHttp_request_line() {
        return http_request_line;
    }

    public void setHttp_request_line(String http_request_line) {
        this.http_request_line = http_request_line;
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

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public long getCommon_egress_link_id() {
        return common_egress_link_id;
    }

    public void setCommon_egress_link_id(long common_egress_link_id) {
        this.common_egress_link_id = common_egress_link_id;
    }

    public long getCommon_ingress_link_id() {
        return common_ingress_link_id;
    }

    public void setCommon_ingress_link_id(long common_ingress_link_id) {
        this.common_ingress_link_id = common_ingress_link_id;
    }

    public String getSsl_san() {
        return ssl_san;
    }

    public void setSsl_san(String ssl_san) {
        this.ssl_san = ssl_san;
    }

    public String getSsl_ja3_fingerprint() {
        return ssl_ja3_fingerprint;
    }

    public void setSsl_ja3_fingerprint(String ssl_ja3_fingerprint) {
        this.ssl_ja3_fingerprint = ssl_ja3_fingerprint;
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

    public String getSsh_version() {
        return ssh_version;
    }

    public void setSsh_version(String ssh_version) {
        this.ssh_version = ssh_version;
    }

    public String getSsh_auth_success() {
        return ssh_auth_success;
    }

    public void setSsh_auth_success(String ssh_auth_success) {
        this.ssh_auth_success = ssh_auth_success;
    }

    public String getSsh_client_version() {
        return ssh_client_version;
    }

    public void setSsh_client_version(String ssh_client_version) {
        this.ssh_client_version = ssh_client_version;
    }

    public String getSsh_server_version() {
        return ssh_server_version;
    }

    public void setSsh_server_version(String ssh_server_version) {
        this.ssh_server_version = ssh_server_version;
    }

    public String getSsh_cipher_alg() {
        return ssh_cipher_alg;
    }

    public void setSsh_cipher_alg(String ssh_cipher_alg) {
        this.ssh_cipher_alg = ssh_cipher_alg;
    }

    public String getSsh_mac_alg() {
        return ssh_mac_alg;
    }

    public void setSsh_mac_alg(String ssh_mac_alg) {
        this.ssh_mac_alg = ssh_mac_alg;
    }

    public String getSsh_compression_alg() {
        return ssh_compression_alg;
    }

    public void setSsh_compression_alg(String ssh_compression_alg) {
        this.ssh_compression_alg = ssh_compression_alg;
    }

    public String getSsh_kex_alg() {
        return ssh_kex_alg;
    }

    public void setSsh_kex_alg(String ssh_kex_alg) {
        this.ssh_kex_alg = ssh_kex_alg;
    }

    public String getSsh_host_key_alg() {
        return ssh_host_key_alg;
    }

    public void setSsh_host_key_alg(String ssh_host_key_alg) {
        this.ssh_host_key_alg = ssh_host_key_alg;
    }

    public String getSsh_host_key() {
        return ssh_host_key;
    }

    public void setSsh_host_key(String ssh_host_key) {
        this.ssh_host_key = ssh_host_key;
    }

    public String getSsh_hassh() {
        return ssh_hassh;
    }

    public void setSsh_hassh(String ssh_hassh) {
        this.ssh_hassh = ssh_hassh;
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

    public long getDomain_reputation_score() {
        return domain_reputation_score;
    }

    public void setDomain_reputation_score(long domain_reputation_score) {
        this.domain_reputation_score = domain_reputation_score;
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

    public String getClient_country() {
        return client_country;
    }

    public void setClient_country(String client_country) {
        this.client_country = client_country;
    }

    public String getClient_province() {
        return client_province;
    }

    public void setClient_province(String client_province) {
        this.client_province = client_province;
    }

    public String getClient_region() {
        return client_region;
    }

    public void setClient_region(String client_region) {
        this.client_region = client_region;
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

    public String getServer_country() {
        return server_country;
    }

    public void setServer_country(String server_country) {
        this.server_country = server_country;
    }

    public String getServer_province() {
        return server_province;
    }

    public void setServer_province(String server_province) {
        this.server_province = server_province;
    }

    public String getServer_region() {
        return server_region;
    }

    public void setServer_region(String server_region) {
        this.server_region = server_region;
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

    public long getApp_risk() {
        return app_risk;
    }

    public void setApp_risk(long app_risk) {
        this.app_risk = app_risk;
    }

    public ArrayList<String> getDns_server_role() {
        return dns_server_role;
    }

    public void setDns_server_role(ArrayList<String> dns_server_role) {
        this.dns_server_role = dns_server_role;
    }

    public String getDns_server_org() {
        return dns_server_org;
    }

    public void setDns_server_org(String dns_server_org) {
        this.dns_server_org = dns_server_org;
    }

    public String getDns_server_os() {
        return dns_server_os;
    }

    public void setDns_server_os(String dns_server_os) {
        this.dns_server_os = dns_server_os;
    }

    public String getDns_server_software() {
        return dns_server_software;
    }

    public void setDns_server_software(String dns_server_software) {
        this.dns_server_software = dns_server_software;
    }

    public String getDns_protocol() {
        return dns_protocol;
    }

    public void setDns_protocol(String dns_protocol) {
        this.dns_protocol = dns_protocol;
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

    public int getCommon_stream_dir() {
        return common_stream_dir;
    }

    public void setCommon_stream_dir(int common_stream_dir) {
        this.common_stream_dir = common_stream_dir;
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

    public String getEgress_link_direction() {
        return egress_link_direction;
    }

    public void setEgress_link_direction(String egress_link_direction) {
        this.egress_link_direction = egress_link_direction;
    }

    public String getIngress_link_direction() {
        return ingress_link_direction;
    }

    public void setIngress_link_direction(String ingress_link_direction) {
        this.ingress_link_direction = ingress_link_direction;
    }

    public Integer getApp_is_protocol() {
        return app_is_protocol;
    }

    public void setApp_is_protocol(Integer app_is_protocol) {
        this.app_is_protocol = app_is_protocol;
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

    public String getClient_whois_owner() {
        return client_whois_owner;
    }

    public void setClient_whois_owner(String client_whois_owner) {
        this.client_whois_owner = client_whois_owner;
    }

    public String getClient_idc_renter() {
        return client_idc_renter;
    }

    public void setClient_idc_renter(String client_idc_renter) {
        this.client_idc_renter = client_idc_renter;
    }

    public String getServer_whois_owner() {
        return server_whois_owner;
    }

    public void setServer_whois_owner(String server_whois_owner) {
        this.server_whois_owner = server_whois_owner;
    }

    public String getServer_idc_renter() {
        return server_idc_renter;
    }

    public void setServer_idc_renter(String server_idc_renter) {
        this.server_idc_renter = server_idc_renter;
    }

    @Override
    public String toString() {
        return "CnRecordLog{" +
                "common_recv_time=" + common_recv_time +
                ", common_direction=" + common_direction +
                ", common_stream_dir=" + common_stream_dir +
                ", common_schema_type='" + common_schema_type + '\'' +
                ", common_client_ip='" + common_client_ip + '\'' +
                ", common_server_ip='" + common_server_ip + '\'' +
                ", common_client_port=" + common_client_port +
                ", common_server_port=" + common_server_port +
                ", common_app_label='" + common_app_label + '\'' +
                ", common_l4_protocol='" + common_l4_protocol + '\'' +
                ", common_l7_protocol='" + common_l7_protocol + '\'' +
                ", common_isp='" + common_isp + '\'' +
                ", common_egress_link_id=" + common_egress_link_id +
                ", common_ingress_link_id=" + common_ingress_link_id +
                ", http_host='" + http_host + '\'' +
                ", http_url='" + http_url + '\'' +
                ", http_cookie='" + http_cookie + '\'' +
                ", http_referer='" + http_referer + '\'' +
                ", http_user_agent='" + http_user_agent + '\'' +
                ", http_request_line='" + http_request_line + '\'' +
                ", http_response_line='" + http_response_line + '\'' +
                ", http_status_code=" + http_status_code +
                ", ssl_sni='" + ssl_sni + '\'' +
                ", ssl_san='" + ssl_san + '\'' +
                ", ssl_ja3_fingerprint='" + ssl_ja3_fingerprint + '\'' +
                ", ssl_ja3_hash='" + ssl_ja3_hash + '\'' +
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
                ", ssh_version='" + ssh_version + '\'' +
                ", ssh_auth_success='" + ssh_auth_success + '\'' +
                ", ssh_client_version='" + ssh_client_version + '\'' +
                ", ssh_server_version='" + ssh_server_version + '\'' +
                ", ssh_cipher_alg='" + ssh_cipher_alg + '\'' +
                ", ssh_mac_alg='" + ssh_mac_alg + '\'' +
                ", ssh_compression_alg='" + ssh_compression_alg + '\'' +
                ", ssh_kex_alg='" + ssh_kex_alg + '\'' +
                ", ssh_host_key_alg='" + ssh_host_key_alg + '\'' +
                ", ssh_host_key='" + ssh_host_key + '\'' +
                ", ssh_hassh='" + ssh_hassh + '\'' +
                ", egress_link_direction='" + egress_link_direction + '\'' +
                ", ingress_link_direction='" + ingress_link_direction + '\'' +
                ", domain='" + domain + '\'' +
                ", domain_category_name='" + domain_category_name + '\'' +
                ", domain_category_group='" + domain_category_group + '\'' +
                ", domain_reputation_score=" + domain_reputation_score +
                ", domain_reputation_level='" + domain_reputation_level + '\'' +
                ", domain_icp_company_name='" + domain_icp_company_name + '\'' +
                ", domain_whois_org='" + domain_whois_org + '\'' +
                ", client_country='" + client_country + '\'' +
                ", client_province='" + client_province + '\'' +
                ", client_region='" + client_region + '\'' +
                ", client_longitude=" + client_longitude +
                ", client_latitude=" + client_latitude +
                ", client_isp='" + client_isp + '\'' +
                ", client_whois_owner='" + client_whois_owner + '\'' +
                ", client_idc_renter='" + client_idc_renter + '\'' +
                ", server_country='" + server_country + '\'' +
                ", server_province='" + server_province + '\'' +
                ", server_region='" + server_region + '\'' +
                ", server_longitude=" + server_longitude +
                ", server_latitude=" + server_latitude +
                ", server_isp='" + server_isp + '\'' +
                ", server_whois_owner='" + server_whois_owner + '\'' +
                ", server_idc_renter='" + server_idc_renter + '\'' +
                ", app_category='" + app_category + '\'' +
                ", app_subcategory='" + app_subcategory + '\'' +
                ", app_is_protocol=" + app_is_protocol +
                ", app_company='" + app_company + '\'' +
                ", app_company_category='" + app_company_category + '\'' +
                ", app_risk=" + app_risk +
                ", dns_server_role=" + dns_server_role +
                ", dns_server_org='" + dns_server_org + '\'' +
                ", dns_server_os='" + dns_server_os + '\'' +
                ", dns_server_software='" + dns_server_software + '\'' +
                ", dns_protocol='" + dns_protocol + '\'' +
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
                '}';
    }
}
