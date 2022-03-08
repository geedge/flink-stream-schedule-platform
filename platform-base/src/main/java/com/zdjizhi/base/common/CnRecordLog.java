package com.zdjizhi.base.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

public class CnRecordLog implements Serializable {
    private long common_recv_time;
    //common_direction
    private int common_direction;
    private String common_schema_type;
    private String common_client_ip;
    private String common_server_ip;
    private int common_client_port;
    private int common_server_port;
    private String common_app_label;
    private String common_l4_protocol;
    //common_l7_protocol
    private String common_l7_protocol;
    //common_isp
    private String common_isp;
    private String http_host;
    private String ssl_sni ;
    //http_url
    private String http_url;
    //http_cookie
    private String http_cookie;
    //http_referer
    private String http_referer;

    private int http_status_code;
    //http_user_agent
    private String http_user_agent;
    private long common_egress_link_id;
    private long common_ingress_link_id;

    //todo ssl
    //ssl_san
    private String ssl_san;
    //ssl_ja3_fingerprint
    private String ssl_ja3_fingerprint;
    //ssl_ja3_hash
    private String ssl_ja3_hash;
    //ssl_cert_issuer
    private String ssl_cert_issuer;
    //ssl_cert_subject
    private String ssl_cert_subject;

    //todo dns
    //dns_qr
    private int dns_qr;
    //dns_opcode
    private int dns_opcode;
    //dns_aa
    private int dns_aa;
    //dns_rcode
    private int dns_rcode;
    //dns_qname
    private String dns_qname;
    //dns_qtype
    private int dns_qtype;
    //dns_qclass
    private int dns_qclass;
    //dns_sub
    private int dns_sub;
    //dns_rr
    private String dns_rr;



    //todo ssh
    //ssh_version
    private String ssh_version;
    //ssh_auth_success
    private String ssh_auth_success;
    //ssh_client_version
    private String ssh_client_version;
    //ssh_server_version
    private String ssh_server_version;
    //ssh_cipher_alg
    private String ssh_cipher_alg;
    //ssh_mac_alg
    private String ssh_mac_alg;
    //ssh_compression_alg
    private String ssh_compression_alg;
    //ssh_kex_alg
    private String ssh_kex_alg;
    //ssh_host_key_alg
    private String ssh_host_key_alg;
    //ssh_host_key
    private String ssh_host_key;
    //ssh_hassh
    private String ssh_hassh;

    private String domain;
    private String domain_category_name;
    private String domain_category_group;
    private long domain_reputation_score;
    private String domain_reputation_level;
    //domain_icp_company_name
    private String domain_icp_company_name;
    //domain_whois_org替换domain_org
//    private String domain_whois_org;
    private String domain_whois_org;
    private String client_country;
    private String client_province;
    private String client_region;
    private Double client_longitude;
    private Double client_latitude;
    private String server_country;
    private String server_province;
    private String server_region;
    private Double server_longitude;
    private Double server_latitude;
    private String app_category;
    private String app_subcategory;
    private long app_risk;
    //dns tags
    //dns_server_role
    private ArrayList<String> dns_server_role;
    //dns_server_org
    private String dns_server_org;
    //dns_server_os
    private String dns_server_os;
    //dns_server_software
    private String dns_server_software;
    //dns_protocol
    private String dns_protocol;
    private int doh_support;
    private int dot_support;

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
    //dns_response_latency_ms
    private long dns_response_latency_ms;

    public CnRecordLog() {
    }

    public int getHttp_status_code() {
        return http_status_code;
    }

    public void setHttp_status_code(int http_status_code) {
        this.http_status_code = http_status_code;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
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

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
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

    public String getDomain_org() {
        return domain_whois_org;
    }

    public void setDomain_org(String domain_org) {
        this.domain_whois_org = domain_whois_org;
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

    public long getApp_risk() {
        return app_risk;
    }

    public void setApp_risk(long app_risk) {
        this.app_risk = app_risk;
    }

    public int getCommon_direction() {
        return common_direction;
    }

    //todo 新增字段
    public void setCommon_direction(int common_direction) {
        this.common_direction = common_direction;
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

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
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

    public String getDomain_whois_org() {
        return domain_whois_org;
    }

    public void setDomain_whois_org(String domain_whois_org) {
        this.domain_whois_org = domain_whois_org;
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

    public long getDns_response_latency_ms() {
        return dns_response_latency_ms;
    }

    public void setDns_response_latency_ms(long dns_response_latency_ms) {
        this.dns_response_latency_ms = dns_response_latency_ms;
    }

    public String getDomain_icp_company_name() {
        return domain_icp_company_name;
    }

    public void setDomain_icp_company_name(String domain_icp_company_name) {
        this.domain_icp_company_name = domain_icp_company_name;
    }

    public int getDoh_support() {
        return doh_support;
    }

    public void setDoh_support(int doh_support) {
        this.doh_support = doh_support;
    }

    public int getDot_support() {
        return dot_support;
    }

    public void setDot_support(int dot_support) {
        this.dot_support = dot_support;
    }

    public String getHttp_url() {
        return http_url;
    }

    public void setHttp_url(String http_url) {
        this.http_url = http_url;
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CnRecordLog)) return false;
        CnRecordLog that = (CnRecordLog) o;
        return getCommon_recv_time() == that.getCommon_recv_time() &&
                getCommon_direction() == that.getCommon_direction() &&
                getCommon_client_port() == that.getCommon_client_port() &&
                getCommon_server_port() == that.getCommon_server_port() &&
                getHttp_status_code() == that.getHttp_status_code() &&
                getCommon_egress_link_id() == that.getCommon_egress_link_id() &&
                getCommon_ingress_link_id() == that.getCommon_ingress_link_id() &&
                getDns_qr() == that.getDns_qr() &&
                getDns_opcode() == that.getDns_opcode() &&
                getDns_aa() == that.getDns_aa() &&
                getDns_rcode() == that.getDns_rcode() &&
                getDns_qtype() == that.getDns_qtype() &&
                getDns_qclass() == that.getDns_qclass() &&
                getDns_sub() == that.getDns_sub() &&
                getDomain_reputation_score() == that.getDomain_reputation_score() &&
                getApp_risk() == that.getApp_risk() &&
                getDoh_support() == that.getDoh_support() &&
                getDot_support() == that.getDot_support() &&
                getCommon_c2s_pkt_num() == that.getCommon_c2s_pkt_num() &&
                getCommon_c2s_byte_num() == that.getCommon_c2s_byte_num() &&
                getCommon_s2c_pkt_num() == that.getCommon_s2c_pkt_num() &&
                getCommon_s2c_byte_num() == that.getCommon_s2c_byte_num() &&
                getCommon_sessions() == that.getCommon_sessions() &&
                getCommon_c2s_tcp_lostlen() == that.getCommon_c2s_tcp_lostlen() &&
                getCommon_s2c_tcp_lostlen() == that.getCommon_s2c_tcp_lostlen() &&
                getCommon_c2s_tcp_unorder_num() == that.getCommon_c2s_tcp_unorder_num() &&
                getCommon_s2c_tcp_unorder_num() == that.getCommon_s2c_tcp_unorder_num() &&
                getCommon_c2s_byte_retrans() == that.getCommon_c2s_byte_retrans() &&
                getCommon_s2c_byte_retrans() == that.getCommon_s2c_byte_retrans() &&
                getCommon_c2s_pkt_retrans() == that.getCommon_c2s_pkt_retrans() &&
                getCommon_s2c_pkt_retrans() == that.getCommon_s2c_pkt_retrans() &&
                getCommon_establish_latency_ms() == that.getCommon_establish_latency_ms() &&
                getHttp_response_latency_ms() == that.getHttp_response_latency_ms() &&
                getSsl_con_latency_ms() == that.getSsl_con_latency_ms() &&
                getDns_response_latency_ms() == that.getDns_response_latency_ms() &&
                Objects.equals(getCommon_schema_type(), that.getCommon_schema_type()) &&
                Objects.equals(getCommon_client_ip(), that.getCommon_client_ip()) &&
                Objects.equals(getCommon_server_ip(), that.getCommon_server_ip()) &&
                Objects.equals(getCommon_app_label(), that.getCommon_app_label()) &&
                Objects.equals(getCommon_l4_protocol(), that.getCommon_l4_protocol()) &&
                Objects.equals(getCommon_l7_protocol(), that.getCommon_l7_protocol()) &&
                Objects.equals(getCommon_isp(), that.getCommon_isp()) &&
                Objects.equals(getHttp_host(), that.getHttp_host()) &&
                Objects.equals(getSsl_sni(), that.getSsl_sni()) &&
                Objects.equals(getHttp_url(), that.getHttp_url()) &&
                Objects.equals(getHttp_cookie(), that.getHttp_cookie()) &&
                Objects.equals(getHttp_referer(), that.getHttp_referer()) &&
                Objects.equals(getHttp_user_agent(), that.getHttp_user_agent()) &&
                Objects.equals(getSsl_san(), that.getSsl_san()) &&
                Objects.equals(getSsl_ja3_fingerprint(), that.getSsl_ja3_fingerprint()) &&
                Objects.equals(getSsl_ja3_hash(), that.getSsl_ja3_hash()) &&
                Objects.equals(getSsl_cert_issuer(), that.getSsl_cert_issuer()) &&
                Objects.equals(getSsl_cert_subject(), that.getSsl_cert_subject()) &&
                Objects.equals(getDns_qname(), that.getDns_qname()) &&
                Objects.equals(getDns_rr(), that.getDns_rr()) &&
                Objects.equals(getSsh_version(), that.getSsh_version()) &&
                Objects.equals(getSsh_auth_success(), that.getSsh_auth_success()) &&
                Objects.equals(getSsh_client_version(), that.getSsh_client_version()) &&
                Objects.equals(getSsh_server_version(), that.getSsh_server_version()) &&
                Objects.equals(getSsh_cipher_alg(), that.getSsh_cipher_alg()) &&
                Objects.equals(getSsh_mac_alg(), that.getSsh_mac_alg()) &&
                Objects.equals(getSsh_compression_alg(), that.getSsh_compression_alg()) &&
                Objects.equals(getSsh_kex_alg(), that.getSsh_kex_alg()) &&
                Objects.equals(getSsh_host_key_alg(), that.getSsh_host_key_alg()) &&
                Objects.equals(getSsh_host_key(), that.getSsh_host_key()) &&
                Objects.equals(getSsh_hassh(), that.getSsh_hassh()) &&
                Objects.equals(getDomain(), that.getDomain()) &&
                Objects.equals(getDomain_category_name(), that.getDomain_category_name()) &&
                Objects.equals(getDomain_category_group(), that.getDomain_category_group()) &&
                Objects.equals(getDomain_reputation_level(), that.getDomain_reputation_level()) &&
                Objects.equals(getDomain_icp_company_name(), that.getDomain_icp_company_name()) &&
                Objects.equals(getDomain_whois_org(), that.getDomain_whois_org()) &&
                Objects.equals(getClient_country(), that.getClient_country()) &&
                Objects.equals(getClient_province(), that.getClient_province()) &&
                Objects.equals(getClient_region(), that.getClient_region()) &&
                Objects.equals(getClient_longitude(), that.getClient_longitude()) &&
                Objects.equals(getClient_latitude(), that.getClient_latitude()) &&
                Objects.equals(getServer_country(), that.getServer_country()) &&
                Objects.equals(getServer_province(), that.getServer_province()) &&
                Objects.equals(getServer_region(), that.getServer_region()) &&
                Objects.equals(getServer_longitude(), that.getServer_longitude()) &&
                Objects.equals(getServer_latitude(), that.getServer_latitude()) &&
                Objects.equals(getApp_category(), that.getApp_category()) &&
                Objects.equals(getApp_subcategory(), that.getApp_subcategory()) &&
                Objects.equals(getDns_server_role(), that.getDns_server_role()) &&
                Objects.equals(getDns_server_org(), that.getDns_server_org()) &&
                Objects.equals(getDns_server_os(), that.getDns_server_os()) &&
                Objects.equals(getDns_server_software(), that.getDns_server_software()) &&
                Objects.equals(getDns_protocol(), that.getDns_protocol());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCommon_recv_time(), getCommon_direction(), getCommon_schema_type(), getCommon_client_ip(), getCommon_server_ip(), getCommon_client_port(), getCommon_server_port(), getCommon_app_label(), getCommon_l4_protocol(), getCommon_l7_protocol(), getCommon_isp(), getHttp_host(), getSsl_sni(), getHttp_url(), getHttp_cookie(), getHttp_referer(), getHttp_status_code(), getHttp_user_agent(), getCommon_egress_link_id(), getCommon_ingress_link_id(), getSsl_san(), getSsl_ja3_fingerprint(), getSsl_ja3_hash(), getSsl_cert_issuer(), getSsl_cert_subject(), getDns_qr(), getDns_opcode(), getDns_aa(), getDns_rcode(), getDns_qname(), getDns_qtype(), getDns_qclass(), getDns_sub(), getDns_rr(), getSsh_version(), getSsh_auth_success(), getSsh_client_version(), getSsh_server_version(), getSsh_cipher_alg(), getSsh_mac_alg(), getSsh_compression_alg(), getSsh_kex_alg(), getSsh_host_key_alg(), getSsh_host_key(), getSsh_hassh(), getDomain(), getDomain_category_name(), getDomain_category_group(), getDomain_reputation_score(), getDomain_reputation_level(), getDomain_icp_company_name(), getDomain_whois_org(), getClient_country(), getClient_province(), getClient_region(), getClient_longitude(), getClient_latitude(), getServer_country(), getServer_province(), getServer_region(), getServer_longitude(), getServer_latitude(), getApp_category(), getApp_subcategory(), getApp_risk(), getDns_server_role(), getDns_server_org(), getDns_server_os(), getDns_server_software(), getDns_protocol(), getDoh_support(), getDot_support(), getCommon_c2s_pkt_num(), getCommon_c2s_byte_num(), getCommon_s2c_pkt_num(), getCommon_s2c_byte_num(), getCommon_sessions(), getCommon_c2s_tcp_lostlen(), getCommon_s2c_tcp_lostlen(), getCommon_c2s_tcp_unorder_num(), getCommon_s2c_tcp_unorder_num(), getCommon_c2s_byte_retrans(), getCommon_s2c_byte_retrans(), getCommon_c2s_pkt_retrans(), getCommon_s2c_pkt_retrans(), getCommon_establish_latency_ms(), getHttp_response_latency_ms(), getSsl_con_latency_ms(), getDns_response_latency_ms());
    }

    @Override
    public String toString() {
        return "CnRecordLog{" +
                "common_recv_time=" + common_recv_time +
                ", common_direction=" + common_direction +
                ", common_schema_type='" + common_schema_type + '\'' +
                ", common_client_ip='" + common_client_ip + '\'' +
                ", common_server_ip='" + common_server_ip + '\'' +
                ", common_client_port=" + common_client_port +
                ", common_server_port=" + common_server_port +
                ", common_app_label='" + common_app_label + '\'' +
                ", common_l4_protocol='" + common_l4_protocol + '\'' +
                ", common_l7_protocol='" + common_l7_protocol + '\'' +
                ", common_isp='" + common_isp + '\'' +
                ", http_host='" + http_host + '\'' +
                ", ssl_sni='" + ssl_sni + '\'' +
                ", http_url='" + http_url + '\'' +
                ", http_cookie='" + http_cookie + '\'' +
                ", http_referer='" + http_referer + '\'' +
                ", http_status_code=" + http_status_code +
                ", http_user_agent='" + http_user_agent + '\'' +
                ", common_egress_link_id=" + common_egress_link_id +
                ", common_ingress_link_id=" + common_ingress_link_id +
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
                ", server_country='" + server_country + '\'' +
                ", server_province='" + server_province + '\'' +
                ", server_region='" + server_region + '\'' +
                ", server_longitude=" + server_longitude +
                ", server_latitude=" + server_latitude +
                ", app_category='" + app_category + '\'' +
                ", app_subcategory='" + app_subcategory + '\'' +
                ", app_risk=" + app_risk +
                ", dns_server_role=" + dns_server_role +
                ", dns_server_org='" + dns_server_org + '\'' +
                ", dns_server_os='" + dns_server_os + '\'' +
                ", dns_server_software='" + dns_server_software + '\'' +
                ", dns_protocol='" + dns_protocol + '\'' +
                ", doh_support=" + doh_support +
                ", dot_support=" + dot_support +
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
