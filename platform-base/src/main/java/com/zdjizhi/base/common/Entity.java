package com.zdjizhi.base.common;

import java.io.Serializable;

public class Entity implements Serializable {

    private int ifError;
    private long common_processing_time ;
    private long common_recv_time ;
    //common_direction
    private int common_direction;

    private String common_schema_type ;
    private String common_client_ip ;
    private String common_server_ip ;

    private String common_app_label ;
    private String common_l4_protocol;
    //common_l7_protocol
    private String common_l7_protocol;
    //common_isp
    private String common_isp;
    private long common_egress_link_id;
    private long common_ingress_link_id;
    //common_server_port
    private int common_server_port;
    private int common_client_port;

    //todo http
    private String http_host;
    //http_url
    private String http_url;
    //http_cookie
    private String http_cookie;
    //http_referer
    private String http_referer;
    //http_user_agent
    private String http_user_agent;

    //todo ssl
    private String ssl_sni ;
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
    private String dns_qname;
    //dns_qtype
    private int dns_qtype;
    //dns_qclass
    private int dns_qclass;
    //dns_sub
    private int dns_sub;
    //dns_rr
    private String  dns_rr;

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

    //todo domain tags
    private String http_domain ;




    private String common_client_asn;
    private String common_server_asn;
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




    public Entity() {
    }


    public int getIfError() {
        return ifError;
    }

    public void setIfError(int ifError) {
        this.ifError = ifError;
    }

   public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public long getCommon_processing_time() {
        return common_processing_time;
    }

    public void setCommon_processing_time(long common_processing_time) {
        this.common_processing_time = common_processing_time;
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

    public String getDns_qname() {
        return dns_qname;
    }

    public void setDns_qname(String dns_qname) {
        this.dns_qname = dns_qname;
    }

    public int getDns_rcode() {
        return dns_rcode;
    }

    public void setDns_rcode(int dns_rcode) {
        this.dns_rcode = dns_rcode;
    }

    public String getHttp_domain() {
        return http_domain;
    }

    public void setHttp_domain(String http_domain) {
        this.http_domain = http_domain;
    }

    public String getCommon_client_asn() {
        return common_client_asn;
    }

    public void setCommon_client_asn(String common_client_asn) {
        this.common_client_asn = common_client_asn;
    }

    public String getCommon_server_asn() {
        return common_server_asn;
    }

    public void setCommon_server_asn(String common_server_asn) {
        this.common_server_asn = common_server_asn;
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


    //todo 新增字段


    public int getCommon_direction() {
        return common_direction;
    }

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

    public long getDns_response_latency_ms() {
        return dns_response_latency_ms;
    }

    public void setDns_response_latency_ms(long dns_response_latency_ms) {
        this.dns_response_latency_ms = dns_response_latency_ms;
    }

    public int getCommon_server_port() {
        return common_server_port;
    }

    public void setCommon_server_port(int common_server_port) {
        this.common_server_port = common_server_port;
    }

    public int getCommon_client_port() {
        return common_client_port;
    }

    public void setCommon_client_port(int common_client_port) {
        this.common_client_port = common_client_port;
    }

    @Override
    public String toString() {
        return "Entity{" +
                "ifError=" + ifError +
                ", common_processing_time=" + common_processing_time +
                ", common_recv_time=" + common_recv_time +
                ", common_direction='" + common_direction + '\'' +
                ", common_schema_type='" + common_schema_type + '\'' +
                ", common_client_ip='" + common_client_ip + '\'' +
                ", common_server_ip='" + common_server_ip + '\'' +
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
                ", http_domain='" + http_domain + '\'' +
                ", common_client_asn='" + common_client_asn + '\'' +
                ", common_server_asn='" + common_server_asn + '\'' +
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

    //    @Override
//    public String toString() {
//        return "Entity{" +
//                "ifError=" + ifError +
//                ", common_processing_time=" + common_processing_time +
//                ", common_recv_time=" + common_recv_time +
//                ", common_schema_type='" + common_schema_type + '\'' +
//                ", common_client_ip='" + common_client_ip + '\'' +
//                ", common_server_ip='" + common_server_ip + '\'' +
//                ", http_host='" + http_host + '\'' +
//                ", ssl_sni='" + ssl_sni + '\'' +
//                ", common_app_label='" + common_app_label + '\'' +
//                ", common_l4_protocol='" + common_l4_protocol + '\'' +
//                ", dns_qname='" + dns_qname + '\'' +
//                ", dns_rcode=" + dns_rcode +
//                ", http_domain='" + http_domain + '\'' +
//                ", common_client_asn='" + common_client_asn + '\'' +
//                ", common_server_asn='" + common_server_asn + '\'' +
//                ", common_c2s_pkt_num=" + common_c2s_pkt_num +
//                ", common_c2s_byte_num=" + common_c2s_byte_num +
//                ", common_s2c_pkt_num=" + common_s2c_pkt_num +
//                ", common_s2c_byte_num=" + common_s2c_byte_num +
//                ", common_sessions=" + common_sessions +
//                ", common_c2s_tcp_lostlen=" + common_c2s_tcp_lostlen +
//                ", common_s2c_tcp_lostlen=" + common_s2c_tcp_lostlen +
//                ", common_c2s_tcp_unorder_num=" + common_c2s_tcp_unorder_num +
//                ", common_s2c_tcp_unorder_num=" + common_s2c_tcp_unorder_num +
//                ", common_c2s_byte_retrans=" + common_c2s_byte_retrans +
//                ", common_s2c_byte_retrans=" + common_s2c_byte_retrans +
//                ", common_c2s_pkt_retrans=" + common_c2s_pkt_retrans +
//                ", common_s2c_pkt_retrans=" + common_s2c_pkt_retrans +
//                ", common_establish_latency_ms=" + common_establish_latency_ms +
//                ", http_response_latency_ms=" + http_response_latency_ms +
//                ", ssl_con_latency_ms=" + ssl_con_latency_ms +
//                ", common_egress_link_id=" + common_egress_link_id +
//                ", common_ingress_link_id=" + common_ingress_link_id +
//                '}';
//    }
}
