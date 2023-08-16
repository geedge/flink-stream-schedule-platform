package com.zdjizhi.pre.common;

public class CnMetricLog {

    private int metricKey;
    private long common_recv_time;
    private long common_start_time;
    private String common_client_ip;
    private String common_server_ip;
    private int common_server_port;
    private String common_app_label;
    private String common_l7_protocol;

    private String domain;
    private String domain_sld;
    private String domain_category_name;
    private String domain_category_group;
    private String zone;
    private String client_country_region;
    private String client_super_admin_area;
    private String client_admin_area;
    private String client_isp;
    private String client_asn;
    private String client_zone;
    private String server_country_region;
    private String server_super_admin_area;
    private String server_admin_area;
    private String server_isp;
    private String server_asn;
    private String server_zone;

    private String app_category;
    private String app_subcategory;
    private String app_company;

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

    private long c2s_tcp_byte_num;
    private long s2c_tcp_byte_num;
    private long c2s_tcp_pkt_num;
    private long s2c_tcp_pkt_num;
    private long tcp_count;
    private long http_count;
    private long ssl_count;
    private long traffic_inbound_byte;
    private long traffic_inbound_pkt;
    private long traffic_outbound_byte;
    private long traffic_outbound_pkt;
    private long traffic_internal_byte;
    private long traffic_internal_pkt;
    private long traffic_through_byte;
    private long traffic_through_pkt;

    private Long common_out_link_id;
    private Long common_in_link_id;
    private String out_link_direction;
    private String in_link_direction;

    public CnMetricLog(int metricKey) {
        this.metricKey = metricKey;
    }

    public int getMetricKey() {
        return metricKey;
    }

    public void setMetricKey(int metricKey) {
        this.metricKey = metricKey;
    }

    public String getCommon_l7_protocol() {
        return common_l7_protocol;
    }

    public void setCommon_l7_protocol(String common_l7_protocol) {
        this.common_l7_protocol = common_l7_protocol;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public long getCommon_start_time() {
        return common_start_time;
    }

    public void setCommon_start_time(long common_start_time) {
        this.common_start_time = common_start_time;
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

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getDomain_sld() {
        return domain_sld;
    }

    public void setDomain_sld(String domain_sld) {
        this.domain_sld = domain_sld;
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

    public String getZone() {
        return zone;
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

    public void setZone(String zone) {
        this.zone = zone;
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

    public String getClient_isp() {
        return client_isp;
    }

    public void setClient_isp(String client_isp) {
        this.client_isp = client_isp;
    }

    public String getClient_asn() {
        return client_asn;
    }

    public void setClient_asn(String client_asn) {
        this.client_asn = client_asn;
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

    public String getServer_isp() {
        return server_isp;
    }

    public void setServer_isp(String server_isp) {
        this.server_isp = server_isp;
    }

    public String getServer_asn() {
        return server_asn;
    }

    public void setServer_asn(String server_asn) {
        this.server_asn = server_asn;
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

    public String getApp_company() {
        return app_company;
    }

    public void setApp_company(String app_company) {
        this.app_company = app_company;
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

    public long getC2s_tcp_byte_num() {
        return c2s_tcp_byte_num;
    }

    public void setC2s_tcp_byte_num(long c2s_tcp_byte_num) {
        this.c2s_tcp_byte_num = c2s_tcp_byte_num;
    }

    public long getS2c_tcp_byte_num() {
        return s2c_tcp_byte_num;
    }

    public void setS2c_tcp_byte_num(long s2c_tcp_byte_num) {
        this.s2c_tcp_byte_num = s2c_tcp_byte_num;
    }

    public long getC2s_tcp_pkt_num() {
        return c2s_tcp_pkt_num;
    }

    public void setC2s_tcp_pkt_num(long c2s_tcp_pkt_num) {
        this.c2s_tcp_pkt_num = c2s_tcp_pkt_num;
    }

    public long getS2c_tcp_pkt_num() {
        return s2c_tcp_pkt_num;
    }

    public void setS2c_tcp_pkt_num(long s2c_tcp_pkt_num) {
        this.s2c_tcp_pkt_num = s2c_tcp_pkt_num;
    }

    public long getTcp_count() {
        return tcp_count;
    }

    public void setTcp_count(long tcp_count) {
        this.tcp_count = tcp_count;
    }

    public long getHttp_count() {
        return http_count;
    }

    public void setHttp_count(long http_count) {
        this.http_count = http_count;
    }

    public long getSsl_count() {
        return ssl_count;
    }

    public void setSsl_count(long ssl_count) {
        this.ssl_count = ssl_count;
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

    @Override
    public String toString() {
        return "CnMetricLog{" +
                "metricKey=" + metricKey +
                ", common_recv_time=" + common_recv_time +
                ", common_start_time=" + common_start_time +
                ", common_client_ip='" + common_client_ip + '\'' +
                ", common_server_ip='" + common_server_ip + '\'' +
                ", common_server_port=" + common_server_port +
                ", common_app_label='" + common_app_label + '\'' +
                ", common_l7_protocol='" + common_l7_protocol + '\'' +
                ", domain='" + domain + '\'' +
                ", domain_sld='" + domain_sld + '\'' +
                ", domain_category_name='" + domain_category_name + '\'' +
                ", domain_category_group='" + domain_category_group + '\'' +
                ", zone='" + zone + '\'' +
                ", client_country_region='" + client_country_region + '\'' +
                ", client_super_admin_area='" + client_super_admin_area + '\'' +
                ", client_admin_area='" + client_admin_area + '\'' +
                ", client_isp='" + client_isp + '\'' +
                ", client_asn='" + client_asn + '\'' +
                ", client_zone='" + client_zone + '\'' +
                ", server_country_region='" + server_country_region + '\'' +
                ", server_super_admin_area='" + server_super_admin_area + '\'' +
                ", server_admin_area='" + server_admin_area + '\'' +
                ", server_isp='" + server_isp + '\'' +
                ", server_asn='" + server_asn + '\'' +
                ", server_zone='" + server_zone + '\'' +
                ", app_category='" + app_category + '\'' +
                ", app_subcategory='" + app_subcategory + '\'' +
                ", app_company='" + app_company + '\'' +
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
                ", c2s_tcp_byte_num=" + c2s_tcp_byte_num +
                ", s2c_tcp_byte_num=" + s2c_tcp_byte_num +
                ", c2s_tcp_pkt_num=" + c2s_tcp_pkt_num +
                ", s2c_tcp_pkt_num=" + s2c_tcp_pkt_num +
                ", tcp_count=" + tcp_count +
                ", http_count=" + http_count +
                ", ssl_count=" + ssl_count +
                ", traffic_inbound_byte=" + traffic_inbound_byte +
                ", traffic_inbound_pkt=" + traffic_inbound_pkt +
                ", traffic_outbound_byte=" + traffic_outbound_byte +
                ", traffic_outbound_pkt=" + traffic_outbound_pkt +
                ", traffic_internal_byte=" + traffic_internal_byte +
                ", traffic_internal_pkt=" + traffic_internal_pkt +
                ", traffic_through_byte=" + traffic_through_byte +
                ", traffic_through_pkt=" + traffic_through_pkt +
                ", common_out_link_id=" + common_out_link_id +
                ", common_in_link_id=" + common_in_link_id +
                ", out_link_direction='" + out_link_direction + '\'' +
                ", in_link_direction='" + in_link_direction + '\'' +
                '}';
    }
}
