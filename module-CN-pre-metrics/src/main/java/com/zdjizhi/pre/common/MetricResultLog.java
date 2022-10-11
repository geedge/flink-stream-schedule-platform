package com.zdjizhi.pre.common;

public class MetricResultLog {
    private String ip;
    private String zone;

    private String country;
    private String province;
    private String city;

    private String asn;
    private String isp;

    private String idc_renter;

    private String side;

    private String common_app_label;
    private String app_category;
    private String app_subcategory;
    private String app_company;

    private String domain;
    private String domain_category_name;
    private String domain_category_group;

    private String http_host;

    private String ssl_sni ;

    private String common_l7_protocol;
    private Integer common_server_port;

    private Long stat_time;

    private Long common_c2s_pkt_num;
    private Long common_c2s_byte_num;
    private Long common_s2c_pkt_num;
    private Long common_s2c_byte_num;
    private Long common_sessions;

    private Long traffic_inbound_byte;
    private Long traffic_inbound_pkt;
    private Long traffic_outbound_byte;
    private Long traffic_outbound_pkt;
    private Long traffic_internal_byte;
    private Long traffic_internal_pkt;
    private Long traffic_through_byte;
    private Long traffic_through_pkt;

    private Float c2s_tcp_lostlen_ratio;
    private Float s2c_tcp_lostlen_ratio;
    private Float tcp_lostlen_ratio;
    private Float c2s_tcp_unorder_num_ratio;
    private Float s2c_tcp_unorder_num_ratio;
    private Float tcp_unorder_num_ratio;
    private Float c2s_byte_retrans_ratio;
    private Float s2c_byte_retrans_ratio;
    private Float byte_retrans_ratio;
    private Float c2s_pkt_retrans_ratio;
    private Float s2c_pkt_retrans_ratio;
    private Float pkt_retrans_ratio;

    private Float avg_establish_latency_ms;
    private Float avg_http_response_latency_ms;
    private Float avg_ssl_con_latency_ms;


    private String client_country;
    private String client_province;
    private String client_region;
    private String server_country;
    private String server_province;
    private String server_region;
    private Long common_egress_link_id;
    private Long common_ingress_link_id;
    private String egress_link_direction;
    private String ingress_link_direction;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getAsn() {
        return asn;
    }

    public void setAsn(String asn) {
        this.asn = asn;
    }

    public String getIsp() {
        return isp;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

    public String getIdc_renter() {
        return idc_renter;
    }

    public void setIdc_renter(String idc_renter) {
        this.idc_renter = idc_renter;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }

    public String getCommon_app_label() {
        return common_app_label;
    }

    public void setCommon_app_label(String common_app_label) {
        this.common_app_label = common_app_label;
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

    public String getCommon_l7_protocol() {
        return common_l7_protocol;
    }

    public void setCommon_l7_protocol(String common_l7_protocol) {
        this.common_l7_protocol = common_l7_protocol;
    }

    public Integer getCommon_server_port() {
        return common_server_port;
    }

    public void setCommon_server_port(Integer common_server_port) {
        this.common_server_port = common_server_port;
    }

    public Long getStat_time() {
        return stat_time;
    }

    public void setStat_time(Long stat_time) {
        this.stat_time = stat_time;
    }

    public Long getCommon_c2s_pkt_num() {
        return common_c2s_pkt_num;
    }

    public void setCommon_c2s_pkt_num(Long common_c2s_pkt_num) {
        this.common_c2s_pkt_num = common_c2s_pkt_num;
    }

    public Long getCommon_c2s_byte_num() {
        return common_c2s_byte_num;
    }

    public void setCommon_c2s_byte_num(Long common_c2s_byte_num) {
        this.common_c2s_byte_num = common_c2s_byte_num;
    }

    public Long getCommon_s2c_pkt_num() {
        return common_s2c_pkt_num;
    }

    public void setCommon_s2c_pkt_num(Long common_s2c_pkt_num) {
        this.common_s2c_pkt_num = common_s2c_pkt_num;
    }

    public Long getCommon_s2c_byte_num() {
        return common_s2c_byte_num;
    }

    public void setCommon_s2c_byte_num(Long common_s2c_byte_num) {
        this.common_s2c_byte_num = common_s2c_byte_num;
    }

    public Long getCommon_sessions() {
        return common_sessions;
    }

    public void setCommon_sessions(Long common_sessions) {
        this.common_sessions = common_sessions;
    }

    public Long getTraffic_inbound_byte() {
        return traffic_inbound_byte;
    }

    public void setTraffic_inbound_byte(Long traffic_inbound_byte) {
        this.traffic_inbound_byte = traffic_inbound_byte;
    }

    public Long getTraffic_inbound_pkt() {
        return traffic_inbound_pkt;
    }

    public void setTraffic_inbound_pkt(Long traffic_inbound_pkt) {
        this.traffic_inbound_pkt = traffic_inbound_pkt;
    }

    public Long getTraffic_outbound_byte() {
        return traffic_outbound_byte;
    }

    public void setTraffic_outbound_byte(Long traffic_outbound_byte) {
        this.traffic_outbound_byte = traffic_outbound_byte;
    }

    public Long getTraffic_outbound_pkt() {
        return traffic_outbound_pkt;
    }

    public void setTraffic_outbound_pkt(Long traffic_outbound_pkt) {
        this.traffic_outbound_pkt = traffic_outbound_pkt;
    }

    public Long getTraffic_internal_byte() {
        return traffic_internal_byte;
    }

    public void setTraffic_internal_byte(Long traffic_internal_byte) {
        this.traffic_internal_byte = traffic_internal_byte;
    }

    public Long getTraffic_internal_pkt() {
        return traffic_internal_pkt;
    }

    public void setTraffic_internal_pkt(Long traffic_internal_pkt) {
        this.traffic_internal_pkt = traffic_internal_pkt;
    }

    public Long getTraffic_through_byte() {
        return traffic_through_byte;
    }

    public void setTraffic_through_byte(Long traffic_through_byte) {
        this.traffic_through_byte = traffic_through_byte;
    }

    public Long getTraffic_through_pkt() {
        return traffic_through_pkt;
    }

    public void setTraffic_through_pkt(Long traffic_through_pkt) {
        this.traffic_through_pkt = traffic_through_pkt;
    }

    public Float getC2s_tcp_lostlen_ratio() {
        return c2s_tcp_lostlen_ratio;
    }

    public void setC2s_tcp_lostlen_ratio(Float c2s_tcp_lostlen_ratio) {
        this.c2s_tcp_lostlen_ratio = c2s_tcp_lostlen_ratio;
    }

    public Float getS2c_tcp_lostlen_ratio() {
        return s2c_tcp_lostlen_ratio;
    }

    public void setS2c_tcp_lostlen_ratio(Float s2c_tcp_lostlen_ratio) {
        this.s2c_tcp_lostlen_ratio = s2c_tcp_lostlen_ratio;
    }

    public Float getTcp_lostlen_ratio() {
        return tcp_lostlen_ratio;
    }

    public void setTcp_lostlen_ratio(Float tcp_lostlen_ratio) {
        this.tcp_lostlen_ratio = tcp_lostlen_ratio;
    }

    public Float getC2s_tcp_unorder_num_ratio() {
        return c2s_tcp_unorder_num_ratio;
    }

    public void setC2s_tcp_unorder_num_ratio(Float c2s_tcp_unorder_num_ratio) {
        this.c2s_tcp_unorder_num_ratio = c2s_tcp_unorder_num_ratio;
    }

    public Float getS2c_tcp_unorder_num_ratio() {
        return s2c_tcp_unorder_num_ratio;
    }

    public void setS2c_tcp_unorder_num_ratio(Float s2c_tcp_unorder_num_ratio) {
        this.s2c_tcp_unorder_num_ratio = s2c_tcp_unorder_num_ratio;
    }

    public Float getTcp_unorder_num_ratio() {
        return tcp_unorder_num_ratio;
    }

    public void setTcp_unorder_num_ratio(Float tcp_unorder_num_ratio) {
        this.tcp_unorder_num_ratio = tcp_unorder_num_ratio;
    }

    public Float getC2s_byte_retrans_ratio() {
        return c2s_byte_retrans_ratio;
    }

    public void setC2s_byte_retrans_ratio(Float c2s_byte_retrans_ratio) {
        this.c2s_byte_retrans_ratio = c2s_byte_retrans_ratio;
    }

    public Float getS2c_byte_retrans_ratio() {
        return s2c_byte_retrans_ratio;
    }

    public void setS2c_byte_retrans_ratio(Float s2c_byte_retrans_ratio) {
        this.s2c_byte_retrans_ratio = s2c_byte_retrans_ratio;
    }

    public Float getByte_retrans_ratio() {
        return byte_retrans_ratio;
    }

    public void setByte_retrans_ratio(Float byte_retrans_ratio) {
        this.byte_retrans_ratio = byte_retrans_ratio;
    }

    public Float getC2s_pkt_retrans_ratio() {
        return c2s_pkt_retrans_ratio;
    }

    public void setC2s_pkt_retrans_ratio(Float c2s_pkt_retrans_ratio) {
        this.c2s_pkt_retrans_ratio = c2s_pkt_retrans_ratio;
    }

    public Float getS2c_pkt_retrans_ratio() {
        return s2c_pkt_retrans_ratio;
    }

    public void setS2c_pkt_retrans_ratio(Float s2c_pkt_retrans_ratio) {
        this.s2c_pkt_retrans_ratio = s2c_pkt_retrans_ratio;
    }

    public Float getPkt_retrans_ratio() {
        return pkt_retrans_ratio;
    }

    public void setPkt_retrans_ratio(Float pkt_retrans_ratio) {
        this.pkt_retrans_ratio = pkt_retrans_ratio;
    }

    public Float getAvg_establish_latency_ms() {
        return avg_establish_latency_ms;
    }

    public void setAvg_establish_latency_ms(Float avg_establish_latency_ms) {
        this.avg_establish_latency_ms = avg_establish_latency_ms;
    }

    public Float getAvg_http_response_latency_ms() {
        return avg_http_response_latency_ms;
    }

    public void setAvg_http_response_latency_ms(Float avg_http_response_latency_ms) {
        this.avg_http_response_latency_ms = avg_http_response_latency_ms;
    }

    public Float getAvg_ssl_con_latency_ms() {
        return avg_ssl_con_latency_ms;
    }

    public void setAvg_ssl_con_latency_ms(Float avg_ssl_con_latency_ms) {
        this.avg_ssl_con_latency_ms = avg_ssl_con_latency_ms;
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

    public Long getCommon_egress_link_id() {
        return common_egress_link_id;
    }

    public void setCommon_egress_link_id(Long common_egress_link_id) {
        this.common_egress_link_id = common_egress_link_id;
    }

    public Long getCommon_ingress_link_id() {
        return common_ingress_link_id;
    }

    public void setCommon_ingress_link_id(Long common_ingress_link_id) {
        this.common_ingress_link_id = common_ingress_link_id;
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

    @Override
    public String toString() {
        return "MetricResultLog{" +
                "ip='" + ip + '\'' +
                ", zone='" + zone + '\'' +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", asn='" + asn + '\'' +
                ", isp='" + isp + '\'' +
                ", idc_renter='" + idc_renter + '\'' +
                ", side='" + side + '\'' +
                ", common_app_label='" + common_app_label + '\'' +
                ", app_category='" + app_category + '\'' +
                ", app_subcategory='" + app_subcategory + '\'' +
                ", app_company='" + app_company + '\'' +
                ", domain='" + domain + '\'' +
                ", domain_category_name='" + domain_category_name + '\'' +
                ", domain_category_group='" + domain_category_group + '\'' +
                ", http_host='" + http_host + '\'' +
                ", ssl_sni='" + ssl_sni + '\'' +
                ", common_l7_protocol='" + common_l7_protocol + '\'' +
                ", common_server_port=" + common_server_port +
                ", client_country=" + client_country +
                ", client_province=" + client_province +
                ", client_region=" + client_region +
                ", server_country=" + server_country +
                ", server_province=" + server_province +
                ", server_region=" + server_region +
                ", common_egress_link_id=" + common_egress_link_id +
                ", common_ingress_link_id=" + common_ingress_link_id +
                ", egress_link_direction=" + egress_link_direction +
                ", ingress_link_direction=" + ingress_link_direction +
                ", stat_time=" + stat_time +
                ", common_c2s_pkt_num=" + common_c2s_pkt_num +
                ", common_c2s_byte_num=" + common_c2s_byte_num +
                ", common_s2c_pkt_num=" + common_s2c_pkt_num +
                ", common_s2c_byte_num=" + common_s2c_byte_num +
                ", common_sessions=" + common_sessions +
                ", traffic_inbound_byte=" + traffic_inbound_byte +
                ", traffic_inbound_pkt=" + traffic_inbound_pkt +
                ", traffic_outbound_byte=" + traffic_outbound_byte +
                ", traffic_outbound_pkt=" + traffic_outbound_pkt +
                ", traffic_internal_byte=" + traffic_internal_byte +
                ", traffic_internal_pkt=" + traffic_internal_pkt +
                ", traffic_through_byte=" + traffic_through_byte +
                ", traffic_through_pkt=" + traffic_through_pkt +
                ", c2s_tcp_lostlen_ratio=" + c2s_tcp_lostlen_ratio +
                ", s2c_tcp_lostlen_ratio=" + s2c_tcp_lostlen_ratio +
                ", tcp_lostlen_ratio=" + tcp_lostlen_ratio +
                ", c2s_tcp_unorder_num_ratio=" + c2s_tcp_unorder_num_ratio +
                ", s2c_tcp_unorder_num_ratio=" + s2c_tcp_unorder_num_ratio +
                ", tcp_unorder_num_ratio=" + tcp_unorder_num_ratio +
                ", c2s_byte_retrans_ratio=" + c2s_byte_retrans_ratio +
                ", s2c_byte_retrans_ratio=" + s2c_byte_retrans_ratio +
                ", byte_retrans_ratio=" + byte_retrans_ratio +
                ", c2s_pkt_retrans_ratio=" + c2s_pkt_retrans_ratio +
                ", s2c_pkt_retrans_ratio=" + s2c_pkt_retrans_ratio +
                ", pkt_retrans_ratio=" + pkt_retrans_ratio +
                ", avg_establish_latency_ms=" + avg_establish_latency_ms +
                ", avg_http_response_latency_ms=" + avg_http_response_latency_ms +
                ", avg_ssl_con_latency_ms=" + avg_ssl_con_latency_ms +
                '}';
    }
}
