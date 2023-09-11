package com.zdjizhi.pre.common;

public class MetricResultLog {
    private String ip;
    private String zone;

    private String country_region;
    private String super_admin_area;
    private String admin_area;

    private String asn;
    private String isp;

    private String side;

    private String common_app_label;
    private String app_category;
    private String app_subcategory;
    private String app_company;

    private String domain;
    private String domain_sld;
    private String domain_category_name;
    private String domain_category_group;

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


    private String client_country_region;
    private String client_super_admin_area;
    private String client_admin_area;
    private String client_zone;
    private String server_country_region;
    private String server_super_admin_area;
    private String server_admin_area;
    private String server_zone;
    private Long common_out_link_id;
    private Long common_in_link_id;
    private String out_link_direction;
    private String in_link_direction;
    private Long asymmetric_sessions;
    private Long bulky_sessions;
    private Long cbr_streaming_sessions;
    private Long download_sessions;
    private Long interactive_sessions;
    private Long pseudo_unidirectional_sessions;
    private Long streaming_sessions;
    private Long unidirectional_sessions;
    private Long random_looking_sessions;
    private Long bidirectional_sessions;

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

    public String getCountry_region() {
        return country_region;
    }

    public void setCountry_region(String country_region) {
        this.country_region = country_region;
    }

    public String getSuper_admin_area() {
        return super_admin_area;
    }

    public void setSuper_admin_area(String super_admin_area) {
        this.super_admin_area = super_admin_area;
    }

    public String getAdmin_area() {
        return admin_area;
    }

    public void setAdmin_area(String admin_area) {
        this.admin_area = admin_area;
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

    public Long getAsymmetric_sessions() {
        return asymmetric_sessions;
    }

    public void setAsymmetric_sessions(Long asymmetric_sessions) {
        this.asymmetric_sessions = asymmetric_sessions;
    }

    public Long getBulky_sessions() {
        return bulky_sessions;
    }

    public void setBulky_sessions(Long bulky_sessions) {
        this.bulky_sessions = bulky_sessions;
    }

    public Long getCbr_streaming_sessions() {
        return cbr_streaming_sessions;
    }

    public void setCbr_streaming_sessions(Long cbr_streaming_sessions) {
        this.cbr_streaming_sessions = cbr_streaming_sessions;
    }

    public Long getDownload_sessions() {
        return download_sessions;
    }

    public void setDownload_sessions(Long download_sessions) {
        this.download_sessions = download_sessions;
    }

    public Long getInteractive_sessions() {
        return interactive_sessions;
    }

    public void setInteractive_sessions(Long interactive_sessions) {
        this.interactive_sessions = interactive_sessions;
    }

    public Long getPseudo_unidirectional_sessions() {
        return pseudo_unidirectional_sessions;
    }

    public void setPseudo_unidirectional_sessions(Long pseudo_unidirectional_sessions) {
        this.pseudo_unidirectional_sessions = pseudo_unidirectional_sessions;
    }

    public Long getStreaming_sessions() {
        return streaming_sessions;
    }

    public void setStreaming_sessions(Long streaming_sessions) {
        this.streaming_sessions = streaming_sessions;
    }

    public Long getUnidirectional_sessions() {
        return unidirectional_sessions;
    }

    public void setUnidirectional_sessions(Long unidirectional_sessions) {
        this.unidirectional_sessions = unidirectional_sessions;
    }

    public Long getRandom_looking_sessions() {
        return random_looking_sessions;
    }

    public void setRandom_looking_sessions(Long random_looking_sessions) {
        this.random_looking_sessions = random_looking_sessions;
    }

    public Long getBidirectional_sessions() {
        return bidirectional_sessions;
    }

    public void setBidirectional_sessions(Long bidirectional_sessions) {
        this.bidirectional_sessions = bidirectional_sessions;
    }

    @Override
    public String toString() {
        return "MetricResultLog{" +
                "ip='" + ip + '\'' +
                ", zone='" + zone + '\'' +
                ", country_region='" + country_region + '\'' +
                ", super_admin_area='" + super_admin_area + '\'' +
                ", admin_area='" + admin_area + '\'' +
                ", asn='" + asn + '\'' +
                ", isp='" + isp + '\'' +
                ", side='" + side + '\'' +
                ", common_app_label='" + common_app_label + '\'' +
                ", app_category='" + app_category + '\'' +
                ", app_subcategory='" + app_subcategory + '\'' +
                ", app_company='" + app_company + '\'' +
                ", domain='" + domain + '\'' +
                ", domain_sld='" + domain_sld + '\'' +
                ", domain_category_name='" + domain_category_name + '\'' +
                ", domain_category_group='" + domain_category_group + '\'' +
                ", common_l7_protocol='" + common_l7_protocol + '\'' +
                ", common_server_port=" + common_server_port +
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
                ", client_country_region='" + client_country_region + '\'' +
                ", client_super_admin_area='" + client_super_admin_area + '\'' +
                ", client_admin_area='" + client_admin_area + '\'' +
                ", client_zone='" + client_zone + '\'' +
                ", server_country_region='" + server_country_region + '\'' +
                ", server_super_admin_area='" + server_super_admin_area + '\'' +
                ", server_admin_area='" + server_admin_area + '\'' +
                ", server_zone='" + server_zone + '\'' +
                ", common_out_link_id=" + common_out_link_id +
                ", common_in_link_id=" + common_in_link_id +
                ", out_link_direction='" + out_link_direction + '\'' +
                ", in_link_direction='" + in_link_direction + '\'' +
                ", asymmetric_sessions=" + asymmetric_sessions +
                ", bulky_sessions=" + bulky_sessions +
                ", cbr_streaming_sessions=" + cbr_streaming_sessions +
                ", download_sessions=" + download_sessions +
                ", interactive_sessions=" + interactive_sessions +
                ", pseudo_unidirectional_sessions=" + pseudo_unidirectional_sessions +
                ", streaming_sessions=" + streaming_sessions +
                ", unidirectional_sessions=" + unidirectional_sessions +
                ", random_looking_sessions=" + random_looking_sessions +
                ", bidirectional_sessions=" + bidirectional_sessions +
                '}';
    }
}
