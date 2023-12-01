package com.zdjizhi.pre.dns.common;

import java.util.List;

public class DnsMetricResultLog {
    private Long stat_time;

    private String server_ip;
    private String server_country_region;
    private String server_super_admin_area;
    private String server_admin_area;
    private String server_isp;
    private List<String> server_role;

    private String qname;
    private String qname_sld;
    private String qname_tld;

    private Long qtype;

    private Long rcode;

    private String rr_a;

    private String rr_aaaa;

    private String rr_cname;

    private Long query_num;
    private Long traffic_inbound_byte;
    private Long traffic_outbound_byte;
    private Long internal_query_num;
    private Long external_query_num;
    private Float avg_response_latency_ms;

    public Long getStat_time() {
        return stat_time;
    }

    public void setStat_time(Long stat_time) {
        this.stat_time = stat_time;
    }

    public String getServer_ip() {
        return server_ip;
    }

    public void setServer_ip(String server_ip) {
        this.server_ip = server_ip;
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

    public List<String> getServer_role() {
        return server_role;
    }

    public void setServer_role(List<String> server_role) {
        this.server_role = server_role;
    }

    public String getQname() {
        return qname;
    }

    public void setQname(String qname) {
        this.qname = qname;
    }

    public String getQname_sld() {
        return qname_sld;
    }

    public void setQname_sld(String qname_sld) {
        this.qname_sld = qname_sld;
    }

    public String getQname_tld() {
        return qname_tld;
    }

    public void setQname_tld(String qname_tld) {
        this.qname_tld = qname_tld;
    }

    public Long getQtype() {
        return qtype;
    }

    public void setQtype(Long qtype) {
        this.qtype = qtype;
    }

    public Long getRcode() {
        return rcode;
    }

    public void setRcode(Long rcode) {
        this.rcode = rcode;
    }

    public String getRr_a() {
        return rr_a;
    }

    public void setRr_a(String rr_a) {
        this.rr_a = rr_a;
    }

    public String getRr_aaaa() {
        return rr_aaaa;
    }

    public void setRr_aaaa(String rr_aaaa) {
        this.rr_aaaa = rr_aaaa;
    }

    public String getRr_cname() {
        return rr_cname;
    }

    public void setRr_cname(String rr_cname) {
        this.rr_cname = rr_cname;
    }

    public Long getQuery_num() {
        return query_num;
    }

    public void setQuery_num(Long query_num) {
        this.query_num = query_num;
    }

    public Float getAvg_response_latency_ms() {
        return avg_response_latency_ms;
    }

    public void setAvg_response_latency_ms(Float avg_response_latency_ms) {
        this.avg_response_latency_ms = avg_response_latency_ms;
    }

    public Long getTraffic_inbound_byte() {
        return traffic_inbound_byte;
    }

    public void setTraffic_inbound_byte(Long traffic_inbound_byte) {
        this.traffic_inbound_byte = traffic_inbound_byte;
    }

    public Long getTraffic_outbound_byte() {
        return traffic_outbound_byte;
    }

    public void setTraffic_outbound_byte(Long traffic_outbound_byte) {
        this.traffic_outbound_byte = traffic_outbound_byte;
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

    @Override
    public String toString() {
        return "DnsMetricResultLog{" +
                "stat_time=" + stat_time +
                ", server_ip='" + server_ip + '\'' +
                ", server_country_region='" + server_country_region + '\'' +
                ", server_super_admin_area='" + server_super_admin_area + '\'' +
                ", server_admin_area='" + server_admin_area + '\'' +
                ", server_isp='" + server_isp + '\'' +
                ", server_role=" + server_role +
                ", qname='" + qname + '\'' +
                ", qname_sld='" + qname_sld + '\'' +
                ", qname_tld='" + qname_tld + '\'' +
                ", qtype=" + qtype +
                ", rcode=" + rcode +
                ", rr_a='" + rr_a + '\'' +
                ", rr_aaaa='" + rr_aaaa + '\'' +
                ", rr_cname='" + rr_cname + '\'' +
                ", query_num=" + query_num +
                ", traffic_inbound_byte=" + traffic_inbound_byte +
                ", traffic_outbound_byte=" + traffic_outbound_byte +
                ", internal_query_num=" + internal_query_num +
                ", external_query_num=" + external_query_num +
                ", avg_response_latency_ms=" + avg_response_latency_ms +
                '}';
    }
}
