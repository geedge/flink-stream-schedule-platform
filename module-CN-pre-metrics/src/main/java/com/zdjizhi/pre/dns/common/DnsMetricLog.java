package com.zdjizhi.pre.dns.common;

import java.util.ArrayList;
import java.util.List;

public class DnsMetricLog {

    private int metricKey;
    private long common_recv_time;
    private long common_start_time;
    private long common_sessions;

    private String common_server_ip;
    private String server_country_region;
    private String server_super_admin_area;
    private String server_admin_area;
    private String server_isp;
    private List<String> server_role = new ArrayList<>();
    private String dns_qname;

    private String qname_sld;
    private String qname_tld;


    private long dns_qtype;

    private long dns_rcode;
    private String dns_rr;
    private String dns_rra;
    private String dns_rraaaa;
    private String dns_rrcname;

    private String record_a;

    private String record_aaaa;

    private String record_cname;

    private long dns_response_latency_ms;

    private long traffic_inbound_byte;
    private long traffic_outbound_byte;
    private long internal_query_num;
    private long external_query_num;

    private long dns_count;

    public DnsMetricLog(int metricKey) {
        this.metricKey = metricKey;
    }

    public int getMetricKey() {
        return metricKey;
    }

    public void setMetricKey(int metricKey) {
        this.metricKey = metricKey;
    }

    public String getDns_rra() {
        return dns_rra;
    }

    public void setDns_rra(String dns_rra) {
        this.dns_rra = dns_rra;
    }

    public String getDns_rraaaa() {
        return dns_rraaaa;
    }

    public void setDns_rraaaa(String dns_rraaaa) {
        this.dns_rraaaa = dns_rraaaa;
    }

    public String getDns_rrcname() {
        return dns_rrcname;
    }

    public void setDns_rrcname(String dns_rrcname) {
        this.dns_rrcname = dns_rrcname;
    }

    public long getDns_count() {
        return dns_count;
    }

    public void setDns_count(long dns_count) {
        this.dns_count = dns_count;
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

    public long getCommon_sessions() {
        return common_sessions;
    }

    public void setCommon_sessions(long common_sessions) {
        this.common_sessions = common_sessions;
    }

    public String getCommon_server_ip() {
        return common_server_ip;
    }

    public void setCommon_server_ip(String common_server_ip) {
        this.common_server_ip = common_server_ip;
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

    public long getDns_response_latency_ms() {
        return dns_response_latency_ms;
    }

    public void setDns_response_latency_ms(long dns_response_latency_ms) {
        this.dns_response_latency_ms = dns_response_latency_ms;
    }


    public String getDns_qname() {
        return dns_qname;
    }

    public void setDns_qname(String dns_qname) {
        this.dns_qname = dns_qname;
    }

    public long getDns_qtype() {
        return dns_qtype;
    }

    public void setDns_qtype(long dns_qtype) {
        this.dns_qtype = dns_qtype;
    }

    public long getDns_rcode() {
        return dns_rcode;
    }

    public void setDns_rcode(long dns_rcode) {
        this.dns_rcode = dns_rcode;
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

    public String getRecord_a() {
        return record_a;
    }

    public void setRecord_a(String record_a) {
        this.record_a = record_a;
    }

    public String getRecord_aaaa() {
        return record_aaaa;
    }

    public void setRecord_aaaa(String record_aaaa) {
        this.record_aaaa = record_aaaa;
    }

    public String getRecord_cname() {
        return record_cname;
    }

    public void setRecord_cname(String record_cname) {
        this.record_cname = record_cname;
    }

    public long getTraffic_outbound_byte() {
        return traffic_outbound_byte;
    }

    public void setTraffic_outbound_byte(long traffic_outbound_byte) {
        this.traffic_outbound_byte = traffic_outbound_byte;
    }

    public long getTraffic_inbound_byte() {
        return traffic_inbound_byte;
    }

    public void setTraffic_inbound_byte(long traffic_inbound_byte) {
        this.traffic_inbound_byte = traffic_inbound_byte;
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


    public String getDns_rr() {
        return dns_rr;
    }

    public void setDns_rr(String dns_rr) {
        this.dns_rr = dns_rr;
    }


}
