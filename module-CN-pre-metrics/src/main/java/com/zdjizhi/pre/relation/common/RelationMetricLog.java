package com.zdjizhi.pre.relation.common;

import com.alibaba.fastjson2.annotation.JSONField;

import java.util.List;

public class RelationMetricLog {

    @JSONField(serialize = false)
    private Integer metricKey;

    private String app_name;
    private String domain;
    private String ip;
    private String ip_country_region;
    private String ip_super_admin_area;
    private String ip_admin_area;
    private String ip_asn;
    private String ip_isp;
    private String domain_category_name;
    private String domain_category_group;
    private String app_category;
    private String app_subcategory;
    private List<String> entity_tags;

    //subscriber
    private String subscriber_id;
    private String imei;
    private String imsi;
    private String phone_number;
    private String apn;


    private String l7_protocol;
    private int port;

    private long stat_time;

    public RelationMetricLog(Integer metricKey) {
        this.metricKey = metricKey;
    }

    public Integer getMetricKey() {
        return metricKey;
    }

    public void setMetricKey(Integer metricKey) {
        this.metricKey = metricKey;
    }

    public String getApp_name() {
        return app_name;
    }

    public void setApp_name(String app_name) {
        this.app_name = app_name;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getIp_country_region() {
        return ip_country_region;
    }

    public void setIp_country_region(String ip_country_region) {
        this.ip_country_region = ip_country_region;
    }

    public String getIp_super_admin_area() {
        return ip_super_admin_area;
    }

    public void setIp_super_admin_area(String ip_super_admin_area) {
        this.ip_super_admin_area = ip_super_admin_area;
    }

    public String getIp_admin_area() {
        return ip_admin_area;
    }

    public void setIp_admin_area(String ip_admin_area) {
        this.ip_admin_area = ip_admin_area;
    }

    public String getIp_asn() {
        return ip_asn;
    }

    public void setIp_asn(String ip_asn) {
        this.ip_asn = ip_asn;
    }

    public String getIp_isp() {
        return ip_isp;
    }

    public void setIp_isp(String ip_isp) {
        this.ip_isp = ip_isp;
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
    public List<String> getEntity_tags() {
        return entity_tags;
    }

    public void setEntity_tags(List<String> entity_tags) {
        this.entity_tags = entity_tags;
    }

    public String getL7_protocol() {
        return l7_protocol;
    }

    public void setL7_protocol(String l7_protocol) {
        this.l7_protocol = l7_protocol;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getStat_time() {
        return stat_time;
    }

    public void setStat_time(long stat_time) {
        this.stat_time = stat_time;
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

    @Override
    public String toString() {
        return "RelationMetricLog{" +
                "metricKey=" + metricKey +
                ", app_name='" + app_name + '\'' +
                ", domain='" + domain + '\'' +
                ", ip='" + ip + '\'' +
                ", ip_country_region='" + ip_country_region + '\'' +
                ", ip_super_admin_area='" + ip_super_admin_area + '\'' +
                ", ip_admin_area='" + ip_admin_area + '\'' +
                ", ip_asn='" + ip_asn + '\'' +
                ", ip_isp='" + ip_isp + '\'' +
                ", domain_category_name='" + domain_category_name + '\'' +
                ", domain_category_group='" + domain_category_group + '\'' +
                ", app_category='" + app_category + '\'' +
                ", app_subcategory='" + app_subcategory + '\'' +
                ", entity_tags=" + entity_tags +
                ", subscriber_id='" + subscriber_id + '\'' +
                ", imei='" + imei + '\'' +
                ", imsi='" + imsi + '\'' +
                ", phone_number='" + phone_number + '\'' +
                ", apn='" + apn + '\'' +
                ", l7_protocol='" + l7_protocol + '\'' +
                ", port=" + port +
                ", stat_time=" + stat_time +
                '}';
    }
}
