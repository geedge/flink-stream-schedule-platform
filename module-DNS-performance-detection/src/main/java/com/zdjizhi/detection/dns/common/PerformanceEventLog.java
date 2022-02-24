package com.zdjizhi.detection.dns.common;

import java.io.Serializable;
import java.util.Objects;

public class PerformanceEventLog implements Serializable {

    private long event_id;
    private String event_key;
    private long start_time;
    private long end_time;
    private long duration_ms;
    private String event_severity;
    private String event_type;
    private String entity_type;
    private String client_ip;
    private String client_location_country;
    private String client_location_province;
    private String client_location_region;
    private String client_asn;
    private String server_ip;
    private String server_location_country;
    private String server_location_province;
    private String server_location_region;
    private String server_asn;
    private String domain;
    private String domain_category_name;
    private String domain_category_group;
    private String domain_reputation_score;
    private String domain_reputation_level;
    private String app_name;
    private String app_category;
    private String app_subcategory;
    private String app_risk;

    public long getEvent_id() {
        return event_id;
    }

    public void setEvent_id(long event_id) {
        this.event_id = event_id;
    }

    public String getEvent_key() {
        return event_key;
    }

    public void setEvent_key(String event_key) {
        this.event_key = event_key;
    }

    public long getStart_time() {
        return start_time;
    }

    public void setStart_time(long start_time) {
        this.start_time = start_time;
    }

    public long getEnd_time() {
        return end_time;
    }

    public void setEnd_time(long end_time) {
        this.end_time = end_time;
    }

    public long getDuration_ms() {
        return duration_ms;
    }

    public void setDuration_ms(long duration_ms) {
        this.duration_ms = duration_ms;
    }

    public String getEvent_severity() {
        return event_severity;
    }

    public void setEvent_severity(String event_severity) {
        this.event_severity = event_severity;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public String getEntity_type() {
        return entity_type;
    }

    public void setEntity_type(String entity_type) {
        this.entity_type = entity_type;
    }

    public String getClient_ip() {
        return client_ip;
    }

    public void setClient_ip(String client_ip) {
        this.client_ip = client_ip;
    }

    public String getClient_location_country() {
        return client_location_country;
    }

    public void setClient_location_country(String client_location_country) {
        this.client_location_country = client_location_country;
    }

    public String getClient_location_province() {
        return client_location_province;
    }

    public void setClient_location_province(String client_location_province) {
        this.client_location_province = client_location_province;
    }

    public String getClient_location_region() {
        return client_location_region;
    }

    public void setClient_location_region(String client_location_region) {
        this.client_location_region = client_location_region;
    }

    public String getClient_asn() {
        return client_asn;
    }

    public void setClient_asn(String client_asn) {
        this.client_asn = client_asn;
    }

    public String getServer_ip() {
        return server_ip;
    }

    public void setServer_ip(String server_ip) {
        this.server_ip = server_ip;
    }

    public String getServer_location_country() {
        return server_location_country;
    }

    public void setServer_location_country(String server_location_country) {
        this.server_location_country = server_location_country;
    }

    public String getServer_location_province() {
        return server_location_province;
    }

    public void setServer_location_province(String server_location_province) {
        this.server_location_province = server_location_province;
    }

    public String getServer_location_region() {
        return server_location_region;
    }

    public void setServer_location_region(String server_location_region) {
        this.server_location_region = server_location_region;
    }

    public String getServer_asn() {
        return server_asn;
    }

    public void setServer_asn(String server_asn) {
        this.server_asn = server_asn;
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

    public String getDomain_reputation_score() {
        return domain_reputation_score;
    }

    public void setDomain_reputation_score(String domain_reputation_score) {
        this.domain_reputation_score = domain_reputation_score;
    }

    public String getDomain_reputation_level() {
        return domain_reputation_level;
    }

    public void setDomain_reputation_level(String domain_reputation_level) {
        this.domain_reputation_level = domain_reputation_level;
    }

    public String getApp_name() {
        return app_name;
    }

    public void setApp_name(String app_name) {
        this.app_name = app_name;
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

    public String getApp_risk() {
        return app_risk;
    }

    public void setApp_risk(String app_risk) {
        this.app_risk = app_risk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PerformanceEventLog)) {
            return false;
        }
        PerformanceEventLog that = (PerformanceEventLog) o;
        return getEvent_id() == that.getEvent_id() &&
                getStart_time() == that.getStart_time() &&
                getEnd_time() == that.getEnd_time() &&
                getDuration_ms() == that.getDuration_ms() &&
                Objects.equals(getEvent_key(), that.getEvent_key()) &&
                Objects.equals(getEvent_severity(), that.getEvent_severity()) &&
                Objects.equals(getEvent_type(), that.getEvent_type()) &&
                Objects.equals(getEntity_type(), that.getEntity_type()) &&
                Objects.equals(getClient_ip(), that.getClient_ip()) &&
                Objects.equals(getClient_location_country(), that.getClient_location_country()) &&
                Objects.equals(getClient_location_province(), that.getClient_location_province()) &&
                Objects.equals(getClient_location_region(), that.getClient_location_region()) &&
                Objects.equals(getClient_asn(), that.getClient_asn()) &&
                Objects.equals(getServer_ip(), that.getServer_ip()) &&
                Objects.equals(getServer_location_country(), that.getServer_location_country()) &&
                Objects.equals(getServer_location_province(), that.getServer_location_province()) &&
                Objects.equals(getServer_location_region(), that.getServer_location_region()) &&
                Objects.equals(getServer_asn(), that.getServer_asn()) &&
                Objects.equals(getDomain(), that.getDomain()) &&
                Objects.equals(getDomain_category_name(), that.getDomain_category_name()) &&
                Objects.equals(getDomain_category_group(), that.getDomain_category_group()) &&
                Objects.equals(getDomain_reputation_score(), that.getDomain_reputation_score()) &&
                Objects.equals(getDomain_reputation_level(), that.getDomain_reputation_level()) &&
                Objects.equals(getApp_name(), that.getApp_name()) &&
                Objects.equals(getApp_category(), that.getApp_category()) &&
                Objects.equals(getApp_subcategory(), that.getApp_subcategory()) &&
                Objects.equals(getApp_risk(), that.getApp_risk());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEvent_id(), getEvent_key(), getStart_time(), getEnd_time(), getDuration_ms(), getEvent_severity(), getEvent_type(), getEntity_type(), getClient_ip(), getClient_location_country(), getClient_location_province(), getClient_location_region(), getClient_asn(), getServer_ip(), getServer_location_country(), getServer_location_province(), getServer_location_region(), getServer_asn(), getDomain(), getDomain_category_name(), getDomain_category_group(), getDomain_reputation_score(), getDomain_reputation_level(), getApp_name(), getApp_category(), getApp_subcategory(), getApp_risk());
    }

    @Override
    public String toString() {
        return "PerformanceEventLog{" +
                "event_id=" + event_id +
                ", event_key='" + event_key + '\'' +
                ", start_time=" + start_time +
                ", end_time=" + end_time +
                ", duration_ms=" + duration_ms +
                ", event_severity='" + event_severity + '\'' +
                ", event_type='" + event_type + '\'' +
                ", entity_type='" + entity_type + '\'' +
                ", client_ip='" + client_ip + '\'' +
                ", client_location_country='" + client_location_country + '\'' +
                ", client_location_province='" + client_location_province + '\'' +
                ", client_location_region='" + client_location_region + '\'' +
                ", client_asn='" + client_asn + '\'' +
                ", server_ip='" + server_ip + '\'' +
                ", server_location_country='" + server_location_country + '\'' +
                ", server_location_province='" + server_location_province + '\'' +
                ", server_location_region='" + server_location_region + '\'' +
                ", server_asn='" + server_asn + '\'' +
                ", domain='" + domain + '\'' +
                ", domain_category_name='" + domain_category_name + '\'' +
                ", domain_category_group='" + domain_category_group + '\'' +
                ", domain_reputation_score='" + domain_reputation_score + '\'' +
                ", domain_reputation_level='" + domain_reputation_level + '\'' +
                ", app_name='" + app_name + '\'' +
                ", app_category='" + app_category + '\'' +
                ", app_subcategory='" + app_subcategory + '\'' +
                ", app_risk='" + app_risk + '\'' +
                '}';
    }
}
