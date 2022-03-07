package com.zdjizhi.security.common;

import java.util.ArrayList;

/**
 * @ Author pengfeixu
 * @ Description
 */
public class SecurityEventLog {
    private long event_id;
    private String event_key;
    private long start_time;
    private long end_time;
    private long duration_ms;
    private String event_severity;
    private String security_type;

    private String victim_ip;
    private String victim_location_country;
    private String victim_location_province;
    private String victim_location_region;
    private String victim_asn;

    private String offender_ip;
    private String offender_location_country;
    private String offender_location_province;
    private String offender_location_region;
    private String offender_asn;

    private String client_ip;
    private String server_ip;
    private String domain;
    private String domain_category_name;
    private String domain_category_group;
    private long domain_reputation_score;
    private String domain_reputation_level;

    private String app_name;
    private String app_category;
    private String app_subcategory;
    private long app_risk;

    private String malware_name;
    private String malware_alias;
    private String malware_description;
    private String malware_platforms;
    private String malware_techniques;
    private String malware_groups;

    public SecurityEventLog() {
    }

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

    public String getSecurity_type() {
        return security_type;
    }

    public void setSecurity_type(String security_type) {
        this.security_type = security_type;
    }

    public String getVictim_ip() {
        return victim_ip;
    }

    public void setVictim_ip(String victim_ip) {
        this.victim_ip = victim_ip;
    }

    public String getVictim_location_country() {
        return victim_location_country;
    }

    public void setVictim_location_country(String victim_location_country) {
        this.victim_location_country = victim_location_country;
    }

    public String getVictim_location_province() {
        return victim_location_province;
    }

    public void setVictim_location_province(String victim_location_province) {
        this.victim_location_province = victim_location_province;
    }

    public String getVictim_location_region() {
        return victim_location_region;
    }

    public void setVictim_location_region(String victim_location_region) {
        this.victim_location_region = victim_location_region;
    }

    public String getVictim_asn() {
        return victim_asn;
    }

    public void setVictim_asn(String victim_asn) {
        this.victim_asn = victim_asn;
    }

    public String getOffender_ip() {
        return offender_ip;
    }

    public void setOffender_ip(String offender_ip) {
        this.offender_ip = offender_ip;
    }

    public String getOffender_location_country() {
        return offender_location_country;
    }

    public void setOffender_location_country(String offender_location_country) {
        this.offender_location_country = offender_location_country;
    }

    public String getOffender_location_province() {
        return offender_location_province;
    }

    public void setOffender_location_province(String offender_location_province) {
        this.offender_location_province = offender_location_province;
    }

    public String getOffender_location_region() {
        return offender_location_region;
    }

    public void setOffender_location_region(String offender_location_region) {
        this.offender_location_region = offender_location_region;
    }

    public String getOffender_asn() {
        return offender_asn;
    }

    public void setOffender_asn(String offender_asn) {
        this.offender_asn = offender_asn;
    }

    public String getClient_ip() {
        return client_ip;
    }

    public void setClient_ip(String client_ip) {
        this.client_ip = client_ip;
    }

    public String getServer_ip() {
        return server_ip;
    }

    public void setServer_ip(String server_ip) {
        this.server_ip = server_ip;
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

    public long getApp_risk() {
        return app_risk;
    }

    public void setApp_risk(long app_risk) {
        this.app_risk = app_risk;
    }

    public String getMalware_name() {
        return malware_name;
    }

    public void setMalware_name(String malware_name) {
        this.malware_name = malware_name;
    }

    public String getMalware_alias() {
        return malware_alias;
    }

    public void setMalware_alias(String malware_alias) {
        this.malware_alias = malware_alias;
    }

    public String getMalware_description() {
        return malware_description;
    }

    public void setMalware_description(String malware_description) {
        this.malware_description = malware_description;
    }

    public String getMalware_platforms() {
        return malware_platforms;
    }

    public void setMalware_platforms(String malware_platforms) {
        this.malware_platforms = malware_platforms;
    }

    public String getMalware_techniques() {
        return malware_techniques;
    }

    public void setMalware_techniques(String malware_techniques) {
        this.malware_techniques = malware_techniques;
    }

    public String getMalware_groups() {
        return malware_groups;
    }

    public void setMalware_groups(String malware_groups) {
        this.malware_groups = malware_groups;
    }


    @Override
    public String toString() {
        return "SecurityEventLog{" +
                "event_id=" + event_id +
                ", event_key='" + event_key + '\'' +
                ", start_time=" + start_time +
                ", end_time=" + end_time +
                ", duration_ms=" + duration_ms +
                ", event_severity='" + event_severity + '\'' +
                ", security_type='" + security_type + '\'' +
                ", victim_ip='" + victim_ip + '\'' +
                ", victim_location_country='" + victim_location_country + '\'' +
                ", victim_location_province='" + victim_location_province + '\'' +
                ", victim_location_region='" + victim_location_region + '\'' +
                ", victim_asn='" + victim_asn + '\'' +
                ", offender_ip='" + offender_ip + '\'' +
                ", offender_location_country='" + offender_location_country + '\'' +
                ", offender_location_province='" + offender_location_province + '\'' +
                ", offender_location_region='" + offender_location_region + '\'' +
                ", offender_asn='" + offender_asn + '\'' +
                ", client_ip='" + client_ip + '\'' +
                ", server_ip='" + server_ip + '\'' +
                ", domain='" + domain + '\'' +
                ", domain_category_name='" + domain_category_name + '\'' +
                ", domain_category_group='" + domain_category_group + '\'' +
                ", domain_reputation_score='" + domain_reputation_score + '\'' +
                ", domain_reputation_level='" + domain_reputation_level + '\'' +
                ", app_name='" + app_name + '\'' +
                ", app_category='" + app_category + '\'' +
                ", app_subcategory='" + app_subcategory + '\'' +
                ", app_risk=" + app_risk +
                ", malware_name='" + malware_name + '\'' +
                ", malware_alias=" + malware_alias +
                ", malware_description='" + malware_description + '\'' +
                ", malware_platforms=" + malware_platforms +
                ", malware_techniques=" + malware_techniques +
                ", malware_groups=" + malware_groups +
////                ", ioc_value=" + ioc_value +
                '}';
    }
}
