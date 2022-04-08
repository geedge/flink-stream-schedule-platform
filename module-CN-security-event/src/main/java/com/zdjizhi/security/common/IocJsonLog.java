package com.zdjizhi.security.common;

import java.util.ArrayList;
import java.util.List;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/1
 * @ Description
 */
public class IocJsonLog {
    private String first_seen_utc;
    private String ioc_id;
    private String ioc_type;
    private String ioc_value;
    private String fk_malware;
    private String last_seen_utc;
    private String confidence_level;
    private String reference;
    private String tags;
    private String anonymous;
    private String reporter;
    private String malware_name;
    private String threat_type;
    private String  malware_alias;

    private String mitre_attack_description;
    private String mitre_attack_platforms;
    private String mitre_attack_techniques;
    private String mitre_attack_groups;


    public String getThreat_type() {
        return threat_type;
    }

    public void setThreat_type(String threat_type) {
        this.threat_type = threat_type;
    }

    public String getIoc_type() {
        return ioc_type;
    }

    public void setIoc_type(String ioc_type) {
        this.ioc_type = ioc_type;
    }

    public String getIoc_value() {
        return ioc_value;
    }

    public void setIoc_value(String ioc_value) {
        this.ioc_value = ioc_value;
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

    public String getFirst_seen_utc() {
        return first_seen_utc;
    }

    public void setFirst_seen_utc(String first_seen_utc) {
        this.first_seen_utc = first_seen_utc;
    }

    public String getIoc_id() {
        return ioc_id;
    }

    public void setIoc_id(String ioc_id) {
        this.ioc_id = ioc_id;
    }

    public String getFk_malware() {
        return fk_malware;
    }

    public void setFk_malware(String fk_malware) {
        this.fk_malware = fk_malware;
    }

    public String getLast_seen_utc() {
        return last_seen_utc;
    }

    public void setLast_seen_utc(String last_seen_utc) {
        this.last_seen_utc = last_seen_utc;
    }

    public String getConfidence_level() {
        return confidence_level;
    }

    public void setConfidence_level(String confidence_level) {
        this.confidence_level = confidence_level;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getAnonymous() {
        return anonymous;
    }

    public void setAnonymous(String anonymous) {
        this.anonymous = anonymous;
    }

    public String getReporter() {
        return reporter;
    }

    public void setReporter(String reporter) {
        this.reporter = reporter;
    }

    public String getMitre_attack_description() {
        return mitre_attack_description;
    }

    public void setMitre_attack_description(String mitre_attack_description) {
        this.mitre_attack_description = mitre_attack_description;
    }

    public String getMitre_attack_platforms() {
        return mitre_attack_platforms;
    }

    public void setMitre_attack_platforms(String mitre_attack_platforms) {
        this.mitre_attack_platforms = mitre_attack_platforms;
    }

    public String getMitre_attack_techniques() {
        return mitre_attack_techniques;
    }

    public void setMitre_attack_techniques(String mitre_attack_techniques) {
        this.mitre_attack_techniques = mitre_attack_techniques;
    }

    public String getMitre_attack_groups() {
        return mitre_attack_groups;
    }

    public void setMitre_attack_groups(String mitre_attack_groups) {
        this.mitre_attack_groups = mitre_attack_groups;
    }

    @Override
    public String toString() {
        return "IocJsonLog{" +
                "ioc_type='" + ioc_type + '\'' +
                ", ioc_value='" + ioc_value + '\'' +
                ", malware_name='" + malware_name + '\'' +
                ", threat_type='" + threat_type + '\'' +
                ", mitre_attack_description=" + mitre_attack_description +
                ", mitre_attack_platforms=" + mitre_attack_platforms +
                ", mitre_attack_techniques=" + mitre_attack_techniques +
                ", mitre_attack_groups=" + mitre_attack_groups +
                '}';
    }
}
