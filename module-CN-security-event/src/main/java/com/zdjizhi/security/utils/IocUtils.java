package com.zdjizhi.security.utils;

import com.zdjizhi.security.common.IocJsonLog;
import com.zdjizhi.utils.StringUtil;

import java.util.Map;

public class IocUtils {

    public static void setCommonField(Map<String, String> obj, IocJsonLog iocJsonLog) {
        iocJsonLog.setIoc_type(obj.get("ioc_type"));
        iocJsonLog.setIoc_value(obj.get("ioc_value"));
        iocJsonLog.setThreat_type(obj.get("threat_type"));
        iocJsonLog.setMalware_name(obj.get("malware_name"));
        //malware_alias
        if (StringUtil.isNotEmpty(obj.get("malware_alias"))) {
            iocJsonLog.setMalware_alias(obj.get("malware_alias"));
        } else {
            iocJsonLog.setMalware_alias(" ");
        }
        //mitre_attack_description
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_description"))) {
            iocJsonLog.setMitre_attack_description(obj.get("mitre_attack_description"));
        } else {
            iocJsonLog.setMitre_attack_description(" ");
        }
        //mitre_attack_platforms
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_platforms"))) {
            iocJsonLog.setMitre_attack_platforms(obj.get("mitre_attack_platforms"));
        } else {
            iocJsonLog.setMitre_attack_platforms(" ");
        }
        //mitre_attack_techniques
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_techniques"))) {
            String mitreAttackTechniques = obj.get("mitre_attack_techniques").replace("\"", "");
            iocJsonLog.setMitre_attack_techniques(mitreAttackTechniques);
        } else {
            iocJsonLog.setMitre_attack_techniques(" ");
        }
        //mitre_attack_groups
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_groups"))) {
            iocJsonLog.setMitre_attack_groups(obj.get("mitre_attack_groups").replace("\"", ""));
        } else {
            iocJsonLog.setMitre_attack_groups(" ");
        }
    }
}
