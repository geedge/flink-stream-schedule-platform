package com.zdjizhi.security.handler;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.utils.SnowflakeId;
import com.zdjizhi.security.common.CommonConfig;
import com.zdjizhi.security.common.IocJsonLog;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.utils.DateUtils;

import java.util.Date;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/4
 * @ Description
 */
public class SecurityHandler {

    public static void initMessage(SecurityEventLog securityEventLog, CnRecordLog cnRecordLog){
        securityEventLog.setEvent_id(SnowflakeId.generateId());
        long recv_time = cnRecordLog.getCommon_recv_time();
        Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recv_time * 1000));
        long start_time = (granularityFiveMinute.getTime()) / 1000;
        long end_time = start_time + CommonConfig.END_TIME;
        securityEventLog.setStart_time(start_time);
        securityEventLog.setEnd_time(end_time);
        securityEventLog.setDuration_ms((end_time - start_time) * 1000);
        securityEventLog.setVictim_ip(cnRecordLog.getCommon_client_ip());
        securityEventLog.setVictim_location_country(cnRecordLog.getClient_country_region());
        securityEventLog.setVictim_location_province(cnRecordLog.getClient_super_admin_area());
        securityEventLog.setVictim_location_region(cnRecordLog.getClient_admin_area());
        securityEventLog.setServer_ip(cnRecordLog.getCommon_server_ip());
        securityEventLog.setClient_ip(cnRecordLog.getCommon_client_ip());
        securityEventLog.setHttp_url(cnRecordLog.getHttp_url());
        //offender_ip
        securityEventLog.setOffender_ip(cnRecordLog.getCommon_server_ip());
        securityEventLog.setOffender_location_country(cnRecordLog.getServer_country_region());
        securityEventLog.setOffender_location_province(cnRecordLog.getServer_super_admin_area());
        securityEventLog.setOffender_location_region(cnRecordLog.getServer_admin_area());
        //domain
        securityEventLog.setDomain(cnRecordLog.getDomain());
        securityEventLog.setDomain_category_name(cnRecordLog.getDomain_category_name());
        securityEventLog.setDomain_category_group(cnRecordLog.getDomain_category_group());
        securityEventLog.setDomain_reputation_level(cnRecordLog.getDomain_reputation_level());
        //app
        securityEventLog.setApp_name(cnRecordLog.getCommon_app_label());
        securityEventLog.setApp_category(cnRecordLog.getApp_category());
        securityEventLog.setApp_subcategory(cnRecordLog.getApp_subcategory());
        securityEventLog.setEvent_severity("critical");
    }

    public static SecurityEventLog updateEventLog(SecurityEventLog securityEventLog, CnRecordLog cnRecordLog) {
        long recvTime = cnRecordLog.getCommon_recv_time();
        Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recvTime * 1000));
        long startTime = (granularityFiveMinute.getTime()) / 1000;
        long endTime = startTime + com.zdjizhi.security.common.CommonConfig.END_TIME;
        securityEventLog.setStart_time(recvTime > startTime ? startTime : recvTime);
        securityEventLog.setEnd_time(recvTime > endTime ? recvTime : endTime);
        securityEventLog.setDuration_ms((endTime - startTime) * 1000);
        return securityEventLog;
    }

    public static void setIocField(SecurityEventLog securityEventLog, String domain, IocJsonLog iocJsonLog) {
        securityEventLog.setSecurity_type(iocJsonLog.getThreat_type());
        securityEventLog.setEvent_key(domain + " " + securityEventLog.getSecurity_type());
        securityEventLog.setMalware_name(iocJsonLog.getMalware_name());
        securityEventLog.setMalware_alias(iocJsonLog.getMalware_alias());
        securityEventLog.setIoc_type(iocJsonLog.getIoc_type());
        securityEventLog.setIoc_value(iocJsonLog.getIoc_value());
        securityEventLog.setMalware_description(iocJsonLog.getMitre_attack_description());
        securityEventLog.setMalware_platforms(iocJsonLog.getMitre_attack_platforms());
        securityEventLog.setMalware_techniques(iocJsonLog.getMitre_attack_techniques());
        securityEventLog.setMalware_groups(iocJsonLog.getMitre_attack_groups());
    }



}
