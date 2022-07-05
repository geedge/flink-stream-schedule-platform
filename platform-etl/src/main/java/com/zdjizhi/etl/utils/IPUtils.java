package com.zdjizhi.etl.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class IPUtils {
    public static IpLookupV2 ipLookup=new IpLookupV2.Builder(false)
            .loadDataFileV4(CommonConfig.IP_MMDB_PATH + "ip_v4.mmdb")
            .loadAsnDataFileV4(CommonConfig.IP_MMDB_PATH + "asn_v4.mmdb")
            .loadAsnDataFileV6(CommonConfig.IP_MMDB_PATH + "asn_v6.mmdb")
            .build();
    private static Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);

    public static void getMmdbMessage(CnRecordLog entity){
        String clientIp = entity.getCommon_client_ip();
        HashMap<String, Object> clientIpMap = jsonMapperInstance.fromJson(ipLookup.infoLookupToJson(clientIp), hashmapJsonType);
        entity.setClient_country(clientIpMap.getOrDefault("country",StringUtil.EMPTY).toString());
        entity.setClient_province(clientIpMap.getOrDefault("superAdministrativeArea",StringUtil.EMPTY).toString());
        entity.setClient_region(clientIpMap.getOrDefault("administrativeArea",StringUtil.EMPTY).toString());
        entity.setClient_isp(clientIpMap.getOrDefault("isp",StringUtil.EMPTY).toString());
        entity.setClient_whois_owner(clientIpMap.getOrDefault("organization",StringUtil.EMPTY).toString());
        entity.setClient_longitude(formatStringtoDouble(clientIpMap.getOrDefault("longitude",StringUtil.EMPTY).toString()));
        entity.setClient_latitude(formatStringtoDouble(clientIpMap.getOrDefault("latitude",StringUtil.EMPTY).toString()));
        entity.setClient_asn(clientIpMap.getOrDefault("asn",StringUtil.EMPTY).toString());

        String serverIp = entity.getCommon_server_ip();
        HashMap<String, Object> serverIpMap = jsonMapperInstance.fromJson(ipLookup.infoLookupToJson(serverIp), hashmapJsonType);
        entity.setServer_country(serverIpMap.getOrDefault("country",StringUtil.EMPTY).toString());
        entity.setServer_province(serverIpMap.getOrDefault("superAdministrativeArea",StringUtil.EMPTY).toString());
        entity.setServer_region(serverIpMap.getOrDefault("administrativeArea",StringUtil.EMPTY).toString());
        entity.setServer_isp(serverIpMap.getOrDefault("isp",StringUtil.EMPTY).toString());
        entity.setServer_whois_owner(serverIpMap.getOrDefault("organization",StringUtil.EMPTY).toString());
        entity.setServer_longitude(formatStringtoDouble(serverIpMap.getOrDefault("longitude",StringUtil.EMPTY).toString()));
        entity.setServer_latitude(formatStringtoDouble(serverIpMap.getOrDefault("latitude",StringUtil.EMPTY).toString()));
        entity.setServer_asn(serverIpMap.getOrDefault("asn",StringUtil.EMPTY).toString());
    }

    private static Double formatStringtoDouble(String value){
        Double result =0.0;
        if("N/A".equals(value) || StringUtil.isBlank(value)){
            return result;
        } else{
            return Double.valueOf(value);
        }
    }

    public static void main(String[] args) throws Exception {
        String ip = "1.1.1.2";
        System.out.println("cityLookupDetail:"+ipLookup.cityLookupDetail(ip));
        System.out.println("cityLookup:"+ipLookup.cityLookup(ip));
        System.out.println("countryLookup:"+ipLookup.countryLookup(ip));
        System.out.println("locationLookupDetail:"+ipLookup.locationLookupDetail(ip));
        System.out.println("provinceLookup:"+ipLookup.provinceLookup(ip));
        System.out.println("----------------------------");

        long start1 = System.currentTimeMillis();
        System.out.println("administrativeAreaLookupDetail:"+ipLookup.administrativeAreaLookupDetail(ip));
        System.out.println("asnLookupDetail:"+ipLookup.asnLookupDetail(ip));
        System.out.println("cityLookupDetail:"+ipLookup.cityLookupDetail(ip));
        System.out.println("locationLookupDetail:"+ipLookup.locationLookupDetail(ip));
        System.out.println("ispLookup:"+ipLookup.ispLookup(ip));
        System.out.println("cityLatLngLookup:"+ipLookup.cityLatLngLookup(ip));
        long end1 = System.currentTimeMillis();
        System.out.println(end1 - start1);

        System.out.println("--------------------------------------");
        System.out.println("infoLookupToCSV:"+ipLookup.infoLookupToCSV(ip));
        System.out.println("infoLookupToJson:"+ipLookup.infoLookupToJson(ip));
        System.out.println("----------------------------");

        long start2 = System.currentTimeMillis();
        HashMap<String, Object> serverIpMap = jsonMapperInstance.fromJson(ipLookup.infoLookupToJson(ip), hashmapJsonType);
        long end2 = System.currentTimeMillis();
        System.out.println(end2 - start2);
        System.out.println("country:"+serverIpMap.getOrDefault("country",StringUtil.EMPTY));
        System.out.println("superAdministrativeArea:"+serverIpMap.getOrDefault("superAdministrativeArea",StringUtil.EMPTY));
        System.out.println("administrativeArea:"+serverIpMap.getOrDefault("administrativeArea",StringUtil.EMPTY));
        System.out.println("isp:"+serverIpMap.getOrDefault("isp",StringUtil.EMPTY));
        System.out.println("organization:"+serverIpMap.getOrDefault("organization",StringUtil.EMPTY));
        System.out.println("longitude:"+serverIpMap.getOrDefault("longitude",StringUtil.EMPTY));
        System.out.println("latitude:"+serverIpMap.getOrDefault("latitude",StringUtil.EMPTY));
        System.out.println("asn:"+serverIpMap.getOrDefault("asn",StringUtil.EMPTY));

//        System.out.println(ipLookup.organizationLookup("120.226.154.122"));

    }
}