package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IPUtils {
    public static IpLookupV2 ipLookup=new IpLookupV2.Builder(false)
            .loadDataFileV4(CommonConfig.IP_MMDB_PATH + "ip_v4.mmdb")
            .loadAsnDataFileV4(CommonConfig.IP_MMDB_PATH + "asn_v4.mmdb")
            .loadAsnDataFileV6(CommonConfig.IP_MMDB_PATH + "asn_v6.mmdb")
            .build();
    private static Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    public static void getMmdbMessage(CnRecordLog entity){
        String clientIp = entity.getCommon_client_ip();
        entity.setClient_country(ipLookup.countryLookup(clientIp));
        entity.setClient_province(ipLookup.provinceLookup(clientIp));
        entity.setClient_region(ipLookup.cityLookup(clientIp));
        entity.setClient_isp(ipLookup.ispLookup(clientIp));
        entity.setClient_whois_owner(ipLookup.organizationLookup(clientIp));

        String serverIp = entity.getCommon_server_ip();
        entity.setServer_country(ipLookup.countryLookup(serverIp));
        entity.setServer_province(ipLookup.provinceLookup(serverIp));
        entity.setServer_region(ipLookup.cityLookup(serverIp));
        entity.setServer_isp(ipLookup.ispLookup(serverIp));
        entity.setServer_whois_owner(ipLookup.organizationLookup(serverIp));

        String ciplnglangs =ipLookup.latLngLookup(clientIp);
        if(StringUtil.isBlank(ciplnglangs) || ",".equals(ciplnglangs)){
            entity.setClient_longitude(formatStringtoDouble("")) ;
            entity.setClient_latitude(formatStringtoDouble(""));
        } else{
            String[] ciplnglang = ciplnglangs.split(",");
            entity.setClient_longitude(formatStringtoDouble(ciplnglang[1]));
            entity.setClient_latitude(formatStringtoDouble(ciplnglang[0]));
        }
        String siplnglangs =ipLookup.latLngLookup(serverIp);
        if(StringUtil.isBlank(siplnglangs) || ",".equals(siplnglangs)){
            entity.setServer_longitude(formatStringtoDouble(""));
            entity.setServer_latitude(formatStringtoDouble(""));
        } else{
            String[] siplnglang = siplnglangs.split(",");
            entity.setServer_longitude(formatStringtoDouble(siplnglang[1]));
            entity.setServer_latitude(formatStringtoDouble(siplnglang[0]));
        }
    }

    private static Double formatStringtoDouble(String value){
        Double result =0.0;
        if("N/A".equals(value) || StringUtil.isBlank(value)){
            return result;
        } else{
            return Double.valueOf(value);
        }
    }

    private static Double getLongitude(String ip){
        Double longitude =0.0;
        try {
            String[] lnglang =  ipLookup.latLngLookup(ip).split(",");
            longitude = Double.valueOf(lnglang[1]);
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return longitude;
    }

    private static Double getLatitude(String ip){
        Double latitude =0.0;
        try {
            String[] lnglang =  ipLookup.latLngLookup(ip).split(",");
            latitude = Double.valueOf(lnglang[0]);
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return latitude;
    }

    public static String getCountry(String ip)  {
        String country="";
        try {
            country = ipLookup.countryLookup(ip);
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return country;
    }

    public static String getCity(String ip)  {
        String city="";
        try {
            city = ipLookup.cityLookup(ip);
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return city;
    }
    public static void main(String[] args) throws Exception {
        String ip = "224.0.0.251";
        String site =  IPUtils.getLongitude(ip)+ "-" + IPUtils.getLatitude(ip);
        String sss =ipLookup.cityLookup(ip);
        String jjj =ipLookup.provinceLookup(ip);
        String hh =ipLookup.countryLookup(ip);
        System.out.println(ipLookup.latLngLookup(ip));
        System.out.println(site);
        System.out.println(jjj);

        System.out.println(hh);

    }
}