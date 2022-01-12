package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.utils.IpLookup;
import com.zdjizhi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IPUtils {
    private static IpLookup ipLookup=new IpLookup.Builder(false)
            .loadDataFileV4(CommonConfig.IP_MMDB_PATH + "ip_v4.mmdb")
            .build();
    private static Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    public static void getMmdbMessage(CnRecordLog entity){
        try {
            entity.setClient_country(ipLookup.countryLookup(entity.getCommon_client_ip()));
            entity.setClient_province(ipLookup.provinceLookup(entity.getCommon_client_ip()));
            entity.setClient_region(ipLookup.cityLookup(entity.getCommon_client_ip()));
            entity.setServer_country(ipLookup.countryLookup(entity.getCommon_server_ip()));
            entity.setServer_province(ipLookup.provinceLookup(entity.getCommon_server_ip()));
            entity.setServer_region(ipLookup.cityLookup(entity.getCommon_server_ip()));

            String ciplnglangs =ipLookup.latLngLookup(entity.getCommon_client_ip());
            if(StringUtil.isBlank(ciplnglangs)){
                entity.setClient_longitude(formatStringtoDouble("")) ;
                entity.setClient_latitude(formatStringtoDouble(""));
            } else{
                String[] ciplnglang = ciplnglangs.split(",");
                entity.setClient_longitude(formatStringtoDouble(ciplnglang[1]));
                entity.setClient_latitude(formatStringtoDouble(ciplnglang[0]));
            }
            String siplnglangs =ipLookup.latLngLookup(entity.getCommon_server_ip());
            if(StringUtil.isBlank(siplnglangs)){
                entity.setServer_longitude(formatStringtoDouble(""));
                entity.setServer_latitude(formatStringtoDouble(""));
            } else{
                String[] siplnglang = siplnglangs.split(",");
                entity.setServer_longitude(formatStringtoDouble(siplnglang[1]));
                entity.setServer_latitude(formatStringtoDouble(siplnglang[0]));
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    private static Double formatStringtoDouble(String value){
        Double result =0.0;
        try {
            if("N/A".equals(value) || StringUtil.isBlank(value)){
                return result;
            } else{
                return Double.valueOf(value);
            }
        } catch (Exception e) {
            LOG.error(e.toString());
            return result;
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
        String ip = "116.178.220.248";
        String site =  IPUtils.getLongitude(ip)+ "-" + IPUtils.getLatitude(ip);
        String sss =ipLookup.cityLookup(ip);
        String jjj =ipLookup.provinceLookup(ip);
        String hh =ipLookup.countryLookup(ip);
        System.out.println(site);
        System.out.println(jjj);

        System.out.println(hh);

    }
}