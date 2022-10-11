package com.zdjizhi.etl.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;

public class IPUtils {
    public static IpLookupV2 ipLookup = new IpLookupV2.Builder(false)
            .loadDataFileV4(CommonConfig.IP_MMDB_PATH + "ip_v4.mmdb")
            .loadAsnDataFileV4(CommonConfig.IP_MMDB_PATH + "asn_v4.mmdb")
            .loadAsnDataFileV6(CommonConfig.IP_MMDB_PATH + "asn_v6.mmdb")
            .build();
    private static Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);

    private static TreeRangeSet<IPAddress> treeRangeSet = readInternalIpCsv(CommonConfig.INTERNAL_IP_PATH);

    public static void setMmdbMessage(CnRecordLog entity) {
        String clientIp = entity.getCommon_client_ip();
        HashMap<String, Object> clientIpMap = jsonMapperInstance.fromJson(ipLookup.infoLookupToJson(clientIp), hashmapJsonType);
        entity.setClient_country(clientIpMap.getOrDefault("country", StringUtil.EMPTY).toString());
        entity.setClient_province(clientIpMap.getOrDefault("superAdministrativeArea", StringUtil.EMPTY).toString());
        entity.setClient_region(clientIpMap.getOrDefault("administrativeArea", StringUtil.EMPTY).toString());
        entity.setClient_isp(clientIpMap.getOrDefault("isp", StringUtil.EMPTY).toString());
        entity.setClient_whois_owner(clientIpMap.getOrDefault("organization", StringUtil.EMPTY).toString());
        entity.setClient_longitude(formatStringtoDouble(clientIpMap.getOrDefault("longitude", StringUtil.EMPTY).toString()));
        entity.setClient_latitude(formatStringtoDouble(clientIpMap.getOrDefault("latitude", StringUtil.EMPTY).toString()));
        entity.setClient_asn(clientIpMap.getOrDefault("asn", StringUtil.EMPTY).toString());

        String serverIp = entity.getCommon_server_ip();
        HashMap<String, Object> serverIpMap = jsonMapperInstance.fromJson(ipLookup.infoLookupToJson(serverIp), hashmapJsonType);
        entity.setServer_country(serverIpMap.getOrDefault("country", StringUtil.EMPTY).toString());
        entity.setServer_province(serverIpMap.getOrDefault("superAdministrativeArea", StringUtil.EMPTY).toString());
        entity.setServer_region(serverIpMap.getOrDefault("administrativeArea", StringUtil.EMPTY).toString());
        entity.setServer_isp(serverIpMap.getOrDefault("isp", StringUtil.EMPTY).toString());
        entity.setServer_whois_owner(serverIpMap.getOrDefault("organization", StringUtil.EMPTY).toString());
        entity.setServer_longitude(formatStringtoDouble(serverIpMap.getOrDefault("longitude", StringUtil.EMPTY).toString()));
        entity.setServer_latitude(formatStringtoDouble(serverIpMap.getOrDefault("latitude", StringUtil.EMPTY).toString()));
        entity.setServer_asn(serverIpMap.getOrDefault("asn", StringUtil.EMPTY).toString());
    }

    private static TreeRangeSet<IPAddress> readInternalIpCsv(String path) {
        TreeRangeSet<IPAddress> rangeSet = TreeRangeSet.create();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 0);

            for (String[] strs : csvReader) {
                String ip = strs[0];
                IPAddressString ipAddressString = new IPAddressString(ip);
                if (ipAddressString.isIPAddress()) {
                    IPAddress address = ipAddressString.getAddress();
                    IPAddress lower = address.getLower();
                    IPAddress upper = address.getUpper();
                    rangeSet.add(Range.closed(lower, upper));
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error("加载Internal Ip Csv文件失败:", e);
        }
        return rangeSet;
    }

    public static void setInternalIpFlag(CnRecordLog recordLog) {
        String commonClientIp = recordLog.getCommon_client_ip();
        IPAddressString clientIpAddressString = new IPAddressString(commonClientIp);
        if (clientIpAddressString.isIPAddress()) {
            IPAddress clientIpAddress = clientIpAddressString.getAddress();
            if (clientIpAddress.isIPv4() && clientIpAddress.toIPv4().isPrivate()) {
                recordLog.setClient_zone("internal");
            } else if (treeRangeSet.contains(clientIpAddress)) {
                recordLog.setClient_zone("internal");
            } else {
                recordLog.setClient_zone("external");
            }
        }

        String commonServertIp = recordLog.getCommon_server_ip();
        IPAddressString serverIpAddressString = new IPAddressString(commonServertIp);
        if (serverIpAddressString.isIPAddress()) {
            IPAddress serverIpAddress = serverIpAddressString.getAddress();
            if (serverIpAddress.isIPv4() && serverIpAddress.toIPv4().isPrivate()) {
                recordLog.setServer_zone("internal");
            } else if (treeRangeSet.contains(serverIpAddress)) {
                recordLog.setServer_zone("internal");
            } else {
                recordLog.setServer_zone("external");
            }
        }

        setNotCommonMetric(recordLog);
        setSessionQueryNum(recordLog);
    }

    private static void setNotCommonMetric(CnRecordLog recordLog) {
        String clientZone = recordLog.getClient_zone();
        String serverZone = recordLog.getServer_zone();
        long commonS2cByteNum = recordLog.getCommon_s2c_byte_num();
        long commonS2cPktNum = recordLog.getCommon_s2c_pkt_num();
        long commonC2sByteNum = recordLog.getCommon_c2s_byte_num();
        long commonC2sPktNum = recordLog.getCommon_c2s_pkt_num();

        if ("internal".equals(clientZone) && "external".equals(serverZone)) {
            recordLog.setTraffic_inbound_byte(commonS2cByteNum);
            recordLog.setTraffic_inbound_pkt(commonS2cPktNum);
        } else if ("external".equals(clientZone) && "internal".equals(serverZone)) {
            recordLog.setTraffic_outbound_byte(commonS2cByteNum);
            recordLog.setTraffic_outbound_pkt(commonS2cPktNum);
        } else if ("internal".equals(clientZone) && "internal".equals(serverZone)) {
            recordLog.setTraffic_internal_byte(commonS2cByteNum + commonC2sByteNum);
            recordLog.setTraffic_internal_pkt(commonS2cPktNum + commonC2sPktNum);
        } else {
            recordLog.setTraffic_through_byte(commonS2cByteNum + commonC2sByteNum);
            recordLog.setTraffic_through_pkt(commonS2cPktNum + commonC2sPktNum);
        }


    }


    private static void setSessionQueryNum(CnRecordLog recordLog) {
        String clientZone = recordLog.getClient_zone();
//        String serverZone = recordLog.getServer_zone();


        if ("internal".equals(clientZone)) {
            recordLog.setInternal_query_num(recordLog.getCommon_sessions());
            recordLog.setExternal_query_num(0);
        }else if ("external".equals(clientZone) ) {
            recordLog.setExternal_query_num(recordLog.getCommon_sessions());
            recordLog.setInternal_query_num(0);
        }


    }

    private static Double formatStringtoDouble(String value) {
        Double result = 0.0;
        if ("N/A".equals(value) || StringUtil.isBlank(value)) {
            return result;
        } else {
            return Double.valueOf(value);
        }
    }

    public static void main(String[] args) throws Exception {
        String ip = "192.168.44.1";
        System.out.println("cityLookupDetail:" + ipLookup.cityLookupDetail(ip));
        System.out.println("cityLookup:" + ipLookup.cityLookup(ip));
        System.out.println("countryLookup:" + ipLookup.countryLookup(ip));
        System.out.println("locationLookupDetail:" + ipLookup.locationLookupDetail(ip));
        System.out.println("provinceLookup:" + ipLookup.provinceLookup(ip));
        System.out.println("----------------------------");

        long start1 = System.currentTimeMillis();
        System.out.println("administrativeAreaLookupDetail:" + ipLookup.administrativeAreaLookupDetail(ip));
        System.out.println("asnLookupDetail:" + ipLookup.asnLookupDetail(ip));
        System.out.println("cityLookupDetail:" + ipLookup.cityLookupDetail(ip));
        System.out.println("locationLookupDetail:" + ipLookup.locationLookupDetail(ip));
        System.out.println("ispLookup:" + ipLookup.ispLookup(ip));
        System.out.println("cityLatLngLookup:" + ipLookup.cityLatLngLookup(ip));
        long end1 = System.currentTimeMillis();
        System.out.println(end1 - start1);

        System.out.println("--------------------------------------");
        System.out.println("infoLookupToCSV:" + ipLookup.infoLookupToCSV(ip));
        System.out.println("infoLookupToJson:" + ipLookup.infoLookupToJson(ip));
        System.out.println("----------------------------");

        long start2 = System.currentTimeMillis();
        HashMap<String, Object> serverIpMap = jsonMapperInstance.fromJson(ipLookup.infoLookupToJson(ip), hashmapJsonType);
        long end2 = System.currentTimeMillis();
        System.out.println(end2 - start2);
        System.out.println("country:" + serverIpMap.getOrDefault("country", StringUtil.EMPTY));
        System.out.println("superAdministrativeArea:" + serverIpMap.getOrDefault("superAdministrativeArea", StringUtil.EMPTY));
        System.out.println("administrativeArea:" + serverIpMap.getOrDefault("administrativeArea", StringUtil.EMPTY));
        System.out.println("isp:" + serverIpMap.getOrDefault("isp", StringUtil.EMPTY));
        System.out.println("organization:" + serverIpMap.getOrDefault("organization", StringUtil.EMPTY));
        System.out.println("longitude:" + serverIpMap.getOrDefault("longitude", StringUtil.EMPTY));
        System.out.println("latitude:" + serverIpMap.getOrDefault("latitude", StringUtil.EMPTY));
        System.out.println("asn:" + serverIpMap.getOrDefault("asn", StringUtil.EMPTY));

        System.out.println("-------------------------------------");
        IPAddressString ipAddressString = new IPAddressString(ip);
        System.out.println(treeRangeSet.contains(ipAddressString.getAddress()));

        Iterator<Range<IPAddress>> iterator = treeRangeSet.asRanges().iterator();
        while (iterator.hasNext()) {
            Range<IPAddress> next = iterator.next();
            IPAddress upperEndpoint = next.upperEndpoint();
            IPAddress lowerEndpoint = next.lowerEndpoint();
            System.out.println("upperEndpoint:" + upperEndpoint + " lowerEndpoint:" + lowerEndpoint);
        }
    }
}