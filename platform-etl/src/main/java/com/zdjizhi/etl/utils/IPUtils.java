package com.zdjizhi.etl.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.FileByteUtils;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.base.utils.HdfsUtils;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class IPUtils {
    public static IpLookupV2 ipLookup ;

    private static Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    private static List<String> geoFieldValueList = Arrays.asList(com.zdjizhi.etl.common.CommonConfig.IP_GEO_PATTERN_VALUE.trim().toLowerCase().split(","));

    private static TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();

    private static byte[] ipv4Bytes;

    private static byte[] asnv4Bytes;

    private static byte[] asnv6Bytes;

    public static void loadIpLook(){
        try {
            IpLookupV2.Builder builder = new IpLookupV2.Builder(false);
            ipv4Bytes = FileUtils.getFileBytes("ip_v4.mmdb");
            if (ipv4Bytes!=null){
                InputStream ipv4InputStream = new ByteArrayInputStream(ipv4Bytes);
                builder.loadDataFileV4(ipv4InputStream);
            }
            asnv4Bytes = FileUtils.getFileBytes("asn_v4.mmdb");
            if (asnv4Bytes!=null){
                InputStream asnv4InputStream = new ByteArrayInputStream(asnv4Bytes);
                builder.loadAsnDataFileV4(asnv4InputStream);
            }
            asnv6Bytes = FileUtils.getFileBytes("asn_v6.mmdb");
            if (asnv6Bytes!=null){
                InputStream asnv6InputStream = new ByteArrayInputStream(asnv6Bytes);
                builder.loadAsnDataFileV6(asnv6InputStream);
            }
            ipLookup = builder.build();
        }catch (Exception e){
            LOG.error("加载失败",e);
        }
    }


    public static void updateIpLook(Map<String, byte[]> knowledgeFileCache){
        try{
            boolean needUpdate = false;
            if (knowledgeFileCache.containsKey("ip_v4.mmdb")) {
                ipv4Bytes = knowledgeFileCache.get("ip_v4.mmdb");
                needUpdate = true;
            }
            if (knowledgeFileCache.containsKey("asn_v4.mmdb")) {
                asnv4Bytes = knowledgeFileCache.get("asn_v4.mmdb");
                needUpdate = true;
            }
            if (knowledgeFileCache.containsKey("asn_v6.mmdb")) {
                asnv6Bytes = knowledgeFileCache.get("asn_v6.mmdb");
                needUpdate = true;
            }
            if (needUpdate) {
                IpLookupV2.Builder builder = new IpLookupV2.Builder(false);
                ipLookup= builder.loadDataFileV4(new ByteArrayInputStream(ipv4Bytes))
                        .loadAsnDataFileV4(new ByteArrayInputStream(asnv4Bytes))
                        .loadAsnDataFileV6(new ByteArrayInputStream(asnv6Bytes))
                        .build();
            }
        }catch (Exception e){
            LOG.error("加载失败",e);
        }

    }

    public static void setMmdbMessage(CnRecordLog entity) {
        try {
            String clientIp = entity.getCommon_client_ip();
            HashMap<String, Object> clientIpMap = JSON.parseObject(ipLookup.infoLookupToJson(clientIp), TypeReference.mapType(HashMap.class, String.class, Object.class));
            entity.setClient_country(clientIpMap.getOrDefault("country", StringUtil.EMPTY).toString());
            entity.setClient_province(clientIpMap.getOrDefault("superAdministrativeArea", StringUtil.EMPTY).toString());
            entity.setClient_region(clientIpMap.getOrDefault("administrativeArea", StringUtil.EMPTY).toString());
            entity.setClient_isp(clientIpMap.getOrDefault("isp", StringUtil.EMPTY).toString());
            entity.setClient_whois_owner(clientIpMap.getOrDefault("organization", StringUtil.EMPTY).toString());
            entity.setClient_longitude(formatStringtoDouble(clientIpMap.getOrDefault("longitude", StringUtil.EMPTY).toString()));
            entity.setClient_latitude(formatStringtoDouble(clientIpMap.getOrDefault("latitude", StringUtil.EMPTY).toString()));
            entity.setClient_asn(clientIpMap.getOrDefault("asn", StringUtil.EMPTY).toString());

            String serverIp = entity.getCommon_server_ip();
            HashMap<String, Object> serverIpMap = JSON.parseObject(ipLookup.infoLookupToJson(serverIp), TypeReference.mapType(HashMap.class, String.class, Object.class));
            entity.setServer_country(serverIpMap.getOrDefault("country", StringUtil.EMPTY).toString());
            entity.setServer_province(serverIpMap.getOrDefault("superAdministrativeArea", StringUtil.EMPTY).toString());
            entity.setServer_region(serverIpMap.getOrDefault("administrativeArea", StringUtil.EMPTY).toString());
            entity.setServer_isp(serverIpMap.getOrDefault("isp", StringUtil.EMPTY).toString());
            entity.setServer_whois_owner(serverIpMap.getOrDefault("organization", StringUtil.EMPTY).toString());
            entity.setServer_longitude(formatStringtoDouble(serverIpMap.getOrDefault("longitude", StringUtil.EMPTY).toString()));
            entity.setServer_latitude(formatStringtoDouble(serverIpMap.getOrDefault("latitude", StringUtil.EMPTY).toString()));
            entity.setServer_asn(serverIpMap.getOrDefault("asn", StringUtil.EMPTY).toString());
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }

    }

    public static void readInternalIpCsv(Reader internalIpReader) {
        try {
            TreeRangeSet<IPAddress> newTreeRangeSet = TreeRangeSet.create();
            CSVReader csvReader = new CSVReader(internalIpReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            List<String[]> lines = csvReader.readAll();
            for (String[] line : lines) {
                try {
                    String addrFormat = line[1];
                    String ip1 = line[2];
                    String ip2 = line[3];

                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddressString(ip1).getAddress();
                        newTreeRangeSet.add(Range.closed(ipAddress, ipAddress));
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddressString(ip1).getAddress();
                        IPAddress endIpAddress = new IPAddressString(ip2).getAddress();
                        newTreeRangeSet.add(Range.closed(startIpAddress, endIpAddress));
                    }
                    if ("CIDR".equals(addrFormat)) {
                        IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        IPAddress startIpAddress = cidrIpAddress.getLower();
                        IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeSet.add(Range.closed(startIpAddress, endIpAddress));
                    }
                } catch (Exception lineException) {
                    System.out.println("IpUtils line: " + Arrays.toString(line) + " 解析异常：" + lineException.getMessage());
                }
            }
            treeRangeSet = newTreeRangeSet;
            csvReader.close();
        } catch (Exception e) {
            LOG.error("加载Internal Ip Csv文件失败:", e);
        }
    }

    public static void setInternalIpFlag(CnRecordLog recordLog) {
        try {
            String commonClientIp = recordLog.getCommon_client_ip();
            IPAddressString clientIpAddressString = new IPAddressString(commonClientIp);

            String commonServertIp = recordLog.getCommon_server_ip();
            IPAddressString serverIpAddressString = new IPAddressString(commonServertIp);

            IPAddress clientIpAddress = clientIpAddressString.getAddress();
            IPAddress serverIpAddress = serverIpAddressString.getAddress();

            switch (com.zdjizhi.etl.common.CommonConfig.IP_INTERNAL_OR_EXTERNAL_PATTERN) {
                case 1:
                    setIpFlagByFile(clientIpAddress, "client", recordLog);
                    setIpFlagByFile(serverIpAddress, "server", recordLog);
                    break;
                case 2:
                    setIpFlagByGeo(recordLog);
                    break;
                case 3:
                    setIpFlagByDirection(recordLog);
                    break;
                default:
                    throw new IllegalArgumentException();
            }

            if (clientIpAddress.isIPv4() && clientIpAddress.toIPv4().isPrivate()) {
                recordLog.setClient_zone("internal");
            } else if (clientIpAddress.isIPv6() && clientIpAddress.toIPv6().isLocal()) {
                recordLog.setClient_zone("internal");
            }

            if (serverIpAddress.isIPv4() && serverIpAddress.toIPv4().isPrivate()) {
                recordLog.setServer_zone("internal");
            } else if (serverIpAddress.isIPv6() && serverIpAddress.toIPv6().isLocal()) {
                recordLog.setServer_zone("internal");
            }

            setNotCommonMetric(recordLog);
            setSessionQueryNum(recordLog);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", recordLog, e);
        }
    }

    private static void setIpFlagByFile(IPAddress ipAddress, String type, CnRecordLog recordLog) {
        if (ipAddress.isIPv4() && ipAddress.toIPv4().isPrivate()){
            return;
        }

        if (treeRangeSet.contains(ipAddress)) {
            if ("client".equals(type)) {
                recordLog.setClient_zone("internal");
            } else {
                recordLog.setServer_zone("internal");
            }
        } else {
            if ("client".equals(type)) {
                recordLog.setClient_zone("external");
            } else {
                recordLog.setServer_zone("external");
            }
        }
    }

    private static void setIpFlagByGeo(CnRecordLog recordLog) {
        String clientGeo = recordLog.getGeo(com.zdjizhi.etl.common.CommonConfig.IP_GEO_PATTERN_FIELD, "client").trim().toLowerCase();
        String serverGeo = recordLog.getGeo(com.zdjizhi.etl.common.CommonConfig.IP_GEO_PATTERN_FIELD, "server").trim().toLowerCase();

        if (StringUtil.isBlank(clientGeo)) {
            recordLog.setClient_zone("");
        } else if (geoFieldValueList.contains(clientGeo)) {
            recordLog.setClient_zone("internal");
        } else {
            recordLog.setClient_zone("external");
        }

        if (StringUtil.isBlank(serverGeo)) {
            recordLog.setServer_zone("");
        } else if (geoFieldValueList.contains(serverGeo)) {
            recordLog.setServer_zone("internal");
        } else {
            recordLog.setServer_zone("external");
        }

    }

    private static void setIpFlagByDirection(CnRecordLog recordLog) {
        int direction = recordLog.getCommon_direction();
        if (direction == 69) {
            recordLog.setClient_zone("internal");
            recordLog.setServer_zone("external");
        } else if (direction == 73) {
            recordLog.setClient_zone("external");
            recordLog.setServer_zone("internal");
        }
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
        } else if ("external".equals(clientZone)) {
            recordLog.setExternal_query_num(recordLog.getCommon_sessions());
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
        HashMap<String, Object> serverIpMap = JSON.parseObject(ipLookup.infoLookupToJson(ip), TypeReference.mapType(HashMap.class, String.class, Object.class));
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