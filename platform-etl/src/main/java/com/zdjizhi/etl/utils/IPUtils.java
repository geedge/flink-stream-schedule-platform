package com.zdjizhi.etl.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CustomFile;
import com.zdjizhi.base.utils.FileByteUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IPUtils {
    public static IpLookupV2 ipLookup ;

    private static Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);

    private static TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();

    public static void loadIpLook(){
        try {
            IpLookupV2.Builder builder = new IpLookupV2.Builder(false);
            if ("CLUSTER".equals(CommonConfig.CLUSTER_OR_SINGLE)) {
                byte[] ipv4Bytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "ip_v4.mmdb");
                if (ipv4Bytes!=null){
                    InputStream ipv4InputStream = new ByteArrayInputStream(ipv4Bytes);
                    builder.loadDataFileV4(ipv4InputStream);
                }

                byte[] asnv4Bytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "asn_v4.mmdb");
                if (asnv4Bytes!=null){
                    InputStream asnv4InputStream = new ByteArrayInputStream(asnv4Bytes);
                    builder.loadAsnDataFileV4(asnv4InputStream);
                }

                byte[] asnv6Bytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "asn_v6.mmdb");
                if (asnv6Bytes!=null){
                    InputStream asnv6InputStream = new ByteArrayInputStream(asnv6Bytes);
                    builder.loadAsnDataFileV6(asnv6InputStream);
                }
            }else if ("SINGLE".equals(CommonConfig.CLUSTER_OR_SINGLE)){
                byte[] ipv4Bytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "ip_v4.mmdb");
                if (ipv4Bytes!=null){
                    InputStream ipv4InputStream = new ByteArrayInputStream(ipv4Bytes);
                    builder.loadDataFileV4(ipv4InputStream);
                }

                byte[] asnv4Bytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "asn_v4.mmdb");
                if (asnv4Bytes!=null){
                    InputStream asnv4InputStream = new ByteArrayInputStream(asnv4Bytes);
                    builder.loadAsnDataFileV4(asnv4InputStream);
                }

                byte[] asnv6Bytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "asn_v6.mmdb");
                if (asnv6Bytes!=null){
                    InputStream asnv6InputStream = new ByteArrayInputStream(asnv6Bytes);
                    builder.loadAsnDataFileV6(asnv6InputStream);
                }
            }
            ipLookup = builder.build();

        }catch (Exception e){
            LOG.error("加载失败",e);
        }
    }


    public static void updateIpLook(Map<String, byte[]> knowledgeFileCache){
        try{
            IpLookupV2.Builder builder = new IpLookupV2.Builder(false);
            ipLookup= builder.loadDataFileV4(new ByteArrayInputStream(knowledgeFileCache.get("ip_v4.mmdb")))
                    .loadAsnDataFileV4(new ByteArrayInputStream(knowledgeFileCache.get("asn_v4.mmdb")))
                    .loadAsnDataFileV6(new ByteArrayInputStream(knowledgeFileCache.get("asn_v6.mmdb")))
                    .build();
        }catch (Exception e){
            LOG.error("加载失败",e);
        }

    }

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

    public static void readInternalIpCsv(Reader internalReader) {
//        TreeRangeSet<IPAddress> rangeSet = TreeRangeSet.create();
        try {
//            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(internalReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 0);

            for (String[] strs : csvReader) {
                String ip = strs[0];
                IPAddressString ipAddressString = new IPAddressString(ip);
                if (ipAddressString.isIPAddress()) {
                    IPAddress address = ipAddressString.getAddress();
                    IPAddress lower = address.getLower();
                    IPAddress upper = address.getUpper();
                    treeRangeSet.add(Range.closed(lower, upper));
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error("加载Internal Ip Csv文件失败:", e);
        }
//        return rangeSet;
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
        }else if ("external".equals(clientZone) ) {
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
        loadIpLook();
        String ip = "10.10.44.1";
        System.out.println("cityLookupDetail:" + ipLookup.cityLookupDetail(ip));
        System.out.println("cityLookup:" + ipLookup.cityLookup(ip));
        System.out.println("countryLookup:" + ipLookup.countryLookup(ip));
        System.out.println("locationLookupDetail:" + ipLookup.locationLookupDetail(ip));
        System.out.println("provinceLookup:" + ipLookup.provinceLookup(ip));
        System.out.println("----------------------------");

    }
}