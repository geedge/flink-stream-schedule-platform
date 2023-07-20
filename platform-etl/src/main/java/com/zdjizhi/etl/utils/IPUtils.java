package com.zdjizhi.etl.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.util.*;

public class IPUtils {
    public static IpLookupV2 ipLookup;

    private static final Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    private static final List<String> GEO_FIELD_VALUE_LIST = Arrays.asList(com.zdjizhi.etl.common.CommonConfig.IP_GEO_PATTERN_VALUE.trim().toLowerCase().split(","));

    private static TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();

    private static byte[] ipv4Bytes;

    private static byte[] ipv6Bytes;

    private static byte[] asnv4Bytes;

    private static byte[] asnv6Bytes;

    private static final String FILE_SUFFIX = ".mmdb";

    private static Exception exceptionCache;

    private static int exceptionCount = 0;

    static {
        try {
            System.out.println("begin init ipUtils");
            IpLookupV2.Builder builder = new IpLookupV2.Builder(false);
            ipv4Bytes = FileUtils.getFileBytes(CommonInternalConfig.IPV4_TYPE + FILE_SUFFIX);
            if (ipv4Bytes != null && ipv4Bytes.length > 0) {
                InputStream ipv4InputStream = new ByteArrayInputStream(ipv4Bytes);
                builder.loadDataFileV4(ipv4InputStream);
            }
            ipv6Bytes = FileUtils.getFileBytes(CommonInternalConfig.IPV6_TYPE + FILE_SUFFIX);
            if (ipv6Bytes != null && ipv6Bytes.length > 0) {
                InputStream ipv6InputStream = new ByteArrayInputStream(ipv6Bytes);
                builder.loadDataFileV6(ipv6InputStream);
            }
            asnv4Bytes = FileUtils.getFileBytes(CommonInternalConfig.ASNV4_TYPE + FILE_SUFFIX);
            if (asnv4Bytes != null && asnv4Bytes.length > 0) {
                InputStream asnv4InputStream = new ByteArrayInputStream(asnv4Bytes);
                builder.loadAsnDataFileV4(asnv4InputStream);
            }
            asnv6Bytes = FileUtils.getFileBytes(CommonInternalConfig.ASNV6_TYPE + FILE_SUFFIX);
            if (asnv6Bytes != null && asnv6Bytes.length > 0) {
                InputStream asnv6InputStream = new ByteArrayInputStream(asnv6Bytes);
                builder.loadAsnDataFileV6(asnv6InputStream);
            }
            ipLookup = builder.build();
            System.out.println("ipUtils init over");
        } catch (Exception e) {
            LOG.error("加载失败", e);
        }
    }


    public static void updateIpLook(Map<String, byte[]> knowledgeFileCache) {
        try {
            boolean needUpdate = false;
            IpLookupV2 newIpLookup;
            InputStream ipv4InputStream = null;
            InputStream ipv6InputStream = null;
            InputStream asnv4InputStream = null;
            InputStream asnv6InputStream = null;
            if (knowledgeFileCache.containsKey(CommonInternalConfig.IPV4_TYPE + FILE_SUFFIX)) {
                ipv4Bytes = knowledgeFileCache.get(CommonInternalConfig.IPV4_TYPE + FILE_SUFFIX);
                if (ipv4Bytes.length != 0) {
                    ipv4InputStream = new ByteArrayInputStream(ipv4Bytes);
                }
                needUpdate = true;
            }
            if (knowledgeFileCache.containsKey(CommonInternalConfig.IPV6_TYPE + FILE_SUFFIX)) {
                ipv6Bytes = knowledgeFileCache.get(CommonInternalConfig.IPV6_TYPE + FILE_SUFFIX);
                if (ipv6Bytes.length != 0) {
                    ipv6InputStream = new ByteArrayInputStream(ipv6Bytes);
                }
                needUpdate = true;
            }
            if (knowledgeFileCache.containsKey(CommonInternalConfig.ASNV4_TYPE + FILE_SUFFIX)) {
                asnv4Bytes = knowledgeFileCache.get(CommonInternalConfig.ASNV4_TYPE + FILE_SUFFIX);
                if (asnv4Bytes.length != 0) {
                    asnv4InputStream = new ByteArrayInputStream(asnv4Bytes);
                }
                needUpdate = true;
            }
            if (knowledgeFileCache.containsKey(CommonInternalConfig.ASNV6_TYPE + FILE_SUFFIX)) {
                asnv6Bytes = knowledgeFileCache.get(CommonInternalConfig.ASNV6_TYPE + FILE_SUFFIX);
                if (asnv6Bytes.length != 0) {
                    asnv6InputStream = new ByteArrayInputStream(asnv6Bytes);
                }
                needUpdate = true;
            }
            if (needUpdate) {
                IpLookupV2.Builder builder = new IpLookupV2.Builder(false);
                newIpLookup = builder.loadDataFileV4(ipv4InputStream)
                        .loadDataFileV6(ipv6InputStream)
                        .loadAsnDataFileV4(asnv4InputStream)
                        .loadAsnDataFileV6(asnv6InputStream)
                        .build();
                ipLookup = newIpLookup;
                exceptionCache = null;
                exceptionCount = 0;
            }
        } catch (Exception e) {
            System.out.println("IpUtils parse error：" + e);
            e.printStackTrace(System.out);
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
            if (exceptionEquals(exceptionCache, e)) {
                exceptionCount++;
                if (exceptionCount < 3) {
                    LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
                }
                if (exceptionCount == 3) {
                    LOG.error("相同的错误异常次数超过3次,后续相同的异常会被隐藏");
                }
            } else {
                exceptionCache = e;
                exceptionCount = 0;
                LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
            }
        }
    }

    private static boolean exceptionEquals(Exception e1, Exception e2) {
        if (e1 == null) {
            return false;
        }
        if (!e1.toString().equals(e2.toString())) {
            return false;
        }
        return Arrays.equals(e1.getStackTrace(), e2.getStackTrace());
    }

    public static void readInternalIpCsv(Reader internalIpReader) {
        try {
            TreeRangeSet<IPAddress> newTreeRangeSet = TreeRangeSet.create();
            CsvReaderUtils csvReader = new CsvReaderUtils(internalIpReader);
            List<Map<String, String>> data = csvReader.readAll();
            for (Map<String, String> line : data) {
                try {
                    String addrFormat = line.get("addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");

                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddressString(ip1).getAddress();
                        if (ipAddress == null) {
                            continue;
                        }
                        newTreeRangeSet.add(Range.closed(ipAddress, ipAddress));
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddressString(ip1).getAddress();
                        IPAddress endIpAddress = new IPAddressString(ip2).getAddress();
                        if (startIpAddress == null || endIpAddress == null) {
                            continue;
                        }
                        newTreeRangeSet.add(Range.closed(startIpAddress, endIpAddress));
                    }
                    if ("CIDR".equals(addrFormat)) {
                        IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        IPAddress startIpAddress = cidrIpAddress.getLower();
                        IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeSet.add(Range.closed(startIpAddress, endIpAddress));
                    }
                } catch (Exception lineException) {
                    System.out.println("IpUtils line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
            treeRangeSet = newTreeRangeSet;
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

            if (clientIpAddress != null && clientIpAddress.isIPv4() && clientIpAddress.toIPv4().isPrivate()) {
                recordLog.setClient_zone("internal");
            } else if (clientIpAddress != null && clientIpAddress.isIPv6() && clientIpAddress.toIPv6().isLocal()) {
                recordLog.setClient_zone("internal");
            }

            if (serverIpAddress != null && serverIpAddress.isIPv4() && serverIpAddress.toIPv4().isPrivate()) {
                recordLog.setServer_zone("internal");
            } else if (serverIpAddress != null && serverIpAddress.isIPv6() && serverIpAddress.toIPv6().isLocal()) {
                recordLog.setServer_zone("internal");
            }

            setNotCommonMetric(recordLog);
            setSessionQueryNum(recordLog);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", recordLog, e);
        }
    }

    private static void setIpFlagByFile(IPAddress ipAddress, String type, CnRecordLog recordLog) {
        if (ipAddress == null) {
            return;
        }
        if (ipAddress.isIPv4() && ipAddress.toIPv4().isPrivate()) {
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
        } else if (GEO_FIELD_VALUE_LIST.contains(clientGeo)) {
            recordLog.setClient_zone("internal");
        } else {
            recordLog.setClient_zone("external");
        }

        if (StringUtil.isBlank(serverGeo)) {
            recordLog.setServer_zone("");
        } else if (GEO_FIELD_VALUE_LIST.contains(serverGeo)) {
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
            recordLog.setTraffic_outbound_byte(commonC2sByteNum);
            recordLog.setTraffic_outbound_pkt(commonC2sPktNum);
        } else if ("external".equals(clientZone) && "internal".equals(serverZone)) {
            recordLog.setTraffic_outbound_byte(commonS2cByteNum);
            recordLog.setTraffic_outbound_pkt(commonS2cPktNum);
            recordLog.setTraffic_inbound_byte(commonC2sByteNum);
            recordLog.setTraffic_inbound_pkt(commonC2sPktNum);
        } else if ("internal".equals(clientZone) && "internal".equals(serverZone)) {
            recordLog.setTraffic_internal_byte(commonS2cByteNum + commonC2sByteNum);
            recordLog.setTraffic_internal_pkt(commonS2cPktNum + commonC2sPktNum);
        } else if ("external".equals(clientZone) && "external".equals(serverZone)) {
            recordLog.setTraffic_through_byte(commonS2cByteNum + commonC2sByteNum);
            recordLog.setTraffic_through_pkt(commonS2cPktNum + commonC2sPktNum);
        }
    }


    private static void setSessionQueryNum(CnRecordLog recordLog) {
        String clientZone = recordLog.getClient_zone();

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