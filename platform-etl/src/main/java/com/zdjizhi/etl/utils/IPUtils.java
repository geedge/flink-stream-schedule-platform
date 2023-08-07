package com.zdjizhi.etl.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.domain.LocationResponseV2;
import com.zdjizhi.utils.GalaxyDataBaseReader;
import com.zdjizhi.utils.IPUtil;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class IPUtils {
    public static IpLookupV2 ipLookup;

    private static final String COUNTRY = "COUNTRY";
    private static final String ISP = "ISP";
    private static final String LONGITUDE = "LONGITUDE";
    private static final String CONTINENT = "CONTINENT";
    private static final String ADMINISTRATIVE_AREA = "ADMINISTRATIVE_AREA";
    private static final String AREA_CODE = "AREA_CODE";
    private static final String ASN = "ASN";
    private static final String LATITUDE = "LATITUDE";
    private static final String SUPER_ADMINISTRATIVE_AREA = "SUPER_ADMINISTRATIVE_AREA";
    private static final String ORGANIZATION = "ORGANIZATION";

    private static final Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    private static final List<String> GEO_FIELD_VALUE_LIST = Arrays.asList(com.zdjizhi.etl.common.CommonConfig.IP_GEO_PATTERN_VALUE.trim().toLowerCase().split(","));

    private static TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();

    private static Exception exceptionCache;

    private static int exceptionCount = 0;

    private static Reader ipv4Reader = null;
    private static Reader ipv6Reader = null;
    private static Reader asnv4Reader = null;
    private static Reader asnv6Reader = null;

    public static void update(String type, InputStream inputStream) throws IOException {
        Reader reader = buildDataReader(inputStream);
        if (CommonInternalConfig.IPV4_TYPE.equals(type)) {
            ipv4Reader = reader;
        }
        if (CommonInternalConfig.IPV6_TYPE.equals(type)) {
            ipv6Reader = reader;
        }
        if (CommonInternalConfig.ASNV4_TYPE.equals(type)) {
            asnv4Reader = reader;
        }
        if (CommonInternalConfig.ASNV6_TYPE.equals(type)) {
            asnv6Reader = reader;
        }
    }

    private static Reader buildDataReader(InputStream stream) throws IOException {
        return stream != null ? new Reader(stream, new CHMCache()) : null;
    }

    private static Map<String, String> getResponse(String ip) {
        Map<String, String> result = new HashMap<>();
        ip = ((String)Optional.ofNullable(ip).orElse("")).trim();
        Reader ipReader = null;
        Reader asnReader = null;
        InetAddress ipAddress = null;
        try {
            ipAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException ignored) {
        }
        if (IPUtil.isIPv4Address(ip)) {
            ipReader = ipv4Reader;
            asnReader = asnv4Reader;
        } else if (IPUtil.isIPv6Address(ip)) {
            ipReader = ipv6Reader;
            asnReader = asnv6Reader;
        }
        if (ipAddress != null) {
            if (StringUtil.isNotEmpty(ipReader)) {
                try {
                    JsonNode response = ipReader.get(ipAddress);
                    if (response != null) {
                        JsonNode countryNode = response.get(COUNTRY);
                        result.put(COUNTRY, countryNode == null ? "" : countryNode.asText(""));
                        JsonNode ispNode = response.get(ISP);
                        result.put(ISP, ispNode == null ? "" : ispNode.asText(""));
                        JsonNode longitudeNode = response.get(LONGITUDE);
                        result.put(LONGITUDE, longitudeNode == null ? "" : longitudeNode.asText(""));
                        JsonNode administrativeAreaNode = response.get(ADMINISTRATIVE_AREA);
                        result.put(ADMINISTRATIVE_AREA, administrativeAreaNode == null ? "" : administrativeAreaNode.asText(""));
                        JsonNode areaCodeNode = response.get(AREA_CODE);
                        result.put(AREA_CODE, areaCodeNode == null ? "" : areaCodeNode.asText(""));
                        JsonNode latitudeNode = response.get(LATITUDE);
                        result.put(LATITUDE, latitudeNode == null ? "" : latitudeNode.asText(""));
                        JsonNode superAdministrativeAreaNode = response.get(SUPER_ADMINISTRATIVE_AREA);
                        result.put(SUPER_ADMINISTRATIVE_AREA, superAdministrativeAreaNode == null ? "" : superAdministrativeAreaNode.asText(""));
                        JsonNode organizationNode = response.get(ORGANIZATION);
                        result.put(ORGANIZATION, organizationNode == null ? "" : organizationNode.asText(""));
                    }
                } catch (Exception e) {
                    LOG.error("location ip of ip error:" + e.getMessage(), e);
                }
            }
            if (StringUtil.isNotEmpty(asnReader)) {
                try {
                    JsonNode asnResponse = asnReader.get(ipAddress);
                    if (asnResponse != null) {
                        JsonNode asnNode = asnResponse.get(ASN);
                        result.put(ASN, asnNode == null ? "" : asnNode.asText(""));
                    }
                } catch (Exception e) {
                    LOG.error("location asn of ip  error:" + e.getMessage(), e);
                }
            }
        }
        return result;
    }

    public static void setMmdbMessage(CnRecordLog entity) {
        try {
            String clientIp = entity.getCommon_client_ip();
            Map<String, String> clientIpResponse = getResponse(clientIp);
            entity.setClient_country(clientIpResponse.getOrDefault(COUNTRY, ""));
            entity.setClient_province(clientIpResponse.getOrDefault(SUPER_ADMINISTRATIVE_AREA, ""));
            entity.setClient_region(clientIpResponse.getOrDefault(ADMINISTRATIVE_AREA, ""));
            entity.setClient_isp(clientIpResponse.getOrDefault(ISP, ""));
            entity.setClient_whois_owner(clientIpResponse.getOrDefault(ORGANIZATION, ""));
            entity.setClient_longitude(formatStringtoDouble(clientIpResponse.getOrDefault(LONGITUDE, "")));
            entity.setClient_latitude(formatStringtoDouble(clientIpResponse.getOrDefault(LATITUDE, "")));
            entity.setClient_asn(clientIpResponse.getOrDefault(ASN, ""));

            String serverIp = entity.getCommon_server_ip();
            Map<String, String> serverIpResponse = getResponse(serverIp);
            entity.setServer_country(serverIpResponse.getOrDefault(COUNTRY, ""));
            entity.setServer_province(serverIpResponse.getOrDefault(SUPER_ADMINISTRATIVE_AREA, ""));
            entity.setServer_region(serverIpResponse.getOrDefault(ADMINISTRATIVE_AREA, ""));
            entity.setServer_isp(serverIpResponse.getOrDefault(ISP, ""));
            entity.setServer_whois_owner(serverIpResponse.getOrDefault(ORGANIZATION, ""));
            entity.setServer_longitude(formatStringtoDouble(serverIpResponse.getOrDefault(LONGITUDE, "")));
            entity.setServer_latitude(formatStringtoDouble(serverIpResponse.getOrDefault(LATITUDE, "")));
            entity.setServer_asn(serverIpResponse.getOrDefault(ASN, ""));
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

    public static void readInternalIpCsv(HighCsvReader csvReader) {
        try {
            TreeRangeSet<IPAddress> newTreeRangeSet = TreeRangeSet.create();
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
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