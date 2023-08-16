package com.zdjizhi.etl.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.utils.IPUtil;
import com.zdjizhi.utils.IpLookupV2;
import com.zdjizhi.utils.StringUtil;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            entity.setClient_country_region(clientIpResponse.getOrDefault(COUNTRY, ""));
            entity.setClient_super_admin_area(clientIpResponse.getOrDefault(SUPER_ADMINISTRATIVE_AREA, ""));
            entity.setClient_admin_area(clientIpResponse.getOrDefault(ADMINISTRATIVE_AREA, ""));
            entity.setClient_isp(clientIpResponse.getOrDefault(ISP, ""));
            entity.setClient_longitude(formatStringtoDouble(clientIpResponse.getOrDefault(LONGITUDE, "")));
            entity.setClient_latitude(formatStringtoDouble(clientIpResponse.getOrDefault(LATITUDE, "")));
            entity.setClient_asn(clientIpResponse.getOrDefault(ASN, ""));

            String serverIp = entity.getCommon_server_ip();
            Map<String, String> serverIpResponse = getResponse(serverIp);
            entity.setServer_country_region(serverIpResponse.getOrDefault(COUNTRY, ""));
            entity.setServer_super_admin_area(serverIpResponse.getOrDefault(SUPER_ADMINISTRATIVE_AREA, ""));
            entity.setServer_admin_area(serverIpResponse.getOrDefault(ADMINISTRATIVE_AREA, ""));
            entity.setServer_isp(serverIpResponse.getOrDefault(ISP, ""));
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
                        IPAddress ipAddress = new IPAddress(ip1);
                        if (ipAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeSet.add(Range.closed(ipAddress, ipAddress));
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddress(ip1);
                        IPAddress endIpAddress = new IPAddress(ip2);
                        if (startIpAddress.getIpAddress() == null || endIpAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeSet.add(Range.closed(startIpAddress, endIpAddress));
                    }
                    if ("CIDR".equals(addrFormat)) {
                        inet.ipaddr.IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        inet.ipaddr.IPAddress startIpAddress = cidrIpAddress.getLower();
                        inet.ipaddr.IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeSet.add(Range.closed(new IPAddress(startIpAddress), new IPAddress(endIpAddress)));
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
            String commonServertIp = recordLog.getCommon_server_ip();

            IPAddress clientIpAddress = new IPAddress(commonClientIp);
            IPAddress serverIpAddress = new IPAddress(commonServertIp);

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

            if (clientIpAddress.getIpAddress() != null && clientIpAddress.getIpAddress().isIPv4() && clientIpAddress.getIpAddress().toIPv4().isPrivate()) {
                recordLog.setClient_zone("internal");
            } else if (clientIpAddress.getIpAddress() != null && clientIpAddress.getIpAddress().isIPv6() && clientIpAddress.getIpAddress().toIPv6().isLocal()) {
                recordLog.setClient_zone("internal");
            }

            if (serverIpAddress.getIpAddress() != null && serverIpAddress.getIpAddress().isIPv4() && serverIpAddress.getIpAddress().toIPv4().isPrivate()) {
                recordLog.setServer_zone("internal");
            } else if (serverIpAddress.getIpAddress() != null && serverIpAddress.getIpAddress().isIPv6() && serverIpAddress.getIpAddress().toIPv6().isLocal()) {
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
        if (ipAddress.getIpAddress().isIPv4() && ipAddress.getIpAddress().toIPv4().isPrivate()) {
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
        long flag = recordLog.getCommon_flags();
        if ((flag & 0x00000008) == 0x00000008) {
            recordLog.setClient_zone("internal");
            recordLog.setServer_zone("external");
        } else if ((flag & 0x00000010) == 0x00000010) {
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
}