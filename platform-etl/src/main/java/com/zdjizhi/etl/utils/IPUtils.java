package com.zdjizhi.etl.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.maxmind.db.CHMCache;
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
import java.io.Reader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class IPUtils {
    public static IpLookupV2 ipLookup;

    private static final Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    private static final List<String> GEO_FIELD_VALUE_LIST = Arrays.asList(com.zdjizhi.etl.common.CommonConfig.IP_GEO_PATTERN_VALUE.trim().toLowerCase().split(","));

    private static TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();

    private static Exception exceptionCache;

    private static int exceptionCount = 0;

    private static GalaxyDataBaseReader ipv4Reader = null;
    private static GalaxyDataBaseReader ipv6Reader = null;
    private static GalaxyDataBaseReader asnv4Reader = null;
    private static GalaxyDataBaseReader asnv6Reader = null;

    public static void update(String type, InputStream inputStream) throws IOException {
        GalaxyDataBaseReader reader = buildDataReader(inputStream);
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

    private static GalaxyDataBaseReader buildDataReader(InputStream stream) throws IOException {
        return stream != null ? (new GalaxyDataBaseReader.Builder(stream)).withCache(new CHMCache()).build() : null;
    }

    private static LocationResponseV2 getResponse(String ip) {
        LocationResponseV2 response = null;
        ip = ((String)Optional.ofNullable(ip).orElse("")).trim();
        if (!IPUtil.isIPAddress(ip)) {
            LOG.warn("Unknown IP format :" + ip);
            return new LocationResponseV2();
        } else {
            GalaxyDataBaseReader ipReader = null;
            GalaxyDataBaseReader asnReader = null;
            InetAddress ipAddress = null;
            try {
                ipAddress = InetAddress.getByName(ip);
            } catch (UnknownHostException unknownHostException) {
                return new LocationResponseV2();
            }
            if (IPUtil.isIPv4Address(ip)) {
                ipReader = ipv4Reader;
                asnReader = asnv4Reader;
            } else if (IPUtil.isIPv6Address(ip)) {
                ipReader = ipv6Reader;
                asnReader = asnv6Reader;
            }

            if (StringUtil.isNotEmpty(ipReader)) {
                try {
                    response = ipReader.locationV2(ipAddress);
                } catch (AddressNotFoundException ignored) {

                }  catch (Exception e) {
                    LOG.error("location ip of ip error:" + e.getMessage(), e);
                }
            }
            if (response == null) {
                response = new LocationResponseV2();
            }
            if (StringUtil.isNotEmpty(asnReader)) {
                try {
                    LocationResponseV2 asnResponse = asnReader.locationV2(ipAddress);
                    if (asnResponse != null && !asnResponse.isPrivateIP()) {
                        if (asnResponse.getAsn() != null) {
                            response.setAsn(asnResponse.getAsn());
                        }
                    }
                } catch (AddressNotFoundException ignored) {

                } catch (Exception e) {
                    LOG.error("location asn of ip  error:" + e.getMessage(), e);
                }
            }
        }
        return response;
    }

    private static String infoLookupToJson(String ip) {
        LocationResponseV2 response = getResponse(ip);
        return JSON.toJSONString(response);
    }

    public static void setMmdbMessage(CnRecordLog entity) {
        try {
            String clientIp = entity.getCommon_client_ip();
            LocationResponseV2 clientIpResponse = getResponse(clientIp);
            entity.setClient_country(clientIpResponse.getCountry());
            entity.setClient_province(clientIpResponse.getSuperAdministrativeArea());
            entity.setClient_region(clientIpResponse.getAdministrativeArea());
            entity.setClient_isp(clientIpResponse.getIsp());
            entity.setClient_whois_owner(clientIpResponse.getOrganization());
            entity.setClient_longitude(formatStringtoDouble(clientIpResponse.getLongitude()));
            entity.setClient_latitude(formatStringtoDouble(clientIpResponse.getLatitude()));
            entity.setClient_asn(clientIpResponse.getAsn());

            String serverIp = entity.getCommon_server_ip();
            LocationResponseV2 serverIpResponse = getResponse(serverIp);
            HashMap<String, Object> serverIpMap = JSON.parseObject(infoLookupToJson(serverIp), TypeReference.mapType(HashMap.class, String.class, Object.class));
            entity.setServer_country(serverIpResponse.getCountry());
            entity.setServer_province(serverIpResponse.getSuperAdministrativeArea());
            entity.setServer_region(serverIpResponse.getAdministrativeArea());
            entity.setServer_isp(serverIpResponse.getIsp());
            entity.setServer_whois_owner(serverIpResponse.getOrganization());
            entity.setServer_longitude(formatStringtoDouble(serverIpResponse.getLongitude()));
            entity.setServer_latitude(formatStringtoDouble(serverIpResponse.getLatitude()));
            entity.setServer_asn(serverIpResponse.getAsn());
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