package com.zdjizhi.etl.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.geedgenetworks.domain.LocationResponse;
import com.geedgenetworks.utils.IPUtil;
import com.geedgenetworks.utils.IpLookupV2;
import com.geedgenetworks.utils.StringUtil;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonInternalConfig;
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

    private static final Logger LOG = LoggerFactory.getLogger(IPUtils.class);

    private static final List<String> GEO_FIELD_VALUE_LIST = Arrays.asList(com.zdjizhi.etl.common.CommonConfig.IP_GEO_PATTERN_VALUE.trim().toLowerCase().split(","));

    private static TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();

    private static Exception exceptionCache;

    private static int exceptionCount = 0;

    private static boolean needReload = true;

    private static IpLookupV2.Builder builder = new IpLookupV2.Builder(false);

    public static void init(String type, InputStream inputStream) {
        needReload = true;
        if (CommonInternalConfig.IPV4_TYPE.equals(type)) {
            builder.loadDataFileV4(inputStream);
        }
        if (CommonInternalConfig.IPV6_TYPE.equals(type)) {
            builder.loadDataFileV6(inputStream);
        }
        if (CommonInternalConfig.ASNV4_TYPE.equals(type)) {
            builder.loadAsnDataFileV4(inputStream);
        }
        if (CommonInternalConfig.ASNV6_TYPE.equals(type)) {
            builder.loadAsnDataFileV6(inputStream);
        }
    }

    public static void load() {
        if (needReload) {
            ipLookup = builder.build();
            needReload = false;
        }
    }

    public static void setMmdbMessage(CnRecordLog entity) {
        try {
            String clientIp = entity.getCommon_client_ip();
            Object clientObject = ipLookup.infoLookup(clientIp);
            if (clientObject instanceof LocationResponse) {
                LocationResponse response = (LocationResponse) clientObject;
                entity.setClient_country_region(response.getCountry());
                entity.setClient_super_admin_area(response.getSuperAdministrativeArea());
                entity.setClient_admin_area(response.getAdministrativeArea());
                entity.setClient_isp(response.getIsp());
                entity.setClient_longitude(formatStringtoDouble(response.getLongitude()));
                entity.setClient_latitude(formatStringtoDouble(response.getLatitude()));
            }
            entity.setClient_asn(ipLookup.asnLookup(clientIp));

            String serverIp = entity.getCommon_server_ip();
            Object serverObject = ipLookup.infoLookup(serverIp);
            if (serverObject instanceof LocationResponse) {
                LocationResponse response = (LocationResponse) serverObject;
                entity.setServer_country_region(response.getCountry());
                entity.setServer_super_admin_area(response.getSuperAdministrativeArea());
                entity.setServer_admin_area(response.getAdministrativeArea());
                entity.setServer_isp(response.getIsp());
                entity.setServer_longitude(formatStringtoDouble(response.getLongitude()));
                entity.setServer_latitude(formatStringtoDouble(response.getLatitude()));
            }
            entity.setServer_asn(ipLookup.asnLookup(serverIp));
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