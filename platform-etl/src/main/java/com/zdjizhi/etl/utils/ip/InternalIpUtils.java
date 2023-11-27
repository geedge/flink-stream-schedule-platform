package com.zdjizhi.etl.utils.ip;

import com.geedgenetworks.utils.StringUtil;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.common.IPAddress;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class InternalIpUtils {

    private static final Logger logger = LoggerFactory.getLogger(InternalIpUtils.class);

    private static TreeRangeSet<IPAddress> treeRangeSet = TreeRangeSet.create();

    private static int policy = 1;

    private static String geoPolicyField = "province";

    private static String geoPolicyValue = "";

    private final static AtomicBoolean IS_INIT = new AtomicBoolean(false);
    private final static CountDownLatch ASYNC_WAIT = new CountDownLatch(1);

    public static void init(int p, String f, String v) throws InterruptedException {
        if (IS_INIT.compareAndSet(false, true)) {
            policy = p;
            geoPolicyField = f;
            geoPolicyValue = v;
            ASYNC_WAIT.countDown();
        }
        ASYNC_WAIT.await();
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
                    logger.error("IpUtils line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            treeRangeSet = newTreeRangeSet;
        } catch (Exception e) {
            logger.error("加载Internal Ip Csv文件失败:", e);
        }
    }

    public static void setInternalIpFlag(CnRecordLog recordLog) {
        try {
            String commonClientIp = recordLog.getCommon_client_ip();
            String commonServertIp = recordLog.getCommon_server_ip();

            IPAddress clientIpAddress = new IPAddress(commonClientIp);
            IPAddress serverIpAddress = new IPAddress(commonServertIp);

            switch (policy) {
                case 1:
                    setIpFlagByFile(clientIpAddress, "client", recordLog);
                    setIpFlagByFile(serverIpAddress, "server", recordLog);
                    break;
                case 2:
                    setIpFlagByGeo(recordLog, geoPolicyField, geoPolicyValue);
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
            logger.error("ETL解析错误\nrecordLog:{} \n", recordLog, e);
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

    private static void setIpFlagByGeo(CnRecordLog recordLog, String geoPolicyField, String geoPolicyValue) {
        List<String> geoList = Arrays.asList(geoPolicyValue.trim().toLowerCase().split(","));
        String clientGeo = recordLog.getGeo(geoPolicyField, "client").trim().toLowerCase();
        String serverGeo = recordLog.getGeo(geoPolicyField, "server").trim().toLowerCase();

        if (StringUtil.isBlank(clientGeo)) {
            recordLog.setClient_zone("");
        } else if (geoList.contains(clientGeo)) {
            recordLog.setClient_zone("internal");
        } else {
            recordLog.setClient_zone("external");
        }

        if (StringUtil.isBlank(serverGeo)) {
            recordLog.setServer_zone("");
        } else if (geoList.contains(serverGeo)) {
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
}
