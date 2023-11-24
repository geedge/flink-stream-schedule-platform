package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.common.IPAddress;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PsiphonUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PsiphonUtils.class);
    private static TreeRangeMap<IPAddress, String> treeRangeMap = TreeRangeMap.create();


    public static void readPsiphonCsv(HighCsvReader csvReader) {
        try {
            TreeRangeMap<IPAddress, String> newTreeRangeMap = TreeRangeMap.create();
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String addrFormat = line.get("addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");
                    String type = line.get("type");
                    if ("0".equals(type)) {
                        type = "Psiphon3 Server";
                    } else if ("1".equals(type)) {
                        type = "Psiphon3 CDN";
                    } else {
                        continue;
                    }

                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddress(ip1);
                        if (ipAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(ipAddress, ipAddress), type);
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddress(ip1);
                        IPAddress endIpAddress = new IPAddress(ip2);
                        if (startIpAddress.getIpAddress() == null || endIpAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(startIpAddress, endIpAddress), type);
                    }
                    if ("CIDR".equals(addrFormat)) {
                        inet.ipaddr.IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        inet.ipaddr.IPAddress startIpAddress = cidrIpAddress.getLower();
                        inet.ipaddr.IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeMap.put(Range.closed(new IPAddress(startIpAddress), new IPAddress(endIpAddress)), type);
                    }
                } catch (Exception lineException) {
                    System.out.println("IdcRenterUtils line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
            treeRangeMap = newTreeRangeMap;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }


    public static void setPsiphonInfo(CnRecordLog cnRecordLog) {
        try {
            IPAddress ipAddress = new IPAddress(cnRecordLog.getCommon_server_ip());
            if (ipAddress.getIpAddress() != null) {
                String type = treeRangeMap.get(ipAddress);
                if (type != null) {
                    cnRecordLog.putServer_ip_tag(type);
                }
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }

    public static void close() {
        treeRangeMap.clear();
        treeRangeMap = null;
    }

}
