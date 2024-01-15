package com.zdjizhi.etl.utils.ai;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.common.IPAddress;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IpVpnUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IpVpnUtils.class);
    private static TreeRangeMap<IPAddress, String> treeRangeMap = TreeRangeMap.create();


    public static void readCsv(HighCsvReader csvReader) {
        try {
            TreeRangeMap<IPAddress, String> newTreeRangeMap = TreeRangeMap.create();
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String addrFormat = line.get("addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");
                    String vpnServiceName = line.get("vpn_service_name");

                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddress(ip1);
                        if (ipAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(ipAddress, ipAddress), vpnServiceName);
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddress(ip1);
                        IPAddress endIpAddress = new IPAddress(ip2);
                        if (startIpAddress.getIpAddress() == null || endIpAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(startIpAddress, endIpAddress), vpnServiceName);
                    }
                    if ("CIDR".equals(addrFormat)) {
                        inet.ipaddr.IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        inet.ipaddr.IPAddress startIpAddress = cidrIpAddress.getLower();
                        inet.ipaddr.IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeMap.put(Range.closed(new IPAddress(startIpAddress), new IPAddress(endIpAddress)), vpnServiceName);
                    }
                } catch (Exception lineException) {
                    LOG.error("IdcRenterUtils line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            treeRangeMap = newTreeRangeMap;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }


    public static void setInfo(CnRecordLog cnRecordLog) {
        try {
            IPAddress ipAddress = new IPAddress(cnRecordLog.getCommon_server_ip());
            if (ipAddress.getIpAddress() != null) {
                String vpnServiceName = treeRangeMap.get(ipAddress);
                if (vpnServiceName != null) {
                    cnRecordLog.putServer_ip_tag(vpnServiceName);
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
