package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IdcRenterUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DnsServerUtils.class);
    private static TreeRangeMap<IPAddress, String> treeRangeMap = TreeRangeMap.create();

    public static void readServerCsv(HighCsvReader csvReader) {
        try {
            TreeRangeMap<IPAddress, String> newTreeRangeMap = TreeRangeMap.create();
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String addrFormat = line.get("addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");
                    String idcRenter = line.get("idc_renter");

                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddress(ip1);
                        if (ipAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(ipAddress, ipAddress), idcRenter);
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddress(ip1);
                        IPAddress endIpAddress = new IPAddress(ip2);
                        if (startIpAddress.getIpAddress() == null || endIpAddress.getIpAddress() == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(startIpAddress, endIpAddress), idcRenter);
                    }
                    if ("CIDR".equals(addrFormat)) {
                        inet.ipaddr.IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        inet.ipaddr.IPAddress startIpAddress = cidrIpAddress.getLower();
                        inet.ipaddr.IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeMap.put(Range.closed(new IPAddress(startIpAddress), new IPAddress(endIpAddress)), idcRenter);
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

    public static void setIdcRenter(CnRecordLog cnRecordLog){
        try {
            IPAddress serverAddress = new IPAddress(cnRecordLog.getCommon_server_ip());
            if (serverAddress.getIpAddress() != null) {
                String serverRenter = treeRangeMap.get(serverAddress);
                if (serverRenter != null) {
                    cnRecordLog.getServer_ip_tags().add(serverRenter);
                }
            }
            IPAddress clientAddress = new IPAddress(cnRecordLog.getCommon_client_ip());
            if (clientAddress.getIpAddress() != null) {
                String clientRenter = treeRangeMap.get(clientAddress);
                if (clientRenter != null) {
                    cnRecordLog.getClient_ip_tags().add(clientRenter);
                }
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }
}
