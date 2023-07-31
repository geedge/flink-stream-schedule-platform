package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
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
                        IPAddress ipAddress = new IPAddressString(ip1).getAddress();
                        if (ipAddress == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(ipAddress, ipAddress), idcRenter);
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddressString(ip1).getAddress();
                        IPAddress endIpAddress = new IPAddressString(ip2).getAddress();
                        if (startIpAddress == null || endIpAddress == null) {
                            continue;
                        }
                        newTreeRangeMap.put(Range.closed(startIpAddress, endIpAddress), idcRenter);
                    }
                    if ("CIDR".equals(addrFormat)) {
                        IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        IPAddress startIpAddress = cidrIpAddress.getLower();
                        IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeMap.put(Range.closed(startIpAddress, endIpAddress), idcRenter);
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
            IPAddress serverAddress = new IPAddressString(cnRecordLog.getCommon_server_ip()).getAddress();
            if (serverAddress != null) {
                String serverRenter = treeRangeMap.get(serverAddress);
                cnRecordLog.setServer_idc_renter(serverRenter);
            }

            IPAddress clientAddress = new IPAddressString(cnRecordLog.getCommon_client_ip()).getAddress();
            if (clientAddress != null) {
                String clientRenter = treeRangeMap.get(clientAddress);
                cnRecordLog.setClient_idc_renter(clientRenter);
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }

    }

    public static void main(String[] args) {
        Map<Range<IPAddress>, String> rangeStringMap = treeRangeMap.asMapOfRanges();
        for (Range<IPAddress> range:rangeStringMap.keySet()){
            String s = rangeStringMap.get(range);
            System.out.println(range + "---" + s);
        }

        System.out.println("-----------------------");
        IPAddress address = new IPAddressString("116.178.77.200").getAddress();
        System.out.println(treeRangeMap.get(address));

    }


}
