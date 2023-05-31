package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class IdcRenterUtils {

    private static Logger LOG = LoggerFactory.getLogger(DnsServerUtils.class);
    private static TreeRangeMap<IPAddress, String> treeRangeMap = TreeRangeMap.create();

    public static void readServerCsv(Reader idcReader) {
        try {

            TreeRangeMap<IPAddress, String> newTreeRangeMap = TreeRangeMap.create();
            CSVReader csvReader = new CSVReader(idcReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            List<String[]> lines = csvReader.readAll();
            for (String[] line : lines) {
                try {
                    String addrFormat = line[1];
                    String ip1 = line[2];
                    String ip2 = line[3];
                    String idcRenter = line[6];

                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddressString(ip1).getAddress();
                        newTreeRangeMap.put(Range.closed(ipAddress, ipAddress), idcRenter);
                    }
                    if ("Range".equals(addrFormat)) {
                        IPAddress startIpAddress = new IPAddressString(ip1).getAddress();
                        IPAddress endIpAddress = new IPAddressString(ip2).getAddress();
                        newTreeRangeMap.put(Range.closed(startIpAddress, endIpAddress), idcRenter);
                    }
                    if ("CIDR".equals(addrFormat)) {
                        IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        IPAddress startIpAddress = cidrIpAddress.getLower();
                        IPAddress endIpAddress = cidrIpAddress.getUpper();
                        newTreeRangeMap.put(Range.closed(startIpAddress, endIpAddress), idcRenter);
                    }
                } catch (Exception lineException) {
                    System.out.println("IdcRenterUtils line: " + Arrays.toString(line) + " 解析异常：" + lineException.getMessage());
                }
            }
            treeRangeMap = newTreeRangeMap;
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void setIdcRenter(CnRecordLog cnRecordLog){
        try {
            IPAddress serverAddress = new IPAddressString(cnRecordLog.getCommon_server_ip()).getAddress();
            String serverRenter = treeRangeMap.get(serverAddress);
            cnRecordLog.setServer_idc_renter(serverRenter);

            IPAddress clientAddress = new IPAddressString(cnRecordLog.getCommon_client_ip()).getAddress();
            String clientRenter = treeRangeMap.get(clientAddress);
            cnRecordLog.setClient_idc_renter(clientRenter);
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
